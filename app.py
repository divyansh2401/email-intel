
from __future__ import annotations
import os
import threading
import time
import math
from datetime import datetime
from typing import Optional, Iterable

from fastapi import FastAPI, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sqlalchemy import text, select, update
from sqlalchemy.orm import Session

from db import Base, engine, SessionLocal
from models import Job, Email
from services.extraction import iter_files, scan_file, looks_like_text
from services.canonicalize import canon_email

# ----------------------------------------------------------------------------
# FastAPI app
# ----------------------------------------------------------------------------

app = FastAPI(title="Email Intel")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Ensure tables exist
Base.metadata.create_all(bind=engine)

# ----------------------------------------------------------------------------
# Dependency
# ----------------------------------------------------------------------------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ----------------------------------------------------------------------------
# Home / Pages
# ----------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/extract", response_class=HTMLResponse)
def page_extract(request: Request, db: Session = Depends(get_db)):
    jobs = db.query(Job).order_by(Job.created_ts.desc()).limit(200).all()
    return templates.TemplateResponse("extract.html", {"request": request, "jobs": jobs})

@app.get("/jobs", response_class=HTMLResponse)
def page_jobs(request: Request, db: Session = Depends(get_db)):
    jobs = db.query(Job).order_by(Job.created_ts.desc()).limit(200).all()
    return templates.TemplateResponse("jobs.html", {"request": request, "jobs": jobs})

@app.get("/emails", response_class=HTMLResponse)
def page_emails(request: Request):
    return templates.TemplateResponse("emails.html", {"request": request})

# ----------------------------------------------------------------------------
# Jobs API
# ----------------------------------------------------------------------------

@app.get("/api/jobs")
def list_jobs(db: Session = Depends(get_db)):
    jobs = db.query(Job).order_by(Job.created_ts.desc()).limit(200).all()
    return [j.as_dict() for j in jobs]

def job_rows(db: Session) -> str:
    rows = []
    for j in db.query(Job).order_by(Job.created_ts.desc()).limit(200).all():
        progress = f"{(j.processed_bytes / j.total_bytes * 100.0):.1f}%" if j.total_bytes else "0.0%"
        eta = f"{int(j.eta_seconds)}s" if j.eta_seconds is not None else "0s"
        mbps = f"{j.mbps:.2f}"
        rows.append(f"""
        <tr>
            <td>{j.name}</td>
            <td>{j.status}</td>
            <td>{progress}</td>
            <td>{mbps}</td>
            <td>{eta}</td>
            <td>{j.emails_found}</td>
            <td>
                <button class="btn btn-yellow" hx-post="/api/jobs/{j.id}/pause" hx-swap="none">Pause</button>
                <button class="btn btn-green" hx-post="/api/jobs/{j.id}/resume" hx-swap="none">Resume</button>
                <button class="btn btn-red"   hx-post="/api/jobs/{j.id}/cancel" hx-swap="none">Cancel</button>
                <button class="btn btn-red"   hx-post="/api/jobs/{j.id}/delete" hx-swap="none">Delete</button>
            </td>
        </tr>
        """.strip())
    return "\n".join(rows)

@app.get("/api/jobs/table", response_class=HTMLResponse)
def jobs_table(db: Session = Depends(get_db)):
    return job_rows(db)

@app.post("/api/jobs")
def create_job(
    name: str = Form(...),
    server_path: str = Form(None),
    workers: int = Form(8),
    include_domains: str = Form(""),
    deny_domains: str = Form(""),
    chunk_mb: int = Form(512),
    business_only: bool = Form(False),
    limit_n: int = Form(0),
    db: Session = Depends(get_db)
):
    if not server_path:
        raise HTTPException(status_code=400, detail="Provide server_path (file or folder)")
    if not os.path.isabs(server_path):
        raise HTTPException(status_code=400, detail="Path must be absolute")
    if not os.path.exists(server_path):
        raise HTTPException(status_code=400, detail="Path not found")

    # Gather files
    files = list(iter_files(server_path))
    if not files:
        raise HTTPException(status_code=400, detail="No readable files found")

    total_bytes = sum(sz for _, sz in files)
    j = Job(
        name=name.strip() or "scan",
        status="queued",
        processed_bytes=0,
        total_bytes=total_bytes,
        mbps=0.0,
        eta_seconds=None,
        emails_found=0,
        include_domains=include_domains,
        deny_domains=deny_domains,
        business_only=business_only,
        chunk_mb=int(chunk_mb or 512),
        workers=max(1, min(int(workers or 8), 64))
    )
    db.add(j)
    db.commit()
    db.refresh(j)

    # Start worker thread
    th = threading.Thread(target=worker_run, args=(j.id, server_path), daemon=True)
    th.start()

    return {"id": j.id, "name": j.name, "status": j.status, "total_bytes": j.total_bytes}

@app.post("/api/jobs/{job_id}/pause")
def pause_job(job_id: int, db: Session = Depends(get_db)):
    db.execute(update(Job).where(Job.id == job_id).values(paused=True))
    db.commit()
    return {"ok": True}

@app.post("/api/jobs/{job_id}/resume")
def resume_job(job_id: int, db: Session = Depends(get_db)):
    db.execute(update(Job).where(Job.id == job_id).values(paused=False, status="running"))
    db.commit()
    return {"ok": True}

@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: int, db: Session = Depends(get_db)):
    db.execute(update(Job).where(Job.id == job_id).values(cancelled=True, status="cancelled"))
    db.commit()
    return {"ok": True}

@app.post("/api/jobs/{job_id}/delete")
def delete_job(job_id: int, db: Session = Depends(get_db)):
    db.query(Job).filter(Job.id == job_id).delete()
    db.commit()
    return {"ok": True}

# ----------------------------------------------------------------------------
# Emails API (simple search)
# ----------------------------------------------------------------------------

@app.get("/api/emails")
def list_emails(q: Optional[str] = None, limit: int = 50, offset: int = 0, db: Session = Depends(get_db)):
    stmt = select(Email).order_by(Email.last_seen_ts.desc()).limit(limit).offset(offset)
    if q:
        stmt = select(Email).where(Email.email.ilike(f"%{q.lower()}%")).order_by(Email.last_seen_ts.desc()).limit(limit).offset(offset)
    rows = db.execute(stmt).scalars().all()
    return [{"email": r.email, "first_seen_ts": r.first_seen_ts.isoformat(), "last_seen_ts": r.last_seen_ts.isoformat(), "seen_count": r.seen_count} for r in rows]

# ----------------------------------------------------------------------------
# Worker
# ----------------------------------------------------------------------------

def worker_run(job_id: int, server_path: str):
    """Background job that scans files and writes unique emails to the DB."""
    db = SessionLocal()
    try:
        job = db.get(Job, job_id)
        if not job:
            return
        job.status = "running"
        job.processed_bytes = 0
        job.started_ts = datetime.utcnow()
        db.commit()

        t0 = time.time()
        processed_bytes = 0
        emails_batch = set()
        batch_size = 2000

        for path, fsize in iter_files(server_path):
            # Responsive controls
            job = db.get(Job, job_id)
            if not job or job.cancelled:
                break
            while job.paused:
                time.sleep(0.5)
                job = db.get(Job, job_id)
                if job.cancelled:
                    break
            if not job or job.cancelled:
                break

            # Skip non-text quickly
            if not looks_like_text(path):
                processed_bytes += fsize
                _update_metrics(db, job, processed_bytes, t0)
                continue

            # Scan one file
            for email in scan_file(path):
                emails_batch.add(canon_email(email))
                if len(emails_batch) >= batch_size:
                    _flush_emails(db, job, emails_batch)
                    emails_batch.clear()

            processed_bytes += fsize
            _update_metrics(db, job, processed_bytes, t0)

        # Final flush
        if emails_batch:
            _flush_emails(db, job, emails_batch)
            emails_batch.clear()

        job = db.get(Job, job_id)
        if job and not job.cancelled:
            job.status = "done"
            job.finished_ts = datetime.utcnow()
            _update_metrics(db, job, processed_bytes, t0)
            db.commit()
    except Exception as e:
        # Mark job failed and continue
        job = db.get(Job, job_id)
        if job:
            job.status = "failed"
            db.commit()
        print("Worker error:", repr(e))
    finally:
        db.close()

def _flush_emails(db: Session, job: Job, emails: set[str]):
    if not emails:
        return
    now = datetime.utcnow().isoformat(sep=' ', timespec='seconds')
    # Raw SQLite UPSERT for speed
    db.execute(text("""
        INSERT INTO emails (email, first_seen_ts, last_seen_ts, seen_count)
        VALUES (:e, :now, :now, 1)
        ON CONFLICT(email) DO UPDATE SET
            last_seen_ts=excluded.last_seen_ts,
            seen_count=emails.seen_count+1
    """), [{"e": e, "now": now} for e in emails])
    # Update job count
    job.emails_found = db.execute(text("SELECT COUNT(*) FROM emails")).scalar() or 0
    db.commit()

def _update_metrics(db: Session, job: Job, processed_bytes: int, t0: float):
    job.processed_bytes = processed_bytes
    elapsed = max(0.001, time.time() - t0)
    job.mbps = (processed_bytes / (1024*1024)) / elapsed
    if job.total_bytes > 0 and job.mbps > 0:
        remaining_mb = max(0.0, (job.total_bytes - processed_bytes) / (1024*1024))
        job.eta_seconds = int(remaining_mb / job.mbps)
    db.commit()

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
