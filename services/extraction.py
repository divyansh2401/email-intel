
from __future__ import annotations
import os, re, subprocess, sys, shutil, io
from typing import Iterable, Tuple

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", re.IGNORECASE)

def rg_path() -> str | None:
    exe = shutil.which("rg")
    return exe

def iter_files(path: str) -> Iterable[Tuple[str, int]]:
    """Yield (file_path, size) for files under path or a single file."""
    if os.path.isfile(path):
        try:
            return [(path, os.path.getsize(path))]
        except OSError:
            return []
    out = []
    for root, _, files in os.walk(path):
        for name in files:
            p = os.path.join(root, name)
            try:
                out.append((p, os.path.getsize(p)))
            except OSError:
                continue
    return out

def looks_like_text(path: str) -> bool:
    # Quick filter for obvious binaries
    ext = os.path.splitext(path)[1].lower()
    if ext in {".png",".jpg",".jpeg",".gif",".bmp",".pdf",".zip",".rar",".7z",".exe",".dll",".so"}:
        return False
    return True

def scan_file(path: str):
    """Yield raw emails from a file. Use ripgrep if available; fallback to Python."""
    rg = rg_path()
    if rg:
        try:
            # -I ignore binary, -o print matches only, --no-messages suppress errors
            proc = subprocess.Popen(
                [rg, "-INo", "--no-messages", "--regexp", EMAIL_RE.pattern, path],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, encoding="utf-8", errors="ignore"
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                line = line.strip()
                if line:
                    yield line
            proc.wait()
            return
        except Exception:
            pass

    # Fallback (stream read)
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for chunk in iter(lambda: f.read(1024*1024*8), ""):
                for m in EMAIL_RE.finditer(chunk):
                    yield m.group(0)
    except Exception:
        return
