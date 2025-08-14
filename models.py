
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float, BigInteger
from sqlalchemy.orm import declarative_base
from db import Base

class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    status = Column(String, default="queued")
    processed_bytes = Column(BigInteger, default=0)
    total_bytes = Column(BigInteger, default=0)
    mbps = Column(Float, default=0.0)
    eta_seconds = Column(Integer, nullable=True)
    emails_found = Column(Integer, default=0)
    include_domains = Column(String, default="")
    deny_domains = Column(String, default="")
    business_only = Column(Boolean, default=False)
    workers = Column(Integer, default=8)
    chunk_mb = Column(Integer, default=512)
    paused = Column(Boolean, default=False)
    cancelled = Column(Boolean, default=False)
    created_ts = Column(DateTime, default=datetime.utcnow)
    started_ts = Column(DateTime, default=datetime.utcnow)
    finished_ts = Column(DateTime, nullable=True)

    def as_dict(self):
        return {
            "id": self.id, "name": self.name, "status": self.status,
            "processed_bytes": int(self.processed_bytes or 0),
            "total_bytes": int(self.total_bytes or 0),
            "mbps": float(self.mbps or 0.0),
            "eta": int(self.eta_seconds or 0),
            "emails_found": int(self.emails_found or 0),
            "created_ts": (self.created_ts or datetime.utcnow()).isoformat(),
        }

class Email(Base):
    __tablename__ = "emails"
    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String, unique=True, index=True, nullable=False)
    first_seen_ts = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_seen_ts = Column(DateTime, default=datetime.utcnow, nullable=False)
    seen_count = Column(Integer, default=1, nullable=False)
