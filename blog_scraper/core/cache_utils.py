"""
Simple DB-backed cache helpers for HTTP/API responses.
Uses the Cache table with TTL semantics.
"""

from datetime import datetime, timedelta
from typing import Optional, Tuple

from database.models import db_manager, Cache


def get_cached(key: str) -> Optional[Tuple[str, str]]:
    """Return (content, content_type) if cache exists and not expired."""
    session = db_manager.get_session()
    try:
        row = session.query(Cache).filter_by(url=key).first()
        if not row:
            return None
        if row.expires_at and row.expires_at < datetime.utcnow():
            # expired; delete lazily
            session.delete(row)
            session.commit()
            return None
        return row.content, (row.content_type or "")
    finally:
        session.close()


def set_cached(key: str, content: str, content_type: str = "", ttl_hours: int = 6) -> None:
    """Upsert cache content with TTL."""
    session = db_manager.get_session()
    try:
        expires = datetime.utcnow() + timedelta(hours=ttl_hours)
        row = session.query(Cache).filter_by(url=key).first()
        if row:
            row.content = content
            row.content_type = content_type
            row.cached_at = datetime.utcnow()
            row.expires_at = expires
        else:
            row = Cache(url=key, content=content, content_type=content_type, expires_at=expires)
            session.add(row)
        session.commit()
    finally:
        session.close()

