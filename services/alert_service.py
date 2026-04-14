from __future__ import annotations

from sqlalchemy import text

from database.db import get_connection
from services.notification_service import create_notification, send_email_if_configured


def already_logged(notice_id: int, threshold: int) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            text("SELECT id FROM alert_log WHERE notice_id = :notice_id AND threshold_days = :threshold"),
            {"notice_id": notice_id, "threshold": threshold},
        ).mappings().first()
    return row is not None


def log_alert(notice_id: int, threshold: int) -> None:
    with get_connection() as conn:
        conn.execute(
            text(
                """
                INSERT INTO alert_log (notice_id, threshold_days)
                VALUES (:notice_id, :threshold)
                ON CONFLICT(notice_id, threshold_days) DO NOTHING
                """
            ),
            {"notice_id": notice_id, "threshold": threshold},
        )


def notify_user(username: str, title: str, body: str, notice_id: int) -> None:
    create_notification(username, title, body, notice_id)
    send_email_if_configured(username, title, body)
