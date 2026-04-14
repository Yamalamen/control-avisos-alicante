from __future__ import annotations

import importlib.util
import smtplib
from email.message import EmailMessage

from sqlalchemy import text

from database.db import BASE_DIR, get_connection


def create_notification(username: str, title: str, body: str, notice_id: int | None = None, channel: str = "app") -> None:
    with get_connection() as conn:
        conn.execute(
            text(
                """
                INSERT INTO notifications (username, title, body, notice_id, channel)
                VALUES (:username, :title, :body, :notice_id, :channel)
                """
            ),
            {
                "username": username,
                "title": title,
                "body": body,
                "notice_id": notice_id,
                "channel": channel,
            },
        )


def list_notifications(username: str):
    with get_connection() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, title, body, notice_id, channel, is_read, created_at
                FROM notifications
                WHERE username = :username
                ORDER BY is_read ASC, created_at DESC
                """
            ),
            {"username": username},
        ).mappings().all()
    return [dict(row) for row in rows]


def mark_notification_read(notification_id: int) -> None:
    with get_connection() as conn:
        conn.execute(
            text("UPDATE notifications SET is_read = 1 WHERE id = :notification_id"),
            {"notification_id": notification_id},
        )


def get_user_email(username: str) -> str:
    with get_connection() as conn:
        row = conn.execute(
            text('SELECT email FROM "users" WHERE username = :username'),
            {"username": username},
        ).mappings().first()
    return row["email"] if row and row["email"] else ""


def load_email_config():
    try:
        import streamlit as st

        if st.secrets.get("EMAIL_ENABLED", False):
            class SecretsConfig:
                EMAIL_ENABLED = bool(st.secrets.get("EMAIL_ENABLED", False))
                SMTP_HOST = str(st.secrets.get("SMTP_HOST", ""))
                SMTP_PORT = int(st.secrets.get("SMTP_PORT", 587))
                SMTP_USER = str(st.secrets.get("SMTP_USER", ""))
                SMTP_PASSWORD = str(st.secrets.get("SMTP_PASSWORD", ""))
                EMAIL_FROM = str(st.secrets.get("EMAIL_FROM", ""))

            return SecretsConfig
    except Exception:
        pass

    config_path = BASE_DIR / "config_email.py"
    if not config_path.exists():
        return None
    spec = importlib.util.spec_from_file_location("config_email", config_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def send_email_if_configured(username: str, subject: str, body: str) -> str:
    email = get_user_email(username)
    if not email:
        return "sin-email"

    config = load_email_config()
    if not config or not getattr(config, "EMAIL_ENABLED", False):
        return "email-deshabilitado"

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = config.EMAIL_FROM
    message["To"] = email
    message.set_content(body)

    with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT) as server:
        server.starttls()
        server.login(config.SMTP_USER, config.SMTP_PASSWORD)
        server.send_message(message)
    return "enviado"
