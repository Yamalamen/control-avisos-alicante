from __future__ import annotations

import hashlib
import os
import sys
from contextlib import contextmanager
from pathlib import Path

USER_SITE = Path.home() / "AppData" / "Roaming" / "Python" / "Python312" / "site-packages"
try:
    if USER_SITE.exists():
        sys.path.insert(0, str(USER_SITE))
except OSError:
    pass

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SQLITE_PATH = DATA_DIR / "avisos.db"


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_database_url() -> str:
    env_url = os.getenv("DATABASE_URL", "").strip()
    if env_url:
        return normalize_database_url(env_url)
    try:
        import streamlit as st

        secret_url = str(st.secrets.get("DATABASE_URL", "")).strip()
        if secret_url:
            return normalize_database_url(secret_url)
    except Exception:
        pass
    ensure_data_dir()
    return f"sqlite:///{SQLITE_PATH.as_posix()}"


def is_sqlite_url(database_url: str) -> bool:
    return database_url.startswith("sqlite:///")


def normalize_database_url(database_url: str) -> str:
    if database_url.startswith("postgresql://"):
        return database_url.replace("postgresql://", "postgresql+psycopg://", 1)
    if database_url.startswith("postgres://"):
        return database_url.replace("postgres://", "postgresql+psycopg://", 1)
    return database_url


def get_engine() -> Engine:
    database_url = get_database_url()
    connect_args = {"check_same_thread": False} if is_sqlite_url(database_url) else {}
    return create_engine(database_url, future=True, connect_args=connect_args)


@contextmanager
def get_connection():
    engine = get_engine()
    with engine.begin() as conn:
        yield conn


def init_db() -> None:
    engine = get_engine()
    sqlite_mode = is_sqlite_url(get_database_url())
    with engine.begin() as conn:
        if sqlite_mode:
            conn.execute(text("PRAGMA foreign_keys = ON"))

        if sqlite_mode:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS app_users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL UNIQUE,
                        full_name TEXT NOT NULL,
                        "role" TEXT NOT NULL,
                        email TEXT,
                        mobile_phone TEXT,
                        password_hash TEXT NOT NULL,
                        active INTEGER NOT NULL DEFAULT 1,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS notices (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        notice_number TEXT NOT NULL UNIQUE,
                        request_date TEXT NOT NULL,
                        ot_generator TEXT,
                        notice_generator TEXT,
                        esm TEXT,
                        work_description TEXT,
                        site TEXT,
                        status TEXT NOT NULL DEFAULT 'En proceso',
                        assigned_coordinator TEXT,
                        drive_url TEXT,
                        material_notes TEXT,
                        observations TEXT,
                        manual_urgency TEXT,
                        completed_at TEXT,
                        source_file TEXT,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS comments (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        notice_id INTEGER NOT NULL,
                        author TEXT NOT NULL,
                        comment TEXT NOT NULL,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(notice_id) REFERENCES notices(id) ON DELETE CASCADE
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS notifications (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL,
                        title TEXT NOT NULL,
                        body TEXT NOT NULL,
                        notice_id INTEGER,
                        channel TEXT NOT NULL DEFAULT 'app',
                        is_read INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(notice_id) REFERENCES notices(id) ON DELETE SET NULL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS alert_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        notice_id INTEGER NOT NULL,
                        threshold_days INTEGER NOT NULL,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(notice_id, threshold_days),
                        FOREIGN KEY(notice_id) REFERENCES notices(id) ON DELETE CASCADE
                    )
                    """
                )
            )
        else:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS app_users (
                        id BIGSERIAL PRIMARY KEY,
                        username TEXT NOT NULL UNIQUE,
                        full_name TEXT NOT NULL,
                        "role" TEXT NOT NULL,
                        email TEXT,
                        mobile_phone TEXT,
                        password_hash TEXT NOT NULL,
                        active INTEGER NOT NULL DEFAULT 1,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS notices (
                        id BIGSERIAL PRIMARY KEY,
                        notice_number TEXT NOT NULL UNIQUE,
                        request_date TEXT NOT NULL,
                        ot_generator TEXT,
                        notice_generator TEXT,
                        esm TEXT,
                        work_description TEXT,
                        site TEXT,
                        status TEXT NOT NULL DEFAULT 'En proceso',
                        assigned_coordinator TEXT,
                        drive_url TEXT,
                        material_notes TEXT,
                        observations TEXT,
                        manual_urgency TEXT,
                        completed_at TEXT,
                        source_file TEXT,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS comments (
                        id BIGSERIAL PRIMARY KEY,
                        notice_id BIGINT NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
                        author TEXT NOT NULL,
                        comment TEXT NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS notifications (
                        id BIGSERIAL PRIMARY KEY,
                        username TEXT NOT NULL,
                        title TEXT NOT NULL,
                        body TEXT NOT NULL,
                        notice_id BIGINT REFERENCES notices(id) ON DELETE SET NULL,
                        channel TEXT NOT NULL DEFAULT 'app',
                        is_read INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS alert_log (
                        id BIGSERIAL PRIMARY KEY,
                        notice_id BIGINT NOT NULL REFERENCES notices(id) ON DELETE CASCADE,
                        threshold_days INTEGER NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(notice_id, threshold_days)
                    )
                    """
                )
            )
        ensure_notice_columns(conn, sqlite_mode)
        ensure_user_columns(conn, sqlite_mode)
        seed_users(conn)


def ensure_notice_columns(conn: Connection, sqlite_mode: bool) -> None:
    if sqlite_mode:
        columns = {
            row[1]
            for row in conn.execute(text("PRAGMA table_info(notices)")).fetchall()
        }
    else:
        columns = {
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'notices'
                    """
                )
            ).fetchall()
        }

    if "manual_urgency" not in columns:
        conn.execute(text("ALTER TABLE notices ADD COLUMN manual_urgency TEXT"))


def ensure_user_columns(conn: Connection, sqlite_mode: bool) -> None:
    if sqlite_mode:
        columns = {
            row[1]
            for row in conn.execute(text("PRAGMA table_info(app_users)")).fetchall()
        }
    else:
        columns = {
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'app_users'
                    """
                )
            ).fetchall()
        }

    if "mobile_phone" not in columns:
        conn.execute(text("ALTER TABLE app_users ADD COLUMN mobile_phone TEXT"))


def seed_users(conn: Connection) -> None:
    users = [
        ("jaime", "Jaime", "Supervisor", "Jaime.gomismartinez@eiffage.com", "+34 692439200", "jaime1234"),
        ("luisreina", "Luis Reina", "Supervisor", "", "", "luis1234"),
        ("gustavo", "Gustavo", "Supervisor", "", "", "gustavo1234"),
        ("fran", "Fran", "Coordinador", "", "", "fran1234"),
        ("andres", "Andrés", "Coordinador", "", "", "andres1234"),
        ("jonatan", "Jonatan", "Coordinador", "", "", "jonatan1234"),
        ("laura", "Laura", "Coordinador", "LauraCecilia.SALCEDOSEPULVEDA@eiffage.com", "+34 615890784", "laura1234"),
    ]

    for username, full_name, role, email, mobile_phone, password in users:
        existing_user = conn.execute(
            text(
                """
                SELECT id, email, mobile_phone
                FROM app_users
                WHERE username = :username
                """
            ),
            {"username": username},
        ).mappings().first()

        params = {
            "username": username,
            "full_name": full_name,
            "role": role,
            "email": email,
            "mobile_phone": mobile_phone,
            "password_hash": hash_password(password),
        }

        if existing_user:
            conn.execute(
                text(
                    """
                    UPDATE app_users
                    SET full_name = :full_name,
                        "role" = :role,
                        email = :email,
                        mobile_phone = :mobile_phone
                    WHERE username = :username
                    """
                ),
                {
                    **params,
                    "email": existing_user["email"] or email,
                    "mobile_phone": existing_user["mobile_phone"] or mobile_phone,
                },
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO app_users (username, full_name, "role", email, mobile_phone, password_hash)
                    VALUES (:username, :full_name, :role, :email, :mobile_phone, :password_hash)
                    """
                ),
                params,
            )
