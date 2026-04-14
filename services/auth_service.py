from __future__ import annotations

from sqlalchemy import text

from database.db import get_connection, hash_password


def authenticate(username: str, password: str):
    with get_connection() as conn:
        user = conn.execute(
            text(
                """
                SELECT username, full_name, role, email, active, password_hash
                FROM users
                WHERE username = :username
                """
            ),
            {"username": username.strip().lower()},
        ).mappings().first()

    if not user or not user["active"]:
        return None
    if user["password_hash"] != hash_password(password):
        return None
    return dict(user)


def list_users():
    with get_connection() as conn:
        rows = conn.execute(
            text(
                """
                SELECT username, full_name, role, email, active, created_at
                FROM users
                ORDER BY
                    CASE role WHEN 'Supervisor' THEN 1 ELSE 2 END,
                    full_name
                """
            )
        ).mappings().all()
    return [dict(row) for row in rows]


def update_user_email(username: str, email: str) -> None:
    with get_connection() as conn:
        conn.execute(
            text("UPDATE users SET email = :email WHERE username = :username"),
            {"email": email.strip(), "username": username},
        )


def change_password(username: str, new_password: str) -> None:
    with get_connection() as conn:
        conn.execute(
            text("UPDATE users SET password_hash = :password_hash WHERE username = :username"),
            {"password_hash": hash_password(new_password), "username": username},
        )
