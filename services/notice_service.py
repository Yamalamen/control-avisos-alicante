from __future__ import annotations

from datetime import datetime

import pandas as pd
from sqlalchemy import text

from database.db import get_connection
from services.alert_service import already_logged, log_alert, notify_user
from services.import_export_service import export_snapshot, validate_excel_columns
from utils.date_utils import age_in_days, parse_date, to_iso


COORDINATORS = ["Fran", "Andrés", "Jonatan", "Laura"]
STATUSES = ["Aviso", "Pte presupuesto", "Falta material", "Acabado"]
OFICIO_OPTIONS = ["Obra civil", "Varios", "Carpinteria", "Fontaneria"]


def normalize_username(full_name: str) -> str:
    mapping = {
        "Fran": "fran",
        "Andrés": "andres",
        "Jonatan": "jonatan",
        "Laura": "laura",
        "Jaime": "jaime",
        "Luis Reina": "luisreina",
        "Gustavo": "gustavo",
    }
    return mapping.get(full_name, full_name.strip().lower())


def derive_site(esm: str) -> str:
    if not esm:
        return ""
    text = esm.upper()
    markers = [
        "BENIDORM",
        "ELCHE",
        "AUDIENCIA",
        "VILLENA",
        "IBI",
        "NOVELDA",
        "AGUILERA 53",
        "ALICANTE",
        "DENIA",
        "ALCOY",
        "SAN VICENTE",
        "TORREVIEJA",
        "ORIHUELA",
    ]
    for marker in markers:
        if marker in text:
            return marker.title()
    return esm.split("-")[-1].strip() if "-" in esm else esm.strip()


def derive_oficio(esm: str, description: str) -> str:
    text = f"{esm or ''} {description or ''}".upper()
    if "OBRA CIVIL" in text:
        return "Obra civil"
    if "CARPINTER" in text:
        return "Carpinteria"
    if "FONTAN" in text or "AGUA" in text or "SANITAR" in text:
        return "Fontaneria"
    return "Varios"


def compute_urgency_color(notice: dict) -> str:
    status = normalize_status(notice.get("status", ""))
    if status in {"Pte presupuesto", "Falta material"}:
        return "Violeta"
    if status == "Acabado":
        return "Verde"
    age = age_in_days(notice.get("request_date"))
    if age is None:
        return ""
    if age > 90:
        return "Rojo"
    if age > 60:
        return "Naranja"
    return "Azul"


def normalize_status(status: str) -> str:
    normalized = (status or "").strip()
    if normalized in {"", "En proceso"}:
        return "Aviso"
    if normalized == "Presupuesto":
        return "Pte presupuesto"
    return normalized


def build_search_blob(notice: dict) -> str:
    base_parts = [
        notice.get("notice_number", ""),
        notice.get("site", ""),
        notice.get("esm", ""),
        notice.get("ot_generator", ""),
        notice.get("notice_generator", ""),
        notice.get("work_description", ""),
        notice.get("status", ""),
        notice.get("assigned_coordinator", ""),
        notice.get("drive_url", ""),
        notice.get("material_notes", ""),
        notice.get("observations", ""),
    ]
    comment_parts = [
        comment.get("comment", "")
        for comment in notice.get("comments", [])
    ]
    return " ".join(str(part) for part in [*base_parts, *comment_parts] if part).lower()


def has_keyword(notice: dict, keyword: str) -> bool:
    return keyword.lower() in build_search_blob(notice)


def list_notices(include_closed: bool = True):
    query = """
        SELECT
            n.*,
            (SELECT COUNT(*) FROM comments c WHERE c.notice_id = n.id) AS comment_count
        FROM notices n
    """
    if not include_closed:
        query += " WHERE n.status <> 'Acabado'"
    query += " ORDER BY n.request_date DESC, n.notice_number DESC"

    with get_connection() as conn:
        rows = conn.execute(text(query)).mappings().all()
        comments_rows = conn.execute(
            text(
                """
                SELECT notice_id, author, comment, created_at
                FROM comments
                ORDER BY created_at DESC
                """
            )
        ).mappings().all()

    comments_by_notice = {}
    for row in comments_rows:
        comments_by_notice.setdefault(row["notice_id"], []).append(dict(row))

    notices = [dict(row) for row in rows]
    for notice in notices:
        notice["status"] = normalize_status(notice["status"])
        notice["comments"] = comments_by_notice.get(notice["id"], [])
        notice["age_days"] = age_in_days(notice["request_date"])
        notice["urgency_color"] = compute_urgency_color(notice)
    return notices


def get_notice(notice_id: int):
    with get_connection() as conn:
        notice = conn.execute(
            text("SELECT * FROM notices WHERE id = :notice_id"),
            {"notice_id": notice_id},
        ).mappings().first()
        comments = conn.execute(
            text(
                """
                SELECT author, comment, created_at
                FROM comments
                WHERE notice_id = :notice_id
                ORDER BY created_at DESC
                """
            ),
            {"notice_id": notice_id},
        ).mappings().all()
    if not notice:
        return None
    data = dict(notice)
    data["status"] = normalize_status(data["status"])
    data["comments"] = [dict(row) for row in comments]
    data["age_days"] = age_in_days(data["request_date"])
    data["urgency_color"] = compute_urgency_color(data)
    data["oficio"] = derive_oficio(data.get("esm", ""), data.get("work_description", ""))
    return data


def import_notices_from_dataframe(df: pd.DataFrame, source_name: str):
    missing = validate_excel_columns(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas: {', '.join(missing)}")

    imported = 0
    skipped = 0
    with get_connection() as conn:
        for _, row in df.iterrows():
            notice_number = str(row["Aviso"]).strip()
            if not notice_number or notice_number.lower() == "nan":
                continue
            exists = conn.execute(
                text("SELECT id FROM notices WHERE notice_number = :notice_number"),
                {"notice_number": notice_number},
            ).mappings().first()
            if exists:
                skipped += 1
                continue

            esm = str(row["E.S.M."]).strip() if pd.notna(row["E.S.M."]) else ""
            conn.execute(
                text(
                    """
                    INSERT INTO notices (
                        notice_number,
                        request_date,
                        ot_generator,
                        notice_generator,
                        esm,
                        work_description,
                        site,
                        source_file
                    )
                    VALUES (
                        :notice_number,
                        :request_date,
                        :ot_generator,
                        :notice_generator,
                        :esm,
                        :work_description,
                        :site,
                        :source_file
                    )
                    """
                ),
                {
                    "notice_number": notice_number,
                    "request_date": to_iso(row["Fecha de solicitud"]),
                    "ot_generator": str(row["Generador OT"]).strip() if pd.notna(row["Generador OT"]) else "",
                    "notice_generator": str(row["Generador Aviso"]).strip() if pd.notna(row["Generador Aviso"]) else "",
                    "esm": esm,
                    "work_description": str(row["Descripción de la OT"]).strip() if pd.notna(row["Descripción de la OT"]) else "",
                    "site": derive_site(esm),
                    "source_file": source_name,
                },
            )
            imported += 1
    sync_export_snapshot()
    return imported, skipped


def update_notice(
    notice_id: int,
    *,
    status: str,
    assigned_coordinator: str,
    drive_url: str,
    observations: str,
    material_notes: str,
    comment_text: str,
    author: str,
):
    with get_connection() as conn:
        row = conn.execute(
            text("SELECT * FROM notices WHERE id = :notice_id"),
            {"notice_id": notice_id},
        ).mappings().first()
        if not row:
            raise ValueError("Aviso no encontrado.")
        current = dict(row)

        completed_at = current["completed_at"]
        normalized_status = normalize_status(status)
        if normalized_status == "Acabado" and not completed_at:
            completed_at = datetime.now().date().isoformat()
        if normalized_status != "Acabado":
            completed_at = None
        if normalized_status == "Falta material" and not material_notes.strip():
            raise ValueError("Debes indicar el material pendiente.")

        conn.execute(
            text(
                """
                UPDATE notices
                SET status = :status,
                    assigned_coordinator = :assigned_coordinator,
                    drive_url = :drive_url,
                    observations = :observations,
                    material_notes = :material_notes,
                    manual_urgency = '',
                    completed_at = :completed_at,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :notice_id
                """
            ),
            {
                "status": normalized_status,
                "assigned_coordinator": assigned_coordinator,
                "drive_url": drive_url.strip(),
                "observations": observations.strip(),
                "material_notes": material_notes.strip(),
                "completed_at": completed_at,
                "notice_id": notice_id,
            },
        )
        if comment_text.strip():
            conn.execute(
                text(
                    """
                    INSERT INTO comments (notice_id, author, comment)
                    VALUES (:notice_id, :author, :comment)
                    """
                ),
                {"notice_id": notice_id, "author": author, "comment": comment_text.strip()},
            )

    previous_coordinator = current.get("assigned_coordinator") or ""
    if assigned_coordinator and assigned_coordinator != previous_coordinator:
        username = normalize_username(assigned_coordinator)
        title = f"Nuevo aviso asignado: {current['notice_number']}"
        body = f"Se te ha asignado el aviso {current['notice_number']} de la sede {current.get('site') or 'sin sede'}."
        notify_user(username, title, body, notice_id)

    if normalized_status == "Pte presupuesto":
        title = f"Aviso {current['notice_number']} pendiente de presupuesto"
        body = (
            f"El aviso {current['notice_number']} se ha movido a Pte presupuesto. "
            f"Contacto móvil asociado: +34 692439200."
        )
        notify_user("jaime", title, body, notice_id)

    if normalized_status == "Falta material":
        title = f"Falta material en aviso {current['notice_number']}"
        body = (
            f"El aviso {current['notice_number']} necesita material: {material_notes.strip()}. "
            f"Contacto móvil asociado: +34 615890784."
        )
        notify_user("laura", title, body, notice_id)

    sync_export_snapshot()


def run_pending_alerts() -> int:
    created = 0
    for notice in list_notices(include_closed=False):
        if not notice.get("assigned_coordinator"):
            continue
        thresholds = []
        age = notice.get("age_days") or 0
        if age >= 30:
            thresholds.append(30)
        if age >= 90:
            thresholds.append(90)

        for threshold in thresholds:
            if already_logged(notice["id"], threshold):
                continue
            title = f"Aviso {notice['notice_number']} con {threshold}+ días"
            body = (
                f"El aviso {notice['notice_number']} de la sede {notice.get('site') or 'sin sede'} "
                f"sigue abierto después de {age} días."
            )
            notify_user(normalize_username(notice["assigned_coordinator"]), title, body, notice["id"])
            log_alert(notice["id"], threshold)
            created += 1
    return created


def sync_export_snapshot():
    export_snapshot(list_notices(include_closed=True))


def dataframe_for_dashboard(notices: list[dict]) -> pd.DataFrame:
    rows = []
    for notice in notices:
        request_date = parse_date(notice["request_date"])
        rows.append(
            {
                "ID": notice["id"],
                "Aviso": notice["notice_number"],
                "Fecha": request_date.strftime("%d/%m/%Y") if request_date else "",
                "Días": notice["age_days"],
                "Urgencia": notice["urgency_color"],
                "Estado": notice["status"],
                "Oficio": derive_oficio(notice.get("esm", ""), notice.get("work_description", "")),
                "Coordinador": notice["assigned_coordinator"],
                "Sede": notice["site"],
                "E.S.M.": notice["esm"],
                "Generador OT": notice["ot_generator"],
                "Generador Aviso": notice["notice_generator"],
                "Descripción": notice["work_description"],
                "Comentarios": notice["comment_count"],
            }
        )
    return pd.DataFrame(rows)
