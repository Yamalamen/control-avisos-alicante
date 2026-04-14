from __future__ import annotations

from pathlib import Path

import pandas as pd

from database.db import DATA_DIR
from utils.date_utils import to_display


EXPORT_PATH = DATA_DIR / "avisos_exportados.xlsx"


EXPECTED_COLUMNS = [
    "Aviso",
    "Fecha de solicitud",
    "Generador OT",
    "Generador Aviso",
    "E.S.M.",
    "Descripción de la OT",
]


def validate_excel_columns(columns):
    return [column for column in EXPECTED_COLUMNS if column not in columns]


def export_snapshot(notices: list[dict]) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    for notice in notices:
        rows.append(
            {
                "Aviso": notice["notice_number"],
                "Fecha de solicitud": to_display(notice["request_date"]),
                "Generador OT": notice["ot_generator"],
                "Generador Aviso": notice["notice_generator"],
                "E.S.M.": notice["esm"],
                "Descripción de la OT": notice["work_description"],
                "Sede": notice["site"],
                "Estado": notice["status"],
                "Coordinador asignado": notice["assigned_coordinator"],
                "Enlace Drive": notice["drive_url"],
                "Material pendiente": notice["material_notes"],
                "Observaciones": notice["observations"],
                "Fecha cierre": to_display(notice["completed_at"]),
                "Actualizado": notice["updated_at"],
            }
        )
    pd.DataFrame(rows).to_excel(EXPORT_PATH, index=False)
    return EXPORT_PATH
