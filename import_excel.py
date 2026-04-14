from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from database.db import init_db
from services.notice_service import import_notices_from_dataframe


def main():
    if len(sys.argv) < 2:
        print("Uso: python import_excel.py ruta_al_excel.xlsx")
        raise SystemExit(1)

    excel_path = Path(sys.argv[1])
    if not excel_path.exists():
        print(f"No existe el archivo: {excel_path}")
        raise SystemExit(1)

    init_db()
    df = pd.read_excel(excel_path)
    imported, skipped = import_notices_from_dataframe(df, excel_path.name)
    print(f"Importados: {imported}")
    print(f"Omitidos por duplicado: {skipped}")


if __name__ == "__main__":
    main()
