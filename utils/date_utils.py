from __future__ import annotations

from datetime import date, datetime, timedelta


DATE_FORMATS = ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y")


def excel_serial_to_date(value):
    if isinstance(value, (int, float)):
        return date(1899, 12, 30) + timedelta(days=float(value))
    return None


def parse_date(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    excel_date = excel_serial_to_date(value)
    if excel_date:
        return excel_date
    text = str(value).strip()
    if not text:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def to_iso(value) -> str | None:
    parsed = parse_date(value)
    return parsed.isoformat() if parsed else None


def to_display(value) -> str:
    parsed = parse_date(value)
    return parsed.strftime("%d/%m/%Y") if parsed else ""


def age_in_days(value) -> int | None:
    parsed = parse_date(value)
    if not parsed:
        return None
    return (date.today() - parsed).days
