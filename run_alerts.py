from database.db import init_db
from services.notice_service import run_pending_alerts


if __name__ == "__main__":
    init_db()
    created = run_pending_alerts()
    print(f"Alertas generadas: {created}")
