from __future__ import annotations

from io import BytesIO

import pandas as pd
import streamlit as st

from database.db import init_db
from services.auth_service import authenticate, change_password, list_users, update_user_email
from services.import_export_service import EXPECTED_COLUMNS, EXPORT_PATH
from services.notice_service import (
    build_search_blob,
    COORDINATORS,
    OFICIO_OPTIONS,
    STATUSES,
    dataframe_for_dashboard,
    get_notice,
    import_notices_from_dataframe,
    list_notices,
    run_pending_alerts,
    update_notice,
)
from services.notification_service import list_notifications, mark_notification_read
from utils.date_utils import to_display


st.set_page_config(page_title="Control de Avisos", page_icon="🛠️", layout="wide")
init_db()
run_pending_alerts()


def ensure_session():
    st.session_state.setdefault("user", None)
    st.session_state.setdefault("selected_notice_id", None)
    st.session_state.setdefault("section", "Dashboard")


def highlight_rows(row):
    color_map = {
        "Rojo": "#ffd7d7",
        "Naranja": "#ffe6bf",
        "Azul": "#dbeeff",
        "Verde": "#d6f3dd",
        "Violeta": "#ead8ff",
    }
    color = color_map.get(row.get("Urgencia"), "")
    return [f"background-color: {color}"] * len(row)


def render_login():
    st.title("🛠️ Control de Avisos Judiciales")
    st.write("App de seguimiento de avisos para mantenimiento de sedes judiciales de Alicante.")

    with st.form("login_form"):
        username = st.text_input("Usuario").strip().lower()
        password = st.text_input("Contraseña", type="password")
        submitted = st.form_submit_button("Entrar", use_container_width=True)

    if submitted:
        user = authenticate(username, password)
        if user:
            st.session_state.user = user
            st.rerun()
        st.error("Usuario o contraseña incorrectos.")

    with st.expander("Usuarios iniciales"):
        st.markdown(
            """
            - `jaime / jaime1234`
            - `luisreina / luis1234`
            - `gustavo / gustavo1234`
            - `fran / fran1234`
            - `andres / andres1234`
            - `jonatan / jonatan1234`
            - `laura / laura1234`
            """
        )


def render_notice_table(df: pd.DataFrame):
    if df.empty:
        st.info("No hay avisos con esos filtros.")
        return

    display_df = df.reset_index(drop=True)
    event = st.dataframe(
        display_df.drop(columns=["ID"]).style.apply(highlight_rows, axis=1),
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )
    selected_rows = event.selection.rows if event and event.selection else []
    if selected_rows:
        selected_idx = selected_rows[0]
        st.session_state.selected_notice_id = int(display_df.iloc[selected_idx]["ID"])
        st.session_state.section = "Detalle de aviso"
        st.rerun()


def render_dashboard():
    notices = list_notices(include_closed=True)
    if not notices:
        st.info("No hay avisos cargados todavía. Importa primero el Excel en Administración.")
        return

    df = dataframe_for_dashboard(notices)

    with st.sidebar:
        st.subheader("Filtros")
        word_filter = st.text_input("Buscar")
        site_filter = st.multiselect("Sede", sorted([x for x in df["Sede"].dropna().unique() if x]))
        oficio_filter = st.multiselect("Oficio", OFICIO_OPTIONS)
        coord_filter = st.multiselect("Coordinador", sorted([x for x in df["Coordinador"].dropna().unique() if x]))
        urgency_filter = st.multiselect("Color / urgencia", sorted([x for x in df["Urgencia"].dropna().unique() if x]))
        esm_filter = st.text_input("E.S.M.")
        ot_filter = st.text_input("Generador OT")
        aviso_filter = st.text_input("Generador Aviso")
        desc_filter = st.text_input("Descripción")

    filtered = df.copy()
    filtered["Aviso"] = filtered["Aviso"].fillna("").astype(str).str.strip()
    if word_filter:
        matching_ids = {
            notice["id"]
            for notice in notices
            if word_filter.lower() in build_search_blob(notice)
        }
        filtered = filtered[filtered["ID"].isin(matching_ids)]
    if site_filter:
        filtered = filtered[filtered["Sede"].isin(site_filter)]
    if oficio_filter:
        filtered = filtered[filtered["Oficio"].isin(oficio_filter)]
    if coord_filter:
        filtered = filtered[filtered["Coordinador"].isin(coord_filter)]
    if urgency_filter:
        filtered = filtered[filtered["Urgencia"].isin(urgency_filter)]
    if esm_filter:
        filtered = filtered[filtered["E.S.M."].astype(str).str.contains(esm_filter, case=False, na=False)]
    if ot_filter:
        filtered = filtered[filtered["Generador OT"].astype(str).str.contains(ot_filter, case=False, na=False)]
    if aviso_filter:
        filtered = filtered[filtered["Generador Aviso"].astype(str).str.contains(aviso_filter, case=False, na=False)]
    if desc_filter:
        filtered = filtered[filtered["Descripción"].astype(str).str.contains(desc_filter, case=False, na=False)]

    tab_open, tab_budget, tab_material, tab_closed = st.tabs(
        [
            f"📋 Avisos ({len(filtered[filtered['Estado'] == 'Aviso'])})",
            f"💶 Pte presupuesto ({len(filtered[filtered['Estado'] == 'Pte presupuesto'])})",
            f"📦 Falta material ({len(filtered[filtered['Estado'] == 'Falta material'])})",
            f"✅ Terminados ({len(filtered[filtered['Estado'] == 'Acabado'])})",
        ]
    )
    with tab_open:
        render_notice_table(filtered[filtered["Estado"] == "Aviso"])
    with tab_budget:
        render_notice_table(filtered[filtered["Estado"] == "Pte presupuesto"])
    with tab_material:
        render_notice_table(filtered[filtered["Estado"] == "Falta material"])
    with tab_closed:
        render_notice_table(filtered[filtered["Estado"] == "Acabado"])


def render_notice_detail():
    notice_id = st.session_state.selected_notice_id
    if not notice_id:
        st.info("Selecciona un aviso desde el dashboard.")
        return

    notice = get_notice(notice_id)
    if not notice:
        st.error("El aviso ya no existe.")
        st.session_state.selected_notice_id = None
        return

    st.subheader(f"Aviso {notice['notice_number']}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Estado", notice["status"])
    c2.metric("Color", notice["urgency_color"])
    c3.metric("Sede", notice["site"] or "Sin detectar")
    c4.metric("Días abiertos", str(notice["age_days"] or 0))

    st.write(f"**Fecha solicitud:** {to_display(notice['request_date'])}")
    st.write(f"**Generador OT:** {notice['ot_generator']}")
    st.write(f"**Generador Aviso:** {notice['notice_generator']}")
    st.write(f"**E.S.M.:** {notice['esm']}")
    st.write(f"**Descripción:** {notice['work_description']}")
    st.write(f"**Oficio:** {notice['oficio']}")

    with st.form("notice_form"):
        status = st.radio(
            "Clasificación del aviso",
            STATUSES,
            index=STATUSES.index(notice["status"]) if notice["status"] in STATUSES else 0,
            horizontal=True,
        )
        coordinator_options = [""] + COORDINATORS
        assigned_coordinator = st.selectbox(
            "Asignar coordinador",
            coordinator_options,
            index=coordinator_options.index(notice["assigned_coordinator"]) if notice["assigned_coordinator"] in coordinator_options else 0,
        )
        drive_url = st.text_input("Enlace de Drive", value=notice["drive_url"] or "")
        observations = st.text_area("Observaciones", value=notice["observations"] or "", height=120)
        material_notes = st.text_area(
            "Material pendiente",
            value=notice["material_notes"] or "",
            height=100,
            help="Obligatorio cuando el aviso esté en 'Falta material'.",
        )
        comment_text = st.text_area("Nuevo comentario", height=120)
        submitted = st.form_submit_button("Guardar cambios", use_container_width=True)

    if submitted:
        try:
            update_notice(
                notice_id,
                status=status,
                assigned_coordinator=assigned_coordinator,
                drive_url=drive_url,
                observations=observations,
                material_notes=material_notes,
                comment_text=comment_text,
                author=st.session_state.user["full_name"],
            )
            st.success("Aviso actualizado.")
            st.rerun()
        except ValueError as exc:
            st.error(str(exc))

    st.subheader("Comentarios")
    if not notice["comments"]:
        st.info("Este aviso todavía no tiene comentarios.")
    else:
        for comment in notice["comments"]:
            st.markdown(f"**{comment['author']}** · {comment['created_at']}")
            st.write(comment["comment"])
            st.divider()


def render_notifications():
    notifications = list_notifications(st.session_state.user["username"])
    st.subheader("Notificaciones")
    if not notifications:
        st.info("No tienes notificaciones.")
        return
    for item in notifications:
        badge = "🔔" if not item["is_read"] else "✅"
        st.markdown(f"{badge} **{item['title']}** · {item['created_at']}")
        st.write(item["body"])
        cols = st.columns([1, 1, 6])
        if not item["is_read"] and cols[0].button("Leída", key=f"read-{item['id']}"):
            mark_notification_read(item["id"])
            st.rerun()
        if item["notice_id"] and cols[1].button("Abrir", key=f"open-{item['id']}"):
            st.session_state.selected_notice_id = item["notice_id"]
            st.rerun()
        st.divider()


def render_admin():
    if st.session_state.user["role"] != "Supervisor":
        st.warning("Solo los supervisores pueden importar y exportar avisos.")
        return

    tab_import, tab_export = st.tabs(["📥 Importar Excel", "📤 Exportar Excel"])
    with tab_import:
        st.write("Columnas esperadas:")
        st.code(", ".join(EXPECTED_COLUMNS))
        uploaded = st.file_uploader("Sube el Excel de avisos", type=["xlsx"])
        if uploaded:
            df = pd.read_excel(uploaded)
            st.dataframe(df.head(5), use_container_width=True)
            if st.button("Importar todos los avisos", type="primary", use_container_width=True):
                try:
                    imported, skipped = import_notices_from_dataframe(df, uploaded.name)
                    st.success(f"Importación completada. Nuevos: {imported}. Duplicados omitidos: {skipped}.")
                except ValueError as exc:
                    st.error(str(exc))

    with tab_export:
        if EXPORT_PATH.exists():
            data = EXPORT_PATH.read_bytes()
            st.download_button(
                "Descargar Excel actualizado",
                data=data,
                file_name="avisos_actualizados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        else:
            st.info("Todavía no hay exportación generada.")


def render_users():
    st.subheader("Usuarios")
    users = list_users()
    st.dataframe(pd.DataFrame(users), use_container_width=True, hide_index=True)

    with st.form("user_form"):
        selected_user = st.selectbox("Usuario", [u["username"] for u in users])
        email = st.text_input("Correo electrónico")
        new_password = st.text_input("Nueva contraseña", type="password")
        submitted = st.form_submit_button("Guardar cambios", use_container_width=True)
    if submitted:
        if email:
            update_user_email(selected_user, email)
        if new_password:
            change_password(selected_user, new_password)
        st.success("Datos de usuario actualizados.")
        st.rerun()


def render_shell():
    user = st.session_state.user
    st.sidebar.title("Panel")
    st.sidebar.write(f"**{user['full_name']}**")
    st.sidebar.write(f"Rol: {user['role']}")
    if st.sidebar.button("Cerrar sesión", use_container_width=True):
        st.session_state.user = None
        st.session_state.selected_notice_id = None
        st.rerun()

    unread = sum(1 for item in list_notifications(user["username"]) if not item["is_read"])
    section = st.sidebar.radio(
        "Ir a",
        ["Dashboard", "Detalle de aviso", f"Notificaciones ({unread})", "Administración", "Usuarios"],
        key="section",
    )

    st.title("Control de Avisos")
    st.caption("Mantenimiento de sedes judiciales de la provincia de Alicante")

    if section == "Dashboard":
        render_dashboard()
    elif section == "Detalle de aviso":
        render_notice_detail()
    elif section.startswith("Notificaciones"):
        render_notifications()
    elif section == "Administración":
        render_admin()
    else:
        render_users()


def main():
    ensure_session()
    if not st.session_state.user:
        render_login()
    else:
        render_shell()


if __name__ == "__main__":
    main()
