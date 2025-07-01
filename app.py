import streamlit as st
from data_processor import process_bill_track, process_provider_alerts, send_email_notification, fetch_new_alerts
import psycopg2
import pandas as pd

st.set_page_config(
    page_title="Update Database",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- Custom CSS for fancy look ---
st.markdown("""
    <style>
    body, .stApp {
        background: linear-gradient(135deg, #232526 0%, #414345 100%) !important;
    }
    .main-header {
        font-size: 2.8rem !important;
        font-weight: 800;
        color: #ffe066;
        letter-spacing: 1px;
        margin-bottom: 0.5em;
        text-shadow: 0 2px 16px #0008;
    }
    .sub-header {
        color: #bfc9d1;
        font-size: 1.2rem;
        margin-bottom: 2em;
    }
    .stButton>button {
        background: linear-gradient(90deg, #ff5858 0%, #f09819 100%);
        color: white;
        border: none;
        border-radius: 8px;
        font-weight: 700;
        font-size: 1.1rem;
        padding: 0.7em 2.2em;
        margin: 0.5em 0.5em 0.5em 0;
        box-shadow: 0 2px 8px #0003;
        transition: 0.2s;
    }
    .stButton>button:hover {
        background: linear-gradient(90deg, #f09819 0%, #ff5858 100%);
        color: #ffe066;
        transform: translateY(-2px) scale(1.04);
        box-shadow: 0 4px 16px #0005;
    }
    .stExpander, .st-cq, .st-cv {
        background: #232526 !important;
        border-radius: 12px !important;
        border: 1px solid #444 !important;
        box-shadow: 0 2px 12px #0002;
        margin-bottom: 1.2em;
    }
    .stMarkdown span[style*="color: #22c55e"] { color: #a3e635 !important; }
    .stMarkdown span[style*="color: #ef4444"] { color: #ff6b6b !important; }
    .stMarkdown span[style*="color: #eab308"] { color: #ffe066 !important; }
    .stMarkdown span[style*="color: #60a5fa"] { color: #38bdf8 !important; }
    .stDataFrame thead tr th {
        background: #232526 !important;
        color: #ffe066 !important;
        font-weight: 700;
        font-size: 1.1em;
    }
    #MainMenu, footer {visibility: hidden;}
    </style>
""", unsafe_allow_html=True)

# --- Add Google Fonts for fancy header ---
st.markdown("""
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@800&display=swap" rel="stylesheet">
    <style>
    .main-header {
        font-family: 'Montserrat', Arial, sans-serif !important;
        font-size: 2.8rem !important;
        font-weight: 800;
        color: #ffe066;
        letter-spacing: 1px;
        margin-bottom: 0.5em;
        text-shadow: 0 2px 16px #0008;
    }
    </style>
""", unsafe_allow_html=True)

# --- Optional: Add a logo (uncomment and set your logo URL) ---
# st.markdown('<img src="https://your-logo-url.png" width="120" style="margin-bottom: -2em;">', unsafe_allow_html=True)

st.markdown('<h1 class="main-header">Medirate Email Alerts Dashboard</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Runs Bill Track and Provider Alerts processing in sequence, and shows detailed logs with animation. Use the separate button to send emails.</p>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    if st.button("🗂️ Update Database", key="update_db", type="primary"):
        st.session_state['logs_by_phase'] = {}
        st.session_state['processing_log'] = []
        with st.spinner("🔗 Connecting to Supabase and Database..."):
            st.markdown("### 🟢 Connecting...")
            from data_processor import log_connection_status
            log_connection_status()
        with st.spinner("⬇️ Downloading and processing Bill Track data..."):
            st.markdown("### 📋 Processing Bill Track...")
            process_bill_track()
        with st.spinner("⬇️ Downloading and processing Provider Alerts data..."):
            st.markdown("### 🔔 Processing Provider Alerts...")
            process_provider_alerts()
        st.success("🎉 Database update complete!")

with col2:
    if st.button("✉️ Send Email Notifications", key="send_emails", type="primary"):
        st.session_state['logs_by_phase'] = st.session_state.get('logs_by_phase', {})
        st.session_state['processing_log'] = st.session_state.get('processing_log', [])
        new_alerts = fetch_new_alerts()
        print("[DEBUG] new_alerts fetched:", new_alerts)
        if new_alerts and len(new_alerts) > 0:
            with st.spinner("✉️ Sending Email Notifications..."):
                st.markdown("### ✉️ Sending Email Notifications...")
                emails_sent = send_email_notification(len(new_alerts))
            if emails_sent > 0:
                st.success("🎉 Email notifications sent!")
            else:
                st.info("No emails sent: No users matched any new alerts based on their preferences.")
        else:
            st.info("No new alerts to send emails for.")

st.markdown("---")
st.markdown("## 📝 Processing Log")
st.markdown("Real-time logs will appear here during processing. Click below to expand/collapse all logs.")

if 'expand_all_logs' not in st.session_state:
    st.session_state['expand_all_logs'] = False
if st.button("Show All Logs" if not st.session_state['expand_all_logs'] else "Collapse All Logs", key="toggle_logs"):
    st.session_state['expand_all_logs'] = not st.session_state['expand_all_logs']

if 'logs_by_phase' in st.session_state:
    for phase in ["Connection", "Download", "Excel", "Database", "Update", "Processing", "Notification", "General"]:
        logs = st.session_state['logs_by_phase'].get(phase, [])
        if logs:
            with st.expander(f"{phase} Logs ({len(logs)})", expanded=st.session_state['expand_all_logs'] or (phase in ["Processing", "Connection", "Notification"])):
                for msg, msg_type in logs:
                    indent = "&nbsp;&nbsp;&nbsp;" if phase != "General" else ""
                    if msg_type == "success":
                        st.markdown(f'{indent}<span style="color: #22c55e; font-weight: bold;">{msg}</span>', unsafe_allow_html=True)
                    elif msg_type == "error":
                        st.markdown(f'{indent}<span style="color: #ef4444; font-weight: bold;">{msg}</span>', unsafe_allow_html=True)
                    elif msg_type == "warning":
                        st.markdown(f'{indent}<span style="color: #eab308; font-weight: bold;">{msg}</span>', unsafe_allow_html=True)
                    else:
                        st.markdown(f'{indent}<span style="color: #60a5fa;">{msg}</span>', unsafe_allow_html=True)
else:
    st.info("No logs yet. Click 'Update Database' or 'Send Email Notifications' to start processing.")

# --- Display full tables from the database, editable ---
st.markdown("---")
st.markdown("## 📊 Database Tables (Editable)")

# --- Supabase DB connection settings ---
SUPABASE_HOST = "db.qpadwftthiuotvnchbvt.supabase.co"
SUPABASE_DB = "postgres"
SUPABASE_USER = "postgres"
SUPABASE_PASS = "dpwM5htP5W4#jFR"

# Use these for all DB connections
DB_HOST = SUPABASE_HOST
DB_NAME = SUPABASE_DB
DB_USER = SUPABASE_USER
DB_PASS = SUPABASE_PASS

# Fetch service categories for dropdowns
try:
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    df_service_categories = pd.read_sql_query("SELECT DISTINCT categories FROM service_category_list", conn)
    conn.close()
    service_categories = sorted(df_service_categories['categories'].dropna().unique().tolist(), key=lambda x: x.lower())
except Exception as e:
    service_categories = []

# Show warning if service_categories is empty
if not service_categories:
    st.warning("No service categories found. Please add entries to the service_category_list table below.")

# Load data from database (only once, outside expanders)
if 'df_bills' not in st.session_state:
    try:
        with st.spinner("Loading bill_track_50 table from database..."):
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            df_bills = pd.read_sql_query("SELECT * FROM bill_track_50", conn)
            conn.close()
        if 'Delete?' not in df_bills.columns:
            df_bills['Delete?'] = False
        st.session_state['df_bills'] = df_bills
        st.session_state['df_bills_original'] = df_bills.copy()
    except Exception as e:
        st.error(f"Error loading bill_track_50: {e}")
        st.session_state['df_bills'] = pd.DataFrame()
        st.session_state['df_bills_original'] = pd.DataFrame()

if 'df_alerts' not in st.session_state:
    try:
        with st.spinner("Loading provider_alerts table from database..."):
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            df_alerts = pd.read_sql_query("SELECT * FROM provider_alerts", conn)
            conn.close()
        if 'Delete?' not in df_alerts.columns:
            df_alerts['Delete?'] = False
        st.session_state['df_alerts'] = df_alerts
        st.session_state['df_alerts_original'] = df_alerts.copy()
    except Exception as e:
        st.error(f"Error loading provider_alerts: {e}")
        st.session_state['df_alerts'] = pd.DataFrame()
        st.session_state['df_alerts_original'] = pd.DataFrame()

if 'df_service_list' not in st.session_state:
    try:
        with st.spinner("Loading service_category_list table from database..."):
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            df_service_list = pd.read_sql_query("SELECT * FROM service_category_list", conn)
            conn.close()
        if 'Delete?' not in df_service_list.columns:
            df_service_list['Delete?'] = False
        st.session_state['df_service_list'] = df_service_list
        st.session_state['df_service_list_original'] = df_service_list.copy()
    except Exception as e:
        st.error(f"Error loading service_category_list: {e}")
        st.session_state['df_service_list'] = pd.DataFrame()
        st.session_state['df_service_list_original'] = pd.DataFrame()

# Add refresh buttons to reload data from database
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("🔄 Refresh Bills Data", key="refresh_bills"):
        try:
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            df_bills = pd.read_sql_query("SELECT * FROM bill_track_50", conn)
            conn.close()
            if 'Delete?' not in df_bills.columns:
                df_bills['Delete?'] = False
            st.session_state['df_bills'] = df_bills
            st.session_state['df_bills_original'] = df_bills.copy()
            st.success("Bills data refreshed!")
        except Exception as e:
            st.error(f"Error refreshing bills data: {e}")

with col2:
    if st.button("🔄 Refresh Alerts Data", key="refresh_alerts"):
        try:
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            df_alerts = pd.read_sql_query("SELECT * FROM provider_alerts", conn)
            conn.close()
            if 'Delete?' not in df_alerts.columns:
                df_alerts['Delete?'] = False
            st.session_state['df_alerts'] = df_alerts
            st.session_state['df_alerts_original'] = df_alerts.copy()
            st.success("Alerts data refreshed!")
        except Exception as e:
            st.error(f"Error refreshing alerts data: {e}")

with col3:
    if st.button("🔄 Refresh Service Categories", key="refresh_service"):
        try:
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            df_service_list = pd.read_sql_query("SELECT * FROM service_category_list", conn)
            conn.close()
            if 'Delete?' not in df_service_list.columns:
                df_service_list['Delete?'] = False
            st.session_state['df_service_list'] = df_service_list
            st.session_state['df_service_list_original'] = df_service_list.copy()
            st.success("Service categories refreshed!")
        except Exception as e:
            st.error(f"Error refreshing service categories: {e}")

# Add this function near the top of the file, before the expanders

def highlight_is_new(df):
    return [
        {"backgroundColor": "#ffe066"} if str(row.get("is_new", "")).strip().lower() == "yes" else {}
        for _, row in df.iterrows()
    ]

# --- Editable bills_test_by_dev Table ---
with st.expander("Edit bill_track_50 Table", expanded=True):
    try:
        df_bills = st.session_state['df_bills']
        col_bills1, col_bills2 = st.columns(2)
        with col_bills1:
            show_no_service = st.button("Show entries with no service category (bills)", key="show_no_service_bills")
        with col_bills2:
            show_new_bills = st.button("Show only new entries (bills)", key="show_new_bills")
        filtered_bills = df_bills
        if show_no_service:
            mask = (
                df_bills['service_lines_impacted'].isna() | (df_bills['service_lines_impacted'].astype(str).str.strip() == '')
            ) & (
                df_bills['service_lines_impacted_1'].isna() | (df_bills['service_lines_impacted_1'].astype(str).str.strip() == '')
            ) & (
                df_bills['service_lines_impacted_2'].isna() | (df_bills['service_lines_impacted_2'].astype(str).str.strip() == '')
            ) & (
                df_bills['service_lines_impacted_3'].isna() | (df_bills['service_lines_impacted_3'].astype(str).str.strip() == '')
            )
            filtered_bills = df_bills[mask]
        elif show_new_bills:
            mask = df_bills['is_new'].astype(str).str.strip().str.lower() == 'yes'
            filtered_bills = df_bills[mask]
        # Dropdowns for service line columns
        column_config = {}
        if service_categories:
            for col in ['service_lines_impacted', 'service_lines_impacted_1', 'service_lines_impacted_2', 'service_lines_impacted_3']:
                if col in filtered_bills.columns:
                    column_config[col] = st.column_config.SelectboxColumn(
                        label=col.replace('_', ' ').title(),
                        options=service_categories,
                        required=False
                    )
        edited_bills = st.data_editor(
            filtered_bills,
            num_rows="dynamic",
            key="edit_bills_editor",
            use_container_width=True,
            disabled=['url'],
            column_config=column_config if column_config else None
        )
        st.session_state['df_bills'] = edited_bills
        # Save Changes button
        if st.button("Save Changes to bill_track_50", key="save_bills_changes"):
            import psycopg2
            import pandas as pd
            try:
                print("[SAVE] Starting save operation for bill_track_50...")
                # Drop 'Delete?' column if present
                if 'Delete?' in edited_bills.columns:
                    edited_bills = edited_bills.drop(columns=['Delete?'])
                # Compare to original and only update changed rows
                original_bills = st.session_state.get('df_bills_original', pd.DataFrame())
                if 'Delete?' in original_bills.columns:
                    original_bills = original_bills.drop(columns=['Delete?'])
                changed_rows = []
                for idx, row in edited_bills.iterrows():
                    url = row['url']
                    orig_row = original_bills[original_bills['url'] == url]
                    if orig_row.empty:
                        continue
                    orig_row = orig_row.iloc[0]
                    # Compare all columns except 'url'
                    changed = False
                    for col in edited_bills.columns:
                        if col == 'url':
                            continue
                        if pd.isna(row[col]) and pd.isna(orig_row[col]):
                            continue
                        if str(row[col]) != str(orig_row[col]):
                            changed = True
                            break
                    if changed:
                        changed_rows.append((idx, row))
                if not changed_rows:
                    print("[SAVE] No changes detected. Nothing to update.")
                    st.info("No changes to save.")
                else:
                    print(f"[SAVE] {len(changed_rows)} rows changed. Updating...")
                    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
                    cursor = conn.cursor()
                    for idx, row in changed_rows:
                        update_cols = [col for col in edited_bills.columns if col != 'url']
                        set_clause = ', '.join([f'"{col}" = %s' for col in update_cols])
                        values = [row[col] for col in update_cols] + [row['url']]
                        print(f"[SAVE] Updating row {idx} (url={row['url']})")
                        cursor.execute(f"UPDATE bill_track_50 SET {set_clause} WHERE url = %s", values)
                    conn.commit()
                    conn.close()
                    print(f"[SAVE] Updated {len(changed_rows)} rows. Reloading table...")
                    st.success(f"Saved {len(changed_rows)} changes to bill_track_50!")
                    # Reload table from DB
                    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
                    df_bills = pd.read_sql_query("SELECT * FROM bill_track_50", conn)
                    conn.close()
                    st.session_state['df_bills'] = df_bills
                    st.session_state['df_bills_original'] = df_bills.copy()
                    print("[SAVE] Reload complete. Triggering rerun.")
                    st.rerun()
            except Exception as e:
                print(f"[SAVE][ERROR] {e}")
                st.error(f"Error saving changes: {e}")
    except Exception as e:
        st.error(f"Error loading or saving bill_track_50: {e}")

# --- Editable provider_alerts Table ---
with st.expander("Edit provider_alerts Table", expanded=True):
    try:
        df_alerts = st.session_state['df_alerts']
        col_alerts1, col_alerts2 = st.columns(2)
        with col_alerts1:
            show_no_service_alerts = st.button("Show entries with no service category (alerts)", key="show_no_service_alerts")
        with col_alerts2:
            show_new_alerts = st.button("Show only new entries (alerts)", key="show_new_alerts")
        filtered_alerts = df_alerts
        service_cols = [col for col in ['service_lines_impacted', 'service_lines_impacted_1', 'service_lines_impacted_2', 'service_lines_impacted_3'] if col in df_alerts.columns]
        if show_no_service_alerts and service_cols:
            mask = True
            for col in service_cols:
                mask = mask & (df_alerts[col].isna() | (df_alerts[col].astype(str).str.strip() == ''))
            filtered_alerts = df_alerts[mask]
        elif show_new_alerts:
            mask = df_alerts['is_new'].astype(str).str.strip().str.lower() == 'yes'
            filtered_alerts = df_alerts[mask]
        # Dropdowns for service line columns
        column_config_alerts = {}
        if service_categories:
            for col in service_cols:
                column_config_alerts[col] = st.column_config.SelectboxColumn(
                    label=col.replace('_', ' ').title(),
                    options=service_categories,
                    required=False
                )
        edited_alerts = st.data_editor(
            filtered_alerts,
            num_rows="dynamic",
            key="edit_alerts_editor",
            use_container_width=True,
            disabled=['id'] if 'id' in df_alerts.columns else [],
            column_config=column_config_alerts if column_config_alerts else None
        )
        st.session_state['df_alerts'] = edited_alerts
        # Save Changes button
        if st.button("Save Changes to provider_alerts", key="save_alerts_changes"):
            import psycopg2
            import pandas as pd
            try:
                print("[SAVE] Starting save operation for provider_alerts...")
                if 'Delete?' in edited_alerts.columns:
                    edited_alerts = edited_alerts.drop(columns=['Delete?'])
                original_alerts = st.session_state.get('df_alerts_original', pd.DataFrame())
                if 'Delete?' in original_alerts.columns:
                    original_alerts = original_alerts.drop(columns=['Delete?'])
                changed_rows = []
                for idx, row in edited_alerts.iterrows():
                    if 'id' not in edited_alerts.columns:
                        continue
                    row_id = row['id']
                    orig_row = original_alerts[original_alerts['id'] == row_id]
                    if orig_row.empty:
                        continue
                    orig_row = orig_row.iloc[0]
                    changed = False
                    for col in edited_alerts.columns:
                        if col == 'id':
                            continue
                        if pd.isna(row[col]) and pd.isna(orig_row[col]):
                            continue
                        if str(row[col]) != str(orig_row[col]):
                            changed = True
                            break
                    if changed:
                        changed_rows.append((idx, row))
                if not changed_rows:
                    print("[SAVE] No changes detected. Nothing to update.")
                    st.info("No changes to save.")
                else:
                    print(f"[SAVE] {len(changed_rows)} rows changed. Updating...")
                    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
                    cursor = conn.cursor()
                    for idx, row in changed_rows:
                        update_cols = [col for col in edited_alerts.columns if col != 'id']
                        set_clause = ', '.join([f'"{col}" = %s' for col in update_cols])
                        values = [row[col] for col in update_cols] + [row['id']]
                        print(f"[SAVE] Updating row {idx} (id={row['id']})")
                        cursor.execute(f"UPDATE provider_alerts SET {set_clause} WHERE id = %s", values)
                    conn.commit()
                    conn.close()
                    print(f"[SAVE] Updated {len(changed_rows)} rows. Reloading table...")
                    st.success(f"Saved {len(changed_rows)} changes to provider_alerts!")
                    # Reload table from DB
                    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
                    df_alerts = pd.read_sql_query("SELECT * FROM provider_alerts", conn)
                    conn.close()
                    st.session_state['df_alerts'] = df_alerts
                    st.session_state['df_alerts_original'] = df_alerts.copy()
                    print("[SAVE] Reload complete. Triggering rerun.")
                    st.rerun()
            except Exception as e:
                print(f"[SAVE][ERROR] {e}")
                st.error(f"Error saving changes: {e}")
    except Exception as e:
        st.error(f"Error loading or saving provider_alerts: {e}")

# --- Editable service_category_list Table ---
st.markdown("---")
st.markdown("## 🗂️ Service Category List (Editable)")
with st.expander("Edit service_category_list Table", expanded=True):
    try:
        df_service_list = st.session_state['df_service_list']
        
        # Show current data in a data editor
        edited_service_list = st.data_editor(
            df_service_list,
            num_rows="dynamic",
            key="edit_service_list_editor",
            use_container_width=True,
            disabled=[]
        )
        st.session_state['df_service_list'] = edited_service_list
        
        # Add new category form
        st.markdown("### Add New Service Category")
        with st.form("add_category_form"):
            new_category = st.text_input("Category Name", key="new_category")
            if st.form_submit_button("Add Category"):
                if new_category and new_category.strip():
                    try:
                        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
                        cursor = conn.cursor()
                        cursor.execute("INSERT INTO service_category_list (categories) VALUES (%s)", (new_category.strip(),))
                        conn.commit()
                        conn.close()
                        st.success(f"Added new category: {new_category}")
                        # Reload table from DB
                        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
                        df_service_list = pd.read_sql_query("SELECT * FROM service_category_list", conn)
                        conn.close()
                        if 'Delete?' not in df_service_list.columns:
                            df_service_list['Delete?'] = False
                        st.session_state['df_service_list'] = df_service_list
                        st.session_state['df_service_list_original'] = df_service_list.copy()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error adding category: {e}")
                else:
                    st.error("Category name is required!")
        
        # Save all changes button (no ON CONFLICT, just insert if not exists)
        if st.button("Save All Changes to service_category_list", key="save_service_list"):
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            cursor = conn.cursor()
            for _, row in st.session_state['df_service_list'].iterrows():
                if row['categories'] and row['categories'].strip():
                    # Try to insert, ignore errors if already exists
                    try:
                        cursor.execute("INSERT INTO service_category_list (categories) VALUES (%s)", [row['categories']])
                    except Exception:
                        pass
            conn.commit()
            conn.close()
            st.success(f"Saved all changes to service_category_list.")
            # Reload data from database to refresh session state
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            df_service_list = pd.read_sql_query("SELECT * FROM service_category_list", conn)
            conn.close()
            if 'Delete?' not in df_service_list.columns:
                df_service_list['Delete?'] = False
            st.session_state['df_service_list'] = df_service_list
            st.session_state['df_service_list_original'] = df_service_list.copy()
            st.rerun()
        
        # Delete checked rows
        edited_service_list['Delete?'] = edited_service_list['Delete?'].fillna(False)
        to_delete = edited_service_list[edited_service_list['Delete?']]
        if st.button("Delete Selected Rows from service_category_list", key="delete_service") and not to_delete.empty:
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            cursor = conn.cursor()
            for _, row in to_delete.iterrows():
                if row['categories'] and row['categories'].strip():
                    cursor.execute("DELETE FROM service_category_list WHERE categories = %s", (row['categories'],))
            conn.commit()
            conn.close()
            st.success(f"Deleted {len(to_delete)} row(s) from service_category_list.")
            # Reload data from database to refresh session state
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            df_service_list = pd.read_sql_query("SELECT * FROM service_category_list", conn)
            conn.close()
            if 'Delete?' not in df_service_list.columns:
                df_service_list['Delete?'] = False
            st.session_state['df_service_list'] = df_service_list
            st.session_state['df_service_list_original'] = df_service_list.copy()
            st.rerun()
    except Exception as e:
        st.error(f"Error loading or saving service_category_list: {e}") 