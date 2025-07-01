from azure.storage.blob import BlobServiceClient
import pandas as pd
import os
import warnings
from dotenv import load_dotenv
import psycopg2
import re
from datetime import datetime, timedelta
import streamlit as st
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from sib_api_v3_sdk.models import SendSmtpEmail

# Load environment variables from .env file
load_dotenv()

# Suppress openpyxl warnings
warnings.simplefilter(action='ignore', category=UserWarning)

# Retrieve Azure and Database credentials from environment variables
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("CONTAINER_NAME", "autoloadingcontainer")

# Supabase connection details
SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_DB = os.getenv("SUPABASE_DB", "postgres")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASS = os.getenv("SUPABASE_PASS")

# US state code <-> name mapping
US_STATE_MAP = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California', 'CO': 'Colorado',
    'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana',
    'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota',
    'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington',
    'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
}
US_STATE_MAP_REV = {v.upper(): k for k, v in US_STATE_MAP.items()}

def get_full_state_name(state_val):
    if not state_val:
        return "Unknown State"
    state_val = state_val.strip().upper()
    if state_val in US_STATE_MAP:
        return US_STATE_MAP[state_val]
    elif state_val in US_STATE_MAP_REV:
        return state_val.title()
    else:
        return state_val.title()

def normalize_state(val):
    if not val:
        return set()
    val = val.strip().upper()
    results = set()
    # If it's a code
    if val in US_STATE_MAP:
        results.add(val)
        results.add(US_STATE_MAP[val].upper())
    # If it's a full name
    elif val in US_STATE_MAP_REV:
        results.add(val)
        results.add(US_STATE_MAP_REV[val])
    else:
        results.add(val)
    return results

def log_message(message, message_type="info", phase="General"):
    """Log message with timestamp, styling, and phase"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    formatted = f"[{timestamp}] {message}"
    if 'logs_by_phase' not in st.session_state:
        st.session_state['logs_by_phase'] = {}
    if phase not in st.session_state['logs_by_phase']:
        st.session_state['logs_by_phase'][phase] = []
    st.session_state['logs_by_phase'][phase].append((formatted, message_type))
    # Also append to flat log for backward compatibility
    if 'processing_log' not in st.session_state:
        st.session_state['processing_log'] = []
    st.session_state['processing_log'].append(formatted)
    # Optionally, print to Streamlit immediately (for dev)
    # if message_type == "success":
    #     st.success(formatted)
    # elif message_type == "error":
    #     st.error(formatted)
    # elif message_type == "warning":
    #     st.warning(formatted)
    # elif message_type == "info":
    #     st.info(formatted)
    # else:
    #     st.write(formatted)

def log_connection_status():
    """Log connection status for Azure Blob Storage and Supabase DB only"""
    log_message("🔗 Attempting to connect to Azure Blob Storage...", "info", phase="Connection")
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        log_message("✅ Azure Blob Storage connection successful", "success", phase="Connection")
    except Exception as e:
        log_message(f"❌ Azure Blob Storage connection failed: {e}", "error", phase="Connection")
        return False
    log_message("🔗 Attempting to connect to Supabase...", "info", phase="Connection")
    try:
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASS,
            port=5432
        )
        conn.close()
        log_message("✅ Supabase connection successful", "success", phase="Connection")
    except Exception as e:
        log_message(f"❌ Supabase connection failed: {e}", "error", phase="Connection")
        return False
    return True

# ============================================================================
# BILL TRACK PROCESSING FUNCTIONS
# ============================================================================

def get_file_name_for_date(date):
    month = date.strftime("%m")
    year = date.strftime("%y")
    file_name = f"{month}{year} Medicaid Rates bill sheet with categories.xlsx"
    return file_name

def check_file_exists(blob_name):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, blob_name)
        return blob_client.exists()
    except Exception as e:
        log_message(f"Error checking if file exists: {e}", "error", phase="Download")
        return False

def get_available_file_name():
    current_date = datetime.now()
    max_attempts = 12
    
    for i in range(max_attempts):
        file_name = get_file_name_for_date(current_date)
        log_message(f"🔍 Checking for file: {file_name}", "info", phase="Download")
        
        if check_file_exists(file_name):
            log_message(f"✅ Found available file: {file_name}", "success", phase="Download")
            return file_name
        
        current_date = current_date - timedelta(days=current_date.day)
    
    raise Exception("No available files found in the last 12 months")

def get_latest_date_sheet(excel_file):
    excel = pd.ExcelFile(excel_file)
    sheet_names = excel.sheet_names
    date_sheets = [sheet for sheet in sheet_names if re.match(r'^\d{6}$', sheet)]
    
    if not date_sheets:
        raise Exception("No sheets found in MMDDYY format")
    
    date_sheets.sort(reverse=True)
    log_message(f"📊 Found date sheets: {date_sheets}", "info", phase="Excel")
    log_message(f"📅 Using latest sheet: {date_sheets[0]}", "success", phase="Excel")
    
    return date_sheets[0]

def download_file(blob_name):
    log_message(f"📥 Downloading file: {blob_name}", "info", phase="Download")
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, blob_name)

    local_filename = blob_name
    with open(local_filename, "wb") as file:
        file.write(blob_client.download_blob().readall())
    log_message(f"✅ File downloaded successfully: {local_filename}", "success", phase="Download")
    return local_filename

def fetch_bills_from_db():
    try:
        log_message("🗄️ Fetching data from Supabase database...", "info", phase="Database")
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASS,
            port=5432
        )
        cur = conn.cursor()
        query = "SELECT * FROM bill_track_50;"
        cur.execute(query)
        columns = [desc[0].strip().lower() for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        db_data = pd.DataFrame(rows, columns=columns)
        log_message(f"✅ Retrieved {len(db_data)} records from Supabase database", "success", phase="Database")
        return db_data
    except Exception as e:
        log_message(f"❌ Error fetching data from Supabase database: {e}", "error", phase="Database")
        return None

def reset_is_new_flags():
    try:
        log_message("🔄 Resetting is_new flags...", "info", phase="Update")
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASS,
            port=5432
        )
        cur = conn.cursor()
        
        cur.execute("UPDATE bill_track_50 SET is_new = 'no';")
        cur.execute("UPDATE provider_alerts SET is_new = 'no';")
        conn.commit()
        
        log_message("✅ Reset all is_new flags to 'no'", "success", phase="Update")
        cur.close()
        conn.close()
    except Exception as e:
        log_message(f"❌ Error resetting is_new flags: {e}", "error", phase="Update")

def insert_new_entries(excel_data, db_data):
    try:
        log_message("➕ Processing new entries...", "info", phase="Update")
        
        excel_data = excel_data.rename(columns={
            'bill number': 'bill_number',
            'bill progress': 'bill_progress',
            'last action': 'last_action',
            'action date': 'action_date',
            'sponsor list': 'sponsor_list',
            'ai summary': 'ai_summary'
        })
        
        excel_data.columns = [col.replace('.', '_') for col in excel_data.columns]
        
        excel_data['action_date'] = pd.to_datetime(excel_data['action_date'], format='%m/%d/%Y', errors='coerce')
        excel_data['created'] = pd.to_datetime(excel_data['created'], format='%m/%d/%Y', errors='coerce')
        
        existing_urls = set(db_data['url'])
        
        new_entries = excel_data[
            ~excel_data['url'].isin(existing_urls) &
            excel_data['url'].notna() &
            (excel_data['url'].str.strip() != '')
        ].copy()
        
        if new_entries.empty:
            log_message("ℹ️ No new entries to insert", "info", phase="Update")
            return
        
        log_message(f"📝 Found {len(new_entries)} new entries to insert", "info", phase="Update")
        
        # Log details of new entries (up to 5)
        preview = new_entries.head(5)
        for idx, row in preview.iterrows():
            log_message(f"🆕 NEW BILL: {row.get('bill_number', '')} | {row.get('name', '')} | {row.get('state', '')} | {row.get('url', '')}", "success", phase="Update")
        if len(new_entries) > 5:
            log_message(f"...and {len(new_entries)-5} more new entries.", "info", phase="Update")
        
        new_entries = new_entries.drop(columns=['source_sheet'])
        new_entries.loc[:, 'date_extracted'] = pd.Timestamp.now().date()
        new_entries.loc[:, 'is_new'] = 'yes'
        
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASS,
            port=5432
        )
        cur = conn.cursor()
        
        inserted_count = 0
        
        for _, row in new_entries.iterrows():
            if pd.notna(row['action_date']):
                row['action_date'] = row['action_date'].strftime('%Y-%m-%d')
            else:
                row['action_date'] = None
            
            if pd.notna(row['created']):
                row['created'] = row['created'].strftime('%Y-%m-%d')
            else:
                row['created'] = None
            
            columns = ', '.join([f'"{col}"' for col in row.index])
            values = ', '.join(['%s'] * len(row))
            query = f"INSERT INTO bill_track_50 ({columns}) VALUES ({values})"
            cur.execute(query, tuple(row))
            inserted_count += 1
        
        conn.commit()
        cur.close()
        conn.close()
        
        log_message(f"✅ Successfully inserted {inserted_count} new entries", "success", phase="Update")
        
    except Exception as e:
        log_message(f"❌ Error inserting new entries: {e}", "error", phase="Update")

def update_all_columns(excel_data, db_data):
    try:
        log_message("🔄 Updating existing entries...", "info", phase="Update")
        
        excel_data = excel_data.rename(columns={
            'bill number': 'bill_number',
            'bill progress': 'bill_progress',
            'last action': 'last_action',
            'action date': 'action_date',
            'sponsor list': 'sponsor_list',
            'ai summary': 'ai_summary',
            'service lines impacted 2': 'service_lines_impacted_2'
        })
        
        excel_data['action_date'] = pd.to_datetime(excel_data['action_date'], format='%m/%d/%Y')
        db_data['action_date'] = pd.to_datetime(db_data['action_date'], format='%Y-%m-%d')
        
        db_data = db_data.replace('NaN', pd.NA)
        
        merged_data = pd.merge(
            excel_data,
            db_data,
            on='url',
            suffixes=('_excel', '_db')
        )
        
        columns_to_compare = [
            'bill_number', 'bill_progress', 'name', 'ai_summary',
            'last_action', 'action_date', 'sponsor_list', 'service_lines_impacted_2'
        ]
        
        needs_update = merged_data[
            merged_data.apply(lambda row: any(
                (pd.isna(row[f'{col}_excel']) != pd.isna(row[f'{col}_db'])) or
                (not pd.isna(row[f'{col}_excel']) and str(row[f'{col}_excel']).strip() != str(row[f'{col}_db']).strip())
                for col in columns_to_compare
            ), axis=1)
        ]
        
        if needs_update.empty:
            log_message("ℹ️ No updates needed", "info", phase="Update")
            return
        
        log_message(f"📝 Found {len(needs_update)} entries that need updates", "info", phase="Update")
        
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASS,
            port=5432
        )
        cur = conn.cursor()
        
        updated_count = 0
        
        for _, row in needs_update.iterrows():
            changed_columns = [
                col for col in columns_to_compare
                if (pd.isna(row[f'{col}_excel']) != pd.isna(row[f'{col}_db'])) or
                   (not pd.isna(row[f'{col}_excel']) and str(row[f'{col}_excel']).strip() != str(row[f'{col}_db']).strip())
            ]
            
            if not changed_columns:
                continue
                
            set_clause = ', '.join([
                f"{col} = %s" for col in changed_columns
            ])
            values = tuple(row[f'{col}_excel'] for col in changed_columns) + (row['url'],)
            
            query = f"""
                UPDATE bill_track_50
                SET {set_clause}
                WHERE url = %s
            """
            cur.execute(query, values)
            updated_count += 1
        
        conn.commit()
        cur.close()
        conn.close()
        
        log_message(f"✅ Successfully updated {updated_count} entries", "success", phase="Update")
        
    except Exception as e:
        log_message(f"❌ Error updating entries: {e}", "error", phase="Update")

def remove_duplicates_from_db():
    try:
        log_message("🧹 Removing duplicate entries...", "info", phase="Update")
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASS,
            port=5432
        )
        cur = conn.cursor()
        
        query = """
            DELETE FROM bill_track_50
            WHERE ctid IN (
                SELECT ctid
                FROM (
                    SELECT ctid,
                           ROW_NUMBER() OVER (
                               PARTITION BY url 
                               ORDER BY date_extracted DESC
                           ) AS rnum
                    FROM bill_track_50
                ) t
                WHERE t.rnum > 1
            );
        """
        
        cur.execute(query)
        conn.commit()
        
        deleted_count = cur.rowcount
        cur.close()
        conn.close()
        
        log_message(f"✅ Deleted {deleted_count} duplicate entries", "success", phase="Update")
        
    except Exception as e:
        log_message(f"❌ Error removing duplicates: {e}", "error", phase="Update")

def replace_nan_with_null():
    try:
        log_message("🧹 Cleaning NaN values...", "info", phase="Update")
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASS,
            port=5432
        )
        cur = conn.cursor()
        
        query = """
            UPDATE bill_track_50
            SET 
                ai_summary = NULLIF(NULLIF(ai_summary, 'NaN'), 'nan'),
                sponsor_list = NULLIF(NULLIF(sponsor_list, 'NaN'), 'nan'),
                bill_progress = NULLIF(NULLIF(bill_progress, 'NaN'), 'nan'),
                last_action = NULLIF(NULLIF(last_action, 'NaN'), 'nan')
        """
        
        cur.execute(query)
        conn.commit()
        
        updated_count = cur.rowcount
        cur.close()
        conn.close()
        
        log_message(f"✅ Replaced {updated_count} NaN values with NULL", "success", phase="Update")
        
    except Exception as e:
        log_message(f"❌ Error replacing NaN values: {e}", "error", phase="Update")

def get_email_recipients():
    """Fetch email recipients from user preferences table in Supabase"""
    try:
        log_message("📧 Fetching email recipients from Supabase database...", "info", phase="Notification")
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASS,
            port=5432
        )
        cur = conn.cursor()
        
        # Query to get email addresses and their preferences
        query = """
            SELECT user_email, preferences 
            FROM user_email_preferences 
            WHERE preferences IS NOT NULL;
        """
        cur.execute(query)
        rows = cur.fetchall()
        
        # Extract emails and log their preferences
        recipients = []
        for row in rows:
            email = row[0]
            preferences = row[1]  # This is the JSONB data
            recipients.append(email)
            log_message(f"  📨 Found recipient: {email}", "info", phase="Notification")
            if preferences and preferences.get('states'):
                states = ', '.join(preferences['states']) if isinstance(preferences['states'], list) else 'No states'
                log_message(f"     States: {states}", "info", phase="Notification")
        
        cur.close()
        conn.close()
        
        if not recipients:
            log_message("⚠️ No email recipients found in user email preferences", "warning", phase="Notification")
            return []
            
        log_message(f"✅ Found {len(recipients)} email recipients from Supabase", "success", phase="Notification")
        return recipients
        
    except Exception as e:
        log_message(f"❌ Error fetching email recipients from Supabase: {e}", "error", phase="Notification")
        return []

def send_email_notification(new_alerts_count):
    """Send personalized email notifications using the HTML template and real alert data, matching user preferences."""
    BREVO_API_KEY = os.getenv("BREVO_API_KEY")
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = BREVO_API_KEY
    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))

    sent_emails = []
    try:
        print("\n" + "="*80)
        print("🔍 EMAIL NOTIFICATION DEBUG - STARTING PROCESS")
        print("="*80)
        
        # Fetch all users and their preferences
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASS,
            port=5432
        )
        cur = conn.cursor()
        cur.execute("SELECT user_email, preferences FROM user_email_preferences WHERE preferences IS NOT NULL;")
        user_rows = cur.fetchall()
        cur.close()
        conn.close()
        
        if not user_rows:
            print("❌ No recipients found in user_email_preferences table")
            log_message("❌ No recipients to send email to", "error", phase="Notification")
            return 0
            
        print(f"📧 Found {len(user_rows)} recipients with preferences:")
        for user_row in user_rows:
            email = user_row[0]
            preferences = user_row[1]
            states = preferences.get('states', []) if preferences else []
            categories = preferences.get('categories', []) if preferences else []
            print(f"   • {email}: States={states}, Categories={categories}")
        
        log_message(f"📧 Preparing personalized email notifications for {len(user_rows)} recipients...", "info", phase="Notification")

        # Fetch all new alerts
        print(f"\n🔍 Fetching new alerts (is_new = 'yes')...")
        alerts = fetch_new_alerts()
        if not alerts:
            print("❌ No new alerts found (is_new = 'yes')")
            log_message("No new alerts to send emails for.", "info", phase="Notification")
            return 0

        print(f"✅ Found {len(alerts)} new alerts:")
        for i, alert in enumerate(alerts[:5]):  # Show first 5 alerts
            source = alert[0]
            state = get_full_state_name(alert[2])
            service_lines = []
            for idx in [4,5,6,7]:
                val = alert[idx]
                if val and str(val).strip():
                    service_lines.append(str(val).strip())
            print(f"   {i+1}. [{source}] {state}: {', '.join(service_lines) if service_lines else 'No service lines'}")
        if len(alerts) > 5:
            print(f"   ... and {len(alerts)-5} more alerts")

        # Preprocess alerts for efficient matching
        print(f"\n🔍 Processing alerts for matching...")
        processed_alerts = []
        for alert in alerts:
            state = (alert[2] or "").strip().upper()
            state_norm = normalize_state(state)
            service_lines = set()
            for idx in [4,5,6,7]:
                val = alert[idx]
                if val and str(val).strip():
                    service_lines.add(str(val).strip().upper())
            processed_alerts.append({
                'alert': alert,
                'state': state,
                'state_norm': state_norm,
                'service_lines': service_lines
            })

        # For each user, filter relevant alerts and send personalized email
        print(f"\n🔍 Matching alerts to user preferences...")
        users_with_alerts = 0
        total_alerts_sent = 0
        
        for user_row in user_rows:
            email = user_row[0]
            preferences = user_row[1]
            if not preferences:
                print(f"   ⚠️ {email}: No preferences found, skipping")
                continue
                
            user_states_raw = [s for s in preferences.get('states', []) if s and str(s).strip()]
            user_states = set()
            for s in user_states_raw:
                user_states.update(normalize_state(s))
            user_categories = set([c.strip().upper() for c in preferences.get('categories', []) if c.strip()])
            
            print(f"\n   👤 {email}:")
            print(f"      States: {list(user_states)}")
            print(f"      Categories: {list(user_categories)}")
            
            if not user_states or not user_categories:
                print(f"      ❌ No states or categories configured, skipping")
                continue
                
            relevant_alerts = []
            for pa in processed_alerts:
                if pa['state_norm'] & user_states and pa['service_lines'] & user_categories:
                    relevant_alerts.append(pa['alert'])
                    
            print(f"      📊 Found {len(relevant_alerts)} relevant alerts")
            
            if not relevant_alerts:
                print(f"      ❌ No relevant alerts for this user")
                continue

            users_with_alerts += 1
            total_alerts_sent += len(relevant_alerts)
            
            # Show which alerts matched
            for alert in relevant_alerts:
                state = get_full_state_name(alert[2])
                service_line = alert[4] or alert[5] or alert[6] or alert[7] or "N/A"
                print(f"         ✅ {state}: {service_line}")

            # Build alert cards HTML for this user
            alert_cards = []
            for alert in relevant_alerts:
                source = alert[0]
                url = alert[1] or "#"
                state = get_full_state_name(alert[2])
                service_lines = ', '.join([str(alert[i]) for i in [4,5,6,7] if alert[i] and str(alert[i]).strip()]) or "N/A"
                card_html = ''
                if source == 'bill':
                    title = alert[8] or alert[3] or "No Title"
                    summary = alert[9] or "No summary available."
                    status = alert[10]
                    committee = alert[11]
                    introduction_date = alert[12]
                    last_action_date = alert[13]
                    sponsors = alert[14]
                    details = []
                    if status: details.append(f'<b>Status:</b> {status}')
                    if committee: details.append(f'<b>Committee:</b> {committee}')
                    if introduction_date: details.append(f'<b>Introduction Date:</b> {introduction_date}')
                    if last_action_date: details.append(f'<b>Last Action Date:</b> {last_action_date}')
                    if sponsors: details.append(f'<b>Sponsors:</b> {sponsors}')
                    card_html = f'''
                    <div class="alert-card" style="background:#f8fafc; border-radius:0; box-shadow:none; border-top:1px solid #e2e8f0; border-bottom:1px solid #e2e8f0; padding:32px 40px; font-family:Arial,sans-serif; color:#0F3557; box-sizing:border-box; margin:32px 48px;">
                      <div style="font-size:16px; font-weight:bold; margin-bottom:8px; color:#0F3557;">
                        {state}: {title}
                      </div>
                      <div style="font-size:14px; margin-bottom:4px;">
                        <span style="font-weight:600; color:#1e293b;">Service Lines:</span>
                        <span style="color:#334155;">{service_lines}</span>
                      </div>
                      <div style="font-size:14px; margin-bottom:12px;">
                        <span style="font-weight:600; color:#1e293b;">Summary:</span>
                        <span style="color:#334155;">{summary}</span>
                      </div>
                      {('<div style="font-size:13px; margin-bottom:8px;">' + '<br>'.join(details) + '</div>') if details else ''}
                      <a href="{url}" style="display:inline-block; background:#0F3557; color:#fff; text-decoration:none; padding:10px 20px; border-radius:6px; font-weight:bold; font-size:14px; margin-top:8px;">
                        View Details
                      </a>
                    </div>
                    '''
                elif source == 'provider_alert':
                    subject = alert[16] or "No Title"
                    summary = alert[9] or ""
                    announcement_date = alert[17]
                    details = []
                    if announcement_date: details.append(f'<b>Announcement Date:</b> {announcement_date}')
                    card_html = f'''
                    <div class="alert-card" style="background:#f8fafc; border-radius:0; box-shadow:none; border-top:1px solid #e2e8f0; border-bottom:1px solid #e2e8f0; padding:32px 40px; font-family:Arial,sans-serif; color:#0F3557; box-sizing:border-box; margin:32px 48px;">
                      <div style="font-size:16px; font-weight:bold; margin-bottom:8px; color:#0F3557;">
                        {state}: {subject}
                      </div>
                      <div style="font-size:14px; margin-bottom:4px;">
                        <span style="font-weight:600; color:#1e293b;">Service Lines:</span>
                        <span style="color:#334155;">{service_lines}</span>
                      </div>
                      {f'<div style="font-size:14px; margin-bottom:12px;"><span style="font-weight:600; color:#1e293b;">Summary:</span> <span style="color:#334155;">{summary}</span></div>' if summary else ''}
                      {('<div style="font-size:13px; margin-bottom:8px;">' + '<br>'.join(details) + '</div>') if details else ''}
                      <a href="{url}" style="display:inline-block; background:#0F3557; color:#fff; text-decoration:none; padding:10px 20px; border-radius:6px; font-weight:bold; font-size:14px; margin-top:8px;">
                        View Details
                      </a>
                    </div>
                    '''
                alert_cards.append(card_html)
            alert_cards_html = "\n".join(alert_cards)

            # Read the HTML template
            with open("email_template.html", "r", encoding="utf-8") as f:
                html_template = f.read()
            html_content = html_template.replace("{{ALERTS}}", alert_cards_html)

            subject = f"New Medicaid Alerts Relevant to You - {len(relevant_alerts)} Updates"
            email_data = SendSmtpEmail(
                to=[{"email": email}],
                sender={"email": "contact@medirate.net", "name": "Medirate"},
                subject=subject,
                html_content=html_content
            )
            try:
                response = api_instance.send_transac_email(email_data)
                print(f"      ✅ Email sent successfully to {email}")
                log_message(f"✅ Email notification sent to {email} with {len(relevant_alerts)} alerts", "success", phase="Notification")
                sent_emails.append(email)
            except ApiException as e:
                print(f"      ❌ API Error sending to {email}: {e}")
                log_message(f"❌ Error sending email notification to {email}: {e}", "error", phase="Notification")
            except Exception as e:
                print(f"      ❌ General Error sending to {email}: {e}")
                log_message(f"❌ Error sending email notification to {email}: {e}", "error", phase="Notification")
        
        # Print and log summary
        print(f"\n" + "="*80)
        print("📊 EMAIL NOTIFICATION SUMMARY")
        print("="*80)
        print(f"Total recipients: {len(user_rows)}")
        print(f"Users with relevant alerts: {users_with_alerts}")
        print(f"Total alerts sent: {total_alerts_sent}")
        print(f"Emails actually sent: {len(sent_emails)}")
        
        if sent_emails:
            summary = f"Emails sent to: {', '.join(sent_emails)}"
            print(f"✅ {summary}")
            log_message(summary, "success", phase="Notification")
        else:
            print("❌ No emails sent (no relevant alerts for any user)")
            log_message("No emails sent (no relevant alerts for any user).", "info", phase="Notification")
            
        print("="*80)
        return len(sent_emails)
        
    except Exception as e:
        print(f"❌ CRITICAL ERROR in send_email_notification: {e}")
        log_message(f"❌ Error in send_email_notification: {e}", "error", phase="Notification")
        return 0

def process_bill_track():
    """Main function to process Bill Track data"""
    try:
        log_message("🚀 Starting Bill Track Processing", "info", phase="Processing")
        
        # Reset is_new flags
        reset_is_new_flags()
        
        # Get the available file name
        EXCEL_FILE_NAME = get_available_file_name()
        
        # Download the Excel file
        local_excel_filename = download_file(EXCEL_FILE_NAME)
        
        # Get the latest date sheet
        latest_sheet = get_latest_date_sheet(local_excel_filename)
        
        # Get Database data
        db_data = fetch_bills_from_db()
        if db_data is None:
            log_message("❌ Failed to fetch database data", "error", phase="Processing")
            return
        
        try:
            # Read the sheet
            excel_data = pd.read_excel(local_excel_filename, sheet_name=latest_sheet, dtype=str)
            excel_data.columns = [col.strip().lower() for col in excel_data.columns]
            excel_data['source_sheet'] = latest_sheet
            
            # Remove rows where the url column contains "** Data provided by www.BillTrack50.com **"
            excel_data = excel_data[excel_data['url'].str.contains(r'\*\* Data provided by www\.BillTrack50\.com \*\*', case=False, na=False) == False]
            
            log_message(f"📊 Processing {len(excel_data)} entries from sheet: {latest_sheet}", "info", phase="Processing")
            
            # Remove duplicates
            remove_duplicates_from_db()
            
            # Insert new entries
            insert_new_entries(excel_data, db_data)
            
            # Update all columns
            update_all_columns(excel_data, db_data)
            
            # Replace NaN/nan values with NULL
            replace_nan_with_null()
            
            # Count new entries before processing
            existing_urls = set(db_data['url'])
            new_entries_count = len(excel_data[
                ~excel_data['url'].isin(existing_urls) &
                excel_data['url'].notna() &
                (excel_data['url'].str.strip() != '')
            ])
            
            # Send email notification if there are new entries
            if new_entries_count > 0:
                send_email_notification(new_entries_count)
            
        except Exception as e:
            log_message(f"❌ Error processing sheet {latest_sheet}: {e}", "error", phase="Processing")
        
        # Remove the downloaded Excel file
        os.remove(local_excel_filename)
        log_message("🗑️ Cleaned up temporary files", "info", phase="Processing")
        
        log_message("🎉 Bill Track Processing Complete!", "success", phase="Processing")
        
    except Exception as e:
        log_message(f"❌ Error in Bill Track processing: {e}", "error", phase="Processing")

# ============================================================================
# PROVIDER ALERTS PROCESSING FUNCTIONS
# ============================================================================

def clear_database():
    try:
        log_message("🗑️ Clearing provider_alerts table...", "info", phase="Update")
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASS,
            port=5432
        )
        cur = conn.cursor()
        query = "TRUNCATE TABLE provider_alerts RESTART IDENTITY;"
        cur.execute(query)
        conn.commit()
        log_message("✅ Successfully cleared provider_alerts table and reset id sequence", "success", phase="Update")
        cur.close()
        conn.close()
    except Exception as e:
        log_message(f"❌ Error clearing database: {e}", "error", phase="Update")

def format_date(date_str):
    if pd.isna(date_str) or date_str is None:
        return None
    try:
        date = pd.to_datetime(date_str)
        return date.strftime('%m/%d/%Y')
    except:
        return None

def clean_dataframe(df):
    df = df.replace(['NaN', 'nan', ''], None)
    df = df.where(pd.notna(df), None)
    
    if 'announcement_date' in df.columns:
        df['announcement_date'] = df['announcement_date'].apply(format_date)
    
    return df

def get_existing_records():
    try:
        log_message("🗄️ Fetching existing provider alerts...", "info", phase="Database")
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASS,
            port=5432
        )
        cur = conn.cursor()
        
        query = "SELECT * FROM provider_alerts;"
        cur.execute(query)
        columns = [desc[0].strip().lower() for desc in cur.description]
        rows = cur.fetchall()
        
        db_data = pd.DataFrame(rows, columns=columns)
        log_message(f"✅ Retrieved {len(db_data)} existing provider alerts", "success", phase="Database")
        
        cur.close()
        conn.close()
        return db_data
    except Exception as e:
        log_message(f"❌ Error fetching existing records: {e}", "error", phase="Database")
        return pd.DataFrame()

def reset_sequence():
    try:
        log_message("🔄 Resetting auto-increment sequence...", "info", phase="Update")
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASS,
            port=5432
        )
        cur = conn.cursor()
        
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM provider_alerts;")
        max_id = cur.fetchone()[0]
        
        cur.execute(f"ALTER SEQUENCE provider_alerts_id_seq RESTART WITH {max_id + 1};")
        conn.commit()
        
        log_message(f"✅ Reset sequence to start from ID: {max_id + 1}", "success", phase="Update")
        cur.close()
        conn.close()
    except Exception as e:
        log_message(f"❌ Error resetting sequence: {e}", "error", phase="Update")

def update_or_insert_provider_data(excel_data):
    try:
        log_message("🔄 Starting provider alerts update/insert process...", "info", phase="Update")
        
        # Reset is_new flags
        reset_is_new_flags()
        
        # Reset the sequence first to avoid ID conflicts
        reset_sequence()
        
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASS,
            port=5432
        )
        cur = conn.cursor()
        
        unnamed_cols = [col for col in excel_data.columns if 'unnamed' in col.lower()]
        if unnamed_cols:
            log_message(f"🧹 Removing unnamed columns: {unnamed_cols}", "info", phase="Update")
            excel_data = excel_data.drop(columns=unnamed_cols)
        
        excel_data = clean_dataframe(excel_data)
        existing_data = get_existing_records()
        
        updated_count = 0
        inserted_count = 0
        skipped_count = 0
        
        # Count new entries
        new_entries_count = 0
        new_alerts_preview = []
        
        for idx, row in excel_data.iterrows():
            row_id = row.get('id')
            
            # If row has an ID and it exists in database, update it
            if row_id and not pd.isna(row_id) and not existing_data.empty and str(row_id) in existing_data['id'].astype(str).values:
                existing_row = existing_data[existing_data['id'].astype(str) == str(row_id)].iloc[0]
                changed_columns = []
                changed_values = []
                
                for col in row.index:
                    if col != 'id':
                        excel_val = row[col]
                        db_val = existing_row[col]
                        if (pd.isna(excel_val) and pd.isna(db_val)) or (excel_val == db_val):
                            continue
                        changed_columns.append(col)
                        changed_values.append(excel_val)
                
                if changed_columns:
                    set_clause = ', '.join([f'"{col}" = %s' for col in changed_columns])
                    query = f"""
                        UPDATE provider_alerts 
                        SET {set_clause}
                        WHERE id = %s
                    """
                    cur.execute(query, tuple(changed_values) + (row_id,))
                    updated_count += 1
                else:
                    skipped_count += 1
            # If row has no ID or ID doesn't exist in database, insert as new record
            else:
                new_entries_count += 1
                insert_columns = [col for col in row.index if col != 'id']
                insert_values = [row[col] for col in insert_columns]
                insert_columns.append('is_new')
                insert_values.append('yes')
                columns = ', '.join([f'"{col}"' for col in insert_columns])
                values = ', '.join(['%s'] * len(insert_columns))
                query = f"INSERT INTO provider_alerts ({columns}) VALUES ({values})"
                cur.execute(query, tuple(insert_values))
                inserted_count += 1
                if len(new_alerts_preview) < 5:
                    new_alerts_preview.append(row)
        
        conn.commit()
        log_message(f"✅ Update complete - Updated: {updated_count}, Inserted: {inserted_count}, Skipped: {skipped_count}", "success", phase="Update")
        
        # Log details of new provider alerts (up to 5)
        if new_entries_count > 0:
            log_message(f"📝 Found {new_entries_count} new provider alerts to insert", "info", phase="Update")
            for row in new_alerts_preview:
                log_message(f"🆕 NEW ALERT: {row.get('subject', '')} | {row.get('state', '')} | {row.get('links', '')}", "success", phase="Update")
            if new_entries_count > 5:
                log_message(f"...and {new_entries_count-5} more new provider alerts.", "info", phase="Update")
            
        cur.close()
        conn.close()
    except Exception as e:
        log_message(f"❌ Error updating/inserting provider data: {e}", "error", phase="Update")

def process_provider_alerts():
    """Main function to process Provider Alerts data"""
    try:
        log_message("🚀 Starting Provider Alerts Processing", "info", phase="Processing")
        
        EXCEL_FILE_NAME = "provideralerts_data.xlsx"
        log_message(f"🔍 Downloading {EXCEL_FILE_NAME}...", "info", phase="Download")
        local_excel_filename = download_file(EXCEL_FILE_NAME)
        
        try:
            log_message(f"📊 Reading Excel file: {local_excel_filename}", "info", phase="Excel")
            excel_data = pd.read_excel(local_excel_filename, sheet_name='provideralerts_data', dtype=str)
            excel_data.columns = [col.strip().lower().replace(' ', '_') for col in excel_data.columns]
            log_message(f"📊 Read {len(excel_data)} rows from Excel", "success", phase="Excel")
            
            update_or_insert_provider_data(excel_data)
            
        except Exception as e:
            log_message(f"❌ Error processing provider alerts: {e}", "error", phase="Processing")
        finally:
            log_message(f"🗑️ Removing downloaded file: {local_excel_filename}", "info", phase="Processing")
            os.remove(local_excel_filename)
        
        log_message("🎉 Provider Alerts Processing Complete!", "success", phase="Processing")
        
    except Exception as e:
        log_message(f"❌ Error in Provider Alerts processing: {e}", "error", phase="Processing")

def fetch_new_alerts():
    """Fetch all new alerts (is_new = 'yes', case-insensitive) from both bills and provider alerts tables."""
    try:
        connection = psycopg2.connect(
            host=SUPABASE_HOST,
            dbname=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASS,
            port=5432
        )
        cursor = connection.cursor()

        query = """
            SELECT
                'bill' AS source,
                url,
                state,
                bill_number,
                service_lines_impacted,
                service_lines_impacted_1,
                service_lines_impacted_2,
                service_lines_impacted_3,
                name AS title,
                ai_summary AS summary,
                bill_progress AS status,
                last_action AS committee,
                created AS introduction_date,
                CAST(action_date AS text) AS last_action_date,
                sponsor_list AS sponsors,
                date_extracted,
                NULL AS subject,
                NULL AS announcement_date,
                is_new
            FROM bill_track_50
            WHERE LOWER(TRIM(is_new)) = 'yes'

            UNION ALL

            SELECT
                'provider_alert' AS source,
                link AS url,
                state,
                NULL AS bill_number,
                service_lines_impacted,
                service_lines_impacted_1,
                service_lines_impacted_2,
                service_lines_impacted_3,
                NULL AS title,
                NULL AS summary,
                NULL AS status,
                NULL AS committee,
                NULL AS introduction_date,
                CAST(announcement_date AS text) AS last_action_date,
                NULL AS sponsors,
                NULL AS date_extracted,
                subject,
                announcement_date,
                is_new
            FROM provider_alerts
            WHERE LOWER(TRIM(is_new)) = 'yes';
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        connection.close()

        print(f"\n✅ Fetched {len(rows)} new alerts (is_new = 'yes')")
        return rows

    except Exception as e:
        print(f"❌ Error fetching new alerts from Supabase DB: {e}")
        return []

# Replace the old fetch_bills_and_alerts call in your main process with fetch_new_alerts
# Example main process update:
if __name__ == "__main__":
    print("\n✅ Fetching user preferences...")
    users = fetch_user_preferences()

    print("\n✅ Fetching new alerts from database...")
    alerts = fetch_new_alerts()

    if alerts:
        match_alerts_to_users(users, alerts)
    else:
        print(f"\n🚫 No new alerts found (is_new = 'yes')") 