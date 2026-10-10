import os
import time
import base64
import logging
from email.message import EmailMessage
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("data/listener.log")
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()
from typing import List, Dict, Any
from src.pipeline import TriagePipeline

# ---- CONFIGURATION ----
MOCK_MODE = False  # Set to False once you download credentials.json from Google Cloud Console
POLL_INTERVAL_SECONDS = 30
MAX_EMAILS_PER_POLL = 10  # Rate limit: max emails to process in one batch
SLEEP_BETWEEN_EMAILS = 2.0  # Rate limit: seconds to wait between Gemini calls
# -----------------------

import sqlite3
import os

# Initialize Supabase client if configured
supabase_client = None
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import create_client, Client
        supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    except ImportError:
        pass

def _init_idempotency_db():
    """Initializes DB to track processed email IDs to prevent duplicates."""
    if supabase_client:
        return # Schema is created manually in Supabase
        
    os.makedirs("data", exist_ok=True)
    with sqlite3.connect("data/listener.db") as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS processed_emails (message_id TEXT PRIMARY KEY, processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")

def _is_already_processed(msg_id: str) -> bool:
    """Returns True if this exact message_id was already processed."""
    if supabase_client:
        try:
            response = supabase_client.table("processed_emails").select("message_id").eq("message_id", msg_id).execute()
            return len(response.data) > 0
        except Exception as e:
            logger.error(f"Supabase read error: {e}")
            
    # Fallback to SQLite
    with sqlite3.connect("data/listener.db") as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM processed_emails WHERE message_id = ?", (msg_id,))
        return cursor.fetchone() is not None

def _mark_as_processed_in_db(msg_id: str):
    """Records the message_id so it is never processed again."""
    if supabase_client:
        try:
            supabase_client.table("processed_emails").insert({"message_id": msg_id}).execute()
        except Exception as e:
            # If it's a duplicate key error, that's fine
            logger.error(f"Supabase insert error: {e}")
        return # Never fallback to SQLite if Supabase is configured
            
    # Fallback to SQLite
    with sqlite3.connect("data/listener.db") as conn:
        conn.execute("INSERT OR IGNORE INTO processed_emails (message_id) VALUES (?)", (msg_id,))

def _log_to_analytics(email_data: dict, decision: dict, classification: dict, tokens: dict):
    """Pushes ticket metrics to Supabase for the Streamlit Admin Dashboard."""
    if supabase_client:
        try:
            action_clean = str(decision['action']).replace("TriageAction.", "")
            init_status = "RESOLVED" if action_clean == "AUTO_REPLY" else "PENDING"
            supabase_client.table("triage_logs").insert({
                "message_id": email_data['id'],
                "student_email": email_data['sender'],
                "category": classification.get('category', 'unknown'),
                "urgency": classification.get('urgency', 1),
                "action_taken": action_clean,
                "status": init_status,
                "prompt_tokens": tokens.get('prompt_tokens', 0),
                "completion_tokens": tokens.get('completion_tokens', 0)
            }).execute()
        except Exception as e:
            logger.error(f"Supabase analytics insert error: {e}")

def get_gmail_service():
    """Authenticates and returns the Gmail API service."""
    if MOCK_MODE:
        return "MOCK_SERVICE"
        
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
    creds = None
    
    # --- RAILWAY SUPPORT ---
    # If deploying on Railway, we pass the raw JSON strings as environment variables
    # because committing token.json to GitHub is insecure!
    if os.environ.get("GOOGLE_CREDENTIALS_JSON") and not os.path.exists("credentials.json"):
        with open("credentials.json", "w") as f:
            f.write(os.environ.get("GOOGLE_CREDENTIALS_JSON"))
            
    if os.environ.get("GOOGLE_TOKEN_JSON") and not os.path.exists("token.json"):
        with open("token.json", "w") as f:
            f.write(os.environ.get("GOOGLE_TOKEN_JSON"))
    # -----------------------

    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return build('gmail', 'v1', credentials=creds)


def _clean_email_body(body: str) -> str:
    """Separates new message from quoted history so the LLM has full context but knows what's new."""
    lines = body.split('\n')
    new_lines = []
    history_lines = []
    in_history = False
    
    for line in lines:
        if not in_history and (
            (line.startswith('On ') and 'wrote:' in line) or
            line.startswith('>') or
            '--- Original Message ---' in line or
            '<div class="gmail_quote">' in line
        ):
            in_history = True
            
        if in_history:
            history_lines.append(line)
        else:
            new_lines.append(line)
            
    new_text = '\n'.join(new_lines).strip()
    history_text = '\n'.join(history_lines).strip()
    
    if history_text:
        return f"{new_text}\n\n--- PREVIOUS THREAD HISTORY ---\n{history_text}"
    return new_text


def get_unread_emails(service) -> List[Dict[str, Any]]:
    """Fetches unread emails from the inbox."""
    if MOCK_MODE:
        return [
            {"id": "msg1", "sender": "student1@example.com", "subject": "App crashing!", "body": "bhai mera app crash ho raha hai kal raat se aur mere device me chal hi nahi raha."},
            {"id": "msg2", "sender": "student2@example.com", "subject": "Refund requested", "body": "I purchased the Arjuna JEE batch yesterday but my school changed my curriculum. I want my money back. Please process my refund quickly."}
        ]

    # Fetch only up to MAX_EMAILS_PER_POLL to prevent rate limit bans
    results = service.users().messages().list(userId='me', q='is:unread', maxResults=MAX_EMAILS_PER_POLL).execute()
    messages = results.get('messages', [])
    
    emails = []
    for msg in messages:
        msg_data = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
        headers = msg_data['payload']['headers']
        
        sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown')
        subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
        message_id = next((h['value'] for h in headers if h['name'] == 'Message-ID'), '')
        references = next((h['value'] for h in headers if h['name'] == 'References'), '')
        in_reply_to = next((h['value'] for h in headers if h['name'] == 'In-Reply-To'), '')
        
        images = []
        
        # Extract body and images
        if 'parts' in msg_data['payload']:
            def extract_parts(parts):
                body_text = ""
                for part in parts:
                    if part['mimeType'] == 'text/plain' and 'data' in part['body']:
                        body_text += base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                    elif part['mimeType'] in ['image/jpeg', 'image/png'] and 'attachmentId' in part['body']:
                        att = service.users().messages().attachments().get(userId='me', messageId=msg['id'], id=part['body']['attachmentId']).execute()
                        images.append({
                            "mime_type": part['mimeType'],
                            "data": att['data']
                        })
                    elif 'parts' in part:
                        body_text += extract_parts(part['parts'])
                return body_text
            body = extract_parts(msg_data['payload']['parts'])
        elif 'body' in msg_data['payload'] and 'data' in msg_data['payload']['body']:
            body = base64.urlsafe_b64decode(msg_data['payload']['body']['data']).decode('utf-8')
            
        # Clean out quoted history so we only process the new message
        cleaned_body = _clean_email_body(body)
            
        emails.append({
            "id": msg['id'],
            "threadId": msg.get('threadId', msg['id']),
            "message_id": message_id,
            "references": references,
            "sender": sender,
            "subject": subject,
            "body": cleaned_body,
            "images": images
        })
    return emails


def get_html_template(body_html: str) -> str:
    """Wraps the email body in a beautiful, branded HTML template."""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
    </head>
    <body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #ffffff; margin: 0; padding: 0;">
        <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #ffffff;">
            <!-- Header -->
            <tr>
                <td style="background-color: #000000; padding: 25px 30px; text-align: center;">
                    <img src="https://i.postimg.cc/rwgXtRzQ/Physics-Wallah-Monogram-Logo.png" alt="Physics Wallah" width="160" style="display: block; margin: 0 auto;">
                    <p style="color: #cccccc; margin: 10px 0 0 0; font-size: 14px; font-weight: 500; letter-spacing: 1px;">SUPPORT TEAM</p>
                </td>
            </tr>
            
            <!-- Body Content -->
            <tr>
                <td style="padding: 40px 30px; color: #333333; line-height: 1.7; font-size: 15px;">
                    {body_html}
                </td>
            </tr>
            
            <!-- Footer -->
            <tr>
                <td style="background-color: #f8f9fa; padding: 25px 30px; text-align: center; border-top: 1px solid #eeeeee;">
                    <p style="margin: 0; font-size: 13px; color: #666666; font-weight: 500;">
                        🤖 This is an automated response generated by PW AI Support.
                    </p>
                    <p style="margin: 8px 0 0 0; font-size: 13px; color: #888888;">
                        If this does not fully resolve your concern, simply reply to this email and a member of our human support team will personally assist you.
                    </p>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """

def send_reply(service, thread_id: str, message_id: str, references: str, to: str, subject: str, reply_text: str):
    """Sends an automated reply via Gmail API."""
    if MOCK_MODE:
        logger.info(f"[MOCK] Sent Auto-Reply to {to} | Subject: Re: {subject}\n{reply_text}")
        return

    message = EmailMessage()
    message.set_content(reply_text)
    
    # Convert markdown newlines to HTML breaks for proper rendering in Gmail
    html_body = reply_text.replace('\n', '<br>')
    # Wrap in beautiful PW template
    final_html = get_html_template(html_body)
    
    message.add_alternative(final_html, subtype='html')
    
    message['To'] = to
    
    # Ensure it stays in the exact same email thread
    if not subject.startswith('Re:'):
        subject = f"Re: {subject}"
    message['Subject'] = subject
    
    if message_id:
        message['In-Reply-To'] = message_id
        message['References'] = f"{references} {message_id}".strip()
    
    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    create_message = {'raw': encoded_message, 'threadId': thread_id}
    service.users().messages().send(userId="me", body=create_message).execute()


def create_draft(service, thread_id: str, message_id: str, references: str, to: str, subject: str, draft_text: str):
    """Creates a draft email for a human agent to review."""
    if MOCK_MODE:
        logger.info(f"[MOCK] Created Draft to {to} | Subject: Re: {subject}\n{draft_text}")
        return

    message = EmailMessage()
    message.set_content(draft_text)
    
    # Convert markdown newlines to HTML breaks for proper rendering in Gmail
    html_body = draft_text.replace('\n', '<br>')
    # Wrap in beautiful PW template
    final_html = get_html_template(html_body)
    
    message.add_alternative(final_html, subtype='html')
    
    message['To'] = to
    
    if not subject.startswith('Re:'):
        subject = f"Re: {subject}"
    message['Subject'] = subject
    
    if message_id:
        message['In-Reply-To'] = message_id
        message['References'] = f"{references} {message_id}".strip()
    
    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    create_draft_body = {'message': {'raw': encoded_message, 'threadId': thread_id}}
    service.users().drafts().create(userId="me", body=create_draft_body).execute()


def mark_as_read(service, msg_id: str):
    """Removes the UNREAD label from an email."""
    if MOCK_MODE:
        logger.info(f"[MOCK] Marked {msg_id} as READ.")
        return
        
    service.users().messages().modify(userId='me', id=msg_id, body={'removeLabelIds': ['UNREAD']}).execute()


def run_listener():
    logger.info("Starting Gmail Triage Listener...")
    _init_idempotency_db()
    
    service = get_gmail_service()
    pipeline = TriagePipeline()
    
    while True:
        try:
            logger.info(f"Polling for new emails (Max {MAX_EMAILS_PER_POLL} per batch)...")
            emails = get_unread_emails(service)
        except (ConnectionResetError, OSError, Exception) as net_err:
            logger.warning(f"Connection issue encountered during Gmail poll ({type(net_err).__name__}: {net_err}). Reconnecting in {POLL_INTERVAL_SECONDS}s...")
            time.sleep(POLL_INTERVAL_SECONDS)
            try:
                service = get_gmail_service()
                logger.info("Gmail service connection re-established successfully.")
            except Exception as auth_err:
                logger.error(f"Failed to re-establish Gmail service: {auth_err}")
            continue
            
        for email in emails:
            try:
                # GAP 4 FIX: Idempotency Check
                if _is_already_processed(email['id']):
                    logger.warning(f"Email {email['id']} was already processed! Marking as read and skipping.")
                    mark_as_read(service, email['id'])
                    continue
                    
                logger.info(f"--- Processing Ticket: {email['subject']} from {email['sender']} ---")
                
                # Process through our pipeline
                result = pipeline.process_ticket(email['body'], images=email.get('images'))
                decision = result["decision"]
                action = decision["action"]
                
                logger.info(f"Decision: {action}")
                
                # Phase 1: Push analytics to Supabase for the Streamlit dashboard
                cls_obj = result.get("classification", {})
                if isinstance(cls_obj, dict):
                    category = cls_obj.get("parent_category") or cls_obj.get("category", "unknown")
                    urgency = cls_obj.get("urgency", 1)
                else:
                    category = getattr(cls_obj, "parent_category", None) or getattr(cls_obj, "category", "unknown")
                    urgency = getattr(cls_obj, "urgency", 1)
                    
                class_dict = {
                    "category": category,
                    "urgency": urgency
                }
                draft_info = result.get("draft") or {}
                tokens_dict = {
                    "prompt_tokens": draft_info.get("prompt_tokens", 0),
                    "completion_tokens": draft_info.get("completion_tokens", 0)
                }
                _log_to_analytics(email, decision, class_dict, tokens_dict)
                
                if action == "AUTO_REPLY" or action == "AUTO_REPLY_WITH_REVIEW":
                    # Footer is now built into the HTML template!
                    send_reply(service, email['threadId'], email['message_id'], email['references'], email['sender'], email['subject'], decision['draft_reply'])
                elif action == "CLARIFY":
                    send_reply(service, email['threadId'], email['message_id'], email['references'], email['sender'], email['subject'], decision['clarification_question'])
                elif action == "APPROVAL_REQUIRED":
                    create_draft(service, email['threadId'], email['message_id'], email['references'], email['sender'], email['subject'], decision['draft_reply'])
                elif action == "ESCALATE":
                    logger.warning(f"Escalated. Creating Handoff Packet draft for human triage.")
                    if decision.get("handoff_packet"):
                        create_draft(service, email['threadId'], email['message_id'], email['references'], email['sender'], email['subject'], decision['handoff_packet'])
                
                # GAP 4 FIX: Record success in DB
                _mark_as_processed_in_db(email['id'])
                
                # Always mark as read so we don't process it again
                mark_as_read(service, email['id'])
                
                # GAP 5 FIX: Rate Limiting
                time.sleep(SLEEP_BETWEEN_EMAILS)
            except Exception as email_err:
                logger.error(f"Error handling email {email.get('id', 'unknown')}: {email_err}", exc_info=True)
                # Continue loop so listener does not crash
            
        if MOCK_MODE:
            logger.info("Mock mode finished processing. Exiting.")
            break
            
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_listener()
