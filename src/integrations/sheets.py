"""
Google Sheets API wrapper for the CRM pipeline.

Uses a service account (JSON stored in GOOGLE_SERVICE_ACCOUNT_JSON env var).
All API calls are wrapped in run_in_executor to avoid blocking the async event loop.

Tab data start rows:
  Pipeline           — data starts row 3 (rows 1-2 are title/headers)
  Call Log           — data starts row 3
  Follow-Up Schedule — data starts row 4 (rows 1-3 are title/subtitle/headers)
  Monthly Metrics    — data starts row 4

NEVER write to Dashboard or Loss Analysis tabs.
"""

import asyncio
import base64
import json
import logging
import os
from datetime import date, datetime
from functools import partial

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

log = logging.getLogger(__name__)

# ── Tab names ─────────────────────────────────────────────────────────────────

TAB_PIPELINE  = "Pipeline"
TAB_CALL_LOG  = "Call Log"
TAB_FOLLOW_UP = "Follow-Up Schedule"
TAB_MONTHLY   = "Monthly Metrics"

PROTECTED_TABS = {"Dashboard", "Loss Analysis"}

# First data row (1-indexed) per tab
TAB_DATA_START = {
    TAB_PIPELINE:  3,
    TAB_CALL_LOG:  3,
    TAB_FOLLOW_UP: 4,
    TAB_MONTHLY:   4,
}

# ── Column mappings ───────────────────────────────────────────────────────────

PIPELINE_COLS = {
    "name": "A", "company": "B", "email": "C", "date_added": "D",
    "campaign": "E", "industry": "F", "company_size": "G", "stage": "H",
    "sentiment": "I", "reply_date": "J", "deal_value": "K", "assigned_to": "L",
    "last_touch": "M", "next_followup": "N", "followup_count": "O",
    "days_in_stage": "P", "loss_reason": "Q", "notes": "R",
}

CALL_LOG_COLS = {
    "name": "A", "call_date": "B", "call_time": "C", "company": "D",
    "campaign": "E", "show_status": "F", "outcome": "G", "next_step": "H",
    "deal_value": "I", "close_date": "J", "reschedule_date": "K",
    "duration": "L", "notes": "M",
}

FOLLOW_UP_COLS = {
    "name": "A", "company": "B", "email": "C", "original_stage": "D",
    "reason": "E", "followup_num": "F", "last_contact": "G",
    "next_followup": "H", "method": "I", "status": "J",
    "response": "K", "re_engaged": "L", "notes": "M",
}

MONTHLY_COLS = {
    "month": "A", "emails_sent": "B", "replies": "C", "reply_rate": "D",
    "positive_replies": "E", "positive_rate": "F", "calls_booked": "G",
    "book_rate": "H", "shows": "I", "show_rate": "J", "deals_closed": "K",
    "close_rate": "L", "revenue": "M", "no_shows": "N", "avg_deal_size": "O",
    "cost_per_lead": "P", "revenue_per_email": "Q", "pipeline_value": "R",
}

COL_MAPS = {
    TAB_PIPELINE:  PIPELINE_COLS,
    TAB_CALL_LOG:  CALL_LOG_COLS,
    TAB_FOLLOW_UP: FOLLOW_UP_COLS,
    TAB_MONTHLY:   MONTHLY_COLS,
}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

_service = None


def _get_service():
    global _service
    if _service is None:
        raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        if not raw:
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON env var not set")
        # Support both raw JSON and base64-encoded JSON
        try:
            info = json.loads(raw)
        except json.JSONDecodeError:
            info = json.loads(base64.b64decode(raw).decode())
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        _service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _service


def _sheet_id() -> str:
    sid = os.getenv("GOOGLE_SHEETS_ID", "")
    if not sid:
        raise RuntimeError("GOOGLE_SHEETS_ID env var not set")
    return sid


async def _run(fn, *args, **kwargs):
    """Run a synchronous function in a thread pool to avoid blocking the event loop."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(fn, *args, **kwargs))


def today_str() -> str:
    return date.today().strftime("%m/%d/%Y")


def date_str(d: date) -> str:
    return d.strftime("%m/%d/%Y")


def parse_date(s: str) -> date | None:
    """Parse MM/DD/YYYY string, return None on failure."""
    try:
        return datetime.strptime(s.strip(), "%m/%d/%Y").date()
    except Exception:
        return None


# ── Core API functions ────────────────────────────────────────────────────────

async def find_row_by_email(tab: str, email: str) -> tuple[int | None, dict | None]:
    """
    Search for a prospect by email in column C of the given tab.
    Returns (sheet_row_number, row_as_dict) or (None, None) if not found.
    Sheet row number is 1-indexed (matches Sheets API range notation).
    """
    def _find():
        svc = _get_service()
        sid = _sheet_id()
        col_map = COL_MAPS.get(tab, {})
        email_col = col_map.get("email", "C")
        result = svc.spreadsheets().values().get(
            spreadsheetId=sid,
            range=f"{tab}!{email_col}:{email_col}",
        ).execute()
        values = result.get("values", [])
        data_start = TAB_DATA_START.get(tab, 3)
        email_lower = email.strip().lower()
        for i, row in enumerate(values):
            sheet_row = i + 1  # 1-indexed
            if sheet_row < data_start:
                continue
            cell = row[0].strip().lower() if row else ""
            if cell == email_lower:
                return sheet_row
        return None

    try:
        row_num = await _run(_find)
        if row_num is None:
            return None, None
        row_data = await _get_row(tab, row_num)
        return row_num, row_data
    except Exception as e:
        log.exception(f"Sheets find_row_by_email failed for {email} in {tab}: {e}")
        return None, None


async def _get_row(tab: str, row_number: int) -> dict:
    """Fetch a single row as a dict keyed by field name."""
    def _fetch():
        svc = _get_service()
        sid = _sheet_id()
        result = svc.spreadsheets().values().get(
            spreadsheetId=sid,
            range=f"{tab}!A{row_number}:Z{row_number}",
        ).execute()
        return result.get("values", [[]])[0] if result.get("values") else []

    raw = await _run(_fetch)
    col_map = COL_MAPS.get(tab, {})
    # Convert column letter to 0-based index
    row_dict = {}
    for field, col_letter in col_map.items():
        idx = ord(col_letter.upper()) - ord("A")
        row_dict[field] = raw[idx].strip() if idx < len(raw) else ""
    return row_dict


async def append_row(tab: str, values: list) -> None:
    """
    Append a row of values to the next empty row in the tab.
    Uses USER_ENTERED so date strings are interpreted as dates.
    """
    assert tab not in PROTECTED_TABS, f"Cannot write to protected tab: {tab}"

    def _append():
        svc = _get_service()
        sid = _sheet_id()
        svc.spreadsheets().values().append(
            spreadsheetId=sid,
            range=f"{tab}!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [values]},
        ).execute()

    try:
        await _run(_append)
        log.info(f"Sheets: appended row to {tab}")
    except HttpError as e:
        log.exception(f"Sheets append_row failed for {tab}: {e}")
        raise


async def update_row(tab: str, row_number: int, column_values: dict) -> None:
    """
    Update specific cells in an existing row.
    column_values: dict of {field_name: value} e.g. {"stage": "Call Booked"}
    Uses batchUpdate for efficiency (one API call regardless of number of columns).
    """
    assert tab not in PROTECTED_TABS, f"Cannot write to protected tab: {tab}"
    col_map = COL_MAPS.get(tab, {})

    data = []
    for field, value in column_values.items():
        col_letter = col_map.get(field)
        if not col_letter:
            log.warning(f"Sheets: unknown field '{field}' for tab {tab}, skipping")
            continue
        data.append({
            "range": f"{tab}!{col_letter}{row_number}",
            "values": [[value]],
        })

    if not data:
        return

    def _update():
        svc = _get_service()
        sid = _sheet_id()
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=sid,
            body={"valueInputOption": "USER_ENTERED", "data": data},
        ).execute()

    try:
        await _run(_update)
        log.info(f"Sheets: updated row {row_number} in {tab} — fields: {list(column_values.keys())}")
    except HttpError as e:
        log.exception(f"Sheets update_row failed for {tab} row {row_number}: {e}")
        raise


async def read_all_rows(tab: str) -> list[dict]:
    """
    Read all data rows from a tab (skipping title/header rows).
    Returns list of dicts keyed by field name.
    """
    def _read():
        svc = _get_service()
        sid = _sheet_id()
        result = svc.spreadsheets().values().get(
            spreadsheetId=sid,
            range=f"{tab}!A1:Z1000",
        ).execute()
        return result.get("values", [])

    try:
        all_rows = await _run(_read)
    except Exception as e:
        log.exception(f"Sheets read_all_rows failed for {tab}: {e}")
        return []

    col_map = COL_MAPS.get(tab, {})
    data_start = TAB_DATA_START.get(tab, 3)
    rows = []
    for i, raw in enumerate(all_rows):
        sheet_row = i + 1
        if sheet_row < data_start:
            continue
        row_dict = {"_row_number": sheet_row}
        for field, col_letter in col_map.items():
            idx = ord(col_letter.upper()) - ord("A")
            row_dict[field] = raw[idx].strip() if idx < len(raw) else ""
        rows.append(row_dict)
    return rows


async def increment_field(tab: str, row_number: int, field: str, default: int = 0) -> None:
    """Read a numeric field, increment by 1, and write it back."""
    row = await _get_row(tab, row_number)
    current = row.get(field, "")
    try:
        new_val = int(current) + 1
    except (ValueError, TypeError):
        new_val = default + 1
    await update_row(tab, row_number, {field: str(new_val)})


async def find_monthly_row(month_label: str) -> int | None:
    """
    Find the row number for a given month label (e.g. 'Apr 2026') in Monthly Metrics.
    Returns the sheet row number or None if not found.
    """
    def _find():
        svc = _get_service()
        sid = _sheet_id()
        result = svc.spreadsheets().values().get(
            spreadsheetId=sid,
            range=f"{TAB_MONTHLY}!A:A",
        ).execute()
        return result.get("values", [])

    try:
        values = await _run(_find)
        data_start = TAB_DATA_START[TAB_MONTHLY]
        for i, row in enumerate(values):
            sheet_row = i + 1
            if sheet_row < data_start:
                continue
            if row and row[0].strip() == month_label:
                return sheet_row
    except Exception as e:
        log.exception(f"Sheets find_monthly_row failed: {e}")
    return None


async def update_monthly_metrics(deal_value: float) -> None:
    """
    On a deal close: find today's month row and increment deals_closed by 1,
    add deal_value to revenue. If the month row doesn't exist, append it.
    """
    today = date.today()
    month_label = today.strftime("%b %Y")  # e.g. "Apr 2026"

    row_num = await find_monthly_row(month_label)

    if row_num:
        row = await _get_row(TAB_MONTHLY, row_num)

        # Increment deals_closed
        try:
            new_closed = int(row.get("deals_closed", "") or 0) + 1
        except (ValueError, TypeError):
            new_closed = 1

        # Add to revenue (strip $ and commas)
        try:
            existing_rev = float(str(row.get("revenue", "0")).replace("$", "").replace(",", "") or 0)
        except (ValueError, TypeError):
            existing_rev = 0.0
        new_rev = existing_rev + deal_value

        # Recalculate avg deal size
        avg = round(new_rev / new_closed) if new_closed else 0

        await update_row(TAB_MONTHLY, row_num, {
            "deals_closed": str(new_closed),
            "revenue": str(round(new_rev)),
            "avg_deal_size": f"${avg:,}",
        })
        log.info(f"Monthly Metrics updated for {month_label}: {new_closed} deals, ${new_rev:,.0f} revenue")
    else:
        # Month row doesn't exist yet — append it
        await append_row(TAB_MONTHLY, [
            month_label, "", "", "", "", "", "", "", "", "", "1", "", str(round(deal_value)),
            "", f"${round(deal_value):,}", "", "", "",
        ])
        log.info(f"Monthly Metrics: appended new row for {month_label}")
