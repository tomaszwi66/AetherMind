"""
collect/google_calendar.py - Google Calendar API collector.

Uses OAuth 2.0 (Desktop App flow) for authentication.
Supports incremental sync via syncToken - only fetches changes since last run.
Works across all calendars the user has access to.

First-time setup requires credentials/google_client_secret.json.
See README.md → Google Calendar Setup for instructions.

Token is saved to credentials/google_token.json after first auth.
Subsequent runs are fully automatic (token auto-refreshes).
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

from collect.base import BaseCollector, CollectorError

SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]

# Where Google auth files live (relative to project root)
CREDENTIALS_DIR = Path("credentials")
CLIENT_SECRET_FILE = CREDENTIALS_DIR / "google_client_secret.json"
TOKEN_FILE = CREDENTIALS_DIR / "google_token.json"
SYNC_TOKEN_FILE = CREDENTIALS_DIR / "calendar_sync_tokens.json"


def _check_google_deps() -> bool:
    """Return True if google-api packages are installed."""
    try:
        import google.auth  # noqa
        import google_auth_oauthlib  # noqa
        import googleapiclient  # noqa
        return True
    except ImportError:
        return False


def _load_token() -> Optional[object]:
    """Load saved OAuth token from disk. Returns credentials object or None."""
    from google.oauth2.credentials import Credentials

    if not TOKEN_FILE.exists():
        return None
    try:
        data = json.loads(TOKEN_FILE.read_text())
        return Credentials(
            token=data.get("token"),
            refresh_token=data.get("refresh_token"),
            token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=data.get("client_id"),
            client_secret=data.get("client_secret"),
            scopes=data.get("scopes", SCOPES),
        )
    except Exception as e:
        logger.warning(f"[gcal] Failed to load token: {e}")
        return None


def _save_token(creds) -> None:
    """Persist OAuth token to disk as JSON (not pickle - more secure)."""
    CREDENTIALS_DIR.mkdir(exist_ok=True)
    data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes) if creds.scopes else SCOPES,
    }
    TOKEN_FILE.write_text(json.dumps(data, indent=2))
    logger.debug("[gcal] Token saved")


def get_credentials():
    """
    Return valid Google OAuth credentials.
    - Loads from token file if available
    - Refreshes automatically if expired
    - Runs browser OAuth flow if no token exists
    Raises CollectorError if client_secret not found.
    """
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow

    if not CLIENT_SECRET_FILE.exists():
        raise CollectorError(
            f"Google credentials not found at {CLIENT_SECRET_FILE}\n"
            f"See README.md → Google Calendar Setup for instructions.\n"
            f"Or run: python setup.py"
        )

    creds = _load_token()

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            _save_token(creds)
            return creds
        except Exception as e:
            logger.warning(f"[gcal] Token refresh failed: {e} - re-authenticating")

    # Full OAuth flow - opens browser
    logger.info("[gcal] Starting Google OAuth flow (browser will open)...")
    flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET_FILE), SCOPES)
    creds = flow.run_local_server(port=0, open_browser=True)
    _save_token(creds)
    logger.info("[gcal] Authentication successful")
    return creds


def _build_service():
    """Build and return the Google Calendar API service."""
    from googleapiclient.discovery import build
    creds = get_credentials()
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


def _load_sync_tokens() -> dict:
    """Load per-calendar syncTokens from disk."""
    if SYNC_TOKEN_FILE.exists():
        try:
            return json.loads(SYNC_TOKEN_FILE.read_text())
        except Exception:
            pass
    return {}


def _save_sync_tokens(tokens: dict) -> None:
    CREDENTIALS_DIR.mkdir(exist_ok=True)
    SYNC_TOKEN_FILE.write_text(json.dumps(tokens, indent=2))


def _list_calendars(service) -> list[dict]:
    """Return all calendar entries accessible to the user."""
    calendars = []
    page_token = None
    while True:
        result = service.calendarList().list(pageToken=page_token).execute()
        calendars.extend(result.get("items", []))
        page_token = result.get("nextPageToken")
        if not page_token:
            break
    return calendars


def _parse_event_time(time_obj: dict) -> Optional[str]:
    """Parse Google Calendar event time object → ISO 8601 string."""
    if not time_obj:
        return None
    # dateTime: "2026-04-14T10:00:00+02:00"
    dt_str = time_obj.get("dateTime")
    if dt_str:
        try:
            from dateutil import parser as dp
            return dp.parse(dt_str).isoformat()
        except Exception:
            return dt_str
    # date (all-day): "2026-04-14"
    date_str = time_obj.get("date")
    if date_str:
        return f"{date_str}T00:00:00"
    return None


def _compute_duration(start: dict, end: dict) -> Optional[int]:
    """Return duration in minutes between start and end time objects."""
    try:
        from dateutil import parser as dp
        s_str = start.get("dateTime") or f"{start.get('date')}T00:00:00"
        e_str = end.get("dateTime") or f"{end.get('date')}T23:59:59"
        s_dt = dp.parse(s_str)
        e_dt = dp.parse(e_str)
        return max(0, int((e_dt - s_dt).total_seconds() / 60))
    except Exception:
        return None


def _fetch_calendar_events(
    service,
    calendar_id: str,
    calendar_name: str,
    sync_token: Optional[str],
    lookback_days: int = 365,
) -> tuple[list[dict], Optional[str]]:
    """
    Fetch events from a single calendar.
    Returns (raw_records, next_sync_token).
    Handles 410 Gone (expired sync token) by falling back to full sync.
    """
    from googleapiclient.errors import HttpError

    def _do_fetch(token: Optional[str]) -> tuple[list, Optional[str]]:
        all_items = []
        page_token = None
        next_sync_token = None

        request_kwargs = {
            "calendarId": calendar_id,
            "singleEvents": True,
            "maxResults": 2500,
            "fields": "nextPageToken,nextSyncToken,items(id,summary,start,end,location,description,status,organizer,attendees,recurrence,recurringEventId)",
        }

        if token:
            request_kwargs["syncToken"] = token
        else:
            # Full sync: fetch events from lookback_days ago
            since = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
            request_kwargs["timeMin"] = since
            request_kwargs["orderBy"] = "startTime"

        while True:
            if page_token:
                request_kwargs["pageToken"] = page_token
            else:
                request_kwargs.pop("pageToken", None)

            response = service.events().list(**request_kwargs).execute()
            all_items.extend(response.get("items", []))
            page_token = response.get("nextPageToken")
            next_sync_token = response.get("nextSyncToken", next_sync_token)
            if not page_token:
                break

        return all_items, next_sync_token

    try:
        items, next_token = _do_fetch(sync_token)
    except HttpError as e:
        if e.resp.status == 410:
            # syncToken expired - fall back to full sync
            logger.warning(f"[gcal] syncToken expired for '{calendar_name}' - full resync")
            items, next_token = _do_fetch(None)
        else:
            raise

    # Convert to raw records
    records = []
    for item in items:
        status = item.get("status", "confirmed")
        if status == "cancelled":
            continue  # Skip deleted events

        start = item.get("start", {})
        end = item.get("end", {})
        timestamp = _parse_event_time(start)
        if not timestamp:
            continue

        title = item.get("summary", "").strip() or "Untitled Event"
        duration_min = _compute_duration(start, end)
        is_all_day = "date" in start and "dateTime" not in start
        is_recurring = bool(item.get("recurringEventId") or item.get("recurrence"))
        attendees = item.get("attendees", [])
        has_attendees = len(attendees) > 1  # >1 means there are other people

        records.append(
            {
                "raw_timestamp": timestamp,
                "raw_summary": title,
                "source": "google_calendar",
                "external_id": item["id"],
                "calendar_id": calendar_id,
                "calendar_name": calendar_name,
                "title": title,
                "duration_minutes": duration_min,
                "is_all_day": is_all_day,
                "is_recurring": is_recurring,
                "location": item.get("location", ""),
                "description": (item.get("description") or "")[:500],
                "organizer": (item.get("organizer") or {}).get("email", ""),
                "has_attendees": has_attendees,
                "attendee_count": len(attendees),
                "status": status,
            }
        )

    return records, next_token


class GoogleCalendarCollector(BaseCollector):
    source_name = "google_calendar"

    def collect(self, last_run_timestamp: Optional[str] = None) -> list[dict]:
        if not _check_google_deps():
            logger.info(
                "[gcal] Google API packages not installed - skipping. "
                "Run: pip install google-api-python-client google-auth-oauthlib google-auth-httplib2"
            )
            return []

        if not CLIENT_SECRET_FILE.exists():
            logger.info("[gcal] No credentials file found - skipping Google Calendar")
            return []

        try:
            service = _build_service()
        except Exception as e:
            logger.warning(f"[gcal] Authentication failed: {e}")
            return []

        # Load per-calendar sync tokens
        sync_tokens = _load_sync_tokens()
        lookback_days = self.collect_config.get("lookback_days", 365)
        calendars_filter = self.collect_config.get("calendars", [])

        try:
            calendars = _list_calendars(service)
        except Exception as e:
            logger.warning(f"[gcal] Failed to list calendars: {e}")
            return []

        if calendars_filter:
            calendars = [c for c in calendars if c.get("summary") in calendars_filter or c["id"] in calendars_filter]

        logger.info(f"[gcal] Syncing {len(calendars)} calendars")

        all_records = []
        new_sync_tokens = dict(sync_tokens)

        for cal in calendars:
            cal_id = cal["id"]
            cal_name = cal.get("summary", cal_id)
            current_token = sync_tokens.get(cal_id)

            try:
                records, next_token = _fetch_calendar_events(
                    service, cal_id, cal_name, current_token, lookback_days
                )
                if next_token:
                    new_sync_tokens[cal_id] = next_token

                logger.info(f"[gcal] '{cal_name}': {len(records)} events")
                all_records.extend(records)
            except Exception as e:
                logger.warning(f"[gcal] Failed to sync calendar '{cal_name}': {e}")

        # Persist updated sync tokens
        _save_sync_tokens(new_sync_tokens)

        logger.info(f"[gcal] Total: {len(all_records)} calendar events")
        return all_records
