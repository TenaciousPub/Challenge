"""Google Sheets integration wrapper using gspread."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import logging

import gspread
from gspread import Worksheet
from gspread.exceptions import WorksheetNotFound

from google.oauth2.service_account import Credentials

from .config import SheetsConfig
from .models import DayOffRequest, DayOffVote, DailyLogEntry, Participant, Workout, Challenge

LOGGER = logging.getLogger(__name__)

PARTICIPANTS_SHEET = "Participants"
CHALLENGES_SHEET = "Challenges"
DAILY_LOG_SHEET = "DailyLog"
PUNISHMENTS_SHEET = "Punishments"
DAY_OFF_VOTES_SHEET = "DayOffVotes"
SETTINGS_SHEET = "Settings"


def _service_account_credentials(creds_path: str) -> Credentials:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
    ]
    return Credentials.from_service_account_file(creds_path, scopes=scopes)


def _strip_headers(headers: List[str]) -> List[str]:
    return [str(h or "").strip() for h in headers]


def _headers_have_blanks_or_dupes(headers: List[str]) -> bool:
    cleaned = _strip_headers(headers)
    nonempty = [h for h in cleaned if h]
    return (len(nonempty) != len(set(nonempty))) or (len(nonempty) != len(cleaned))


def _safe_get_all_records(ws: Worksheet, *, expected_headers: Optional[List[str]] = None) -> List[dict]:
    """gspread raises if header row contains duplicates or blanks."""
    try:
        headers = ws.row_values(1)
        if expected_headers and _headers_have_blanks_or_dupes(headers):
            LOGGER.warning("⚠️ Sheet '%s' has blank/duplicate headers: %s. Using expected_headers.", ws.title, headers)
            return ws.get_all_records(expected_headers=expected_headers, head=1, default_blank="")
        return ws.get_all_records(head=1, default_blank="")
    except Exception as e:
        if expected_headers:
            LOGGER.warning("⚠️ get_all_records failed on '%s' (%s). Using expected_headers fallback.", ws.title, e)
            return ws.get_all_records(expected_headers=expected_headers, head=1, default_blank="")
        raise


@dataclass(slots=True)
class GoogleSheetsService:
    config: SheetsConfig
    client: gspread.Client = field(init=False)
    spreadsheet: gspread.Spreadsheet = field(init=False)

    def __post_init__(self) -> None:
        if not self.config.credentials_path:
            raise RuntimeError("Google Sheets credentials path is not configured")
        credentials = _service_account_credentials(str(self.config.credentials_path))
        self.client = gspread.authorize(credentials)
        self.spreadsheet = self.client.open_by_key(self.config.spreadsheet_id)

    def _worksheet(self, title: str) -> Worksheet:
        try:
            return self.spreadsheet.worksheet(title)
        except WorksheetNotFound as exc:
            raise RuntimeError(f"Worksheet '{title}' not found in spreadsheet {self.config.spreadsheet_id}") from exc

    # ---------------- Settings ----------------
    def _ensure_settings_headers(self, ws: Worksheet) -> None:
        required = ["key", "value"]
        headers = _strip_headers(ws.row_values(1))
        if not headers:
            ws.insert_row(required, 1)
            return
        if _headers_have_blanks_or_dupes(headers):
            return
        changed = False
        for h in required:
            if h not in headers:
                headers.append(h); changed = True
        if changed:
            ws.delete_rows(1)
            ws.insert_row(headers, 1)

    def get_setting(self, key: str) -> Optional[str]:
        ws = self._worksheet(SETTINGS_SHEET)
        self._ensure_settings_headers(ws)
        expected_headers = ["key", "value"]
        rows = _safe_get_all_records(ws, expected_headers=expected_headers)
        k = str(key or "").strip()
        for r in rows:
            if str(r.get("key", "")).strip() == k:
                v = r.get("value")
                return str(v).strip() if v is not None else None
        return None

    def set_setting(self, key: str, value: str) -> None:
        ws = self._worksheet(SETTINGS_SHEET)
        self._ensure_settings_headers(ws)
        expected_headers = ["key", "value"]
        rows = _safe_get_all_records(ws, expected_headers=expected_headers)
        k = str(key or "").strip()
        # find row
        for idx, r in enumerate(rows, start=2):
            if str(r.get("key", "")).strip() == k:
                ws.update(f"B{idx}:B{idx}", [[str(value)]])
                return
        ws.append_row([k, str(value)], value_input_option="USER_ENTERED")

    # ---------------- Participants ----------------
    def _ensure_participants_headers(self, ws: Worksheet) -> None:
        required = [
            "discord_id",
            "discord_tag",
            "display_name",
            "gender",
            "is_disabled",
            "timezone",
            "joined_on",
            "last_punished_on",
            "last_congrats_on",
            "preferred_challenge_id",
            "height_cm",
            "weight_kg",
            "nutrition_goal",
        ]
        headers = _strip_headers(ws.row_values(1))
        if not headers:
            ws.insert_row(required, 1)
            return
        if _headers_have_blanks_or_dupes(headers):
            return
        changed = False
        for h in required:
            if h not in headers:
                headers.append(h)
                changed = True
        if changed:
            ws.delete_rows(1)
            ws.insert_row(headers, 1)

    def fetch_participants(self) -> List[Participant]:
        ws = self._worksheet(PARTICIPANTS_SHEET)
        self._ensure_participants_headers(ws)

        expected_headers = [
            "discord_id",
            "discord_tag",
            "display_name",
            "gender",
            "is_disabled",
            "timezone",
            "joined_on",
            "last_punished_on",
            "last_congrats_on",
            "preferred_challenge_id",
            "height_cm",
            "weight_kg",
            "nutrition_goal",
        ]
        rows = _safe_get_all_records(ws, expected_headers=expected_headers)

        participants: List[Participant] = []
        for r in rows:
            try:
                joined_on_raw = str(r.get("joined_on") or "").strip()
                joined_on_val: Optional[date] = None
                if joined_on_raw:
                    try:
                        joined_on_val = date.fromisoformat(joined_on_raw)
                    except Exception:
                        joined_on_val = None

                # Parse height and weight
                height_cm_val: Optional[float] = None
                weight_kg_val: Optional[float] = None
                try:
                    height_raw = str(r.get("height_cm") or "").strip()
                    if height_raw:
                        height_cm_val = float(height_raw)
                except Exception:
                    pass
                try:
                    weight_raw = str(r.get("weight_kg") or "").strip()
                    if weight_raw:
                        weight_kg_val = float(weight_raw)
                except Exception:
                    pass

                # Parse nutrition goal
                nutrition_goal_val = str(r.get("nutrition_goal") or "").strip() or None

                participants.append(
                    Participant(
                        discord_id=str(r.get("discord_id", "")).strip(),
                        discord_tag=str(r.get("discord_tag", "")).strip(),
                        display_name=str(r.get("display_name", "")).strip(),
                        gender=(str(r.get("gender", "")).strip().lower() or None),
                        is_disabled=str(r.get("is_disabled", "")).strip().lower() in ("true", "1", "yes"),
                        timezone=str(r.get("timezone", "")).strip(),
                        joined_on=joined_on_val,
                        last_punished_on=str(r.get("last_punished_on") or "").strip() or None,
                        last_congrats_on=str(r.get("last_congrats_on") or "").strip() or None,
                        preferred_challenge_id=str(r.get("preferred_challenge_id") or "").strip() or None,
                        height_cm=height_cm_val,
                        weight_kg=weight_kg_val,
                        nutrition_goal=nutrition_goal_val,
                    )
                )
            except Exception as e:
                LOGGER.warning("⚠️ Skipping malformed participant row: %s | %s", r, e)

        return participants

    def append_participant(self, participant: Participant) -> None:
        ws = self._worksheet(PARTICIPANTS_SHEET)
        self._ensure_participants_headers(ws)
        ws.append_row(
            [
                participant.discord_id,
                participant.discord_tag,
                participant.display_name,
                participant.gender or "",
                str(bool(participant.is_disabled)),
                participant.timezone,
                participant.joined_on.isoformat() if participant.joined_on else "",
                participant.last_punished_on or "",
                participant.last_congrats_on or "",
                participant.preferred_challenge_id or "",
                str(participant.height_cm) if participant.height_cm is not None else "",
                str(participant.weight_kg) if participant.weight_kg is not None else "",
                participant.nutrition_goal or "",
            ],
            value_input_option="USER_ENTERED",
        )

    def update_participant_field(self, discord_id: str, field_name: str, value: str) -> bool:
        ws = self._worksheet(PARTICIPANTS_SHEET)
        self._ensure_participants_headers(ws)

        headers = _strip_headers(ws.row_values(1))
        expected_headers = [
            "discord_id","discord_tag","display_name","gender","is_disabled","timezone","joined_on","last_punished_on","last_congrats_on","preferred_challenge_id","height_cm","weight_kg","nutrition_goal"
        ]

        if _headers_have_blanks_or_dupes(headers):
            rows = _safe_get_all_records(ws, expected_headers=expected_headers)
            for i, r in enumerate(rows, start=2):
                if str(r.get("discord_id","")).strip() == str(discord_id).strip():
                    if field_name not in expected_headers:
                        return False
                    col = expected_headers.index(field_name) + 1
                    ws.update_cell(i, col, value)
                    return True
            return False

        if field_name not in headers:
            headers.append(field_name)
            ws.delete_rows(1)
            ws.insert_row(headers, 1)

        col = headers.index(field_name) + 1
        id_col = headers.index("discord_id") + 1
        ids = ws.col_values(id_col)
        for i, v in enumerate(ids, start=1):
            if i == 1:
                continue
            if str(v).strip() == str(discord_id).strip():
                ws.update_cell(i, col, value)
                return True
        return False

    def get_participant_field(self, discord_id: str, field_name: str) -> Optional[str]:
        ws = self._worksheet(PARTICIPANTS_SHEET)
        expected_headers = [
            "discord_id","discord_tag","display_name","gender","is_disabled","timezone","joined_on","last_punished_on","last_congrats_on","preferred_challenge_id"
        ]
        rows = _safe_get_all_records(ws, expected_headers=expected_headers)
        for r in rows:
            if str(r.get("discord_id","")).strip() == str(discord_id).strip():
                val = r.get(field_name)
                return str(val).strip() if val is not None else None
        return None

    # ---------------- Challenges ----------------
    def _ensure_challenges_headers(self, ws: Worksheet) -> None:
        required = ["challenge_id","discord_id","challenge_type","daily_target","unit","active","created_at"]
        headers = _strip_headers(ws.row_values(1))
        if not headers:
            ws.insert_row(required, 1)
            return
        if _headers_have_blanks_or_dupes(headers):
            return
        changed = False
        for h in required:
            if h not in headers:
                headers.append(h); changed = True
        if changed:
            ws.delete_rows(1)
            ws.insert_row(headers, 1)

    def fetch_challenges(self, *, discord_id: Optional[str] = None, active_only: bool = False) -> List[Challenge]:
        ws = self._worksheet(CHALLENGES_SHEET)
        self._ensure_challenges_headers(ws)
        expected_headers = ["challenge_id","discord_id","challenge_type","daily_target","unit","active","created_at"]
        rows = _safe_get_all_records(ws, expected_headers=expected_headers)

        items: List[Challenge] = []
        for r in rows:
            try:
                cid = str(r.get("challenge_id","")).strip()
                pid = str(r.get("discord_id","")).strip()
                if not cid or not pid:
                    continue
                if discord_id and pid != str(discord_id).strip():
                    continue

                active_val = r.get("active", True)
                is_active = bool(active_val) if isinstance(active_val, bool) else str(active_val).strip().lower() in {"true","1","yes"}
                if active_only and not is_active:
                    continue

                created_at_val = r.get("created_at")
                try:
                    created_at = datetime.fromisoformat(str(created_at_val)) if created_at_val else None
                except Exception:
                    created_at = None

                def _to_int(x) -> int:
                    try:
                        return int(str(x).strip() or "0")
                    except Exception:
                        return 0

                items.append(
                    Challenge(
                        challenge_id=cid,
                        discord_id=pid,
                        challenge_type=str(r.get("challenge_type","")).strip() or "custom",
                        daily_target=max(0, _to_int(r.get("daily_target", 0))),
                        unit=str(r.get("unit","reps")).strip() or "reps",
                        active=is_active,
                        created_at=created_at,
                    )
                )
            except Exception as e:
                LOGGER.warning("⚠️ Skipping malformed challenge row: %s | %s", r, e)
        return items

    def append_challenge(self, challenge: Challenge) -> None:
        ws = self._worksheet(CHALLENGES_SHEET)
        self._ensure_challenges_headers(ws)
        ws.append_row(
            [
                challenge.challenge_id,
                challenge.discord_id,
                challenge.challenge_type,
                int(challenge.daily_target),
                challenge.unit,
                "TRUE" if challenge.active else "FALSE",
                (challenge.created_at.isoformat() if challenge.created_at else datetime.utcnow().isoformat()),
            ],
            value_input_option="USER_ENTERED",
        )

    def set_challenge_active(self, challenge_id: str, active: bool) -> bool:
        ws = self._worksheet(CHALLENGES_SHEET)
        expected_headers = ["challenge_id","discord_id","challenge_type","daily_target","unit","active","created_at"]
        rows = _safe_get_all_records(ws, expected_headers=expected_headers)
        for idx, r in enumerate(rows, start=2):
            if str(r.get("challenge_id","")).strip() == str(challenge_id).strip():
                ws.update_cell(idx, 6, "TRUE" if active else "FALSE")
                return True
        return False

    # ---------------- Daily Log ----------------
    def append_daily_log(self, entry: DailyLogEntry) -> None:
        ws = self._worksheet(DAILY_LOG_SHEET)
        # allow legacy sheets that don't have challenge_id column yet
        headers = _strip_headers(ws.row_values(1))
        has_challenge_id = "challenge_id" in headers

        row = [
            entry.log_date.isoformat(),
            entry.discord_id,
            int(entry.pushup_count),
            int(entry.workout_bonus) if entry.workout_bonus is not None else "",
            str(bool(entry.penalized)),
            entry.notes or "",
            (entry.logged_at.isoformat() if entry.logged_at else datetime.utcnow().isoformat()),
        ]
        if has_challenge_id:
            row.append(entry.challenge_id or "")

        ws.append_row(row, value_input_option="USER_ENTERED")

    def fetch_daily_logs(self, log_date: date) -> List[DailyLogEntry]:
        ws = self._worksheet(DAILY_LOG_SHEET)

        # Support both schemas (with/without challenge_id)
        headers = _strip_headers(ws.row_values(1))
        if "challenge_id" in headers:
            expected_headers = ["date","discord_id","pushup_count","workout_bonus","penalized","notes","logged_at","challenge_id"]
        else:
            expected_headers = ["date","discord_id","pushup_count","workout_bonus","penalized","notes","logged_at"]

        rows = _safe_get_all_records(ws, expected_headers=expected_headers)

        entries: List[DailyLogEntry] = []
        for row in rows:
            date_value = row.get("date")
            if not date_value:
                continue
            try:
                row_date = date.fromisoformat(str(date_value))
            except ValueError:
                continue
            if row_date != log_date:
                continue

            def _to_int(x) -> int:
                try:
                    return int(str(x).strip() or "0")
                except Exception:
                    return 0

            pushups = _to_int(row.get("pushup_count", 0))
            bonus = row.get("workout_bonus")
            bonus_i = _to_int(bonus) if str(bonus or "").strip() else None

            penalized_value = row.get("penalized", False)
            penalized = (
                bool(penalized_value)
                if isinstance(penalized_value, bool)
                else str(penalized_value).lower() in {"true", "1", "yes"}
            )

            logged_at_value = row.get("logged_at")
            try:
                logged_at = datetime.fromisoformat(str(logged_at_value)) if logged_at_value else None
            except Exception:
                logged_at = None

            entries.append(
                DailyLogEntry(
                    log_date=row_date,
                    discord_id=str(row.get("discord_id", "")).strip(),
                    pushup_count=pushups,
                    workout_bonus=bonus_i,
                    penalized=penalized,
                    notes=(row.get("notes") or None),
                    logged_at=logged_at,
                    challenge_id=(str(row.get("challenge_id") or "").strip() or None),
                )
            )
        return entries

    def daily_amounts_by_challenge(self, log_date: date, *, include_bonus: bool = True) -> Dict[tuple[str, str], int]:
        """Return {(discord_id, challenge_id): amount} for the day."""
        totals: Dict[tuple[str, str], int] = {}
        for entry in self.fetch_daily_logs(log_date):
            cid = str(entry.challenge_id or "legacy").strip()
            key = (entry.discord_id, cid)
            totals[key] = totals.get(key, 0) + int(entry.pushup_count)
            if include_bonus and entry.workout_bonus:
                totals[key] += int(entry.workout_bonus)
        return totals

    def daily_pushup_totals(self, log_date: date, *, include_bonus: bool = True) -> Dict[str, int]:
        """Legacy helper: sums ALL logs for the day, ignoring challenge_id."""
        totals: Dict[str, int] = {}
        for entry in self.fetch_daily_logs(log_date):
            totals[entry.discord_id] = totals.get(entry.discord_id, 0) + int(entry.pushup_count)
            if include_bonus and entry.workout_bonus:
                totals[entry.discord_id] += int(entry.workout_bonus)
        return totals

    def total_pushup_totals(self, *, include_bonus: bool = True) -> Dict[str, int]:
        ws = self._worksheet(DAILY_LOG_SHEET)
        headers = _strip_headers(ws.row_values(1))
        if "challenge_id" in headers:
            expected_headers = ["date","discord_id","pushup_count","workout_bonus","penalized","notes","logged_at","challenge_id"]
        else:
            expected_headers = ["date","discord_id","pushup_count","workout_bonus","penalized","notes","logged_at"]

        rows = _safe_get_all_records(ws, expected_headers=expected_headers)

        totals: Dict[str, int] = {}
        for row in rows:
            discord_id = str(row.get("discord_id","")).strip()
            if not discord_id:
                continue

            def _to_int(x) -> int:
                try:
                    return int(str(x).strip() or "0")
                except Exception:
                    return 0

            val = _to_int(row.get("pushup_count", 0))
            if include_bonus:
                bonus_raw = row.get("workout_bonus")
                if str(bonus_raw or "").strip():
                    val += _to_int(bonus_raw)
            totals[discord_id] = totals.get(discord_id, 0) + val
        return totals

    def mark_penalized_for_day(self, discord_id: str, log_date: date) -> bool:
        """Mark penalized=true for the first matching row on that date; if none exists, append a marker row."""
        ws = self._worksheet(DAILY_LOG_SHEET)
        headers = _strip_headers(ws.row_values(1))
        if "challenge_id" in headers:
            expected_headers = ["date","discord_id","pushup_count","workout_bonus","penalized","notes","logged_at","challenge_id"]
        else:
            expected_headers = ["date","discord_id","pushup_count","workout_bonus","penalized","notes","logged_at"]

        rows = _safe_get_all_records(ws, expected_headers=expected_headers)

        for i, row in enumerate(rows, start=2):
            if str(row.get("date","")).strip() == log_date.isoformat() and str(row.get("discord_id","")).strip() == str(discord_id).strip():
                try:
                    ws.update_cell(i, 5, "TRUE")
                    return True
                except Exception:
                    break

        try:
            marker = [log_date.isoformat(), str(discord_id), 0, "", "TRUE", "punishment assigned", datetime.utcnow().isoformat()]
            if "challenge_id" in headers:
                marker.append("")
            ws.append_row(marker, value_input_option="USER_ENTERED")
            return True
        except Exception:
            return False

    # ---------------- Workouts (Punishments) ----------------
    def fetch_workouts(self) -> List[Workout]:
        ws = self._worksheet(PUNISHMENTS_SHEET)
        expected_headers = ["id","description","category","difficulty"]
        rows = _safe_get_all_records(ws, expected_headers=expected_headers)

        workouts: List[Workout] = []
        for row in rows:
            try:
                workouts.append(
                    Workout(
                        id=str(row.get("id","")).strip() or "0",
                        description=str(row.get("description","")).strip(),
                        category=str(row.get("category","standard")).strip().lower() or "standard",
                        difficulty=int(str(row.get("difficulty", 1)).strip() or "1"),
                        source="sheet",
                    )
                )
            except Exception as e:
                LOGGER.warning("Skipping malformed workout row: %s | %s", row, e)
        return workouts

    # ---------------- Day-off votes ----------------
    def persist_day_off_request(self, request: DayOffRequest) -> None:
        ws = self._worksheet(DAY_OFF_VOTES_SHEET)
        expected_headers = ["request_id","target_day","request_date","requested_by","deadline","participant_id","vote","voted_at","reason"]
        rows = _safe_get_all_records(ws, expected_headers=expected_headers)
        existing_ids = {str(row.get("request_id") or "").strip() for row in rows}
        if request.request_id in existing_ids:
            return

        for vote in request.votes.values():
            ws.append_row(
                [
                    vote.request_id,
                    request.target_day.isoformat(),
                    vote.request_date.isoformat(),
                    vote.requested_by,
                    vote.deadline.isoformat(),
                    vote.participant_id,
                    vote.vote,
                    (vote.voted_at.isoformat() if vote.voted_at else ""),
                    request.reason or "",
                ],
                value_input_option="USER_ENTERED",
            )

    def update_day_off_vote(self, vote: DayOffVote, *, target_day: Optional[date] = None, reason: Optional[str] = None) -> None:
        ws = self._worksheet(DAY_OFF_VOTES_SHEET)
        expected_headers = ["request_id","target_day","request_date","requested_by","deadline","participant_id","vote","voted_at","reason"]
        rows = _safe_get_all_records(ws, expected_headers=expected_headers)

        for idx, row in enumerate(rows, start=2):
            if str(row.get("request_id","")).strip() == vote.request_id and str(row.get("participant_id","")).strip() == vote.participant_id:
                ws.update(f"G{idx}:I{idx}", [[vote.vote, (vote.voted_at.isoformat() if vote.voted_at else ""), (reason or row.get("reason") or "")]])
                if target_day:
                    ws.update_cell(idx, 2, target_day.isoformat())
                return

        ws.append_row(
            [
                vote.request_id,
                (target_day.isoformat() if target_day else ""),
                vote.request_date.isoformat(),
                vote.requested_by,
                vote.deadline.isoformat(),
                vote.participant_id,
                vote.vote,
                (vote.voted_at.isoformat() if vote.voted_at else ""),
                reason or "",
            ],
            value_input_option="USER_ENTERED",
        )

    def fetch_day_off_requests(self) -> Dict[str, DayOffRequest]:
        ws = self._worksheet(DAY_OFF_VOTES_SHEET)
        expected_headers = ["request_id","target_day","request_date","requested_by","deadline","participant_id","vote","voted_at","reason"]
        rows = _safe_get_all_records(ws, expected_headers=expected_headers)

        grouped: Dict[str, List[dict]] = defaultdict(list)
        for row in rows:
            request_id = str(row.get("request_id") or "").strip()
            if not request_id:
                continue
            grouped[request_id].append(row)

        requests: Dict[str, DayOffRequest] = {}
        for request_id, entries in grouped.items():
            first = entries[0]
            try:
                target_day = date.fromisoformat(str(first.get("target_day") or first.get("request_date")))
            except Exception:
                continue
            try:
                request_date = date.fromisoformat(str(first.get("request_date")))
            except Exception:
                request_date = target_day

            requested_by = str(first.get("requested_by", "")).strip()
            deadline_value = first.get("deadline")
            try:
                deadline = datetime.fromisoformat(str(deadline_value)) if deadline_value else datetime.combine(target_day, datetime.min.time())
            except Exception:
                deadline = datetime.combine(target_day, datetime.min.time())

            reason = str(first.get("reason") or "").strip() or None

            votes: Dict[str, DayOffVote] = {}
            for row in entries:
                participant_id = str(row.get("participant_id", "")).strip()
                if not participant_id:
                    continue
                vote_value = str(row.get("vote", "pending")).strip().lower() or "pending"
                voted_at_value = row.get("voted_at")
                try:
                    voted_at = datetime.fromisoformat(str(voted_at_value)) if voted_at_value else None
                except Exception:
                    voted_at = None

                votes[participant_id] = DayOffVote(
                    request_id=request_id,
                    request_date=request_date,
                    requested_by=requested_by,
                    deadline=deadline,
                    participant_id=participant_id,
                    vote=vote_value,
                    voted_at=voted_at,
                )

            requests[request_id] = DayOffRequest(
                request_id=request_id,
                target_day=target_day,
                request_date=request_date,
                requested_by=requested_by,
                deadline=deadline,
                votes=votes,
                reason=reason,
            )

        return requests

    def normalize_all_participant_timezones(self, default_tz: str) -> Tuple[int, int]:
        from .timezones import normalize_timezone
        participants = self.fetch_participants()
        total = len(participants)
        changed = 0
        ws = self._worksheet(PARTICIPANTS_SHEET)
        headers = _strip_headers(ws.row_values(1))
        tz_col = (headers.index("timezone") + 1) if ("timezone" in headers and not _headers_have_blanks_or_dupes(headers)) else 6

        for i, p in enumerate(participants, start=2):
            normalized_tz = normalize_timezone(p.timezone, default=default_tz)
            if normalized_tz != p.timezone:
                try:
                    ws.update_cell(i, tz_col, normalized_tz)
                    changed += 1
                except Exception as e:
                    LOGGER.warning("Failed to normalize timezone for %s: %s", p.discord_id, e)
        return total, changed
