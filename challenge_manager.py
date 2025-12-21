from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
import secrets

import pytz

from .config import AppConfig
from .models import (
    Participant,
    DailyLogEntry,
    Workout,
    ComplianceResult,
    DayOffRequest,
    DayOffVote,
    Challenge,
)
from .sheets import GoogleSheetsService
from .timezones import normalize_timezone

LOGGER = logging.getLogger(__name__)


def _now_utc() -> datetime:
    return datetime.utcnow().replace(tzinfo=pytz.UTC)


class ChallengeManager:
    """Orchestrates participants, daily logs, challenges, compliance checks, workouts, and day-off voting."""

    def __init__(
        self,
        *,
        app_config: AppConfig,
        sheets: GoogleSheetsService,
        workouts,
    ) -> None:
        self.app_config = app_config
        self.sheets = sheets
        self.workouts = workouts

        self.default_timezone_name: str = self.app_config.challenge.default_timezone
        self.default_timezone = pytz.timezone(self.default_timezone_name)

        self._participants: Dict[str, Participant] = {}
        self.refresh_participants()

        try:
            self._day_off_requests: Dict[str, DayOffRequest] = self.sheets.fetch_day_off_requests()
        except Exception as e:
            LOGGER.warning("Could not load day-off requests from Sheets: %s", e)
            self._day_off_requests = {}

    # ---------------- Settings (stored in Settings sheet) ----------------
    def compliance_mode(self) -> str:
        v = None
        try:
            v = self.sheets.get_setting("compliance_mode")
        except Exception:
            v = None
        mode = (str(v or self.app_config.challenge.compliance_mode_default).strip().lower() or "strict")
        return mode if mode in {"strict", "lenient", "points"} else "strict"

    def points_target(self) -> int:
        v = None
        try:
            v = self.sheets.get_setting("points_daily_target")
        except Exception:
            v = None
        try:
            return max(1, int(str(v or self.app_config.challenge.points_daily_target_default).strip()))
        except Exception:
            return max(1, int(self.app_config.challenge.points_daily_target_default))

    def set_compliance_mode(self, mode: str) -> str:
        m = (mode or "").strip().lower()
        if m not in {"strict", "lenient", "points"}:
            raise RuntimeError("mode must be strict | lenient | points")
        try:
            self.sheets.set_setting("compliance_mode", m)
        except Exception as e:
            LOGGER.warning("Failed to persist compliance_mode: %s", e)
        return m

    def set_points_target(self, target: int) -> int:
        t = int(target)
        if t < 1:
            raise RuntimeError("points target must be >= 1")
        try:
            self.sheets.set_setting("points_daily_target", str(t))
        except Exception as e:
            LOGGER.warning("Failed to persist points_daily_target: %s", e)
        return t

    # ---------------- Participants ----------------
    def refresh_participants(self) -> None:
        try:
            participants = self.sheets.fetch_participants()
        except Exception as e:
            LOGGER.error("Failed to fetch participants: %s", e)
            return

        mapping: Dict[str, Participant] = {}
        for p in participants:
            tz = normalize_timezone(p.timezone, default=self.default_timezone_name)
            mapping[str(p.discord_id)] = Participant(
                discord_id=str(p.discord_id),
                discord_tag=p.discord_tag,
                display_name=p.display_name,
                gender=p.gender,
                is_disabled=bool(p.is_disabled),
                timezone=tz,
                joined_on=p.joined_on,
                last_punished_on=p.last_punished_on,
                last_congrats_on=p.last_congrats_on,
                preferred_challenge_id=p.preferred_challenge_id,
            )
        self._participants = mapping
        LOGGER.info("Loaded %d participants", len(self._participants))

    def get_participants(self) -> List[Participant]:
        return list(self._participants.values())

    def get_participant(self, discord_id: str) -> Optional[Participant]:
        return self._participants.get(str(discord_id))

    def get_participant_by_id(self, discord_id: str) -> Optional[Participant]:
        return self.get_participant(discord_id)

    def add_participant(
        self,
        *,
        discord_user,
        gender: str,
        is_disabled: bool = False,
        timezone: str = "America/Los_Angeles",
    ) -> Participant:
        gender = (gender or "").strip().lower()
        if gender not in {"female", "male"}:
            raise RuntimeError("gender must be 'female' or 'male'")

        tz_canonical = normalize_timezone(timezone, default=self.default_timezone_name)
        pid = str(discord_user.id)
        if pid in self._participants:
            raise RuntimeError("User is already a participant")

        p = Participant(
            discord_id=pid,
            discord_tag=str(discord_user),
            display_name=discord_user.display_name,
            gender=gender,
            is_disabled=bool(is_disabled),
            timezone=tz_canonical,
            joined_on=date.today(),
            preferred_challenge_id=None,
        )
        self.sheets.append_participant(p)
        self._participants[pid] = p
        return p

    # ---------------- Challenges ----------------
    def _new_challenge_id(self) -> str:
        # short + unique; fits nicely in sheet cells
        return "c_" + secrets.token_hex(3)

    def list_challenges(self, discord_id: str, *, active_only: bool = True) -> List[Challenge]:
        return self.sheets.fetch_challenges(discord_id=str(discord_id), active_only=active_only)

    def add_challenge(
        self,
        *,
        discord_id: str,
        challenge_type: str,
        daily_target: int,
        unit: str = "reps",
        set_default: bool = False,
    ) -> Challenge:
        pid = str(discord_id).strip()
        if not pid:
            raise RuntimeError("missing discord_id")

        ctype = (challenge_type or "").strip()
        if not ctype:
            raise RuntimeError("challenge_type is required")
        if len(ctype) > 32:
            raise RuntimeError("challenge_type max length is 32")

        unit = (unit or "reps").strip()
        if len(unit) > 16:
            raise RuntimeError("unit max length is 16")

        target = int(daily_target)
        if target <= 0:
            raise RuntimeError("daily_target must be > 0")

        ch = Challenge(
            challenge_id=self._new_challenge_id(),
            discord_id=pid,
            challenge_type=ctype.lower(),
            daily_target=target,
            unit=unit.lower(),
            active=True,
            created_at=_now_utc(),
        )
        self.sheets.append_challenge(ch)

        if set_default:
            self.set_default_challenge(discord_id=pid, challenge_id=ch.challenge_id)
        return ch

    def remove_challenge(self, *, discord_id: str, challenge_id: str) -> bool:
        pid = str(discord_id).strip()
        cid = str(challenge_id).strip()
        # ensure ownership
        items = self.sheets.fetch_challenges(discord_id=pid, active_only=False)
        owned = next((c for c in items if c.challenge_id == cid), None)
        if not owned:
            raise RuntimeError("challenge_id not found for this user")
        ok = self.sheets.set_challenge_active(cid, False)
        # if they removed default, clear it
        p = self.get_participant(pid)
        if p and p.preferred_challenge_id == cid:
            self.set_default_challenge(discord_id=pid, challenge_id="")
        return ok

    def set_default_challenge(self, *, discord_id: str, challenge_id: str) -> None:
        pid = str(discord_id).strip()
        cid = str(challenge_id or "").strip() or None

        if cid:
            items = self.sheets.fetch_challenges(discord_id=pid, active_only=True)
            if not any(c.challenge_id == cid for c in items):
                raise RuntimeError("challenge_id is not an active challenge for you")

        self.sheets.update_participant_field(pid, "preferred_challenge_id", cid or "")
        p = self.get_participant(pid)
        if p:
            p.preferred_challenge_id = cid

    def resolve_default_challenge_id(self, participant: Participant) -> Optional[str]:
        # 1) participant field
        cid = (participant.preferred_challenge_id or "").strip()
        if cid:
            return cid
        # 2) if exactly one active challenge, pick it
        active = self.sheets.fetch_challenges(discord_id=participant.discord_id, active_only=True)
        if len(active) == 1:
            return active[0].challenge_id
        return None

    # ---------------- Targets & logging (legacy fallback) ----------------
    def target_for(self, participant: Participant) -> int:
        disabled_target = self.app_config.challenge.disabled_daily_target
        if participant.is_disabled and isinstance(disabled_target, int):
            return max(0, disabled_target)

        g = (participant.gender or "").strip().lower()
        if g == "female":
            return int(self.app_config.challenge.target_female)
        if g == "male":
            return int(self.app_config.challenge.target_male)
        return int(self.app_config.challenge.target_default)

    def record_amount(
        self,
        *,
        participant_id: str,
        log_date: date,
        amount: int,
        challenge_id: Optional[str],
        workout_bonus: Optional[int] = None,
        notes: Optional[str] = None,
    ) -> None:
        p = self.get_participant(participant_id)
        if not p:
            raise RuntimeError("Participant not found")

        tz_name = normalize_timezone(p.timezone, default=self.default_timezone_name)
        tz = pytz.timezone(tz_name)

        entry = DailyLogEntry(
            log_date=log_date,
            discord_id=str(participant_id),
            pushup_count=int(amount),
            workout_bonus=int(workout_bonus) if workout_bonus is not None else None,
            penalized=False,
            notes=notes,
            logged_at=datetime.now(tz=tz),
            challenge_id=(str(challenge_id).strip() if challenge_id else None),
        )
        self.sheets.append_daily_log(entry)

    # ---------------- Compliance ----------------
    def _challenge_totals_for_day(self, log_date: date) -> Dict[Tuple[str, str], int]:
        # {(discord_id, challenge_id): total}
        return self.sheets.daily_amounts_by_challenge(log_date, include_bonus=True)

    def evaluate_compliance(self, log_date: date) -> List[ComplianceResult]:
        """Legacy: returns a single target/total per person (sum of all logs)."""
        totals = self.sheets.daily_pushup_totals(log_date, include_bonus=True)
        results: List[ComplianceResult] = []

        for p in self.get_participants():
            total = int(totals.get(p.discord_id, 0))
            target = self.target_for(p)
            compliant = total >= target
            results.append(
                ComplianceResult(
                    participant=p,
                    logged_total=total,
                    pushup_target=target,
                    compliant=compliant,
                    assigned_workout=None,
                )
            )
        return results

    def evaluate_multi_compliance(self, log_date: date) -> Dict[str, dict]:
        """Return compliance details per participant for multi-challenge mode."""
        totals = self._challenge_totals_for_day(log_date)
        mode = self.compliance_mode()
        points_target = self.points_target()

        out: Dict[str, dict] = {}
        for p in self.get_participants():
            active = self.sheets.fetch_challenges(discord_id=p.discord_id, active_only=True)

            # If user has no challenges configured yet, treat it as legacy pushups target.
            if not active:
                done = int(self.sheets.daily_pushup_totals(log_date, include_bonus=True).get(p.discord_id, 0))
                target = self.target_for(p)
                out[p.discord_id] = {
                    "mode": "legacy",
                    "compliant": done >= target,
                    "points": 1 if done >= target else 0,
                    "points_target": 1,
                    "missing": [] if done >= target else [{"challenge_id": "legacy", "type": "pushups", "need": max(0, target - done), "unit": "reps"}],
                    "met": [{"challenge_id": "legacy", "type": "pushups", "done": done, "target": target, "unit": "reps"}],
                }
                continue

            met: List[dict] = []
            missing: List[dict] = []
            points = 0

            for ch in active:
                done = int(totals.get((p.discord_id, ch.challenge_id), 0))
                ok = done >= int(ch.daily_target)
                met.append({"challenge_id": ch.challenge_id, "type": ch.challenge_type, "done": done, "target": ch.daily_target, "unit": ch.unit, "ok": ok})
                if ok:
                    points += 1
                else:
                    missing.append({"challenge_id": ch.challenge_id, "type": ch.challenge_type, "need": max(0, int(ch.daily_target) - done), "unit": ch.unit})

            compliant = False
            if mode == "strict":
                compliant = (len(missing) == 0)
            elif mode == "lenient":
                compliant = (points >= 1)
            else:  # points
                compliant = (points >= points_target)

            out[p.discord_id] = {
                "mode": mode,
                "compliant": compliant,
                "points": points,
                "points_target": (points_target if mode == "points" else (1 if mode == "lenient" else len(active))),
                "missing": missing,
                "met": met,
            }

        return out

    # ---------------- Day-off voting ----------------
    def _new_request_id(self) -> str:
        now = datetime.now(tz=self.default_timezone)
        return f"DOR-{int(now.timestamp())}"

    def create_day_off_request(
        self,
        *,
        requested_by: str,
        target_day: date,
        reason: Optional[str],
        deadline: datetime,
    ) -> DayOffRequest:
        request_id = self._new_request_id()
        req_date = date.today()

        votes: Dict[str, DayOffVote] = {}

        votes[str(requested_by)] = DayOffVote(
            request_id=request_id,
            request_date=req_date,
            requested_by=str(requested_by),
            deadline=deadline,
            participant_id=str(requested_by),
            vote="yes",
            voted_at=datetime.now(tz=self.default_timezone),
        )

        for p in self.get_participants():
            pid = str(p.discord_id)
            if pid == str(requested_by):
                continue
            votes[pid] = DayOffVote(
                request_id=request_id,
                request_date=req_date,
                requested_by=str(requested_by),
                deadline=deadline,
                participant_id=pid,
                vote="pending",
                voted_at=None,
            )

        req = DayOffRequest(
            request_id=request_id,
            target_day=target_day,
            request_date=req_date,
            requested_by=str(requested_by),
            deadline=deadline,
            votes=votes,
            reason=reason,
        )

        self._day_off_requests[request_id] = req
        try:
            self.sheets.persist_day_off_request(req)
        except Exception as e:
            LOGGER.warning("persist_day_off_request failed: %s", e)
        return req

    def register_vote(self, *, request_id: str, voter_id: str, vote: str) -> None:
        vote = (vote or "").strip().lower()
        if vote not in {"yes", "no"}:
            raise RuntimeError("vote must be yes/no")

        req = self._day_off_requests.get(request_id)
        if not req:
            raise RuntimeError("request not found")

        if datetime.utcnow().replace(tzinfo=pytz.UTC) > req.deadline.astimezone(pytz.UTC):
            raise RuntimeError("Voting is closed for this request")

        dv = req.votes.get(str(voter_id))
        if not dv:
            raise RuntimeError("You are not eligible to vote on this request")

        if dv.vote in {"yes", "no"}:
            raise RuntimeError("You already voted")

        dv.vote = vote
        dv.voted_at = datetime.now(tz=pytz.UTC)

        try:
            self.sheets.update_day_off_vote(dv, target_day=req.target_day, reason=req.reason)
        except Exception as e:
            LOGGER.warning("update_day_off_vote failed: %s", e)

    def is_request_approved(self, request_id: str) -> bool:
        state = self.compute_vote_state(request_id)
        return state["state"] == "approved"

    def compute_vote_state(self, request_id: str) -> Dict[str, int | str]:
        req = self._day_off_requests.get(request_id)
        if not req:
            raise RuntimeError("request not found")

        yes = sum(1 for v in req.votes.values() if v.vote == "yes")
        no = sum(1 for v in req.votes.values() if v.vote == "no")
        total = len(req.votes)

        voted = sum(1 for v in req.votes.values() if v.vote in {"yes", "no"})
        threshold = 3  # minimum yes threshold

        now_utc = datetime.utcnow().replace(tzinfo=pytz.UTC)
        closed = now_utc > req.deadline.astimezone(pytz.UTC)

        state = "open"
        if yes >= threshold:
            state = "approved"
        elif voted >= 3:
            if yes > no:
                state = "approved"
            elif no > yes:
                state = "rejected"
        if closed and state == "open":
            state = "approved" if yes > no else "rejected"

        return {"state": state, "yes": yes, "no": no, "total": total, "threshold": threshold}

    def has_approved_dayoff(self, *, participant_id: str, local_day: date) -> bool:
        for req in self._day_off_requests.values():
            if req.target_day != local_day:
                continue
            if self.is_request_approved(req.request_id):
                return True
        return False
