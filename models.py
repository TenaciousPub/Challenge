from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Optional


@dataclass(slots=True)
class Participant:
    discord_id: str
    discord_tag: str
    display_name: str
    gender: Optional[str] = None  # "male" | "female" | None
    is_disabled: bool = False
    timezone: str = "America/Los_Angeles"
    joined_on: Optional[date] = None
    last_punished_on: Optional[str] = None  # YYYY-MM-DD in participant tz
    last_congrats_on: Optional[str] = None  # YYYY-MM-DD in participant tz
    preferred_challenge_id: Optional[str] = None


@dataclass(slots=True)
class Challenge:
    challenge_id: str
    discord_id: str
    challenge_type: str           # e.g. "pushups", "plank", "steps", "custom"
    daily_target: int             # numeric target
    unit: str = "reps"            # reps | seconds | minutes | steps | etc
    active: bool = True
    created_at: Optional[datetime] = None


@dataclass(slots=True)
class DailyLogEntry:
    log_date: date
    discord_id: str
    pushup_count: int             # kept for backwards compat; treat as amount
    workout_bonus: Optional[int] = None
    penalized: bool = False
    notes: Optional[str] = None
    logged_at: Optional[datetime] = None
    challenge_id: Optional[str] = None

    @property
    def amount(self) -> int:
        return int(self.pushup_count)


@dataclass(slots=True)
class Workout:
    id: str
    description: str
    category: str = "standard"     # e.g., "floor", "chair", "standard"
    difficulty: int = 1
    source: str = "sheet"


@dataclass(slots=True)
class ComplianceResult:
    participant: Participant
    logged_total: int
    pushup_target: int
    compliant: bool
    assigned_workout: Optional[Workout] = None


@dataclass(slots=True)
class DayOffVote:
    request_id: str
    request_date: date
    requested_by: str
    deadline: datetime
    participant_id: str
    vote: str = "pending"  # "yes" | "no" | "pending"
    voted_at: Optional[datetime] = None


@dataclass(slots=True)
class DayOffRequest:
    request_id: str
    target_day: date
    request_date: date
    requested_by: str
    deadline: datetime
    votes: Dict[str, DayOffVote]
    reason: Optional[str] = None
    message_id: Optional[int] = None  # Discord message ID for reaction voting
    results_posted: bool = False  # Track if results have been posted to avoid duplicates
