from __future__ import annotations

import random
from typing import List, Optional

from .models import Workout
from .sheets import GoogleSheetsService


class WorkoutCatalog:
    """Loads punishments from Sheets and provides helpers."""

    def __init__(self, sheets: GoogleSheetsService) -> None:
        self.sheets = sheets
        self._cache: List[Workout] = []

    def refresh(self) -> None:
        self._cache = self.sheets.fetch_workouts()

    def all(self) -> List[Workout]:
        if not self._cache:
            self.refresh()
        return list(self._cache)

    def random(self) -> Optional[Workout]:
        items = self.all()
        return random.choice(items) if items else None

    def random_floor_or_chair(self) -> Optional[Workout]:
        items = [w for w in self.all() if (w.category or "").lower() in ("floor", "chair")]
        return random.choice(items) if items else None
