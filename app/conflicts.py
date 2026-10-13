"""Field-level conflict detection and policy-based resolution.

A conflict is raised only when two *different* sources report *different* values
for the *same field* of the *same business* within the detection window.

* Copies of one broadcast (same ``update_id``) never conflict with each other.
* Updates from the same source never conflict with each other (a later update
  from the same source simply supersedes the earlier one).
* Values are compared after normalisation (case, spacing and punctuation are
  ignored), so formatting variants such as "No. 12, MG Road" and "NO 12 MG ROAD"
  are not raised as conflicts.

Detection must be run on payloads expressed in one shared vocabulary (the SWS
field names); the caller translates department payloads before calling
``detect_conflicts``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import asyncpg


class ConflictPolicy(str, Enum):
    SWS_WINS = "sws_wins"  # the SWS value is kept, the department value is discarded
    LAST_WRITE_WINS = "last_write_wins"  # the value with the later timestamp is kept
    MANUAL_REVIEW = "manual_review"  # delivery is held until an operator decides


# Fields whose wrong value has legal or financial consequences go to a person.
HIGH_STAKES_FIELDS = {"pan", "signatory_pan", "gstin", "signatory_name"}
LOW_RISK_FIELDS = {"employee_count", "nature_of_business"}
DEFAULT_POLICY = ConflictPolicy.SWS_WINS


def policy_for_field(field_name: str) -> ConflictPolicy:
    if field_name in HIGH_STAKES_FIELDS:
        return ConflictPolicy.MANUAL_REVIEW
    if field_name in LOW_RISK_FIELDS:
        return ConflictPolicy.LAST_WRITE_WINS
    return DEFAULT_POLICY


_NON_ALNUM = re.compile(r"[^0-9a-z]+")


def normalise_value(value: Any) -> str:
    """Case-, space- and punctuation-insensitive form used for comparison."""
    if value is None:
        return ""
    return _NON_ALNUM.sub("", str(value).lower())


@dataclass
class _Observation:
    timestamp: float
    source: str
    value: Any
    normalised: str
    update_id: str | None


@dataclass
class FieldConflict:
    ubid: str
    field_name: str
    policy: ConflictPolicy
    current_source: str
    current_value: Any
    current_timestamp: float
    other_source: str
    other_value: Any
    other_timestamp: float
    other_update_id: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def sws_value(self) -> Any:
        if self.current_source == "SWS":
            return self.current_value
        if self.other_source == "SWS":
            return self.other_value
        return self.other_value

    @property
    def dept_value(self) -> Any:
        if self.current_source == "SWS":
            return self.other_value
        return self.current_value

    @property
    def dept_source(self) -> str:
        return self.other_source if self.current_source == "SWS" else self.current_source

    def automatic_winner_is_current(self) -> bool | None:
        """For automatic policies, whether the update being processed wins.

        Returns ``None`` for manual review (no automatic winner).
        """
        if self.policy == ConflictPolicy.MANUAL_REVIEW:
            return None
        if self.policy == ConflictPolicy.SWS_WINS:
            if self.current_source == "SWS":
                return True
            if self.other_source == "SWS":
                return False
            # Two departments disagree and neither is SWS: fall back to recency.
        return self.current_timestamp >= self.other_timestamp


class ConflictResolver:
    """In-memory detector over a sliding window, plus persistence of outcomes."""

    def __init__(self, pg_pool: asyncpg.pool.Pool | None, detection_window_seconds: int = 60):
        self.pg_pool = pg_pool
        self.detection_window = detection_window_seconds
        # (ubid, field) -> latest observation per source
        self._recent: dict[tuple[str, str], dict[str, _Observation]] = {}

    def _prune(self, key: tuple[str, str], now: float) -> dict[str, _Observation]:
        kept = {src: o for src, o in self._recent.get(key, {}).items() if abs(o.timestamp - now) <= self.detection_window}
        self._recent[key] = kept
        return kept

    def discard(self, ubid: str, field_name: str, source: str | None = None) -> None:
        """Forget observations once a conflict is decided (one source, or all sources)."""
        key = (ubid, field_name)
        if source is None:
            self._recent.pop(key, None)
        else:
            self._recent.get(key, {}).pop(source, None)

    def detect_conflicts(
        self,
        ubid: str,
        source: str,
        payload: dict[str, Any],
        timestamp: float,
        update_id: str | None = None,
    ) -> list[FieldConflict]:
        """Register this update and return one entry per conflicting field."""
        conflicts: list[FieldConflict] = []
        for field_name, value in payload.items():
            key = (ubid, field_name)
            observations = self._prune(key, timestamp)
            norm = normalise_value(value)
            for obs in observations.values():
                if update_id is not None and obs.update_id == update_id:
                    continue  # another copy of the same broadcast
                if obs.source == source:
                    continue  # same source: a newer value supersedes, not a conflict
                if obs.normalised == norm:
                    continue  # same value, possibly formatted differently
                conflicts.append(
                    FieldConflict(
                        ubid=ubid,
                        field_name=field_name,
                        policy=policy_for_field(field_name),
                        current_source=source,
                        current_value=value,
                        current_timestamp=timestamp,
                        other_source=obs.source,
                        other_value=obs.value,
                        other_timestamp=obs.timestamp,
                        other_update_id=obs.update_id,
                    )
                )
                break  # one conflict per field is enough
            observations[source] = _Observation(timestamp, source, value, norm, update_id)
        return conflicts

    async def detect_conflict(self, ubid, source, payload, timestamp, update_id=None):
        """Backward-compatible wrapper returning the first conflict as a dict, or None."""
        found = self.detect_conflicts(ubid, source, payload, timestamp, update_id)
        if not found:
            return None
        c = found[0]
        return {
            "ubid": c.ubid,
            "field": c.field_name,
            "source1": c.current_source,
            "source2": c.other_source,
            "timestamp1": c.current_timestamp,
            "timestamp2": c.other_timestamp,
            "payload1": {c.field_name: c.current_value},
            "payload2": {c.field_name: c.other_value},
        }

    async def resolve_conflict(self, conflict: dict[str, Any], policy: ConflictPolicy = ConflictPolicy.SWS_WINS) -> dict[str, Any]:
        """Resolve a conflict dict (as returned by ``detect_conflict``) and log it."""
        source1, source2 = conflict["source1"], conflict["source2"]
        ts1, ts2 = conflict["timestamp1"], conflict["timestamp2"]
        payload1, payload2 = conflict["payload1"], conflict["payload2"]

        if policy == ConflictPolicy.SWS_WINS:
            if source1 == "SWS":
                winner, reason = payload1, f"{source1} (SWS) wins"
            elif source2 == "SWS":
                winner, reason = payload2, f"{source2} (SWS) wins"
            else:
                winner = payload1 if ts1 >= ts2 else payload2
                reason = "Neither source is SWS; later write wins"
        elif policy == ConflictPolicy.LAST_WRITE_WINS:
            winner = payload1 if ts1 >= ts2 else payload2
            reason = f"Last write at {max(ts1, ts2)} wins"
        else:
            winner, reason = None, "Manual review required"

        if self.pg_pool is not None:
            async with self.pg_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO conflict_log(ubid, source1, source2, policy, resolution, created_at) "
                    "VALUES ($1, $2, $3, $4, $5, now())",
                    conflict["ubid"], source1, source2, policy.value, reason,
                )

        return {
            "ubid": conflict["ubid"],
            "policy": policy.value,
            "reason": reason,
            "winning_payload": winner,
            "source1": source1,
            "source2": source2,
        }
