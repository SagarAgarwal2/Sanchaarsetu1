"""Field matching and payload transformation between system vocabularies.

* Each field is described by its name, description, data type and a few
  sample values (table ``schema_fields``). Sample values are scrubbed of PII
  before any model sees them.
* Candidates are scored with a multilingual sentence-embedding model, loaded
  once per process. If the model cannot be loaded, a lexical similarity is
  used instead and ``MATCHER_BACKEND`` says so.
* Confirmed or auto-applied mappings are stored in ``schema_mappings`` and take
  precedence over the model for later events.
* A field with no usable mapping is never dropped silently: it is returned as
  unmapped and recorded in ``unmapped_fields`` for an operator.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Literal

import asyncpg

log = logging.getLogger("sanchaarsetu.transform")

AUTO_MAP_THRESHOLD = float(os.getenv("AUTO_MAP_THRESHOLD", "0.85"))
REVIEW_THRESHOLD = float(os.getenv("REVIEW_THRESHOLD", "0.50"))
EMBED_MODEL_NAME = os.getenv("EMBED_MODEL", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

Direction = Literal["sws_to_dept", "dept_to_sws"]
_no_match_cache: dict[tuple, tuple[float, float]] = {}

# ---------------------------------------------------------------------------
# PII scrubbing
# ---------------------------------------------------------------------------

_PII_PATTERNS = [
    (re.compile(r"[\w.\-]+@[\w.\-]+\.\w+"), "[EMAIL]"),
    (re.compile(r"\b\d{4}\s?\d{4}\s?\d{4}\b"), "[AADHAAR]"),
    (re.compile(r"\b[A-Z]{5}[0-9]{4}[A-Z]\b", re.I), "[PAN]"),
    (re.compile(r"\b\d{2}[A-Z]{5}\d{4}[A-Z][A-Z0-9]Z[A-Z0-9]\b", re.I), "[GSTIN]"),
    (re.compile(r"(?:\+91[\s-]?)?\b[6-9]\d{9}\b"), "[PHONE]"),
]


def scrub_pii(text: Any) -> Any:
    """Mask email, Aadhaar, GSTIN, PAN and phone patterns."""
    if not isinstance(text, str):
        return text
    # GSTIN contains a PAN, so mask it before the PAN pattern runs.
    ordered = [_PII_PATTERNS[0], _PII_PATTERNS[1], _PII_PATTERNS[3], _PII_PATTERNS[2], _PII_PATTERNS[4]]
    for pattern, token in ordered:
        text = pattern.sub(token, text)
    return text


# ---------------------------------------------------------------------------
# Field descriptions and scoring
# ---------------------------------------------------------------------------


@dataclass
class FieldInfo:
    name: str
    description: str | None = None
    data_type: str | None = None
    samples: list[str] | None = None

    def as_text(self, use_metadata: bool = True) -> str:
        words = re.sub(r"[_\-]+", " ", self.name).strip()
        if not use_metadata:
            return words
        parts = [words]
        if self.description:
            parts.append(self.description)
        if self.data_type:
            parts.append(f"type {self.data_type}")
        if self.samples:
            cleaned = [str(scrub_pii(s)) for s in self.samples[:3] if s not in (None, "")]
            if cleaned:
                parts.append("examples: " + "; ".join(cleaned))
        return ". ".join(parts)


_model = None
_model_lock = threading.Lock()
_model_failed = False
MATCHER_BACKEND = "uninitialised"


def get_model():
    """Load the sentence-embedding model once per process; None if unavailable."""
    global _model, _model_failed, MATCHER_BACKEND
    if _model is not None or _model_failed:
        return _model
    with _model_lock:
        if _model is not None or _model_failed:
            return _model
        try:
            from sentence_transformers import SentenceTransformer

            _model = SentenceTransformer(EMBED_MODEL_NAME)
            MATCHER_BACKEND = f"embedding:{EMBED_MODEL_NAME}"
            log.info("Loaded embedding model %s", EMBED_MODEL_NAME)
        except Exception as exc:  # model not installed or not downloadable
            _model_failed = True
            MATCHER_BACKEND = "lexical"
            log.warning("Embedding model unavailable (%s); using lexical similarity", exc)
    return _model


def lexical_scores(source: str, targets: list[str]) -> list[float]:
    s = source.lower()
    return [SequenceMatcher(None, s, t.lower()).ratio() for t in targets]


def embedding_scores(source: str, targets: list[str]) -> list[float] | None:
    model = get_model()
    if model is None:
        return None
    import numpy as np

    emb = np.asarray(model.encode([source] + targets, normalize_embeddings=True))
    return [float(x) for x in emb[1:] @ emb[0]]


def score_candidates(
    source: FieldInfo,
    targets: list[FieldInfo],
    method: Literal["lexical", "embedding_name", "embedding_full"] = "embedding_full",
) -> list[float]:
    """Similarity between one source field and each target field."""
    if method == "lexical":
        return lexical_scores(source.name, [t.name for t in targets])
    use_meta = method == "embedding_full"
    scores = embedding_scores(source.as_text(use_meta), [t.as_text(use_meta) for t in targets])
    if scores is None:
        return lexical_scores(source.name, [t.name for t in targets])
    return scores


def best_match(source: FieldInfo, targets: list[FieldInfo], method="embedding_full") -> tuple[str | None, float]:
    if not targets:
        return None, 0.0
    scores = score_candidates(source, targets, method)
    i = max(range(len(scores)), key=scores.__getitem__)
    return targets[i].name, float(scores[i])


# ---------------------------------------------------------------------------
# Database-backed registry
# ---------------------------------------------------------------------------


async def load_fields(conn: asyncpg.Connection, system_code: str) -> list[FieldInfo]:
    rows = await conn.fetch(
        "SELECT field_name, description, data_type, sample_values FROM schema_fields WHERE system_code = $1 ORDER BY field_name",
        system_code,
    )
    return [FieldInfo(r["field_name"], r["description"], r["data_type"], list(r["sample_values"] or [])) for r in rows]


async def resolve_department(conn: asyncpg.Connection, department: str) -> asyncpg.Record | None:
    return await conn.fetchrow("SELECT id, code FROM departments WHERE code = $1 OR domain = $1", department)


async def find_or_create_mapping(
    pg_pool: asyncpg.pool.Pool,
    department_code: str,
    source_field: str,
    target_fields: list[str] | None = None,
    direction: Direction = "sws_to_dept",
) -> tuple[str | None, float, str]:
    """Return (target_field or None, confidence, status).

    status is one of: confirmed, auto_mapped, pending_review, no_match.
    A target is returned only for confirmed or auto-applied mappings.
    """
    async with pg_pool.acquire() as conn:
        dept = await resolve_department(conn, department_code)
        if not dept:
            return None, 0.0, "no_match"
        dept_id, dept_code = dept["id"], dept["code"]

        if direction == "sws_to_dept":
            src_col, tgt_col = "sws_field", "dept_field"
            src_system, tgt_system = "SWS", dept_code
        else:
            src_col, tgt_col = "dept_field", "sws_field"
            src_system, tgt_system = dept_code, "SWS"

        existing = await conn.fetchrow(
            f"""
            SELECT {tgt_col} AS target, confidence_score, status FROM schema_mappings
            WHERE department_id = $1 AND {src_col} = $2 AND status IN ('confirmed','auto_mapped','pending_review','rejected')
            ORDER BY (status = 'confirmed') DESC, version DESC, confidence_score DESC, created_at DESC
            LIMIT 1
            """,
            dept_id,
            source_field,
        )
        if existing:
            status = existing["status"]
            conf = float(existing["confidence_score"] or 0.0)
            if status in ("confirmed", "auto_mapped"):
                return existing["target"], conf, status
            return None, conf, status  # pending_review or rejected: not applied

        cache_key = (dept_code, direction, source_field)
        cached = _no_match_cache.get(cache_key)
        if cached and cached[0] > time.time():
            return None, cached[1], "no_match"

        targets = await load_fields(conn, tgt_system)
        if not targets and target_fields:
            targets = [FieldInfo(t) for t in target_fields]
        if not targets:
            return None, 0.0, "no_match"
        source_rows = await load_fields(conn, src_system)
        source_info = next((f for f in source_rows if f.name == source_field), FieldInfo(source_field))

        target, score = best_match(source_info, targets)
        if target is None or score < REVIEW_THRESHOLD:
            _no_match_cache[cache_key] = (time.time() + 300, score)
            return None, score, "no_match"
        status = "auto_mapped" if score >= AUTO_MAP_THRESHOLD else "pending_review"
        sws_field, dept_field = (source_field, target) if direction == "sws_to_dept" else (target, source_field)
        await conn.execute(
            """
            INSERT INTO schema_mappings(department_id, sws_field, dept_field, confidence_score, status, version)
            VALUES ($1,$2,$3,$4,$5,1)
            ON CONFLICT DO NOTHING
            """,
            dept_id, sws_field, dept_field, round(float(score), 3), status,
        )
    return (target if status == "auto_mapped" else None), float(score), status


async def record_unmapped(
    pg_pool: asyncpg.pool.Pool,
    department: str,
    direction: Direction,
    ubid: str | None,
    fields: list[tuple[str, str]],
    propagation_event_id: str | None = None,
) -> None:
    """Store (field, reason) pairs that could not be translated. Values are not stored."""
    if not fields:
        return
    async with pg_pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO unmapped_fields(department, direction, field_name, reason, ubid, propagation_event_id)
            VALUES ($1,$2,$3,$4,$5,$6)
            """,
            [(department, direction, f, reason, ubid, propagation_event_id) for f, reason in fields],
        )


async def transform_payload(
    pg_pool: asyncpg.pool.Pool,
    department: str,
    source_payload: dict[str, Any],
    target_schema: list[str] | None = None,
    direction: Direction = "sws_to_dept",
    ubid: str | None = None,
    propagation_event_id: str | None = None,
    record: bool = True,
) -> tuple[dict[str, Any], list[tuple[str, str]]]:
    """Translate a payload into the destination vocabulary.

    Returns (transformed_payload, unmapped) where unmapped lists (field, status)
    for every source field that was not translated. Unmapped fields are recorded
    for review unless ``record`` is False.
    """
    transformed: dict[str, Any] = {}
    unmapped: list[tuple[str, str]] = []
    for src_field, value in source_payload.items():
        target, _conf, status = await find_or_create_mapping(pg_pool, department, src_field, target_schema, direction)
        if target:
            transformed[target] = value
        else:
            unmapped.append((src_field, status))
    if record and unmapped:
        await record_unmapped(pg_pool, department, direction, ubid, unmapped, propagation_event_id)
    return transformed, unmapped
