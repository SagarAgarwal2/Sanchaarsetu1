import asyncio
import hashlib
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime
from typing import Any

import asyncpg
import httpx
import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import Depends, FastAPI, HTTPException, Request, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

from app import transform as transform_mod
from app.conflicts import ConflictPolicy, ConflictResolver, FieldConflict
from app.detection import ChangeDetector
from app.transform import transform_payload

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("sanchaarsetu")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/sanchaar")
SELF_BASE_URL = os.getenv("SELF_BASE_URL", "http://127.0.0.1:8000")
TOPIC = "sanchaar-events"
DLQ_TOPIC = "sanchaar-dlq"
# Seconds between delivery attempts; len() = number of retries before the dead-letter store.
RETRY_DELAYS = [float(x) for x in os.getenv("RETRY_DELAYS", "2,4,8,16").split(",")]
IDEMPOTENCY_TTL_SECONDS = int(os.getenv("IDEMPOTENCY_TTL_SECONDS", "3600"))
RATE_LIMIT_PER_SECOND = int(os.getenv("RATE_LIMIT_PER_SECOND", "50"))
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "sws-secret-key")


class SWSWebhook(BaseModel):
    ubid: str
    event_type: str
    payload: dict
    timestamp: float | None = None


class ConflictResolveBody(BaseModel):
    winning_value: str | None = None
    resolved_by: str | None = None
    resolution_reason: str | None = None


class MappingCreateBody(BaseModel):
    department_id: str
    sws_field: str
    dept_field: str
    confidence_score: float = 0.0


class MappingUpdateBody(BaseModel):
    status: str | None = None
    reviewed_by: str | None = None
    reviewed_at: datetime | None = None
    confidence_score: float | None = None


class DepartmentUpdateBody(BaseModel):
    status: str


class SimulateBody(BaseModel):
    count: int = 8


class ReplayEventBody(BaseModel):
    ubid: str
    event_type: str
    source_system: str
    destination_system: str
    payload_hash: str | None = None
    direction: str = 'sws_to_dept'
    payload: dict | None = None


class DeptWebhook(BaseModel):
    # A department identifies the business by its own record id; ubid is optional.
    ubid: str | None = None
    dept_record_id: str | None = None
    event_type: str
    payload: dict
    timestamp: float | None = None


class DeliveryBody(BaseModel):
    ubid: str
    payload: dict
    idempotency: str | None = None
    dept_record_id: str | None = None


class SimulateChangeBody(BaseModel):
    department: str = "factories"  # domain or code
    ubid: str
    event_type: str = "address_update"
    payload: dict


class FailureModeBody(BaseModel):
    department: str  # code or domain
    offline_seconds: float = 0.0
    reject_probability: float = 0.0


app = FastAPI(title="SanchaarSetu")

# Allow simple frontend development (restrict in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.state.kafka_producer = None
app.state.kafka_consumer = None
app.state.dlq_consumer = None
app.state.redis = None
app.state.pg_pool = None
app.state.http = None
app.state.consumer_task = None
app.state.dlq_consumer_task = None
app.state.change_ingest_task = None
app.state.conflict_resolver = None
app.state.change_detector = None
app.state.dest_queues = {}
app.state.dest_workers = {}
app.state.schemas = {}
app.state.dept_aliases = {}
app.state.sim_failures = {}
app.state.audit_lock = asyncio.Lock()
app.state.auto_resolved = set()


# ------------------------
# Keys and hashing
# ------------------------

def payload_hash(payload: dict) -> str:
    s = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(s.encode()).hexdigest()


def make_update_key(ubid: str, event_type: str, payload: dict) -> str:
    """Identity of one logical update: same business, type and content."""
    raw = f"{ubid}:{event_type}:{payload_hash(payload)}"
    return hashlib.sha256(raw.encode()).hexdigest()


def derive_idempotency_key(base: str, destination: str, direction: str) -> str:
    raw = f"{base}:{direction}:{destination}"
    return hashlib.sha256(raw.encode()).hexdigest()


def make_unique_idempotency_key(ubid: str, event_type: str) -> str:
    raw = f"{ubid}:{event_type}:{uuid.uuid4().hex}"
    return hashlib.sha256(raw.encode()).hexdigest()


def make_change_event_idempotency_key(change_event_id: int, ubid: str, event_type: str) -> str:
    raw = f"change_event:{change_event_id}:{ubid}:{event_type}"
    return hashlib.sha256(raw.encode()).hexdigest()


# ------------------------
# Database helpers
# ------------------------

async def fetch_rows(pg_pool, sql: str, *args):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]


async def fetch_row(pg_pool, sql: str, *args):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(sql, *args)
        return dict(row) if row else None


async def count_rows(pg_pool, table: str, where_sql: str = "", *args):
    sql = f"SELECT COUNT(*) AS count FROM {table} {where_sql}"
    row = await fetch_row(pg_pool, sql, *args)
    return int(row["count"]) if row else 0


AUDIT_LOCK_ID = 7314  # pg advisory lock serialising audit appends


def _audit_digest(prev_hash, ubid, event_type, source, destination, p_hash, idemp_key, outcome) -> str:
    chain_data = f"{prev_hash}{ubid}{event_type}{source}{destination}{p_hash}{idemp_key}{outcome}"
    return hashlib.sha256(chain_data.encode()).hexdigest()


async def write_audit(pg_pool, ubid, event_type, source, destination, p_hash, idemp_key, outcome):
    """Append to the hash-chained audit log. Appends are serialised so the chain stays linear."""
    async with pg_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1)", AUDIT_LOCK_ID)
            row = await conn.fetchrow("SELECT chained_hash FROM audit ORDER BY id DESC LIMIT 1")
            prev_hash = row["chained_hash"] if row and row["chained_hash"] else "GENESIS_HASH"
            chained = _audit_digest(prev_hash, ubid, event_type, source, destination, p_hash, idemp_key, outcome)
            await conn.execute(
                """
                INSERT INTO audit(ubid, event_type, source_system, destination_system,
                                  payload_hash, idempotency_key, outcome, previous_hash, chained_hash)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                """,
                ubid, event_type, source, destination, p_hash, idemp_key, outcome, prev_hash, chained,
            )


async def verify_audit_chain(pg_pool) -> dict[str, Any]:
    prev = "GENESIS_HASH"
    checked = 0
    async with pg_pool.acquire() as conn:
        async with conn.transaction():
            async for r in conn.cursor("SELECT * FROM audit ORDER BY id ASC"):
                expected = _audit_digest(prev, r["ubid"], r["event_type"], r["source_system"], r["destination_system"],
                                         r["payload_hash"], r["idempotency_key"], r["outcome"])
                if r["previous_hash"] != prev or r["chained_hash"] != expected:
                    return {"ok": False, "checked": checked, "first_bad_id": r["id"]}
                prev = r["chained_hash"]
                checked += 1
    return {"ok": True, "checked": checked, "first_bad_id": None}


async def ensure_db_schema(pg_pool: asyncpg.pool.Pool) -> None:
    """Columns added after the first release (safe no-ops on a fresh database)."""
    async with pg_pool.acquire() as conn:
        await conn.execute("ALTER TABLE propagation_events ADD COLUMN IF NOT EXISTS payload JSONB")
        await conn.execute("ALTER TABLE propagation_events ADD COLUMN IF NOT EXISTS held_by_conflict UUID")
        await conn.execute("ALTER TABLE propagation_events ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMPTZ")
        await conn.execute("ALTER TABLE change_events ADD COLUMN IF NOT EXISTS processed_at TIMESTAMP")


async def load_registry(pg_pool) -> None:
    """Field vocabularies and department name aliases, keyed by code and domain."""
    schemas: dict[str, set[str]] = {}
    aliases: dict[str, str] = {"SWS": "SWS"}
    for r in await fetch_rows(pg_pool, "SELECT system_code, field_name FROM schema_fields"):
        schemas.setdefault(r["system_code"], set()).add(r["field_name"])
    for d in await fetch_rows(pg_pool, "SELECT code, domain FROM departments"):
        aliases[d["code"]] = d["code"]
        aliases[d["domain"]] = d["code"]
        if d["code"] in schemas:
            schemas[d["domain"]] = schemas[d["code"]]
    app.state.schemas = schemas
    app.state.dept_aliases = aliases


def dept_code_for(name: str | None) -> str | None:
    if not name:
        return None
    return app.state.dept_aliases.get(name)


async def create_propagation_event(pg_pool: asyncpg.pool.Pool, *,
                                   ubid: str,
                                   event_type: str,
                                   source_system: str,
                                   destination_system: str,
                                   direction: str,
                                   idempotency_key: str,
                                   payload: dict,
                                   outcome: str = "pending",
                                   conflict_flag: bool = False,
                                   resolution_applied: str | None = None,
                                   retry_count: int = 0,
                                   error_message: str | None = None,
                                   propagation_ms: int | None = None,
                                   ) -> dict[str, Any]:
    row = await fetch_row(
        pg_pool,
        """
        INSERT INTO propagation_events(
          ubid,event_type,source_system,destination_system,payload_hash,idempotency_key,
          outcome,direction,conflict_flag,resolution_applied,retry_count,error_message,propagation_ms,payload,accepted_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,now())
        RETURNING *
        """,
        ubid, event_type, source_system, destination_system, payload_hash(payload), idempotency_key,
        outcome, direction, conflict_flag, resolution_applied, retry_count, error_message, propagation_ms,
        json.dumps(payload),
    )
    return row


_UNSET = object()


async def update_propagation_event(pg_pool: asyncpg.pool.Pool, event_id: str, *,
                                   outcome: str | None = None,
                                   retry_count: int | None = None,
                                   error_message: Any = _UNSET,
                                   propagation_ms: int | None = None,
                                   conflict_flag: bool | None = None,
                                   resolution_applied: str | None = None,
                                   payload: dict | None = None,
                                   held_by_conflict: Any = _UNSET,
                                   ) -> None:
    sets = []
    args: list[Any] = []

    def add(col, val, cast=""):
        args.append(val)
        sets.append(f"{col} = ${len(args)}{cast}")

    if outcome is not None:
        add("outcome", outcome)
    if retry_count is not None:
        add("retry_count", retry_count)
    if error_message is not _UNSET:
        add("error_message", error_message)  # None clears it
    if propagation_ms is not None:
        add("propagation_ms", propagation_ms)
    if conflict_flag is not None:
        add("conflict_flag", conflict_flag)
    if resolution_applied is not None:
        add("resolution_applied", resolution_applied)
    if held_by_conflict is not _UNSET:
        add("held_by_conflict", held_by_conflict, "::uuid")
    if payload is not None:
        add("payload", json.dumps(payload), "::jsonb")
        add("payload_hash", payload_hash(payload))
    if not sets:
        return
    args.append(event_id)
    sql = f"UPDATE propagation_events SET {', '.join(sets)} WHERE id = ${len(args)}"
    async with pg_pool.acquire() as conn:
        await conn.execute(sql, *args)


# ------------------------
# Publishing (ingestion side)
# ------------------------

class Duplicate(Exception):
    pass


async def publish_event(*, ubid: str, event_type: str, payload: dict, source_system: str,
                        destination_system: str, direction: str, idempotency_key: str,
                        update_id: str, timestamp: float, extra: dict | None = None) -> dict:
    """Reserve the key, record the event, publish it; undo the reservation if publishing fails."""
    r = app.state.redis
    pg = app.state.pg_pool
    reserved = await r.set(f"idem:{idempotency_key}", "pending", ex=60, nx=True)
    if not reserved:
        raise Duplicate(idempotency_key)
    try:
        pe = await create_propagation_event(
            pg, ubid=ubid, event_type=event_type, source_system=source_system,
            destination_system=destination_system, direction=direction,
            idempotency_key=idempotency_key, payload=payload, outcome="pending",
        )
    except asyncpg.UniqueViolationError:
        await r.set(f"idem:{idempotency_key}", "done", ex=IDEMPOTENCY_TTL_SECONDS)
        raise Duplicate(idempotency_key)
    except Exception:
        await r.delete(f"idem:{idempotency_key}")
        raise
    message = {
        "propagation_event_id": str(pe["id"]),
        "ubid": ubid,
        "event_type": event_type,
        "payload": payload,
        "idempotency_key": idempotency_key,
        "update_id": update_id,
        "timestamp": timestamp,
        "accepted_at": time.time(),
        "source_system": source_system,
        "destination_system": destination_system,
        "direction": direction,
        "retry_count": 0,
    }
    if extra:
        message.update(extra)
    try:
        await app.state.kafka_producer.send_and_wait(TOPIC, json.dumps(message).encode())
    except Exception:
        # Publishing failed: remove the record and the reservation so the sender can retry.
        async with pg.acquire() as conn:
            await conn.execute("DELETE FROM propagation_events WHERE id = $1", pe["id"])
        await r.delete(f"idem:{idempotency_key}")
        raise
    await r.set(f"idem:{idempotency_key}", "done", ex=IDEMPOTENCY_TTL_SECONDS)
    return pe


async def resolve_ubid_from_dept_record(dept_code: str, dept_record_id: str) -> str | None:
    row = await fetch_row(
        app.state.pg_pool,
        """
        SELECT b.ubid FROM business_departments bd
        JOIN businesses b ON b.id = bd.business_id
        JOIN departments d ON d.id = bd.department_id
        WHERE d.code = $1 AND bd.dept_record_id = $2 AND bd.link_status = 'confirmed'
        """,
        dept_code, dept_record_id,
    )
    return row["ubid"] if row else None


async def lookup_dept_record_id(ubid: str, dept_code: str) -> str | None:
    row = await fetch_row(
        app.state.pg_pool,
        """
        SELECT bd.dept_record_id FROM business_departments bd
        JOIN businesses b ON b.id = bd.business_id
        JOIN departments d ON d.id = bd.department_id
        WHERE b.ubid = $1 AND d.code = $2 AND bd.link_status = 'confirmed'
        """,
        ubid, dept_code,
    )
    return row["dept_record_id"] if row else None


async def publish_dept_change(*, dept_code: str, ubid: str, event_type: str, dept_payload: dict,
                              change_event_id: int | None = None, timestamp: float | None = None) -> dict:
    ts = timestamp or time.time()
    base = (make_change_event_idempotency_key(change_event_id, ubid, event_type)
            if change_event_id is not None else make_update_key(ubid, event_type, dept_payload))
    idemp = derive_idempotency_key(base, "SWS", "dept_to_sws")
    try:
        pe = await publish_event(
            ubid=ubid, event_type=event_type, payload=dept_payload, source_system=dept_code,
            destination_system="SWS", direction="dept_to_sws", idempotency_key=idemp,
            update_id=base, timestamp=ts,
        )
    except Duplicate:
        return {"status": "duplicate", "idempotency_key": idemp}
    await write_audit(app.state.pg_pool, ubid, event_type, dept_code, "kafka", payload_hash(dept_payload), idemp, "queued")
    return {"status": "accepted", "propagation_event_id": str(pe["id"]), "idempotency_key": idemp}


async def change_event_ingest_loop():
    """Publish unprocessed rows of `change_events` (Tier 2/3 detections) into the pipeline."""
    pg_pool = app.state.pg_pool
    while True:
        try:
            rows = await fetch_rows(
                pg_pool,
                "SELECT id, department, event_type, payload FROM change_events WHERE processed_at IS NULL ORDER BY id ASC LIMIT 100",
            )
            if not rows:
                await asyncio.sleep(2)
                continue
            for ce in rows:
                ce_id = int(ce["id"])
                raw = ce.get("payload")
                try:
                    parsed = json.loads(raw) if isinstance(raw, str) else (raw or {})
                except Exception:
                    parsed = {}
                dept_payload = parsed.get("payload") if isinstance(parsed, dict) and "payload" in parsed else parsed
                event_type = parsed.get("event_type") or ce.get("event_type") or "snapshot_diff"
                dept_code = dept_code_for(ce.get("department"))
                ubid = parsed.get("ubid")
                if not ubid and dept_code and parsed.get("dept_record_id"):
                    ubid = await resolve_ubid_from_dept_record(dept_code, parsed["dept_record_id"])
                if dept_code and ubid and isinstance(dept_payload, dict):
                    dept_payload = {k: v for k, v in dept_payload.items() if k not in ("ubid", "dept_record_id")}
                    await publish_dept_change(dept_code=dept_code, ubid=ubid, event_type=event_type,
                                              dept_payload=dept_payload, change_event_id=ce_id)
                else:
                    log.warning("change_event %s skipped: department or business not resolved", ce_id)
                async with pg_pool.acquire() as conn:
                    await conn.execute("UPDATE change_events SET processed_at = now() WHERE id = $1", ce_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Change ingest error: %s", e)
            await asyncio.sleep(2)


# ------------------------
# Conflict handling (consumer side)
# ------------------------

async def record_conflict(conflict: FieldConflict, propagation_event_id: str | None, *,
                          status: str, winning_value: Any = None, resolved_by: str | None = None,
                          reason: str | None = None) -> dict:
    pg = app.state.pg_pool
    dept_row = await fetch_row(pg, "SELECT id FROM departments WHERE code = $1", conflict.dept_source)
    if status != "resolved":
        existing = await fetch_row(
            pg,
            "SELECT * FROM conflicts WHERE ubid = $1 AND field_name = $2 AND status IN ('open','pending_review') ORDER BY created_at DESC LIMIT 1",
            conflict.ubid, conflict.field_name,
        )
        if existing:
            return existing
    row = await fetch_row(
        pg,
        """
        INSERT INTO conflicts(ubid, field_name, sws_value, dept_value, source_department_id, resolution_policy,
                              status, winning_value, resolved_by, resolved_at, propagation_event_id, resolution_reason)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, CASE WHEN $7 = 'resolved' THEN now() END, $10, $11)
        RETURNING *
        """,
        conflict.ubid, conflict.field_name,
        None if conflict.sws_value is None else str(conflict.sws_value),
        None if conflict.dept_value is None else str(conflict.dept_value),
        dept_row["id"] if dept_row else None, conflict.policy.value, status,
        None if winning_value is None else str(winning_value), resolved_by,
        propagation_event_id, reason,
    )
    async with pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO conflict_log(ubid, source1, source2, policy, resolution, created_at) VALUES ($1,$2,$3,$4,$5,now())",
            conflict.ubid, conflict.current_source, conflict.other_source, conflict.policy.value,
            reason or status,
        )
    return row


async def apply_conflict_policies(message: dict, canonical: dict, conflicts: list[FieldConflict]) -> tuple[dict, str | None]:
    """Apply each field's policy. Returns (canonical payload to deliver, conflict id holding the event or None)."""
    pe_id = message.get("propagation_event_id")
    held_by = None
    for c in conflicts:
        if c.policy == ConflictPolicy.MANUAL_REVIEW:
            row = await record_conflict(c, pe_id, status="pending_review")
            held_by = str(row["id"])
            continue
        current_wins = c.automatic_winner_is_current()
        winner = c.current_value if current_wins else c.other_value
        key = (message.get("update_id"), c.field_name, c.other_update_id)
        if key in app.state.auto_resolved:  # another copy of the same broadcast already recorded it
            if not current_wins:
                canonical.pop(c.field_name, None)
            continue
        app.state.auto_resolved.add(key)
        await record_conflict(
            c, pe_id, status="resolved", winning_value=winner, resolved_by=f"system:{c.policy.value}",
            reason=f"{c.policy.value}: {'this update' if current_wins else c.other_source} kept for '{c.field_name}'",
        )
        # The decided loser's report no longer counts against later updates.
        app.state.conflict_resolver.discard(c.ubid, c.field_name, c.other_source if current_wins else c.current_source)
        if not current_wins:
            canonical.pop(c.field_name, None)  # this update loses the field; do not overwrite the winner
    return canonical, held_by


# ------------------------
# Delivery (consumer side)
# ------------------------

async def dispatch(message: dict) -> None:
    """Prepare one consumed event and hand it to its destination's ordered queue."""
    pg = app.state.pg_pool
    pe_id = message.get("propagation_event_id")
    ubid = message["ubid"]
    direction = message.get("direction", "sws_to_dept")
    source = message.get("source_system") or "SWS"
    dest = message.get("destination_system") or ""
    raw_payload = message.get("payload") or {}
    overrides = message.get("field_overrides") or {}

    # 1. Express the update in the shared (SWS) vocabulary.
    if direction == "dept_to_sws":
        canonical, unmapped = await transform_payload(
            pg, source, raw_payload, direction="dept_to_sws", ubid=ubid, propagation_event_id=pe_id)
    else:
        canonical, unmapped = dict(raw_payload), []
    canonical.update(overrides)

    # 2. Field-level conflict detection, skipped for events already resolved or replayed.
    if not message.get("resolution_applied"):
        conflicts = app.state.conflict_resolver.detect_conflicts(
            ubid, source, canonical, float(message.get("timestamp") or time.time()), message.get("update_id"))
        if conflicts:
            canonical, held_by = await apply_conflict_policies(message, canonical, conflicts)
            if held_by:
                if pe_id:
                    await update_propagation_event(pg, pe_id, outcome="conflict", conflict_flag=True,
                                                   error_message="held for manual review", held_by_conflict=held_by)
                return

    # 3. Translate into the destination vocabulary.
    if direction == "dept_to_sws":
        delivery_payload = canonical
    else:
        delivery_payload, unmapped = await transform_payload(
            pg, dest, canonical, direction="sws_to_dept", ubid=ubid, propagation_event_id=pe_id)

    if not delivery_payload:
        reason = "no mapped fields; sent to review" if unmapped else "all fields superseded by conflict policy"
        outcome = "held" if unmapped else "success"
        if pe_id:
            await update_propagation_event(pg, pe_id, outcome=outcome, error_message=reason)
        await write_audit(pg, ubid, message.get("event_type"), source, dest, payload_hash({}), message.get("idempotency_key"), outcome)
        return

    message["delivery_payload"] = delivery_payload
    message["unmapped_fields"] = [f for f, _ in unmapped]
    await enqueue_for_destination(dest, message)


async def enqueue_for_destination(dest: str, message: dict) -> None:
    q = app.state.dest_queues.get(dest)
    if q is None:
        q = asyncio.Queue()
        app.state.dest_queues[dest] = q
        app.state.dest_workers[dest] = asyncio.create_task(destination_worker(dest, q))
    await q.put(message)


async def deliver_once(message: dict) -> tuple[bool, str]:
    """One delivery attempt. Returns (success, outcome text)."""
    pg = app.state.pg_pool
    dest = message["destination_system"]
    ubid = message["ubid"]
    body = {"ubid": ubid, "payload": message["delivery_payload"], "idempotency": message["idempotency_key"]}
    if message.get("direction") == "dept_to_sws":
        url = f"{SELF_BASE_URL}/sws/update"
    else:
        body["dept_record_id"] = message.get("dept_record_id")
        url = f"{SELF_BASE_URL}/dept/{dest}/update"
    try:
        r = await app.state.http.post(url, json=body, headers={"X-API-Key": INTERNAL_API_KEY})
        return r.status_code == 200, ("delivered" if r.status_code == 200 else f"failed:{r.status_code}")
    except Exception as e:
        return False, f"error:{type(e).__name__}"


async def destination_worker(dest: str, q: asyncio.Queue) -> None:
    """Deliver events for one destination in order; retries block only this destination."""
    pg = app.state.pg_pool
    while True:
        message = await q.get()
        try:
            await deliver_with_retries(message)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Delivery worker %s error: %s", dest, e)
        finally:
            q.task_done()


async def deliver_with_retries(message: dict) -> None:
    pg = app.state.pg_pool
    pe_id = message.get("propagation_event_id")
    ubid = message["ubid"]
    dest = message["destination_system"]
    source = message.get("source_system") or "SWS"
    event_type = message.get("event_type")
    idemp = message.get("idempotency_key")
    p_hash = payload_hash(message["delivery_payload"])

    # Deliver only over a confirmed identifier link.
    if message.get("direction") != "dept_to_sws":
        record_id = await lookup_dept_record_id(ubid, dest)
        if not record_id:
            if pe_id:
                await update_propagation_event(pg, pe_id, outcome="held", error_message="no confirmed identifier link")
            await write_audit(pg, ubid, event_type, source, dest, p_hash, idemp, "held:no_identifier_link")
            return
        message["dept_record_id"] = record_id

    retry_count = int(message.get("retry_count", 0) or 0)
    while True:
        ok, outcome = await deliver_once(message)
        await write_audit(pg, ubid, event_type, source, dest, p_hash, idemp, outcome)
        if ok:
            accepted = float(message.get("accepted_at") or time.time())
            took_ms = int((time.time() - accepted) * 1000)
            if pe_id:
                await update_propagation_event(
                    pg, pe_id, outcome="success", propagation_ms=took_ms, retry_count=retry_count,
                    error_message=("unmapped fields sent to review: " + ", ".join(message["unmapped_fields"]))
                    if message.get("unmapped_fields") else None,
                )
            return
        if retry_count >= len(RETRY_DELAYS):
            message["retry_count"] = retry_count
            await app.state.kafka_producer.send_and_wait(DLQ_TOPIC, json.dumps(message).encode())
            await write_audit(pg, ubid, event_type, source, dest, p_hash, idemp, "dlq")
            if pe_id:
                await update_propagation_event(pg, pe_id, outcome="dlq", retry_count=retry_count, error_message=outcome)
            return
        delay = RETRY_DELAYS[retry_count]
        retry_count += 1
        if pe_id:
            await update_propagation_event(pg, pe_id, outcome="failure", retry_count=retry_count, error_message=outcome)
        await asyncio.sleep(delay)


async def kafka_consumer_loop():
    consumer = app.state.kafka_consumer
    async for msg in consumer:
        try:
            await dispatch(json.loads(msg.value.decode()))
        except Exception as e:
            log.exception("Consumer error: %s", e)


async def dlq_consumer_loop():
    """Store dead-lettered messages for manual review and replay."""
    pg_pool = app.state.pg_pool
    async for msg in app.state.dlq_consumer:
        try:
            payload = json.loads(msg.value.decode())
            async with pg_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO dlq_messages(propagation_event_id, ubid, event_type, source_system, destination_system, payload, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, now())
                    ON CONFLICT (propagation_event_id) DO UPDATE
                      SET payload = EXCLUDED.payload, created_at = now(), replayed_at = NULL
                    """,
                    payload.get("propagation_event_id"), payload.get("ubid", "unknown"), payload.get("event_type"),
                    payload.get("source_system"), payload.get("destination_system"), json.dumps(payload),
                )
        except Exception as e:
            log.exception("DLQ consumer error: %s", e)


async def requeue_unfinished() -> int:
    """After a restart, re-dispatch events that were accepted but not finished."""
    rows = await fetch_rows(
        app.state.pg_pool,
        """SELECT * FROM propagation_events
           WHERE outcome IN ('pending','failure') AND payload IS NOT NULL AND accepted_at IS NOT NULL
           ORDER BY created_at ASC""",
    )
    for pe in rows:
        payload = pe["payload"] if isinstance(pe["payload"], dict) else json.loads(pe["payload"])
        await dispatch({
            "propagation_event_id": str(pe["id"]), "ubid": pe["ubid"], "event_type": pe["event_type"],
            "payload": payload, "idempotency_key": pe["idempotency_key"], "update_id": None,
            "timestamp": pe["accepted_at"].timestamp(), "accepted_at": pe["accepted_at"].timestamp(),
            "source_system": pe["source_system"], "destination_system": pe["destination_system"],
            "direction": pe["direction"], "retry_count": 0, "resolution_applied": "recovered",
        })
    return len(rows)


# ------------------------
# Startup / Shutdown
# ------------------------

@app.on_event("startup")
async def startup():
    app.state.redis = redis.from_url(REDIS_URL)
    app.state.http = httpx.AsyncClient(timeout=10)

    for attempt in range(10):
        try:
            app.state.pg_pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=10)
            await ensure_db_schema(app.state.pg_pool)
            break
        except Exception as e:
            log.warning("Postgres not ready (attempt %s/10): %s", attempt + 1, e)
            if attempt == 9:
                raise
            await asyncio.sleep(min(2 ** attempt, 10))
    await load_registry(app.state.pg_pool)

    app.state.kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await app.state.kafka_producer.start()
    app.state.kafka_consumer = AIOKafkaConsumer(TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP, group_id="sanchaar-group")
    await app.state.kafka_consumer.start()
    app.state.dlq_consumer = AIOKafkaConsumer(DLQ_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP, group_id="sanchaar-dlq-group")
    await app.state.dlq_consumer.start()

    app.state.conflict_resolver = ConflictResolver(app.state.pg_pool)
    app.state.change_detector = ChangeDetector(app.state.pg_pool)

    # Load the embedding model once, off the event loop.
    asyncio.get_running_loop().run_in_executor(None, transform_mod.get_model)

    recovered = await requeue_unfinished()
    if recovered:
        log.info("Re-dispatched %s unfinished events", recovered)

    app.state.consumer_task = asyncio.create_task(kafka_consumer_loop())
    app.state.dlq_consumer_task = asyncio.create_task(dlq_consumer_loop())
    app.state.change_ingest_task = asyncio.create_task(change_event_ingest_loop())


@app.on_event("shutdown")
async def shutdown():
    for t in [app.state.consumer_task, app.state.dlq_consumer_task, app.state.change_ingest_task,
              *app.state.dest_workers.values()]:
        if t:
            t.cancel()
    for c in [app.state.kafka_consumer, app.state.dlq_consumer, app.state.kafka_producer]:
        if c:
            await c.stop()
    if app.state.http:
        await app.state.http.aclose()
    if app.state.pg_pool:
        await app.state.pg_pool.close()
    if app.state.redis:
        await app.state.redis.close()


# ------------------------
# Access control, rate limiting and schema validation
# ------------------------

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)

# Demo keys only; set API_KEYS_JSON in any real deployment.
API_KEYS: dict[str, str] = json.loads(os.getenv("API_KEYS_JSON", "null") or "null") or {
    "sws-secret-key": "SWS",
    "factories-secret-key": "DFB",
    "labour-secret-key": "DOL",
    "ctd-secret-key": "CTD",
    "sea-secret-key": "SEA",
    "bbmp-secret-key": "BBMP",
    "kpcb-secret-key": "KPCB",
    "kfes-secret-key": "KFES",
    "dis-secret-key": "DIS",
}


async def verify_rbac(req: Request, api_key: str = Security(api_key_header)):
    source_system = API_KEYS.get(api_key)
    if not source_system:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    path = req.url.path
    if path.startswith("/sws/") and source_system != "SWS":
        raise HTTPException(status_code=403, detail="Not authorized for SWS endpoints")
    if path.startswith("/dept/"):
        target = dept_code_for(path.split("/")[2])
        if source_system not in ("SWS", target):
            raise HTTPException(status_code=403, detail="Not authorized for this department")
    return source_system


async def rate_limit(req: Request, source_system: str = Depends(verify_rbac)):
    r = app.state.redis
    if not r:
        return source_system
    key = f"rate_limit:{source_system}:{int(time.time())}"
    count = await r.incr(key)
    if count == 1:
        await r.expire(key, 2)
    if count > RATE_LIMIT_PER_SECOND:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    return source_system


def validate_schema(payload: dict, system: str):
    expected = app.state.schemas.get(system)
    if not expected:
        return
    unexpected = set(payload) - expected
    if unexpected:
        raise HTTPException(status_code=400, detail=f"Schema validation failed. Unexpected fields: {sorted(unexpected)}")


def simulated_failure(dest_code: str) -> bool:
    mode = app.state.sim_failures.get(dest_code)
    if not mode:
        return False
    if time.time() < mode.get("offline_until", 0):
        return True
    return random.random() < mode.get("reject_probability", 0.0)


async def record_receipt(idempotency_key: str | None, destination: str, ubid: str) -> None:
    if not idempotency_key:
        return
    async with app.state.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO destination_receipts(idempotency_key, destination, ubid) VALUES ($1,$2,$3)
            ON CONFLICT (idempotency_key, destination) DO UPDATE SET times_received = destination_receipts.times_received + 1
            """,
            idempotency_key, destination, ubid,
        )


# ------------------------
# Ingestion and simulated destination routes
# ------------------------

@app.post("/sws/webhook")
async def sws_webhook(event: SWSWebhook, source: str = Depends(rate_limit)):
    validate_schema(event.payload, "SWS")
    ts = event.timestamp or time.time()
    base = make_update_key(event.ubid, event.event_type, event.payload)

    departments = await fetch_rows(
        app.state.pg_pool,
        "SELECT code FROM departments WHERE status IN ('active','degraded') ORDER BY ingestion_tier, code",
    )
    if not departments:
        raise HTTPException(status_code=400, detail="no departments configured")

    accepted, duplicates = [], 0
    for d in departments:
        idemp = derive_idempotency_key(base, d["code"], "sws_to_dept")
        try:
            pe = await publish_event(
                ubid=event.ubid, event_type=event.event_type, payload=event.payload, source_system="SWS",
                destination_system=d["code"], direction="sws_to_dept", idempotency_key=idemp,
                update_id=base, timestamp=ts,
            )
        except Duplicate:
            duplicates += 1
            continue
        accepted.append({"dept": d["code"], "propagation_event_id": str(pe["id"]), "idempotency_key": idemp})

    await write_audit(app.state.pg_pool, event.ubid, event.event_type, "SWS", "kafka",
                      payload_hash(event.payload), base, f"broadcast_queued:{len(accepted)}")
    return {
        "status": "accepted" if accepted else "duplicate",
        "broadcast": True,
        "targets": len(departments),
        "accepted": len(accepted),
        "duplicates": duplicates,
        "base_idempotency_key": base,
        "items": accepted,
    }


@app.post("/dept/{dept}/webhook")
async def dept_webhook(dept: str, event: DeptWebhook, source: str = Depends(rate_limit)):
    dept_code = dept_code_for(dept)
    if not dept_code or dept_code == "SWS":
        raise HTTPException(status_code=404, detail="unknown department")
    validate_schema(event.payload, dept_code)
    ubid = event.ubid
    if not ubid and event.dept_record_id:
        ubid = await resolve_ubid_from_dept_record(dept_code, event.dept_record_id)
    if not ubid:
        raise HTTPException(status_code=404, detail="business not linked: no confirmed identifier link for this record")
    return await publish_dept_change(dept_code=dept_code, ubid=ubid, event_type=event.event_type,
                                     dept_payload=event.payload, timestamp=event.timestamp)


@app.post("/dept/{dept}/update")
async def dept_update(dept: str, body: DeliveryBody, source: str = Depends(verify_rbac)):
    """Simulated department endpoint receiving SWS updates."""
    dept_code = dept_code_for(dept) or dept
    if simulated_failure(dept_code):
        raise HTTPException(status_code=503, detail="department unavailable (simulated)")
    validate_schema(body.payload or {}, dept_code)
    await record_receipt(body.idempotency, dept_code, body.ubid)
    await write_audit(app.state.pg_pool, body.ubid, "dept_write", dept_code, "SanchaarSetu",
                      payload_hash(body.payload or {}), body.idempotency, "accepted")
    return {"status": "ok", "dept": dept_code}


@app.post("/sws/update")
async def sws_update(body: DeliveryBody, source: str = Depends(verify_rbac)):
    """Simulated SWS endpoint receiving department updates."""
    if simulated_failure("SWS"):
        raise HTTPException(status_code=503, detail="SWS unavailable (simulated)")
    validate_schema(body.payload, "SWS")
    await record_receipt(body.idempotency, "SWS", body.ubid)
    await write_audit(app.state.pg_pool, body.ubid, "sws_write", "SanchaarSetu", "SWS",
                      payload_hash(body.payload), body.idempotency, "accepted")
    return {"status": "ok"}


@app.post("/simulate/failure-mode")
async def set_failure_mode(body: FailureModeBody):
    code = dept_code_for(body.department) or body.department
    app.state.sim_failures[code] = {
        "offline_until": time.time() + body.offline_seconds if body.offline_seconds else 0,
        "reject_probability": body.reject_probability,
    }
    return {"department": code, **app.state.sim_failures[code]}


@app.delete("/simulate/failure-mode")
async def clear_failure_modes():
    app.state.sim_failures = {}
    return {"status": "cleared"}


@app.get("/audit/verify")
async def audit_verify():
    return await verify_audit_chain(app.state.pg_pool)


@app.get("/unmapped-fields")
async def list_unmapped_fields(limit: int = 100):
    return await fetch_rows(
        app.state.pg_pool,
        """SELECT department, direction, field_name, reason, COUNT(*) AS occurrences, MAX(created_at) AS last_seen
           FROM unmapped_fields GROUP BY department, direction, field_name, reason
           ORDER BY occurrences DESC LIMIT $1""",
        limit,
    )


@app.get("/system/info")
async def system_info():
    return {
        "matcher_backend": transform_mod.MATCHER_BACKEND,
        "auto_map_threshold": transform_mod.AUTO_MAP_THRESHOLD,
        "review_threshold": transform_mod.REVIEW_THRESHOLD,
        "destination_queues": {k: q.qsize() for k, q in app.state.dest_queues.items()},
    }


@app.get("/metrics/delivery")
async def delivery_metrics(since: float | None = None):
    """Counts and end-to-end latencies for events accepted after `since` (unix seconds)."""
    pg = app.state.pg_pool
    since_dt = datetime.fromtimestamp(since) if since else datetime.fromtimestamp(0)
    outcomes = await fetch_rows(
        pg, "SELECT outcome, COUNT(*) AS n FROM propagation_events WHERE accepted_at >= $1 GROUP BY outcome", since_dt)
    lat = await fetch_row(
        pg,
        """SELECT COUNT(*) AS n,
                  percentile_cont(0.5) WITHIN GROUP (ORDER BY propagation_ms) AS p50,
                  percentile_cont(0.95) WITHIN GROUP (ORDER BY propagation_ms) AS p95,
                  MAX(propagation_ms) AS max
           FROM propagation_events WHERE accepted_at >= $1 AND outcome = 'success' AND propagation_ms IS NOT NULL""",
        since_dt,
    )
    dup = await fetch_row(
        pg,
        """SELECT COUNT(*) AS keys, COALESCE(SUM(times_received - 1), 0) AS duplicate_deliveries
           FROM destination_receipts WHERE first_received_at >= $1""",
        since_dt,
    )
    per_dest = await fetch_rows(
        pg,
        """SELECT destination_system,
                  COUNT(*) FILTER (WHERE outcome = 'success') AS delivered,
                  COUNT(*) AS accepted,
                  percentile_cont(0.5) WITHIN GROUP (ORDER BY propagation_ms) AS p50,
                  percentile_cont(0.95) WITHIN GROUP (ORDER BY propagation_ms) AS p95
           FROM propagation_events WHERE accepted_at >= $1
           GROUP BY destination_system ORDER BY destination_system""",
        since_dt,
    )
    return {"outcomes": {r["outcome"]: r["n"] for r in outcomes}, "latency_ms": lat, "receipts": dup,
            "by_destination": per_dest}


@app.get("/health")
async def health():
    return {"status": "ok"}


# --- Admin / Frontend-friendly endpoints ---
@app.get("/audit")
async def get_audit(limit: int = 100, offset: int = 0):
    pg = app.state.pg_pool
    rows = await pg.fetch(
        "SELECT * FROM audit ORDER BY created_at DESC LIMIT $1 OFFSET $2",
        limit,
        offset,
    )
    return [dict(r) for r in rows]


@app.get("/departments")
async def list_departments():
    return await fetch_rows(app.state.pg_pool, "SELECT * FROM departments ORDER BY ingestion_tier, name")


@app.patch("/departments/{dept_id}")
async def update_department(dept_id: str, body: DepartmentUpdateBody):
    row = await fetch_row(
        app.state.pg_pool,
        "UPDATE departments SET status = $1 WHERE id = $2 RETURNING *",
        body.status,
        dept_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail="department not found")
    return row


@app.get("/businesses")
async def list_businesses():
    return await fetch_rows(app.state.pg_pool, "SELECT * FROM businesses ORDER BY created_at DESC")


@app.get("/propagation-events")
async def list_propagation_events(limit: int = 25, offset: int = 0, outcome: str | None = None, q: str | None = None):
    clauses = []
    args: list[Any] = []
    if outcome and outcome != "all":
        clauses.append(f"outcome = ${len(args) + 1}")
        args.append(outcome)
    if q:
        idx = len(args)
        clauses.append(
            f"(ubid ILIKE ${idx + 1} OR event_type ILIKE ${idx + 1} OR source_system ILIKE ${idx + 1} OR destination_system ILIKE ${idx + 1})"
        )
        args.append(f"%{q}%")
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = await fetch_rows(
        app.state.pg_pool,
        f"SELECT * FROM propagation_events {where_sql} ORDER BY created_at DESC LIMIT ${len(args) + 1} OFFSET ${len(args) + 2}",
        *args,
        limit,
        offset,
    )
    total = await count_rows(app.state.pg_pool, "propagation_events", where_sql, *args)
    return {"data": rows, "total": total}


@app.post("/simulate/event")
async def simulate_event_backend():
    ubids = [
        "UBID-KA-2024-001847", "UBID-KA-2024-002341", "UBID-KA-2024-003892",
        "UBID-KA-2024-005123", "UBID-KA-2024-006789", "UBID-KA-2024-008234",
        "UBID-KA-2024-009012", "UBID-KA-2024-010345",
    ]
    event_types = ["address_update", "signatory_update", "license_renewal", "gstin_update", "compliance_status", "registration_new"]
    departments = await fetch_rows(app.state.pg_pool, "SELECT id, code FROM departments ORDER BY code")
    if not departments:
        return {"status": "no_departments"}
    dept = random.choice(departments)
    outcome = random.choice(["success", "failure", "duplicate", "pending", "dlq", "conflict", "success", "success"])
    direction = random.choice(["sws_to_dept", "dept_to_sws"])
    ubid = random.choice(ubids)
    event_type = random.choice(event_types)
    idemp = make_unique_idempotency_key(ubid, event_type)
    row = await fetch_row(
        app.state.pg_pool,
        """
        INSERT INTO propagation_events(
          ubid,event_type,source_system,destination_system,payload_hash,idempotency_key,
          outcome,direction,conflict_flag,resolution_applied,retry_count,error_message,propagation_ms
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
        RETURNING *
        """,
        ubid,
        event_type,
        "SWS" if direction == "sws_to_dept" else dept["code"],
        dept["code"] if direction == "sws_to_dept" else "SWS",
        payload_hash({"ubid": ubid, "event_type": event_type}),
        idemp,
        outcome,
        direction,
        outcome == "conflict",
        "sws_wins" if outcome == "conflict" else None,
        0 if outcome in ("success", "pending") else random.randint(1, 5),
        "Simulated backend event" if outcome in ("failure", "dlq") else None,
        random.randint(80, 2500) if outcome in ("success", "duplicate") else None,
    )
    if outcome == "conflict":
        field = random.choice(["registered_address", "signatory_name", "gstin", "pan_number"])
        if field == "registered_address":
            street_num = random.randint(10, 999)
            street_names = ["MG Road", "Brigade Road", "Ring Road", "Tech Park", "Industrial Area", "Cross Street", "Main Road"]
            areas = ["Bengaluru", "Mysuru", "Hubli", "Mangaluru", "Belagavi"]
            sws_val = f"No. {street_num}, {random.choice(street_names)}, {random.choice(areas)}"
            dept_val = f"Plot {street_num}, {random.choice(street_names)}, {random.choice(areas)}"
        elif field == "signatory_name":
            first_names = ["Rajesh", "Priya", "Abdul", "Anjali", "Suresh", "Kavita", "Ramesh", "Deepa"]
            last_names = ["Kumar", "Sharma", "Rahman", "Desai", "Patil", "Reddy", "Gowda", "Rao"]
            first = random.choice(first_names)
            last = random.choice(last_names)
            sws_val = f"{first} {last}"
            dept_val = f"{first} {last[0]}."
        elif field == "gstin":
            base = f"29{random.randint(10000, 99999)}A1Z"
            sws_val = f"{base}5"
            dept_val = f"{base}6"
        else:
            base = f"ABCDE{random.randint(1000, 9999)}"
            sws_val = f"{base}F"
            dept_val = f"{base}G"

        await app.state.pg_pool.execute(
            """
            INSERT INTO conflicts(ubid, field_name, sws_value, dept_value, source_department_id, resolution_policy, status, winning_value, resolved_by, resolved_at, propagation_event_id, resolution_reason)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now(),$10,$11)
            """,
            ubid,
            field,
            sws_val,
            dept_val,
            dept["id"],
            random.choice(["sws_wins", "last_write_wins", "manual_review"]),
            random.choice(["open", "pending_review"]),
            None,
            None,
            row["id"],
            "Simulated conflict from backend",
        )
    if outcome == "success":
        await app.state.pg_pool.execute(
            "UPDATE departments SET records_synced = records_synced + 1, last_sync_at = now() WHERE id = $1",
            dept["id"],
        )
    if outcome == "dlq":
        source_system = "SWS" if direction == "sws_to_dept" else dept["code"]
        destination_system = dept["code"] if direction == "sws_to_dept" else "SWS"
        await app.state.pg_pool.execute(
            """
            INSERT INTO dlq_messages(propagation_event_id, ubid, event_type, source_system, destination_system, payload)
            VALUES ($1,$2,$3,$4,$5,$6)
            """,
            row["id"], ubid, event_type, source_system, destination_system, json.dumps({"simulated": True})
        )

    # Ensure simulated event appears in true audit trail
    source_system = "SWS" if direction == "sws_to_dept" else dept["code"]
    destination_system = dept["code"] if direction == "sws_to_dept" else "SWS"
    await write_audit(
        app.state.pg_pool,
        ubid,
        event_type,
        source_system,
        destination_system,
        payload_hash({"ubid": ubid, "event_type": event_type}),
        idemp,
        outcome
    )

    return row


@app.post("/simulate/burst")
async def simulate_burst_backend(body: SimulateBody):
    rows = []
    for _ in range(max(1, min(body.count, 20))):
        rows.append(await simulate_event_backend())
    return {"count": len(rows), "rows": rows}


@app.post("/simulate/change-detected")
async def simulate_change_detected(body: SimulateChangeBody):
    """Create a synthetic `change_events` row (Tier-2/Tier-3 style) which the ingest loop will publish to Kafka."""
    payload = {"ubid": body.ubid, "event_type": body.event_type, "payload": body.payload}
    row = await fetch_row(
        app.state.pg_pool,
        "INSERT INTO change_events(department, event_type, payload) VALUES ($1,$2,$3) RETURNING id, department, event_type, payload, created_at",
        body.department,
        body.event_type,
        json.dumps(payload),
    )
    return row


@app.post("/propagation-events/replay")
async def replay_propagation_event(body: ReplayEventBody):
    # Create a new pending event and re-queue it to Kafka.
    idemp = make_unique_idempotency_key(body.ubid, body.event_type)
    payload = body.payload or {"replay": True}
    row = await create_propagation_event(
        app.state.pg_pool,
        ubid=body.ubid,
        event_type=body.event_type,
        source_system=body.source_system,
        destination_system=body.destination_system,
        direction=body.direction,
        idempotency_key=idemp,
        payload=payload,
        outcome="pending",
    )

    message = {
        "propagation_event_id": str(row["id"]),
        "ubid": body.ubid,
        "event_type": body.event_type,
        "payload": payload,
        "idempotency_key": idemp,
        "timestamp": time.time(),
        "source_system": body.source_system,
        "destination_system": body.destination_system,
        "direction": body.direction,
        "retry_count": 0,
        "accepted_at": time.time(),
        "update_id": None,
        "resolution_applied": "replayed",
    }
    await app.state.kafka_producer.send_and_wait(TOPIC, json.dumps(message).encode())
    return row


@app.get("/conflicts")
async def list_conflicts(status: str | None = None, q: str | None = None, limit: int = 100, offset: int = 0):
    clauses = []
    args: list[Any] = []
    if status and status != "all":
        clauses.append(f"status = ${len(args) + 1}")
        args.append(status)
    if q:
        clauses.append(f"(ubid ILIKE ${len(args) + 1} OR field_name ILIKE ${len(args) + 1})")
        args.append(f"%{q}%")
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = await fetch_rows(
        app.state.pg_pool,
        f"SELECT * FROM conflicts {where_sql} ORDER BY created_at DESC LIMIT ${len(args) + 1} OFFSET ${len(args) + 2}",
        *args,
        limit,
        offset,
    )
    total = await count_rows(app.state.pg_pool, "conflicts", where_sql, *args)
    return {"data": rows, "total": total}


@app.patch("/conflicts/{conflict_id}")
async def update_conflict(conflict_id: str, body: ConflictResolveBody):
    row = await fetch_row(
        app.state.pg_pool,
        """
        UPDATE conflicts
        SET status = 'resolved', winning_value = COALESCE($1, winning_value), resolved_by = $2,
            resolved_at = now(), resolution_reason = $3
        WHERE id = $4
        RETURNING *
        """,
        body.winning_value,
        body.resolved_by,
        body.resolution_reason,
        conflict_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail="conflict not found")

    # The operator's decision closes the disagreement for this field.
    if app.state.conflict_resolver is not None:
        app.state.conflict_resolver.discard(row["ubid"], row["field_name"])

    # Re-queue every event held by this conflict, carrying the chosen value.
    if row.get("winning_value") is not None:
        held = await fetch_rows(
            app.state.pg_pool,
            "SELECT * FROM propagation_events WHERE held_by_conflict = $1 OR (id = $2 AND outcome = 'conflict')",
            row["id"], row.get("propagation_event_id"),
        )
        for pe in held:
            payload = pe["payload"] if isinstance(pe["payload"], dict) else json.loads(pe["payload"] or "{}")
            await update_propagation_event(
                app.state.pg_pool, str(pe["id"]), outcome="pending", conflict_flag=False,
                resolution_applied=row.get("resolution_policy") or "manual_review", retry_count=0,
                error_message=None, held_by_conflict=None,
            )
            message = {
                "propagation_event_id": str(pe["id"]),
                "ubid": pe["ubid"],
                "event_type": pe["event_type"],
                "payload": payload,
                "field_overrides": {row["field_name"]: row["winning_value"]},
                "idempotency_key": pe["idempotency_key"],
                "update_id": None,
                "timestamp": time.time(),
                "accepted_at": pe["accepted_at"].timestamp() if pe.get("accepted_at") else time.time(),
                "source_system": pe["source_system"],
                "destination_system": pe["destination_system"],
                "direction": pe["direction"],
                "retry_count": 0,
                "resolution_applied": row.get("resolution_policy") or "manual_review",
            }
            await app.state.kafka_producer.send_and_wait(TOPIC, json.dumps(message).encode())

    return row


@app.get("/conflicts/{conflict_id}/suggest")
async def suggest_conflict_resolution(conflict_id: str):
    """Simulate an LLM analyzing the conflict context and suggesting a resolution."""
    row = await fetch_row(
        app.state.pg_pool,
        "SELECT * FROM conflicts WHERE id = $1",
        conflict_id
    )
    if not row:
        raise HTTPException(status_code=404, detail="Conflict not found")
    
    # Mock LLM reasoning based on field type and values
    field = row.get("field_name")
    sws_val = row.get("sws_value", "")
    dept_val = row.get("dept_value", "")
    
    await asyncio.sleep(1) # Simulate LLM latency

    reasoning = ""
    suggested_winner = "sws"
    
    if field == "registered_address":
        if len(str(sws_val)) > len(str(dept_val)):
            reasoning = f"The SWS address '{sws_val}' contains more granular locality information than the Department address. Suggesting SWS as the authoritative record."
            suggested_winner = "sws"
        else:
            reasoning = f"The Department address '{dept_val}' appears more complete or recently updated. Suggesting Department value."
            suggested_winner = "dept"
    elif field == "signatory_name":
        if "." in str(dept_val) and "." not in str(sws_val):
            reasoning = f"The Department name '{dept_val}' uses an initial, whereas SWS '{sws_val}' uses the full expanded name. Full names are preferred for legal compliance."
            suggested_winner = "sws"
        else:
            reasoning = "Both names appear valid, but the SWS record aligns better with the standard KYC format."
            suggested_winner = "sws"
    elif field == "gstin" or field == "pan_number":
        reasoning = f"Detected standard alphanumeric format for {field.upper()}. The SWS value was verified via API gateway during onboarding. High confidence in SWS."
        suggested_winner = "sws"
    else:
        reasoning = "Based on historical resolution patterns for this department, SWS is typically the system of record."
        suggested_winner = "sws"

    return {
        "suggested_winner": suggested_winner,
        "reasoning": f"✨ AI Analysis: {reasoning}"
    }


@app.get("/schema-mappings")
async def list_schema_mappings(status: str | None = None, department_id: str | None = None, q: str | None = None, limit: int = 100, offset: int = 0):
    clauses = []
    args: list[Any] = []
    if status and status != "all":
        clauses.append(f"status = ${len(args) + 1}")
        args.append(status)
    if department_id and department_id != "all":
        clauses.append(f"department_id = ${len(args) + 1}")
        args.append(department_id)
    if q:
        clauses.append(f"(sws_field ILIKE ${len(args) + 1} OR dept_field ILIKE ${len(args) + 1})")
        args.append(f"%{q}%")
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = await fetch_rows(
        app.state.pg_pool,
        f"SELECT * FROM schema_mappings {where_sql} ORDER BY confidence_score DESC, created_at DESC LIMIT ${len(args) + 1} OFFSET ${len(args) + 2}",
        *args,
        limit,
        offset,
    )
    total = await count_rows(app.state.pg_pool, "schema_mappings", where_sql, *args)
    return {"data": rows, "total": total}


@app.post("/schema-mappings")
async def create_schema_mapping(body: MappingCreateBody):
    status = "auto_mapped" if body.confidence_score >= 0.85 else "pending_review"
    row = await fetch_row(
        app.state.pg_pool,
        """
        INSERT INTO schema_mappings(department_id, sws_field, dept_field, confidence_score, status, version)
        VALUES ($1,$2,$3,$4,$5,1)
        RETURNING *
        """,
        body.department_id,
        body.sws_field,
        body.dept_field,
        body.confidence_score,
        status,
    )
    return row


@app.patch("/schema-mappings/{mapping_id}")
async def update_schema_mapping(mapping_id: str, body: MappingUpdateBody):
    row = await fetch_row(
        app.state.pg_pool,
        """
        UPDATE schema_mappings
        SET status = COALESCE($1, status), reviewed_by = $2, reviewed_at = COALESCE($3::timestamptz, reviewed_at), confidence_score = COALESCE($4, confidence_score)
        WHERE id = $5
        RETURNING *
        """,
        body.status,
        body.reviewed_by,
        body.reviewed_at,
        body.confidence_score,
        mapping_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail="mapping not found")
    return row


@app.get("/dashboard/stats")
async def dashboard_stats():
    pg = app.state.pg_pool
    events = await fetch_rows(pg, "SELECT outcome, direction, propagation_ms FROM propagation_events ORDER BY created_at DESC")
    departments = await fetch_rows(pg, "SELECT * FROM departments ORDER BY ingestion_tier, name")
    recent = await fetch_rows(pg, "SELECT * FROM propagation_events ORDER BY created_at DESC LIMIT 12")
    return {
        "events": events,
        "recentEvents": recent,
        "departments": departments,
        "totalEvents": len(events),
        "activeConflicts": await count_rows(pg, "conflicts", "WHERE status IN ('open', 'pending_review')"),
        "pendingMappings": await count_rows(pg, "schema_mappings", "WHERE status = 'pending_review'"),
        "totalBusinesses": await count_rows(pg, "businesses"),
        "activeDepts": len([d for d in departments if d.get("status") == "active"]),
        "avgPropMs": round(sum([e["propagation_ms"] for e in events if e.get("propagation_ms") is not None]) / max(1, len([e for e in events if e.get("propagation_ms") is not None]))),
        "swsToDept": len([e for e in events if e.get("direction") == "sws_to_dept"]),
        "deptToSws": len([e for e in events if e.get("direction") == "dept_to_sws"]),
    }


@app.get("/conflict-log")
async def get_conflict_log(limit: int = 100, offset: int = 0):
    pg = app.state.pg_pool
    rows = await pg.fetch(
        "SELECT * FROM conflict_log ORDER BY created_at DESC LIMIT $1 OFFSET $2",
        limit,
        offset,
    )
    return [dict(r) for r in rows]


@app.post("/conflicts/resolve")
async def api_resolve_conflict(body: dict):
    conflict = body.get("conflict")
    policy_str = body.get("policy", "sws_wins")
    if not conflict:
        raise HTTPException(status_code=400, detail="missing conflict in body")
    try:
        policy = ConflictPolicy(policy_str)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid policy")

    res = await app.state.conflict_resolver.resolve_conflict(conflict, policy)
    return res


@app.get("/mappings")
async def get_mappings(limit: int = 100, offset: int = 0):
    pg = app.state.pg_pool
    rows = await pg.fetch(
        "SELECT * FROM mapping_registry ORDER BY created_at DESC LIMIT $1 OFFSET $2",
        limit,
        offset,
    )
    return [dict(r) for r in rows]


@app.post("/mappings/{mapping_id}/approve")
async def approve_mapping(mapping_id: int):
    pg = app.state.pg_pool
    # mark as approved by setting confidence high (simple flow)
    await pg.execute("UPDATE mapping_registry SET confidence = $1 WHERE id = $2", 0.99, mapping_id)
    row = await pg.fetchrow("SELECT * FROM mapping_registry WHERE id = $1", mapping_id)
    return dict(row)


@app.post("/mappings/{mapping_id}/reject")
async def reject_mapping(mapping_id: int):
    pg = app.state.pg_pool
    # mark as rejected by setting confidence low
    await pg.execute("UPDATE mapping_registry SET confidence = $1 WHERE id = $2", 0.0, mapping_id)
    row = await pg.fetchrow("SELECT * FROM mapping_registry WHERE id = $1", mapping_id)
    return dict(row)


@app.get("/dlq")
async def list_dlq_messages(limit: int = 100, offset: int = 0):
    """List all dead-lettered messages for manual review and replay."""
    pg = app.state.pg_pool
    clauses = ["dlq_messages"]
    rows = await pg.fetch(
        "SELECT id, propagation_event_id, ubid, event_type, source_system, destination_system, payload, created_at FROM dlq_messages WHERE replayed_at IS NULL ORDER BY created_at DESC LIMIT $1 OFFSET $2",
        limit,
        offset,
    )
    total = await count_rows(pg, "dlq_messages", "WHERE replayed_at IS NULL")
    return {"data": [dict(r) for r in rows], "total": total}


@app.post("/dlq/{dlq_message_id}/replay")
async def replay_dlq_message(dlq_message_id: int):
    """Replay a DLQ message: re-publish it to the main Kafka topic and mark it as replayed."""
    pg = app.state.pg_pool
    producer = app.state.kafka_producer

    try:
        dlq_msg = await fetch_row(
            pg,
            "SELECT id, propagation_event_id, payload FROM dlq_messages WHERE id = $1",
            dlq_message_id,
        )
        if not dlq_msg:
            raise HTTPException(status_code=404, detail="DLQ message not found")

        # Parse payload and reset retry count to 0 for fresh retry
        try:
            payload = json.loads(dlq_msg["payload"]) if isinstance(dlq_msg["payload"], str) else dlq_msg["payload"]
        except Exception:
            payload = dlq_msg["payload"]

        payload["retry_count"] = 0
        payload["resolution_applied"] = payload.get("resolution_applied") or "replayed"
        propagation_event_id = dlq_msg.get("propagation_event_id")

        # Re-publish to main Kafka topic
        await producer.send_and_wait(TOPIC, json.dumps(payload).encode())

        # Mark as replayed in DB
        async with pg.acquire() as conn:
            await conn.execute(
                "UPDATE dlq_messages SET replayed_at = now() WHERE id = $1",
                dlq_message_id,
            )

        # If this was tied to a propagation event, reset it to pending
        if propagation_event_id:
            await update_propagation_event(
                pg,
                propagation_event_id,
                outcome="pending",
                retry_count=0,
                error_message=None,
            )

        return {"status": "replayed", "dlq_message_id": dlq_message_id, "propagation_event_id": propagation_event_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Replay failed: {str(e)}")