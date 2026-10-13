"""Table 7: delivery under failure, replaying real trade-licence renewals.

    python -m eval.load_test --n 300 --rate 10 --offline-dept SEA --offline-seconds 20 \
                             --reject-dept DOL --reject-probability 0.2

Runs three conditions in turn against a running SanchaarSetu instance:
no failures, one department offline, and one department rejecting requests
at random. Each renewal becomes an SWS update (name, address, activity) for its
business and is broadcast to every active department.

Identifier links and field mappings for the replayed businesses are supplied
(written directly to the database), so that this experiment measures delivery
only and not matching quality. Run it on a fresh database.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time

import asyncpg
import httpx
import pandas as pd

from eval.common import DEFAULT_PATHS, RESULTS, load_licences

# SWS field -> each simulated department's field (from db/init.sql schema_fields).
SUPPLIED_MAPPINGS = {
    "DFB": {"business_name": "factory_name", "registered_address": "factory_address"},
    "SEA": {"business_name": "establishment_name", "registered_address": "postal_address", "nature_of_business": "nature_of_business"},
    "DOL": {"business_name": "establishment_name", "registered_address": "employer_address"},
    "BBMP": {"business_name": "trade_name", "registered_address": "property_address", "nature_of_business": "trade_type"},
    "CTD": {"business_name": "legal_name", "registered_address": "principal_place_of_business"},
    "KPCB": {"business_name": "unit_name", "registered_address": "unit_address", "nature_of_business": "industry_category"},
    "KFES": {"business_name": "building_name", "registered_address": "premises_address"},
    "DIS": {"business_name": "unit_name", "registered_address": "address"},
}


def updates_from_licences(path: str, n: int, offset: int) -> list[dict]:
    df = load_licences(path)
    df = df[df["Application Type"] == "Renewal"].sort_values(["Date", "Application ID"]).iloc[offset:offset + n]
    out = []
    for _, r in df.iterrows():
        key = f"{str(r['Trade Name']).strip().upper()}|{r['pin']}"
        ubid = "UBID-LT-" + hashlib.sha1(key.encode()).hexdigest()[:10].upper()
        out.append({
            "ubid": ubid,
            "business_name": str(r["Trade Name"]).strip(),
            "payload": {
                "business_name": str(r["Trade Name"]).strip(),
                "registered_address": " ".join(str(r["address_full"]).split()) + (f" {r['pin']}" if isinstance(r["pin"], str) else ""),
                "nature_of_business": str(r["Minor Trade"]).strip() if isinstance(r["Minor Trade"], str) else "",
            },
        })
    for u in out:
        u["payload"] = {k: v for k, v in u["payload"].items() if v}
    return out


async def setup(dsn: str, updates: list[dict]) -> None:
    pg = await asyncpg.connect(dsn)
    depts = {r["code"]: r["id"] for r in await pg.fetch("SELECT id, code FROM departments")}
    for code, mapping in SUPPLIED_MAPPINGS.items():
        for sws_field, dept_field in mapping.items():
            await pg.execute(
                """INSERT INTO schema_mappings(department_id, sws_field, dept_field, confidence_score, status, version, reviewed_by)
                   VALUES ($1,$2,$3,1.0,'confirmed',1,'load_test:supplied')
                   ON CONFLICT (department_id, sws_field, dept_field, version) DO UPDATE SET status = 'confirmed'""",
                depts[code], sws_field, dept_field)
    seen = {}
    for u in updates:
        seen.setdefault(u["ubid"], u["business_name"])
    for i, (ubid, name) in enumerate(seen.items()):
        bid = await pg.fetchval(
            """INSERT INTO businesses(ubid, business_name, status) VALUES ($1,$2,'active')
               ON CONFLICT (ubid) DO UPDATE SET business_name = EXCLUDED.business_name RETURNING id""", ubid, name)
        for code, did in depts.items():
            await pg.execute(
                """INSERT INTO business_departments(business_id, department_id, dept_record_id, link_status)
                   VALUES ($1,$2,$3,'confirmed') ON CONFLICT (business_id, department_id) DO NOTHING""",
                bid, did, f"{code}-LT-{i:05d}")
    await pg.close()


async def run_condition(client: httpx.AsyncClient, name: str, updates: list[dict], rate: float,
                        failure: dict | None, drain_timeout: float) -> dict:
    await client.delete("/simulate/failure-mode")
    if failure:
        await client.post("/simulate/failure-mode", json=failure)
    start = time.time()
    accepted = duplicates = errors = 0
    interval = 1.0 / rate
    for i, u in enumerate(updates):
        t_due = start + i * interval
        if (d := t_due - time.time()) > 0:
            await asyncio.sleep(d)
        r = await client.post("/sws/webhook", headers={"X-API-Key": "sws-secret-key"},
                              json={"ubid": u["ubid"], "event_type": f"licence_renewal:{name}", "payload": u["payload"]})
        if r.status_code == 200:
            body = r.json()
            accepted += body["accepted"]
            duplicates += body["duplicates"]
        else:
            errors += 1
    sent_done = time.time()

    # Wait until nothing is pending or being retried.
    while time.time() - sent_done < drain_timeout:
        m = (await client.get("/metrics/delivery", params={"since": start})).json()
        if not (m["outcomes"].get("pending", 0) or m["outcomes"].get("failure", 0)):
            break
        await asyncio.sleep(1)
    m = (await client.get("/metrics/delivery", params={"since": start})).json()
    await client.delete("/simulate/failure-mode")
    o = m["outcomes"]
    finished = sum(o.get(k, 0) for k in ("success", "dlq", "held", "conflict"))
    return {
        "condition": name,
        "failure_mode": failure,
        "updates_sent": len(updates),
        "events_accepted": accepted,
        "rejected_as_duplicate_at_ingest": duplicates,
        "http_errors": errors,
        "delivered": o.get("success", 0),
        "dead_letter": o.get("dlq", 0),
        "held": o.get("held", 0),
        "lost": accepted - finished,
        "still_pending_after_drain": o.get("pending", 0) + o.get("failure", 0),
        "duplicate_deliveries_at_destination": int(m["receipts"]["duplicate_deliveries"] or 0),
        "latency_ms": {k: m["latency_ms"][k] for k in ("p50", "p95", "max")},
        "by_destination": m["by_destination"],
        "send_seconds": round(sent_done - start, 1),
    }


async def main_async(a) -> None:
    conditions = [
        ("no_failures", None),
        ("one_department_offline", {"department": a.offline_dept, "offline_seconds": a.offline_seconds}),
        ("intermittent_rejection", {"department": a.reject_dept, "reject_probability": a.reject_probability}),
    ]
    all_updates = [updates_from_licences(a.licences, a.n, a.n * i) for i in range(len(conditions))]
    await setup(a.dsn, [u for us in all_updates for u in us])
    async with httpx.AsyncClient(base_url=a.base_url, timeout=30) as client:
        info = (await client.get("/system/info")).json()
        results = []
        for (name, failure), updates in zip(conditions, all_updates):
            print(f"running {name} ...", flush=True)
            results.append(await run_condition(client, name, updates, a.rate, failure, a.drain_timeout))
        verify = (await client.get("/audit/verify")).json()
    out = {"settings": vars(a), "system": info, "audit_chain": verify, "conditions": results}
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "table7_delivery.json").write_text(json.dumps(out, indent=2, default=str))
    for r in results:
        print(json.dumps({k: r[k] for k in ("condition", "events_accepted", "delivered", "dead_letter", "held", "lost",
                                            "duplicate_deliveries_at_destination", "latency_ms")}, default=str))
    print("audit chain:", verify)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://127.0.0.1:8000")
    ap.add_argument("--dsn", default="postgres://postgres:postgres@localhost:5432/sanchaar")
    ap.add_argument("--licences", default=str(DEFAULT_PATHS["licences"]))
    ap.add_argument("--n", type=int, default=300, help="updates per condition")
    ap.add_argument("--rate", type=float, default=10.0, help="updates per second")
    ap.add_argument("--offline-dept", default="SEA")
    ap.add_argument("--offline-seconds", type=float, default=20.0)
    ap.add_argument("--reject-dept", default="DOL")
    ap.add_argument("--reject-probability", type=float, default=0.2)
    ap.add_argument("--drain-timeout", type=float, default=120.0)
    asyncio.run(main_async(ap.parse_args()))


if __name__ == "__main__":
    main()
