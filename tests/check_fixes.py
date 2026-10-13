"""End-to-end checks for the fixes, run against a live local instance.

Start the app first (python -m tests.run_local), then: python -m tests.check_fixes
Each check prints PASS or FAIL with the evidence.
"""
import asyncio
import json
import os
import time

import asyncpg
import httpx

BASE = os.getenv("BASE_URL", "http://127.0.0.1:8000")
DSN = os.getenv("POSTGRES_DSN", "postgres://postgres@localhost:5432/sanchaar")
SWS = {"X-API-Key": "sws-secret-key"}
UBID = "UBID-KA-2024-001847"  # seeded business, linked to every department
results = []


def report(name, ok, detail):
    results.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")


async def wait_settled(pg, ids, timeout=20):
    """Wait until none of the given events is pending."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        rows = await pg.fetch("SELECT outcome FROM propagation_events WHERE id = ANY($1::uuid[])", ids)
        if rows and all(r["outcome"] not in ("pending",) for r in rows):
            return
        await asyncio.sleep(0.3)


async def outcomes(pg, ids):
    rows = await pg.fetch("SELECT destination_system, outcome, error_message FROM propagation_events WHERE id = ANY($1::uuid[]) ORDER BY destination_system", ids)
    return {r["destination_system"]: (r["outcome"], r["error_message"]) for r in rows}


async def main():
    pg = await asyncpg.connect(DSN)
    c = httpx.Client(base_url=BASE, timeout=30)

    # 1. A broadcast to every department raises no conflict with itself.
    before = await pg.fetchval("SELECT count(*) FROM conflicts")
    r = c.post("/sws/webhook", headers=SWS, json={"ubid": UBID, "event_type": "address_update",
                                                    "payload": {"registered_address": "21 Residency Road, Bengaluru 560025"}}).json()
    ids = [i["propagation_event_id"] for i in r["items"]]
    await wait_settled(pg, ids)
    after = await pg.fetchval("SELECT count(*) FROM conflicts")
    oc = await outcomes(pg, ids)
    report("broadcast does not conflict with itself", after == before and "conflict" not in [o for o, _ in oc.values()],
           f"{r['accepted']} copies queued; new conflicts = {after - before}; outcomes = { {k: v[0] for k, v in oc.items()} }")

    # 2. Unmapped fields are recorded for review, not silently dropped.
    held_nomap = [k for k, (o, e) in oc.items() if o == "held" and e and "no mapped" in e]
    unm = await pg.fetch("SELECT department, field_name, reason FROM unmapped_fields WHERE ubid = $1", UBID)
    delivered = [k for k, (o, _) in oc.items() if o == "success"]
    report("unmapped fields recorded for review", len(unm) >= len(held_nomap) and len(unm) > 0,
           f"delivered to {delivered}; held for review at {held_nomap}; review rows = {[(u['department'], u['field_name'], u['reason']) for u in unm]}")

    # 3. Duplicate detection uses the payload: a resend is rejected, a different update in the same minute is not.
    resend = c.post("/sws/webhook", headers=SWS, json={"ubid": UBID, "event_type": "address_update",
                                                         "payload": {"registered_address": "21 Residency Road, Bengaluru 560025"}}).json()
    other = c.post("/sws/webhook", headers=SWS, json={"ubid": UBID, "event_type": "address_update",
                                                        "payload": {"registered_address": "9 Lavelle Road, Bengaluru 560001"}}).json()
    report("resend rejected, distinct update accepted", resend["accepted"] == 0 and resend["duplicates"] == 7 and other["accepted"] == 7,
           f"resend accepted={resend['accepted']} duplicates={resend['duplicates']}; distinct update accepted={other['accepted']}")
    await wait_settled(pg, [i["propagation_event_id"] for i in other["items"]])

    # 4. A failing department does not delay the others, and catches up in order.
    c.post("/simulate/failure-mode", json={"department": "DFB", "offline_seconds": 3})
    t0 = time.time()
    r1 = c.post("/sws/webhook", headers=SWS, json={"ubid": UBID, "event_type": "owner_update", "payload": {"owner_name": "Ramesh K. Jain"}}).json()
    r2 = c.post("/sws/webhook", headers=SWS, json={"ubid": UBID, "event_type": "owner_update", "payload": {"owner_name": "Ramesh Kumar Jain"}}).json()
    dol_ids = [i["propagation_event_id"] for i in r1["items"] + r2["items"] if i["dept"] == "DOL"]
    dfb_ids = [i["propagation_event_id"] for i in r1["items"] + r2["items"] if i["dept"] == "DFB"]
    await wait_settled(pg, dol_ids, 10)
    dol_done = time.time() - t0
    t_end = time.time() + 15
    while time.time() < t_end:
        rows = await pg.fetch("SELECT outcome FROM propagation_events WHERE id = ANY($1::uuid[])", dfb_ids)
        if all(x["outcome"] == "success" for x in rows):
            break
        await asyncio.sleep(0.3)
    dfb_rows = await pg.fetch("SELECT outcome, retry_count FROM propagation_events WHERE id = ANY($1::uuid[])", dfb_ids)
    order = await pg.fetch(
        """SELECT a.id FROM audit a WHERE a.idempotency_key = ANY($1::text[]) AND a.outcome = 'delivered' ORDER BY a.id""",
        [i["idempotency_key"] for i in r1["items"] + r2["items"] if i["dept"] == "DFB"])
    first_key = [i["idempotency_key"] for i in r1["items"] if i["dept"] == "DFB"][0]
    first_delivered = await pg.fetchval("SELECT idempotency_key FROM audit WHERE id = $1", order[0]["id"]) if order else None
    report("failing department does not block others; catches up in order",
           dol_done < 3 and all(x["outcome"] == "success" for x in dfb_rows) and first_delivered == first_key,
           f"DOL settled after {dol_done:.1f}s while DFB was offline; DFB outcomes = {[(x['outcome'], x['retry_count']) for x in dfb_rows]}; first DFB delivery was the earlier update = {first_delivered == first_key}")
    c.delete("/simulate/failure-mode")

    # 5. No delivery without a confirmed identifier link.
    await pg.execute("INSERT INTO businesses(ubid, business_name, status) VALUES ('UBID-TEST-UNLINKED', 'Unlinked Test Business', 'active') ON CONFLICT DO NOTHING")
    r = c.post("/sws/webhook", headers=SWS, json={"ubid": "UBID-TEST-UNLINKED", "event_type": "owner_update", "payload": {"owner_name": "Test Owner"}}).json()
    ids = [i["propagation_event_id"] for i in r["items"]]
    await wait_settled(pg, ids)
    oc = await outcomes(pg, ids)
    receipts = await pg.fetchval("SELECT count(*) FROM destination_receipts WHERE ubid = 'UBID-TEST-UNLINKED'")
    report("unlinked business is held, not delivered", receipts == 0 and all(o == "held" for o, _ in oc.values()),
           f"outcomes = {sorted(set(e for _, e in oc.values()))}; deliveries received = {receipts}")

    # 6. A real disagreement on a high-stakes field is held for review, then released with the chosen value.
    dept = await pg.fetchrow("SELECT id FROM departments WHERE code = 'CTD'")
    await pg.execute("""INSERT INTO schema_mappings(department_id, sws_field, dept_field, confidence_score, status, version)
                        VALUES ($1, 'signatory_name', 'authorised_signatory', 1.0, 'confirmed', 1) ON CONFLICT DO NOTHING""", dept["id"])
    rec = await pg.fetchval("""SELECT bd.dept_record_id FROM business_departments bd JOIN businesses b ON b.id = bd.business_id
                               JOIN departments d ON d.id = bd.department_id WHERE b.ubid = $1 AND d.code = 'CTD'""", UBID)
    s1 = c.post("/sws/webhook", headers=SWS, json={"ubid": UBID, "event_type": "signatory_update", "payload": {"signatory_name": "Priya Sharma"}}).json()
    d1 = c.post("/dept/taxation/webhook", headers={"X-API-Key": "ctd-secret-key"},
                json={"dept_record_id": rec, "event_type": "signatory_update", "payload": {"authorised_signatory": "P. Sharma & Sons"}}).json()
    await asyncio.sleep(2)
    conf = await pg.fetchrow("SELECT * FROM conflicts WHERE ubid = $1 AND field_name = 'signatory_name' AND status = 'pending_review' ORDER BY created_at DESC LIMIT 1", UBID)
    held = await pg.fetchval("SELECT outcome FROM propagation_events WHERE id = $1", d1.get("propagation_event_id"))
    ok6a = conf is not None and held == "conflict"
    released = None
    if conf:
        c.patch(f"/conflicts/{conf['id']}", json={"winning_value": "Priya Sharma", "resolved_by": "test-operator", "resolution_reason": "verified against PAN records"})
        await wait_settled(pg, [d1["propagation_event_id"]], 10)
        released = await pg.fetchrow("SELECT outcome, payload FROM propagation_events WHERE id = $1", d1["propagation_event_id"])
    report("high-stakes disagreement held, then released with chosen value",
           ok6a and released is not None and released["outcome"] == "success",
           f"dept record {rec} resolved to {UBID}; conflict raised = {conf is not None} (sws='{conf['sws_value'] if conf else None}', dept='{conf['dept_value'] if conf else None}'); held outcome = {held}; after resolution = {released['outcome'] if released else None}")

    # 7. Formatting-only difference is not a conflict.
    before = await pg.fetchval("SELECT count(*) FROM conflicts")
    c.post("/sws/webhook", headers=SWS, json={"ubid": UBID, "event_type": "signatory_update", "payload": {"signatory_name": "Anil Kumar"}})
    d2 = c.post("/dept/CTD/webhook", headers={"X-API-Key": "ctd-secret-key"},
                json={"dept_record_id": rec, "event_type": "signatory_update", "payload": {"authorised_signatory": "ANIL  KUMAR."}}).json()
    await asyncio.sleep(2)
    after = await pg.fetchval("SELECT count(*) FROM conflicts")
    report("formatting-only difference not raised", after == before, f"new conflicts = {after - before}")

    # 8. Repeated failure goes to the dead-letter store; a second failure of the same event is kept too.
    c.post("/simulate/failure-mode", json={"department": "KPCB", "reject_probability": 1.0})
    r = c.post("/sws/webhook", headers=SWS, json={"ubid": UBID, "event_type": "address_update", "payload": {"business_name": "Bangalore Textiles Pvt Ltd"}}).json()
    kp = [i["propagation_event_id"] for i in r["items"] if i["dept"] == "KPCB"]
    await asyncio.sleep(1)
    st = await pg.fetchrow("SELECT outcome, error_message FROM propagation_events WHERE id = $1", kp[0]) if kp else None
    if st and st["outcome"] == "held":
        # KPCB has no mapping for this field in a fresh database; force a mapping so delivery is attempted.
        d = await pg.fetchrow("SELECT id FROM departments WHERE code='KPCB'")
        await pg.execute("""INSERT INTO schema_mappings(department_id, sws_field, dept_field, confidence_score, status, version)
                            VALUES ($1,'business_name','unit_name',1.0,'confirmed',1)
                            ON CONFLICT (department_id, sws_field, dept_field, version) DO UPDATE SET status = 'confirmed'""", d["id"])
        r = c.post("/sws/webhook", headers=SWS, json={"ubid": UBID, "event_type": "address_update", "payload": {"business_name": "Bangalore Textiles Private Limited"}}).json()
        kp = [i["propagation_event_id"] for i in r["items"] if i["dept"] == "KPCB"]
    t_end = time.time() + 15
    while time.time() < t_end:
        if await pg.fetchval("SELECT outcome FROM propagation_events WHERE id = $1", kp[0]) == "dlq":
            break
        await asyncio.sleep(0.3)
    await asyncio.sleep(1)
    dlq = await pg.fetchrow("SELECT id FROM dlq_messages WHERE propagation_event_id = $1", kp[0])
    c.post(f"/dlq/{dlq['id']}/replay") if dlq else None
    t_end = time.time() + 15
    while time.time() < t_end:
        if await pg.fetchval("SELECT replayed_at IS NULL FROM dlq_messages WHERE propagation_event_id = $1", kp[0]):
            break
        await asyncio.sleep(0.3)
    again = await pg.fetchrow("SELECT replayed_at FROM dlq_messages WHERE propagation_event_id = $1", kp[0])
    report("dead-letter store keeps repeat failures", dlq is not None and again is not None and again["replayed_at"] is None,
           f"first DLQ entry = {dlq is not None}; after replay failed again it is back in the inbox = {again is not None and again['replayed_at'] is None}")
    c.delete("/simulate/failure-mode")

    # 9. The audit chain verifies after all of the above.
    v = c.get("/audit/verify").json()
    report("audit hash chain verifies", v["ok"], f"{v['checked']} entries checked")

    info = c.get("/system/info").json()
    print(f"\nmatcher backend: {info['matcher_backend']}")
    print(f"\n{sum(results)}/{len(results)} checks passed")
    await pg.close()


if __name__ == "__main__":
    asyncio.run(main())
