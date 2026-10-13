# SanchaarSetu Demo - Full Stack Implementation

A complete reference implementation of SanchaarSetu: an AI-powered bidirectional interoperability layer for Karnataka's Single Window System and department systems.

## Features

- **Bidirectional Webhooks**: SWS→Dept and Dept→SWS propagation
- **Idempotency**: Redis-backed deduplication (at-least-once semantics)
- **Semantic Schema Mapping**: SentenceTransformers + pgvector for automatic field matching
- **Conflict Detection & Resolution**: SWS-wins, last-write-wins, and manual-review policies
- **Tiered ingestion**: webhooks, periodic polling and snapshot comparison (`change_events`). Log-based CDC (Debezium) and durable workflows (Temporal) are planned, not implemented.
- **Field-level conflict detection**: only different sources disagreeing on the same field raise a conflict; per-field policies (SWS-wins, last-write-wins, manual review).
- **Identifier registry**: updates are delivered only over a confirmed UBID-to-department-record link (`business_departments`).
- **Audit Trail**: Append-only Postgres log with **Tamper-Evident Hash Chaining**
- **Kafka Message Queue**: Durable at-least-once delivery with **Exponential Backoff & Dead Letter Queues (DLQ)**
- **Security & Privacy**: PII Scrubbing, Role-Based Access Control (RBAC), and Rate Limiting
- **UI/UX**: High-precision "GraphSentinel" enterprise light-mode design system.

## Architecture

- **API**: FastAPI (async, lightweight)
- **Message Queue**: Apache Kafka / Redpanda
- **Idempotency**: Redis
- **Data Store**: PostgreSQL (with pgvector for mappings)
- **Change Detection**: Debezium placeholders + custom pollers

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+ (for local testing scripts)

### Run Full Stack

```bash
cd sanchaarsetu_demo
docker-compose up --build
```

Services will start on:
- API: http://localhost:8000
- Postgres: localhost:5432
- Redis: localhost:6379
- Kafka (Redpanda): localhost:9092

### Frontend (Developer)

A small React + Vite developer UI is included under `frontend/`.

Run it locally:

```bash
cd sanchaarsetu_demo/frontend
npm install
npm run dev
```

Or run the built preview from Docker Compose (optional):

```bash
docker-compose up --build frontend
```

### Test End-to-End Scenarios

In a new terminal:

```bash
cd sanchaarsetu_demo
pip install requests

# Run all demo tests
python test_e2e.py
```

Tests cover:
1. Basic SWS→Department propagation
2. Idempotency (duplicate rejection)
3. High-stakes field updates (signatory, GSTIN)
4. Simultaneous conflict detection
5. Batch load scenarios

### Inspect Audit Trail & Mappings

```bash
cd sanchaarsetu_demo

# View all audit events
python inspect_db.py audit

# View learned field mappings
python inspect_db.py mappings

# View conflict resolutions
python inspect_db.py conflicts
```

### Manual API Test

```bash
curl -X POST http://localhost:8000/sws/webhook \
  -H 'Content-Type: application/json' \
  -d '{
    "ubid": "UBID-001",
    "event_type": "address_update",
    "payload": {
      "address": "123 MG Road, Bengaluru",
      "proprietor": "Raj Kumar"
    }
  }'
```

Response:
```json
{
  "status": "accepted",
  "idempotency_key": "8db9be79d71069cd0e32bbd1582a1e8c8ced2f435eefb30de6de6c168830ed68"
}
```

## Project Structure

```
sanchaarsetu_demo/
├── app/
│   ├── __init__.py
│   ├── main.py           # Lightweight SQLite demo
│   ├── full_main.py      # Full-stack async FastAPI
│   ├── transform.py      # Semantic Transform Engine
│   ├── conflicts.py      # Conflict Resolver
│   └── detection.py      # Change Detection (polling + snapshot diff)
├── db/
│   └── init.sql          # Postgres init: audit, mappings, conflicts tables
├── test_e2e.py           # End-to-end test suite
├── test_send.py          # Simple webhook test
├── inspect_db.py         # Audit trail & mapping inspector
├── docker-compose.yml    # Full-stack services
├── Dockerfile            # API container
└── requirements.txt      # Python dependencies
```

## How It Works

### 1. Webhook Ingestion (SWS → Department)

```
POST /sws/webhook
├─ Validate idempotency key (Redis)
├─ Produce to Kafka queue
└─ Write audit record (Postgres)
```

### 2. Kafka Consumer

```
Consume from sanchaar-events topic
├─ Detect conflicts (ConflictResolver)
├─ Transform payload (SentenceTransformers + pgvector)
├─ Apply conflict policy (SWS-wins by default)
└─ Deliver to department HTTP endpoint
```

### 3. Schema Mapping

- **Tier 1**: Existing mappings (lookup in pgvector)
- **Tier 2**: Semantic matching (SentenceTransformers if new field)
- **Tier 3**: Manual review (fields below 0.75 confidence threshold)

### 4. Conflict Detection

When two updates for the same UBID arrive within 60 seconds:
- **SWS-wins** (default): SWS payload applies
- **Last-write-wins**: Later timestamp wins
- **Manual-review**: Both held for human decision

### 5. Audit Trail

Every event recorded:
- UBID + event type + source + destination
- Payload hash + idempotency key
- Outcome (delivered/failed/error)
- Full conflict resolution details

## Example: Address Change

**Scenario**: Business updates address on SWS

```
1. SWS webhook fires
   POST /sws/webhook → {ubid, payload}

2. Idempotency check
   Redis: SET key=idemp_hash NX (duplicates rejected)

3. Kafka produce
   Topic: sanchaar-events
   Payload: {ubid, event_type, payload, idempotency_key, timestamp, source}

4. Transform
   Source: {address: "123 MG Road"}
   → Semantic match → Target: {registered_address: "123 MG Road"}

5. Deliver
   POST http://dept-api/update
   ├─ Success → audit outcome="delivered"
   └─ Failure → retry with exponential backoff + DLQ

6. Audit record
   (UBID-001, "address_update", "SWS", "factories", hash, idemp, "delivered", timestamp)
```

## Example: Conflict

**Scenario**: SWS and Factories both update signatory within 60 seconds

```
1. SWS update arrives
   {ubid: "U123", source: "SWS", payload: {signatory: "Alice"}, ts: 1000}

2. Factories update arrives
   {ubid: "U123", source: "factories", payload: {signatory: "Bob"}, ts: 1010}

3. Conflict detected (delta_t = 10s < 60s)
   ConflictResolver → flags conflict

4. Policy applied (SWS-wins)
   Winner: {signatory: "Alice"}

5. Audit record
   conflict_log: (ubid, "SWS", "factories", "sws_wins", "SWS wins", timestamp)
   audit: records the chosen payload
```

## Configuration

Edit `docker-compose.yml` to customize:
- Kafka bootstrap servers
- Postgres credentials
- Redis URL
- Environment variables

## Checks for the propagation pipeline

Unit tests (no services needed):

```bash
pip install pytest numpy
python -m pytest tests/
```

End-to-end checks against local Postgres and Redis, using an in-process stand-in for Kafka:

```bash
createdb sanchaar && psql -d sanchaar -f db/init.sql
POSTGRES_DSN=postgres://postgres@localhost:5432/sanchaar REDIS_URL=redis://localhost:6379 \
  RETRY_DELAYS=0.5,1,1.5,2 python -m tests.run_local &
python -m tests.check_fixes
```

## Experiments for the paper

Install `requirements-eval.txt`, put the four registry files in `data/raw/` (names in `eval/common.py`), then:

| Step | Command | Produces |
|---|---|---|
| Section 3 statistics | `python -m eval.build_dataset` | `results/section3.json` |
| Labelling workbook | `python -m eval.make_label_sheet` | `results/field_labelling.xlsx` (two annotators fill it independently) |
| Table 5, Fig. 2 | `python -m eval.eval_matching` | `results/table5_matching.json`, `results/fig2_pr_curves.pdf` (needs the embedding model) |
| Conflict sample | `python -m eval.eval_conflicts --make-sample 300` | `results/conflict_pairs_to_label.csv` (label same/different) |
| Table 6 | `python -m eval.eval_conflicts --labels results/conflict_pairs_to_label.csv` | `results/table6_conflicts.json` |
| Table 7 | `python -m eval.load_test` (fresh database, full Docker stack running) | `results/table7_delivery.json` |
| Field samples | `python -m eval.load_field_samples` | sample values in `schema_fields` |

After `eval_matching`, set `AUTO_MAP_THRESHOLD` in `docker-compose.yml` to the reported operating threshold.

The database schema changed in this version. If you ran Docker Compose before, reset the volume once with `docker compose down -v`.

## Local Development (Without Docker)

```bash
# Install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run lightweight SQLite demo
uvicorn app.main:app --reload

# In another terminal, send a test
python test_send.py
```

This runs the simple in-memory demo; audit is stored in SQLite.

## Production Considerations

- **Deployment**: Kubernetes with Helm charts
- **Kafka**: Multi-broker cluster with replication factor 3
- **Postgres**: Managed RDS with automated backups
- **Redis**: Cluster mode for HA
- **Monitoring**: Prometheus + Grafana for queue depth, latency, conflicts
- **PII Handling**: Pre-processor to scrub sensitive fields before LLM inference
- **Rate Limiting**: Per-department circuit breaker + anomaly detection

## Security

- **In Transit**: TLS 1.3
- **At Rest**: Postgres encryption
- **Authentication**: mTLS + per-department API keys (RBAC)
- **Audit Immutability**: Append-only with cryptographic **Hash-Chaining** to prove tamper-evidence.
- **Validation**: Strict schema validation on all incoming payloads.
- **Rate Limiting**: Redis-backed circuit breakers and anomaly detection to prevent payload flooding.
- **Privacy**: Regex-based PII Scrubber removes PAN, Aadhaar, Phone, and Email before any AI inference occurs.

## References

- **Document**: SanchaarSetu_Final.docx (AI for Bharat 2026, Theme 2)
- **Standards**: Unified Business Identifier (UBID) registry
- **Tools**: SentenceTransformers, pgvector, Kafka, Redpanda, Temporal.io


