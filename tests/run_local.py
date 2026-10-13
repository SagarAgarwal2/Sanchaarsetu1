"""Run the API against local Postgres and Redis with the in-process Kafka stand-in.

    POSTGRES_DSN=... REDIS_URL=... python -m tests.run_local
"""
import uvicorn

import app.full_main as fm
from tests.fake_kafka import FakeConsumer, FakeProducer

fm.AIOKafkaProducer = FakeProducer
fm.AIOKafkaConsumer = FakeConsumer

if __name__ == "__main__":
    uvicorn.run(fm.app, host="127.0.0.1", port=8000, log_level="warning")
