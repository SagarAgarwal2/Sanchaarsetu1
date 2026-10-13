"""Fill schema_fields.sample_values for the running system from the registry files.

    python -m eval.load_field_samples --dsn postgres://postgres:postgres@localhost:5432/sanchaar

Only fields that cannot identify a person get samples, and every value passes
through the same PII scrubber the matcher uses.
"""
from __future__ import annotations

import argparse
import asyncio

import asyncpg
import pandas as pd

from app.transform import scrub_pii
from eval.common import DEFAULT_PATHS

# (system code, registry field in schema_fields) -> (data file key, column in that file)
SOURCES = {
    ("SEA", "nature_of_business"): ("shops", "nature_of_business"),
    ("SEA", "commencement_date"): ("shops", "registration_date"),
    ("SEA", "employees_total"): ("shops", "employees_total"),
    ("BBMP", "trade_type"): ("licences", "Major Trade"),
    ("SWS", "nature_of_business"): ("shops", "nature_of_business"),
    ("SWS", "employee_count"): ("shops", "employees_total"),
    ("SWS", "registration_date"): ("company", "registration_date"),
}


def samples(path: str, column: str, k: int = 3) -> list[str]:
    s = pd.read_csv(path, dtype=str, usecols=[column], nrows=20000)[column].dropna().str.strip()
    s = s[s != ""]
    return [str(scrub_pii(v))[:60] for v in s.value_counts().index[:k]]


async def main_async(dsn: str, paths: dict) -> None:
    pg = await asyncpg.connect(dsn)
    for (system, field), (key, column) in SOURCES.items():
        vals = samples(paths[key], column)
        await pg.execute("UPDATE schema_fields SET sample_values = $1 WHERE system_code = $2 AND field_name = $3",
                         vals, system, field)
        print(f"{system}.{field}: {vals}")
    await pg.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="postgres://postgres:postgres@localhost:5432/sanchaar")
    for k, v in DEFAULT_PATHS.items():
        ap.add_argument(f"--{k}", default=str(v))
    a = ap.parse_args()
    asyncio.run(main_async(a.dsn, {k: getattr(a, k) for k in DEFAULT_PATHS}))


if __name__ == "__main__":
    main()
