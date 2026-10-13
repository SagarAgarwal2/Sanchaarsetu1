"""Shared loading and normalisation for the evaluation scripts.

The raw files are not part of the repository. Put them in data/raw/ with
these names (or pass other paths on the command line):

    company_registrations.csv           company registrations (8 columns, CIN based)
    shops_establishments.csv            Bengaluru shops and establishments registrations
    trade_licences_byatarayanapura.csv  BBMP trade licences, one constituency
    rera_projects.csv                   Karnataka RERA project registrations
"""
from __future__ import annotations

import hashlib
import hmac
import os
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
DERIVED = ROOT / "data" / "derived"
RESULTS = ROOT / "results"

DEFAULT_PATHS = {
    "company": RAW / "company_registrations.csv",
    "shops": RAW / "shops_establishments.csv",
    "licences": RAW / "trade_licences_byatarayanapura.csv",
    "rera": RAW / "rera_projects.csv",
}

# Columns never carried past loading: financial details, photographs and documents.
RERA_DROP = re.compile(r"^(bank_|amount_withdrawn|signatory_photo|.*_link$|.*certificate)")

_LEGAL_SUFFIX = re.compile(r"\b(PRIVATE|PVT|LIMITED|LTD|LLP|M S|MS|OPC)\b")
_NON_ALNUM_SPACE = re.compile(r"[^A-Z0-9 ]")
_SPACES = re.compile(r"\s+")
PIN = re.compile(r"\b(5[6-9]\d{4})\b")
PAN_RE = re.compile(r"[A-Z]{5}\d{4}[A-Z]")
GSTIN_RE = re.compile(r"\d{2}[A-Z]{5}\d{4}[A-Z][A-Z0-9]Z[A-Z0-9]")
CIN_RE = re.compile(r"[LU]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}")


def norm_name(s: pd.Series) -> pd.Series:
    """Business-name key: upper case, punctuation and legal-form words removed."""
    s = s.fillna("").astype(str).str.upper().str.replace(r"\(OPC\)", " ", regex=True)
    s = s.str.replace(_NON_ALNUM_SPACE, " ", regex=True).str.replace(_LEGAL_SUFFIX, " ", regex=True)
    return s.str.replace(_SPACES, " ", regex=True).str.strip()


def norm_text(s: pd.Series) -> pd.Series:
    """Case-, space- and punctuation-insensitive form (same rule as the conflict detector)."""
    return s.fillna("").astype(str).str.lower().str.replace(r"[^0-9a-z]+", "", regex=True)


def collapse_ws_upper(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.upper().str.replace(_SPACES, " ", regex=True).str.strip()


def pan_key(pan: str | float) -> str | None:
    """Keyed hash of a PAN, used only for grouping. Set PAN_HASH_KEY to keep keys stable across runs."""
    if not isinstance(pan, str) or not pan.strip():
        return None
    key = os.getenv("PAN_HASH_KEY", "sanchaarsetu-research").encode()
    return hmac.new(key, pan.strip().upper().encode(), hashlib.sha256).hexdigest()[:16]


def load_company(path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    df["state_code"] = df["cin"].str[6:8]
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")
    return df


def load_shops(path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    for c in ("employees_male", "employees_female", "employees_total"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")
    df["pin"] = df["address"].str.extract(PIN)[0]
    return df


def load_licences(path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["pin"] = df["Address PIN"].str.extract(r"(\d{6})")[0]
    df["address_full"] = (df["Address Door Number"].fillna("") + " " + df["Address Street"].fillna("") + " "
                          + df["Address Area"].fillna(""))
    return df


def load_rera(path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, low_memory=False)
    df = df[[c for c in df.columns if not RERA_DROP.match(c)]].copy()
    df["pan_clean"] = df["promoter_pan"].fillna("").str.upper().str.strip()
    df["promoter_key"] = df["pan_clean"].map(pan_key)
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")
    return df
