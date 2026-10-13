"""Section 3: measure record divergence in the four public registries.

    python -m eval.build_dataset [--company PATH --shops PATH --licences PATH --rera PATH]

Writes results/section3.json (every number quoted in Section 3) and
data/derived/*.csv (cleaned, PAN-hashed, sensitive columns removed; not for
redistribution).
"""
from __future__ import annotations

import argparse
import json

import pandas as pd

from eval.common import (CIN_RE, DEFAULT_PATHS, DERIVED, GSTIN_RE, PAN_RE, RESULTS, collapse_ws_upper,
                         load_company, load_licences, load_rera, load_shops, norm_name, norm_text)

RERA_ATTRS = ["promoter_address", "signatory_address", "signatory_name", "promoter_name",
              "promoter_pin_code", "promoter_gstin"]


def period(series: pd.Series) -> dict:
    s = series.dropna()
    return {"from": str(s.min().date()), "to": str(s.max().date())}


def company_stats(df: pd.DataFrame) -> dict:
    return {
        "records": int(len(df)),
        "unique_cin": int(df["cin"].nunique()),
        "cin_well_formed_share": round(float(df["cin"].str.fullmatch(CIN_RE).mean()), 5),
        "karnataka_records": int((df["state_code"] == "KA").sum()),
        "period": period(df["registration_date"]),
        "columns": list(df.columns[:8]),
    }


def shops_stats(df: pd.DataFrame) -> dict:
    nob = df["nature_of_business"].fillna("").str.upper().str.strip()
    return {
        "records": int(len(df)),
        "columns": 17,
        "wards": int(df["ward_name"].nunique()),
        "registration_period": period(df["registration_date"]),
        "address_has_pin_share": round(float(df["pin"].notna().mean()), 4),
        "nature_of_business_distinct": int(nob.nunique()),
        "nature_of_business_most_frequent": nob.value_counts().index[0],
        "nature_of_business_most_frequent_count": int(nob.value_counts().iloc[0]),
    }


def licence_stats(df: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
    df = df.copy()
    df["name_key"] = norm_name(df["Trade Name"])
    df["addr_text"] = collapse_ws_upper(df["address_full"])
    groups = df.groupby(["name_key", "pin"])
    sizes = groups.size()
    multi_keys = sizes[sizes >= 2].index
    multi = df.set_index(["name_key", "pin"]).loc[multi_keys].reset_index()
    varies = multi.groupby(["name_key", "pin"])["addr_text"].nunique()
    stats = {
        "filings": int(len(df)),
        "renewals": int((df["Application Type"] == "Renewal").sum()),
        "period": period(df["Date"]),
        "businesses_name_pin": int(groups.ngroups),
        "businesses_with_2plus_filings": int(len(multi_keys)),
        "address_text_varies": int((varies > 1).sum()),
        "address_text_varies_share": round(float((varies > 1).mean()), 4),
        "definition": "business = (normalised trade name, PIN); address text = door number + street + area, upper-cased, whitespace collapsed",
    }
    return stats, df


def rera_stats(df: pd.DataFrame) -> dict:
    pan = df["pan_clean"]
    gst = df["promoter_gstin"].fillna("").str.upper().str.strip()
    has_gst = gst != ""
    with_key = df[df["promoter_key"].notna()]
    counts = with_key.groupby("promoter_key").size()
    multi = with_key[with_key["promoter_key"].isin(counts[counts >= 2].index)]
    variation = {}
    for attr in RERA_ATTRS:
        sub = multi.assign(v=norm_text(multi[attr]))
        sub = sub[sub["v"] != ""]
        per = sub.groupby("promoter_key")["v"].agg(["size", "nunique"])
        eligible = per[per["size"] >= 2]
        variation[attr] = {
            "promoters_with_attribute_in_2plus_filings": int(len(eligible)),
            "promoters_where_it_varies": int((eligible["nunique"] > 1).sum()),
            "share": round(float((eligible["nunique"] > 1).mean()), 4) if len(eligible) else None,
        }
    return {
        "projects": int(len(df)),
        "period": period(df["registration_date"]),
        "pan_present_share": round(float((pan != "").mean()), 4),
        "pan_well_formed_share": round(float(pan[pan != ""].str.fullmatch(PAN_RE).mean()), 4),
        "promoters_by_pan": int(counts.size),
        "promoters_with_2plus_projects": int((counts >= 2).sum()),
        "projects_of_those_promoters": int(len(multi)),
        "gstin_stated": int(has_gst.sum()),
        "gstin_malformed_share": round(float(1 - gst[has_gst].str.fullmatch(GSTIN_RE).mean()), 4),
        "gstin_not_containing_pan_share": round(float((gst[has_gst].str[2:12] != pan[has_gst]).mean()), 4),
        "variation_same_pan": variation,
        "normalisation": "lower case, everything except letters and digits removed",
    }


def cross_registry(lic: pd.DataFrame, shops: pd.DataFrame, company: pd.DataFrame) -> dict:
    lk = lic[lic["name_key"].str.len() >= 5].drop_duplicates(["name_key", "pin"])
    sh = shops.assign(name_key=norm_name(shops["establishment_name"]))
    sk = sh[sh["name_key"].str.len() >= 5]
    m = lk.merge(sk[["name_key", "pin", "address", "nature_of_business"]], on=["name_key", "pin"])
    matched = m.drop_duplicates(["name_key", "pin"])
    ka = company[company["state_code"] == "KA"].assign(name_key=lambda d: norm_name(d["company_name"]))
    ka = ka[ka["name_key"].str.len() >= 6]
    name_only = ka.merge(sk[["name_key"]].drop_duplicates(), on="name_key")
    return {
        "licence_businesses_considered": int(len(lk)),
        "licence_matched_to_shops_name_pin": int(len(matched)),
        "licence_matched_share": round(len(matched) / len(lk), 4),
        "ka_companies_considered": int(len(ka)),
        "ka_companies_name_only_matches_in_shops": int(name_only["cin"].nunique()),
        "note": "name-only linking over-matches; treat as an upper bound",
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    for k, v in DEFAULT_PATHS.items():
        ap.add_argument(f"--{k}", default=str(v))
    a = ap.parse_args()

    company = load_company(a.company)
    shops = load_shops(a.shops)
    lic_stats, lic = licence_stats(load_licences(a.licences))
    rera = load_rera(a.rera)

    out = {
        "company_registrations": company_stats(company),
        "shops_establishments": shops_stats(shops),
        "trade_licences": lic_stats,
        "rera": rera_stats(rera),
        "cross_registry": cross_registry(lic, shops, company),
    }
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "section3.json").write_text(json.dumps(out, indent=2, default=str))

    DERIVED.mkdir(parents=True, exist_ok=True)
    rera.drop(columns=["promoter_pan", "pan_clean"]).to_csv(DERIVED / "rera_clean.csv", index=False)
    lic.to_csv(DERIVED / "trade_licences_clean.csv", index=False)
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
