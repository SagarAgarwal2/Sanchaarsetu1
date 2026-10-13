"""Build the field-labelling workbook for the matching evaluation (Table 5).

    python -m eval.make_label_sheet            -> results/field_labelling.xlsx

One row per column of the four registries. Each annotator fills their own
column independently with a concept from the "concepts" sheet (or "none" if
the field has no counterpart in another registry). Do not look at each other's
column until both are done; then fill "concept_final" by discussion.

Sample values are shown only for fields that cannot identify a person, and
are passed through the same PII scrubber the system uses.
"""
from __future__ import annotations

import argparse
import re

import pandas as pd

from app.transform import scrub_pii
from eval.common import DEFAULT_PATHS, RESULTS

REGISTRY_FILES = {
    "company": ("company", {}),
    "shops": ("shops", {}),
    "licences": ("licences", {}),
    "rera": ("rera", {"low_memory": False}),
}

# Field labels printed on Form A (Karnataka Shops and Commercial Establishments Rules).
FORM_A = {
    "registration_no": "Registration certificate number",
    "establishment_name": "Name of the establishment",
    "nature_of_business": "Nature of business",
    "address": "Postal address of the establishment",
    "employees_male": "Number of employees: men",
    "employees_female": "Number of employees: women",
    "employees_total": "Number of employees: total",
    "registration_fee_rs": "Particulars of fees remitted: amount",
    "fee_payment_date": "Particulars of fees remitted: date",
    "registration_date": "Date of registration",
    "ward_name": "Ward",
    "ward_number": "Ward number",
}

PERSONAL = re.compile(r"(pan|gstin|bank|account|ifsc|signatory|phone|mobile|email|photo|link|certificate|aadhaar|"
                      r"promoter_name|promoter_address|address|name$|^trade name$|owner|occupier|applicant|proprietor|"
                      r"latitude|longitude|schedule|survey)", re.I)

CONCEPTS = [
    ("business_name", "Registered or trade name of the business or project"),
    ("legal_form", "Company class, type or form of organisation"),
    ("business_activity", "What the business does (activity, trade, project type)"),
    ("address_full", "Complete address in one field"),
    ("address_door", "Door, plot or building number"),
    ("address_street", "Street or road"),
    ("address_area", "Locality or area"),
    ("pin_code", "Postal PIN code"),
    ("district", "District"),
    ("taluk", "Taluk or sub-district"),
    ("ward", "Municipal ward name or number"),
    ("registration_number", "The registry's own identifier for the record"),
    ("cin", "Corporate Identification Number"),
    ("registration_date", "Date of registration or incorporation"),
    ("validity_date", "Date until which the registration or licence is valid"),
    ("owner_name", "Proprietor, promoter, partner or director"),
    ("signatory_name", "Authorised signatory"),
    ("signatory_address", "Address of the authorised signatory"),
    ("pan", "Permanent account number"),
    ("gstin", "GST identification number"),
    ("employees_total", "Total number employed"),
    ("fee_amount", "Fee or amount paid"),
    ("status", "Status of the registration or project"),
    ("none", "No counterpart in any other registry"),
]


def infer_type(s: pd.Series) -> str:
    v = s.dropna().astype(str).head(200)
    if v.empty:
        return "empty"
    if pd.to_numeric(v, errors="coerce").notna().mean() > 0.9:
        return "number"
    if v.str.fullmatch(r"\d{4}-\d{2}-\d{2}.*|\d{2}[-/]\d{2}[-/]\d{4}.*").mean() > 0.9:
        return "date"
    return "text"


def samples(s: pd.Series, field: str) -> str:
    if PERSONAL.search(field):
        return ""
    vals = s.dropna().astype(str).str.strip()
    vals = vals[vals != ""].drop_duplicates().head(3)
    return " | ".join(str(scrub_pii(x))[:60] for x in vals)


def main() -> None:
    ap = argparse.ArgumentParser()
    for k, v in DEFAULT_PATHS.items():
        ap.add_argument(f"--{k}", default=str(v))
    ap.add_argument("--out", default=str(RESULTS / "field_labelling.xlsx"))
    a = ap.parse_args()

    rows = []
    for reg, (arg, kw) in REGISTRY_FILES.items():
        df = pd.read_csv(getattr(a, arg), dtype=str, nrows=5000, **kw)
        for col in df.columns:
            rows.append({
                "field_id": f"{reg}:{col}",
                "registry": reg,
                "field_name": col,
                "description": FORM_A.get(col, "") if reg == "shops" else "",
                "data_type": infer_type(df[col]),
                "sample_values": samples(df[col], col),
                "annotator_1": "",
                "annotator_2": "",
                "concept_final": "",
                "notes": "",
            })
    sheet = pd.DataFrame(rows)
    RESULTS.mkdir(exist_ok=True)
    with pd.ExcelWriter(a.out) as xw:
        sheet.to_excel(xw, sheet_name="fields", index=False)
        pd.DataFrame(CONCEPTS, columns=["concept", "meaning"]).to_excel(xw, sheet_name="concepts", index=False)
        pd.DataFrame({"instructions": [
            "Label every row of 'fields' with one concept from 'concepts', or 'none'.",
            "Each annotator fills only their own column and does not look at the other's until both are done.",
            "A field split across several fields elsewhere (e.g. one address vs door/street/area) gets its own concept: address_full vs address_door etc.",
            "If no concept fits but the field does have a counterpart in another registry, add a new concept to 'concepts' and tell the other annotator its name only.",
            "After both are done, compare, discuss disagreements, and fill 'concept_final'.",
        ]}).to_excel(xw, sheet_name="instructions", index=False)
    print(f"{len(sheet)} fields written to {a.out}")
    print(sheet.groupby("registry").size().to_string())


if __name__ == "__main__":
    main()
