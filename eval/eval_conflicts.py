"""Table 6: conflict detection on real filing pairs from the RERA registry.

Step 1, make a sample for a person to label:
    python -m eval.eval_conflicts --make-sample 300
    -> results/conflict_pairs_to_label.csv; fill "label" with "same" or "different"
       (same = the two values describe the same thing, e.g. only formatting differs)

Step 2, evaluate:
    python -m eval.eval_conflicts --labels results/conflict_pairs_to_label.csv

Each labelled pair is replayed as two updates from different sources inside the
detection window, through the system's own ConflictResolver. Three rules are
compared:

  entity-level window  any two updates to the same business conflict (the original rule)
  exact comparison     field-level, but values compared character for character
  field-level          field-level with normalisation (the system's rule)

"Unrelated fields" pairs are built from the same filings: one update carries
attribute X, the other attribute Y, so no rule should flag them.
"""
from __future__ import annotations

import argparse
import itertools
import json

import pandas as pd

from app.conflicts import ConflictResolver
from eval.common import DEFAULT_PATHS, RESULTS, load_rera, norm_text

ATTRS = ["promoter_name", "promoter_address", "signatory_name", "signatory_address", "promoter_gstin", "promoter_pin_code"]


def filing_pairs(rera: pd.DataFrame) -> pd.DataFrame:
    """Consecutive filings (by registration date) of each promoter with two or more projects."""
    df = rera[rera["promoter_key"].notna()].sort_values(["promoter_key", "registration_date", "project_id"])
    rows = []
    for key, g in df.groupby("promoter_key"):
        if len(g) < 2:
            continue
        recs = g.to_dict("records")
        for i, (r1, r2) in enumerate(zip(recs, recs[1:])):
            for attr in ATTRS:
                v1, v2 = r1.get(attr), r2.get(attr)
                if not isinstance(v1, str) or not isinstance(v2, str) or not v1.strip() or not v2.strip():
                    continue
                rows.append({"pair_id": f"{key}:{i}:{attr}", "promoter_key": key, "attribute": attr,
                             "value_1": v1.strip(), "value_2": v2.strip()})
    pairs = pd.DataFrame(rows)
    n1, n2 = norm_text(pairs["value_1"]), norm_text(pairs["value_2"])
    pairs["auto_category"] = "substantive"
    pairs.loc[n1 == n2, "auto_category"] = "formatting_variant"
    pairs.loc[pairs["value_1"] == pairs["value_2"], "auto_category"] = "identical"
    return pairs


def flagged_field_level(row) -> bool:
    r = ConflictResolver(pg_pool=None)
    r.detect_conflicts(row["promoter_key"], "SWS", {row["attribute"]: row["value_1"]}, 1000.0, "u1")
    return bool(r.detect_conflicts(row["promoter_key"], "DEPT", {row["attribute"]: row["value_2"]}, 1010.0, "u2"))


def unrelated_pairs(pairs: pd.DataFrame, limit: int = 1000) -> pd.DataFrame:
    rows = []
    for key, g in pairs.groupby("promoter_key"):
        attrs = g.drop_duplicates("attribute")
        for (_, x), (_, y) in itertools.combinations(attrs.iterrows(), 2):
            rows.append({"promoter_key": key, "attr_x": x["attribute"], "value_x": x["value_1"],
                         "attr_y": y["attribute"], "value_y": y["value_2"]})
            if len(rows) >= limit:
                return pd.DataFrame(rows)
    return pd.DataFrame(rows)


def flagged_unrelated(row) -> bool:
    r = ConflictResolver(pg_pool=None)
    r.detect_conflicts(row["promoter_key"], "SWS", {row["attr_x"]: row["value_x"]}, 1000.0, "u1")
    return bool(r.detect_conflicts(row["promoter_key"], "DEPT", {row["attr_y"]: row["value_y"]}, 1010.0, "u2"))


def evaluate(lab: pd.DataFrame, unrel: pd.DataFrame, truth_col: str) -> dict:
    diff = lab[truth_col] == "different"
    same_but_raw_differs = (lab[truth_col] == "same") & (lab["value_1"] != lab["value_2"])
    rules = {
        "entity_level_window": pd.Series(True, index=lab.index),
        "exact_comparison": lab["value_1"] != lab["value_2"],
        "field_level_normalised": lab.apply(flagged_field_level, axis=1),
    }
    unrelated_flags = {
        "entity_level_window": len(unrel),
        "exact_comparison": 0,
        "field_level_normalised": int(unrel.apply(flagged_unrelated, axis=1).sum()) if len(unrel) else 0,
    }
    out = {}
    for name, flags in rules.items():
        out[name] = {
            "substantive_detected": int((flags & diff).sum()),
            "substantive_total": int(diff.sum()),
            "substantive_detected_share": round(float((flags & diff).sum() / diff.sum()), 4) if diff.sum() else None,
            "false_conflicts_formatting": int((flags & same_but_raw_differs).sum()),
            "formatting_total": int(same_but_raw_differs.sum()),
            "false_conflicts_formatting_share": round(float((flags & same_but_raw_differs).sum() / same_but_raw_differs.sum()), 4)
            if same_but_raw_differs.sum() else None,
            "false_conflicts_unrelated": unrelated_flags[name],
            "unrelated_total": int(len(unrel)),
        }
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rera", default=str(DEFAULT_PATHS["rera"]))
    ap.add_argument("--make-sample", type=int, default=0, help="write N pairs for manual labelling")
    ap.add_argument("--labels", default="")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()

    pairs = filing_pairs(load_rera(a.rera))
    summary = {
        "pairs": int(len(pairs)),
        "promoters": int(pairs["promoter_key"].nunique()),
        "by_auto_category": pairs["auto_category"].value_counts().to_dict(),
        "by_attribute": pairs.groupby(["attribute", "auto_category"]).size().unstack(fill_value=0).to_dict("index"),
    }
    RESULTS.mkdir(exist_ok=True)

    if a.make_sample:
        # Oversample the informative cases: non-identical pairs, spread over attributes.
        non_ident = pairs[pairs["auto_category"] != "identical"]
        ident = pairs[pairs["auto_category"] == "identical"]
        n_ident = max(1, a.make_sample // 10)
        per_attr = (a.make_sample - n_ident) // len(ATTRS)
        parts = [g.sample(min(len(g), per_attr), random_state=a.seed) for _, g in non_ident.groupby("attribute")]
        parts.append(ident.sample(min(n_ident, len(ident)), random_state=a.seed))
        sample = pd.concat(parts).sample(frac=1, random_state=a.seed)
        sample = sample[["pair_id", "promoter_key", "attribute", "value_1", "value_2"]].assign(label="", notes="")
        path = RESULTS / "conflict_pairs_to_label.csv"
        sample.to_csv(path, index=False)
        print(f"{len(sample)} pairs written to {path}; fill 'label' with same/different.")
        print(json.dumps(summary, indent=2))
        return

    unrel = unrelated_pairs(pairs)
    if a.labels:
        lab = pd.read_csv(a.labels, dtype=str).fillna("")
        lab["label"] = lab["label"].str.strip().str.lower()
        if not lab["label"].isin(["same", "different"]).all():
            raise SystemExit("Every row needs label 'same' or 'different'.")
        result = {"ground_truth": "manual labels", "labelled_pairs": int(len(lab)),
                  "rules": evaluate(lab, unrel, "label"), "registry_pairs": summary}
        out = RESULTS / "table6_conflicts.json"
    else:
        # Without manual labels, report against the automatic categories, which use the same
        # normalisation as the field-level rule; this is descriptive, not an evaluation.
        lab = pairs.assign(auto_truth=lambda d: d["auto_category"].map(
            {"substantive": "different", "formatting_variant": "same", "identical": "same"}))
        result = {"ground_truth": "automatic categories (circular for the field-level rule; not for the paper)",
                  "rules": evaluate(lab, unrel, "auto_truth"), "registry_pairs": summary}
        out = RESULTS / "conflicts_descriptive.json"
    out.write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
