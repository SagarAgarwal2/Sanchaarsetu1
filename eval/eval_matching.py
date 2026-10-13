"""Table 5 and Fig. 2: field-matching evaluation on the labelled registry fields.

    python -m eval.eval_matching --labels results/field_labelling.xlsx

Requires the labelling workbook with "annotator_1", "annotator_2" and
"concept_final" filled. Ground truth: two fields in different registries
correspond when they have the same final concept (other than "none").

For every ordered pair of registries (A, B) and every field a in A, each method
proposes the best-scoring field of B. A proposal counts when its score is at or
above the threshold; it is correct when the two fields share a concept.

    precision = correct proposals / proposals
    recall    = correct proposals / fields of A that have a counterpart in B
    review    = share of fields whose best score lies between the review and
                auto-map thresholds (they would go to an operator)

The embedding methods need the sentence-embedding model. The script stops if
the model cannot be loaded, rather than silently reporting string similarity
under an embedding label; pass --allow-missing-model to report only the baseline.
"""
from __future__ import annotations

import argparse
import itertools
import json

import numpy as np
import pandas as pd

from app import transform as T
from app.transform import FieldInfo, score_candidates
from eval.common import RESULTS

METHODS = {
    "lexical": "String similarity on names (baseline)",
    "embedding_name": "Embeddings on names",
    "embedding_full": "Embeddings on names, types and sample values",
}


def cohen_kappa(a: pd.Series, b: pd.Series) -> float:
    a = a.astype(str).str.strip().str.lower()
    b = b.astype(str).str.strip().str.lower()
    labels = sorted(set(a) | set(b))
    idx = {l: i for i, l in enumerate(labels)}
    m = np.zeros((len(labels), len(labels)))
    for x, y in zip(a, b):
        m[idx[x], idx[y]] += 1
    n = m.sum()
    po = np.trace(m) / n
    pe = (m.sum(0) * m.sum(1)).sum() / n**2
    return float((po - pe) / (1 - pe)) if pe < 1 else 1.0


def load_labels(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="fields", dtype=str) if path.endswith(".xlsx") else pd.read_csv(path, dtype=str)
    df = df.fillna("")
    missing = (df["concept_final"].str.strip() == "").sum()
    if missing:
        raise SystemExit(f"{missing} fields have no concept_final; finish the labelling first.")
    df["concept"] = df["concept_final"].str.strip().str.lower()
    return df


def field_info(r: pd.Series) -> FieldInfo:
    samples = [s for s in str(r.get("sample_values", "")).split(" | ") if s]
    return FieldInfo(r["field_name"], r.get("description") or None, r.get("data_type") or None, samples)


def best_scores(df: pd.DataFrame, method: str) -> pd.DataFrame:
    """For each (field a, registry B) the best target in B and its score."""
    out = []
    regs = sorted(df["registry"].unique())
    infos = {r["field_id"]: field_info(r) for _, r in df.iterrows()}
    for ra, rb in itertools.permutations(regs, 2):
        A = df[df["registry"] == ra]
        B = df[df["registry"] == rb]
        targets = [infos[f] for f in B["field_id"]]
        b_ids = list(B["field_id"])
        b_concepts = list(B["concept"])
        counterpart = set(c for c in b_concepts if c != "none")
        for _, a in A.iterrows():
            scores = score_candidates(infos[a["field_id"]], targets, method)
            j = int(np.argmax(scores))
            out.append({
                "field": a["field_id"], "target_registry": rb, "best": b_ids[j], "score": float(scores[j]),
                "correct": a["concept"] != "none" and b_concepts[j] == a["concept"],
                "has_counterpart": a["concept"] != "none" and a["concept"] in counterpart,
            })
    return pd.DataFrame(out)


def metrics(bs: pd.DataFrame, upper: float, lower: float) -> dict:
    proposed = bs["score"] >= upper
    tp = int((proposed & bs["correct"]).sum())
    n_prop = int(proposed.sum())
    n_rel = int(bs["has_counterpart"].sum())
    p = tp / n_prop if n_prop else 0.0
    r = tp / n_rel if n_rel else 0.0
    f1 = 2 * p * r / (p + r) if p + r else 0.0
    review = float(((bs["score"] >= lower) & (bs["score"] < upper)).mean())
    return {"precision": round(p, 4), "recall": round(r, 4), "f1": round(f1, 4), "review_share": round(review, 4),
            "proposals": n_prop, "correct": tp, "with_counterpart": n_rel}


def pr_curve(bs: pd.DataFrame) -> list[dict]:
    pts = []
    for t in np.round(np.arange(0.0, 1.0001, 0.01), 2):
        m = metrics(bs, t, t)
        pts.append({"threshold": float(t), "precision": m["precision"], "recall": m["recall"], "proposals": m["proposals"]})
    return pts


def choose_threshold(curve: list[dict], target_precision: float) -> float:
    ok = [c["threshold"] for c in curve if c["proposals"] > 0 and c["precision"] >= target_precision]
    return min(ok) if ok else 1.0


def plot(curves: dict, chosen: dict, path_base) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(4.6, 3.2))
    styles = {"lexical": ("#7a7a7a", "--"), "embedding_name": ("#2b6cb0", "-."), "embedding_full": ("#1a1a1a", "-")}
    for m, pts in curves.items():
        pts = [p for p in pts if p["proposals"] > 0]
        color, ls = styles[m]
        ax.plot([p["recall"] for p in pts], [p["precision"] for p in pts], ls, color=color, lw=1.4, label=METHODS[m])
        c = next(p for p in pts if p["threshold"] >= chosen[m]) if any(p["threshold"] >= chosen[m] for p in pts) else pts[-1]
        ax.plot(c["recall"], c["precision"], "o", color=color, ms=5)
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1.02)
    ax.grid(alpha=0.3, lw=0.5)
    ax.legend(fontsize=7, loc="lower left", frameon=False)
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(f"{path_base}.{ext}", dpi=300)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--labels", default=str(RESULTS / "field_labelling.xlsx"))
    ap.add_argument("--target-precision", type=float, default=0.95)
    ap.add_argument("--review-threshold", type=float, default=T.REVIEW_THRESHOLD)
    ap.add_argument("--allow-missing-model", action="store_true")
    a = ap.parse_args()

    df = load_labels(a.labels)
    kappa = cohen_kappa(df["annotator_1"], df["annotator_2"])

    methods = list(METHODS)
    if T.get_model() is None:
        if not a.allow_missing_model:
            raise SystemExit("Embedding model could not be loaded; run where it can be downloaded, "
                             "or pass --allow-missing-model to report the baseline only.")
        methods = ["lexical"]

    results, curves, chosen = {}, {}, {}
    for m in methods:
        bs = best_scores(df, m)
        curves[m] = pr_curve(bs)
        chosen[m] = choose_threshold(curves[m], a.target_precision)
        results[m] = {"label": METHODS[m], "operating_threshold": chosen[m],
                      **metrics(bs, chosen[m], min(a.review_threshold, chosen[m]))}
        bs.to_csv(RESULTS / f"matching_proposals_{m}.csv", index=False)

    concept_fields = df[df["concept"] != "none"]
    pairs = sum(len(g) * (len(g) - 1) // 2 for _, g in concept_fields.groupby("concept")
                if g["registry"].nunique() > 1)
    out = {
        "fields": int(len(df)),
        "fields_with_a_concept": int(len(concept_fields)),
        "corresponding_pairs_across_registries": int(pairs),
        "cohen_kappa": round(kappa, 4),
        "model": T.MATCHER_BACKEND,
        "target_precision": a.target_precision,
        "methods": results,
        "pr_curves": curves,
    }
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "table5_matching.json").write_text(json.dumps(out, indent=2))
    plot(curves, chosen, RESULTS / "fig2_pr_curves")
    print(json.dumps({k: v for k, v in out.items() if k != "pr_curves"}, indent=2))
    print("\nSet AUTO_MAP_THRESHOLD in docker-compose.yml to the embedding_full operating_threshold.")


if __name__ == "__main__":
    main()
