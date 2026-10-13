"""Checks the matching evaluation's arithmetic on a tiny hand-made label set.

The labels here are invented test fixtures, not study data.
"""
import pandas as pd

from eval.eval_matching import best_scores, choose_threshold, cohen_kappa, metrics, pr_curve


def toy():
    rows = [
        ("a:company_name", "a", "company_name", "business_name"),
        ("a:reg_date", "a", "reg_date", "registration_date"),
        ("a:internal_id", "a", "internal_id", "none"),
        ("b:company_name", "b", "company_name", "business_name"),
        ("b:registration_date", "b", "registration_date", "registration_date"),
        ("b:ward", "b", "ward", "ward"),
    ]
    df = pd.DataFrame(rows, columns=["field_id", "registry", "field_name", "concept"])
    df["description"] = ""
    df["data_type"] = ""
    df["sample_values"] = ""
    return df


def test_kappa_perfect_and_chance():
    a = pd.Series(["x", "y", "x", "y"])
    assert cohen_kappa(a, a) == 1.0
    assert abs(cohen_kappa(pd.Series(["x", "x", "y", "y"]), pd.Series(["x", "y", "x", "y"]))) < 1e-9


def test_metrics_on_toy_labels():
    bs = best_scores(toy(), "lexical")
    m = metrics(bs, upper=0.99, lower=0.5)
    # Only identical names (company_name <-> company_name, both directions) score 1.0.
    assert m["proposals"] == 2 and m["correct"] == 2 and m["precision"] == 1.0
    # Fields with a counterpart: company_name and reg_date (a->b), company_name and registration_date (b->a).
    assert m["with_counterpart"] == 4 and m["recall"] == 0.5
    curve = pr_curve(bs)
    assert choose_threshold(curve, 0.95) <= 0.99
