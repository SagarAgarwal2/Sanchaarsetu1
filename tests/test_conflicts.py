from app.conflicts import ConflictPolicy, ConflictResolver, normalise_value, policy_for_field


def make():
    return ConflictResolver(pg_pool=None, detection_window_seconds=60)


def test_broadcast_copies_do_not_conflict():
    r = make()
    payload = {"registered_address": "12 MG Road"}
    for _dept in range(7):
        assert r.detect_conflicts("U1", "SWS", payload, 1000.0, update_id="upd-1") == []


def test_same_source_never_conflicts():
    r = make()
    assert r.detect_conflicts("U1", "SWS", {"registered_address": "A"}, 1000.0, "u1") == []
    assert r.detect_conflicts("U1", "SWS", {"registered_address": "B"}, 1005.0, "u2") == []


def test_formatting_variants_are_not_conflicts():
    r = make()
    r.detect_conflicts("U1", "SWS", {"registered_address": "No. 12, MG Road"}, 1000.0, "u1")
    assert r.detect_conflicts("U1", "DFB", {"registered_address": "NO 12 MG ROAD"}, 1010.0, "u2") == []


def test_different_fields_do_not_conflict():
    r = make()
    r.detect_conflicts("U1", "SWS", {"registered_address": "A"}, 1000.0, "u1")
    assert r.detect_conflicts("U1", "DFB", {"business_name": "X"}, 1010.0, "u2") == []


def test_real_disagreement_is_detected_per_field():
    r = make()
    r.detect_conflicts("U1", "SWS", {"signatory_name": "Priya Sharma", "registered_address": "A"}, 1000.0, "u1")
    found = r.detect_conflicts("U1", "SEA", {"signatory_name": "P. Sharma & Sons", "registered_address": "A"}, 1010.0, "u2")
    assert [c.field_name for c in found] == ["signatory_name"]
    c = found[0]
    assert c.policy == ConflictPolicy.MANUAL_REVIEW
    assert c.sws_value == "Priya Sharma" and c.dept_value == "P. Sharma & Sons"
    assert c.automatic_winner_is_current() is None


def test_outside_window_is_not_a_conflict():
    r = make()
    r.detect_conflicts("U1", "SWS", {"registered_address": "A"}, 1000.0, "u1")
    assert r.detect_conflicts("U1", "DFB", {"registered_address": "B"}, 1100.0, "u2") == []


def test_other_business_is_not_a_conflict():
    r = make()
    r.detect_conflicts("U1", "SWS", {"registered_address": "A"}, 1000.0, "u1")
    assert r.detect_conflicts("U2", "DFB", {"registered_address": "B"}, 1001.0, "u2") == []


def test_sws_wins_policy():
    r = make()
    r.detect_conflicts("U1", "DFB", {"registered_address": "Dept value"}, 1000.0, "u1")
    (c,) = r.detect_conflicts("U1", "SWS", {"registered_address": "SWS value"}, 1001.0, "u2")
    assert c.policy == ConflictPolicy.SWS_WINS
    assert c.automatic_winner_is_current() is True  # current is SWS
    r2 = make()
    r2.detect_conflicts("U1", "SWS", {"registered_address": "SWS value"}, 1000.0, "u1")
    (c2,) = r2.detect_conflicts("U1", "DFB", {"registered_address": "Dept value"}, 1001.0, "u2")
    assert c2.automatic_winner_is_current() is False  # department loses to SWS


def test_last_write_wins_policy():
    assert policy_for_field("employee_count") == ConflictPolicy.LAST_WRITE_WINS
    r = make()
    r.detect_conflicts("U1", "SWS", {"employee_count": 10}, 1000.0, "u1")
    (c,) = r.detect_conflicts("U1", "DOL", {"employee_count": 12}, 1005.0, "u2")
    assert c.automatic_winner_is_current() is True


def test_normalise_value():
    assert normalise_value(" No.12,  MG-Road ") == normalise_value("NO 12 MG ROAD")
    assert normalise_value(None) == ""


def test_only_latest_report_per_source_counts():
    r = make()
    r.detect_conflicts("U1", "SWS", {"signatory_name": "Priya Sharma"}, 1000.0, "u1")
    r.detect_conflicts("U1", "SWS", {"signatory_name": "Anil Kumar"}, 1005.0, "u2")
    assert r.detect_conflicts("U1", "CTD", {"signatory_name": "ANIL  KUMAR."}, 1010.0, "u3") == []


def test_discard_after_decision():
    r = make()
    r.detect_conflicts("U1", "CTD", {"signatory_name": "P. Sharma & Sons"}, 1000.0, "u1")
    assert len(r.detect_conflicts("U1", "SWS", {"signatory_name": "Priya Sharma"}, 1001.0, "u2")) == 1
    r.discard("U1", "signatory_name")
    assert r.detect_conflicts("U1", "SWS", {"signatory_name": "Anil Kumar"}, 1002.0, "u3") == []
