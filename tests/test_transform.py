import numpy as np

import app.transform as t
from app.transform import FieldInfo, best_match, scrub_pii


class FakeModel:
    """Deterministic stand-in: bag-of-words vectors, so related words score higher."""
    vocab = ["establishment", "name", "business", "address", "postal", "registered", "employees", "count", "total", "type", "text", "integer"]

    def encode(self, texts, normalize_embeddings=True):
        out = []
        for s in texts:
            words = s.lower().replace(".", " ").replace(":", " ").split()
            v = np.array([sum(w.startswith(k) for w in words) for k in self.vocab], dtype=float) + 1e-6
            out.append(v / np.linalg.norm(v))
        return np.vstack(out)


def test_scrub_pii():
    s = scrub_pii("PAN ABCDE1234F, GSTIN 29ABCDE1234F1Z5, call 9845012345, mail a.b@c.in, 1234 5678 9012")
    assert "ABCDE1234F" not in s and "9845012345" not in s and "a.b@c.in" not in s and "1234 5678 9012" not in s
    assert "[GSTIN]" in s and "[PAN]" in s and "[PHONE]" in s and "[EMAIL]" in s and "[AADHAAR]" in s


def test_field_text_includes_metadata_and_scrubs_samples():
    f = FieldInfo("postal_address", "Postal address of the establishment", "text", ["Call 9845012345"])
    txt = f.as_text()
    assert "postal address" in txt and "type text" in txt and "9845012345" not in txt
    assert f.as_text(use_metadata=False) == "postal address"


def test_embedding_path_and_lexical_fallback(monkeypatch):
    targets = [FieldInfo("postal_address", "Postal address of the establishment", "text"),
               FieldInfo("employees_total", "Number of employees, total", "integer"),
               FieldInfo("establishment_name", "Name of the establishment", "text")]
    src = FieldInfo("registered_address", "Registered office address", "text")
    monkeypatch.setattr(t, "_model", FakeModel())
    name, score = best_match(src, targets, "embedding_full")
    assert name == "postal_address" and 0 < score <= 1.0
    monkeypatch.setattr(t, "_model", None)
    monkeypatch.setattr(t, "_model_failed", True)
    name2, _ = best_match(src, targets, "embedding_full")  # falls back to lexical
    assert name2 in {t_.name for t_ in targets}
