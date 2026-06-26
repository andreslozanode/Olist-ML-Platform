"""Test de smoke del entrenamiento/scoring con un k pequeño."""
from __future__ import annotations

from olist_seg.config import ModelParams
from olist_seg.features import build_customer_features
from olist_seg.score import score
from olist_seg.train import _build_pipeline


def test_pipeline_fits_and_scores(raw_df):
    feats = build_customer_features(raw_df)
    params = ModelParams(k=2, k_search_min=2, k_search_max=2, max_iter=5)
    model = _build_pipeline(params.features, k=2, max_iter=5, seed=1).fit(feats)
    result = score(model, feats, "test")
    rows = result.collect()
    assert len(rows) == 3
    assert all(r.segment_id is not None for r in rows)
    assert all(r.segment_label is not None for r in rows)
    assert {r.model_version for r in rows} == {"test"}
