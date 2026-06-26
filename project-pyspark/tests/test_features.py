"""Tests del feature engineering."""
from __future__ import annotations

from olist_seg.features import build_customer_features


def test_grain_is_unique_customer(raw_df):
    feats = build_customer_features(raw_df).collect()
    ids = [r.customer_unique_id for r in feats]
    assert len(ids) == len(set(ids)) == 3  # 3 personas, no 4 órdenes


def test_repeat_customer_flag(raw_df):
    feats = {r.customer_unique_id: r for r in build_customer_features(raw_df).collect()}
    assert feats["personA"].num_orders == 2
    assert feats["personA"].is_repeat_customer == 1
    assert feats["personB"].is_repeat_customer == 0


def test_region_mapping(raw_df):
    feats = {r.customer_unique_id: r for r in build_customer_features(raw_df).collect()}
    assert feats["personA"].primary_region == "Sudeste"
    assert feats["personC"].primary_region == "Sul"


def test_zip_macro_region(raw_df):
    feats = {r.customer_unique_id: r for r in build_customer_features(raw_df).collect()}
    assert feats["personA"].zip_macro_region == 0  # CEP 010xx -> macro 0
    assert feats["personC"].zip_macro_region == 9  # CEP 900xx -> macro 9


def test_market_share_bounded(raw_df):
    feats = build_customer_features(raw_df).collect()
    for r in feats:
        assert 0.0 <= r.state_market_share <= 1.0
