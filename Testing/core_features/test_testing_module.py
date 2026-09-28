"""Unit tests for the user-facing payloaded.testing verification module."""

import pandas as pd
import pytest

import payloaded as pld
from payloaded.testing import (
    CANONICAL_META_COLUMNS,
    CANONICAL_PAYLOAD_COLUMNS,
    assert_meta_balanced,
    assert_node_counts,
    assert_payload_schema,
    assert_reconciled,
)


@pytest.fixture
def sample_payload_data():
    """Generates a small valid payload DataFrame and metadata DataFrame for assertion testing."""
    df_source = pd.DataFrame([
        {"Order_ID": "O1", "Status": "Active", "Item": "A"},
        {"Order_ID": "O1", "Status": "Active", "Item": "B"},
        {"Order_ID": "O2", "Status": "Active", "Item": "C"},
    ])
    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [{"order_id": "{order_id}", "items": [{"item": "{item}"}]}],
                "entities": [
                    {
                        "path": "root",
                        "repeat_limit": 1,
                        "group_by": ["order_id"],
                        "mappings": [{"payload_key": "order_id", "source_key": "Order_ID"}],
                    },
                    {
                        "path": "items",
                        "mappings": [{"payload_key": "item", "source_key": "Item"}],
                    },
                ],
            }
        ],
    }
    payloads_df, meta_df = pld.build_payloads(df_source, config=config, output_format="dict")
    return df_source, payloads_df, meta_df


def test_assert_payload_schema_success(sample_payload_data):
    """Test assert_payload_schema passes with canonical 8 columns."""
    _, payloads_df, _ = sample_payload_data
    assert_payload_schema(payloads_df)
    assert_payload_schema(payloads_df, expected_columns=CANONICAL_PAYLOAD_COLUMNS)


def test_assert_payload_schema_failure():
    """Test assert_payload_schema raises AssertionError when missing columns."""
    bad_df = pd.DataFrame([{"index": 1, "payload": {}}])
    with pytest.raises(AssertionError, match="missing expected columns"):
        assert_payload_schema(bad_df)


def test_assert_reconciled_success(sample_payload_data):
    """Test assert_reconciled passes when row counts balance."""
    df_source, payloads_df, _ = sample_payload_data
    report = assert_reconciled(payloads_df, expected_rows=len(df_source))
    assert report.is_balanced
    assert report.total_source_rows == 3
    assert report.total_packed_rows == 3


def test_assert_reconciled_failure(sample_payload_data):
    """Test assert_reconciled raises AssertionError when row counts mismatch."""
    _, payloads_df, _ = sample_payload_data
    with pytest.raises(AssertionError, match="Payload reconciliation failed"):
        assert_reconciled(payloads_df, expected_rows=999)


def test_assert_node_counts_totals_and_per_payload(sample_payload_data):
    """Test assert_node_counts validates both cumulative sums and per-payload items."""
    _, payloads_df, _ = sample_payload_data

    # Total orders = 2, total items = 3
    assert_node_counts(payloads_df, expected_totals={"items": 3})

    # Per payload expectations:
    # Payload #1 (O1): 2 items
    # Payload #2 (O2): 1 item
    assert_node_counts(
        payloads_df,
        per_payload=[
            {"items": 2},
            {"items": 1},
        ],
    )


def test_assert_node_counts_failure(sample_payload_data):
    """Test assert_node_counts fails with descriptive message on discrepancy."""
    _, payloads_df, _ = sample_payload_data

    with pytest.raises(AssertionError, match="Total node count mismatch"):
        assert_node_counts(payloads_df, expected_totals={"items": 100})

    with pytest.raises(AssertionError, match="Node count mismatch in payload #1"):
        assert_node_counts(payloads_df, per_payload=[{"items": 99}, {"items": 1}])


def test_assert_meta_balanced_success(sample_payload_data):
    """Test assert_meta_balanced validates summary row and counts."""
    _, _, meta_df = sample_payload_data
    assert_meta_balanced(
        meta_df,
        expected_payload_count=2,
        expected_total_rows=3,
        expected_node_totals={"items": 3},
    )


def test_assert_meta_balanced_failure(sample_payload_data):
    """Test assert_meta_balanced raises when totals mismatch."""
    _, _, meta_df = sample_payload_data
    with pytest.raises(AssertionError, match="Meta payload_count mismatch"):
        assert_meta_balanced(meta_df, expected_payload_count=99)

    with pytest.raises(AssertionError, match="Meta total_rows mismatch"):
        assert_meta_balanced(meta_df, expected_total_rows=99)
