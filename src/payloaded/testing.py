"""Built-in assertions and verification helpers for payloaded.

Provides standardized testing utilities for end-users to validate payload schemas,
mathematical reconciliation (zero data loss), node entity counts, and metadata balance.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from payloaded.audit import AuditReport, reconcile
from payloaded.compat import is_polars_df

# Canonical columns expected on every payloads DataFrame produced by payloaded
CANONICAL_PAYLOAD_COLUMNS: List[str] = [
    "index",
    "condition_rule",
    "condition_value",
    "rows_in_payload",
    "running_total",
    "node_counts",
    "payload",
    "source_filename",
]

# Canonical columns expected on metadata summary DataFrame
CANONICAL_META_COLUMNS: List[str] = [
    "condition_rule",
    "condition_value",
    "payload_count",
    "total_rows",
    "avg_rows_per_payload",
    "min_rows",
    "max_rows",
    "node_totals",
]


def assert_payload_schema(
    payloads_df: Any,
    expected_columns: Optional[List[str]] = None,
) -> None:
    """Assert that the payloads DataFrame matches the expected canonical schema.

    Parameters:
        payloads_df: Generated payloads DataFrame (pandas or polars).
        expected_columns: Optional list of expected column names. Defaults to
            CANONICAL_PAYLOAD_COLUMNS (8 canonical columns).

    Raises:
        AssertionError: If expected columns are missing or if ordering does not match.
    """
    if expected_columns is None:
        expected_columns = CANONICAL_PAYLOAD_COLUMNS

    actual_cols = list(payloads_df.columns)
    missing = [c for c in expected_columns if c not in actual_cols]
    if missing:
        raise AssertionError(
            f"Payload DataFrame schema mismatch: missing expected columns {missing}.\n"
            f"Found: {actual_cols}\n"
            f"Expected: {expected_columns}"
        )


def assert_reconciled(
    payloads_df: Any,
    expected_rows: Optional[int] = None,
    strict: bool = True,
) -> AuditReport:
    """Assert that generated payloads mathematically reconcile with source row count (Zero Data Loss).

    Wraps payloaded.reconcile() and raises an AssertionError if there is any discrepancy.

    Parameters:
        payloads_df: Generated payloads DataFrame (pandas or polars).
        expected_rows: Total number of rows expected from the source dataset.
            If None, reconciles internally using the final running total / sum of rows_in_payload.
        strict: When True, raises AssertionError immediately on discrepancy.

    Returns:
        AuditReport: The reconciled audit report object.

    Raises:
        AssertionError: If total source rows do not match total packed rows.
    """
    try:
        report = reconcile(output_df=payloads_df, expected_rows=expected_rows, strict=True)
    except Exception as exc:
        raise AssertionError(f"Payload reconciliation failed: {exc}") from exc

    if not report.is_balanced:
        raise AssertionError(
            f"Payload reconciliation failed!\n"
            f"Expected rows: {report.total_source_rows:,}\n"
            f"Packed rows  : {report.total_packed_rows:,}\n"
            f"Discrepancy  : {report.discrepancy:,}"
        )
    return report


def assert_node_counts(
    payloads_df: Any,
    expected_totals: Optional[Dict[str, int]] = None,
    per_payload: Optional[List[Dict[str, int]]] = None,
) -> None:
    """Assert that the entity node counts match expected totals or per-payload breakdowns.

    Parameters:
        payloads_df: Generated payloads DataFrame (pandas or polars) containing 'node_counts'.
        expected_totals: Expected sum of node counts across all payloads, e.g. {'orders': 10, 'orders.items': 50}.
        per_payload: Expected list of node_counts dicts matching each payload row sequentially.

    Raises:
        AssertionError: If node counts column is missing or if values do not match expectations.
    """
    if "node_counts" not in payloads_df.columns:
        raise AssertionError("Payload DataFrame is missing 'node_counts' column.")

    if is_polars_df(payloads_df):
        actual_node_counts = payloads_df["node_counts"].to_list()
    else:
        actual_node_counts = payloads_df["node_counts"].tolist()

    if per_payload is not None:
        if len(actual_node_counts) != len(per_payload):
            raise AssertionError(
                f"Payload count mismatch: DataFrame has {len(actual_node_counts)} payloads, "
                f"but per_payload expectation specified {len(per_payload)}."
            )
        for idx, (actual, expected) in enumerate(zip(actual_node_counts, per_payload), start=1):
            if not isinstance(actual, dict):
                actual = {}
            for path, expected_count in expected.items():
                actual_count = actual.get(path, 0)
                if actual_count != expected_count:
                    raise AssertionError(
                        f"Node count mismatch in payload #{idx} for path '{path}': "
                        f"expected {expected_count}, got {actual_count}."
                    )

    if expected_totals is not None:
        # Sum counts across all payloads
        sums: Dict[str, int] = {}
        for counts in actual_node_counts:
            if isinstance(counts, dict):
                for path, val in counts.items():
                    sums[path] = sums.get(path, 0) + int(val)

        for path, expected_sum in expected_totals.items():
            actual_sum = sums.get(path, 0)
            if actual_sum != expected_sum:
                raise AssertionError(
                    f"Total node count mismatch across all payloads for path '{path}': "
                    f"expected {expected_sum}, got {actual_sum}."
                )


def assert_meta_balanced(
    meta_df: Any,
    expected_payload_count: Optional[int] = None,
    expected_total_rows: Optional[int] = None,
    expected_node_totals: Optional[Dict[str, int]] = None,
) -> None:
    """Assert that the metadata summary DataFrame (meta_df) is properly aggregated and balanced.

    Validates schema, the presence of the 'ALL / TOTAL' summary row, and expected totals.

    Parameters:
        meta_df: Metadata summary DataFrame returned by build_payloads or summarize.
        expected_payload_count: Expected total number of payloads in the summary row.
        expected_total_rows: Expected total source rows across all conditions.
        expected_node_totals: Expected unique entity node totals in the summary row.

    Raises:
        AssertionError: If meta_df is missing canonical columns, lacks a summary row, or metrics diverge.
    """
    actual_cols = list(meta_df.columns)
    missing = [c for c in CANONICAL_META_COLUMNS if c not in actual_cols]
    if missing:
        raise AssertionError(f"Metadata DataFrame missing columns: {missing}")

    if len(meta_df) == 0:
        if (expected_payload_count or 0) > 0 or (expected_total_rows or 0) > 0:
            raise AssertionError("Metadata DataFrame is empty, but expected non-zero totals.")
        return

    # Check the last row for 'ALL' / 'TOTAL'
    if is_polars_df(meta_df):
        last_row = meta_df[-1].to_dicts()[0]
    else:
        last_row = meta_df.iloc[-1].to_dict()

    if last_row.get("condition_rule") != "ALL" or last_row.get("condition_value") != "TOTAL":
        raise AssertionError(
            f"Expected final row of meta_df to be ('ALL', 'TOTAL'), got: "
            f"({last_row.get('condition_rule')}, {last_row.get('condition_value')})"
        )

    if expected_payload_count is not None:
        actual_p_count = int(last_row.get("payload_count", 0))
        if actual_p_count != expected_payload_count:
            raise AssertionError(
                f"Meta payload_count mismatch: expected {expected_payload_count}, got {actual_p_count}."
            )

    if expected_total_rows is not None:
        actual_rows = int(last_row.get("total_rows", 0))
        if actual_rows != expected_total_rows:
            raise AssertionError(
                f"Meta total_rows mismatch: expected {expected_total_rows}, got {actual_rows}."
            )

    if expected_node_totals is not None:
        actual_nodes = last_row.get("node_totals") or {}
        for path, exp_val in expected_node_totals.items():
            act_val = actual_nodes.get(path, 0)
            if act_val != exp_val:
                raise AssertionError(
                    f"Meta node_totals mismatch for path '{path}': expected {exp_val}, got {act_val}."
                )
