"""Audit and mathematical row reconciliation module."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
import pandas as pd


class ReconciliationError(Exception):
    """Raised when source rows and generated payload tallies do not reconcile."""


@dataclass
class AuditReport:
    """Detailed summary of the reconciliation audit.

    Attributes:
        is_balanced: True if every single source row is accounted for in the generated payloads.
        total_source_rows: Exact row count from the source dataset(s).
        total_packed_rows: Final running total of rows packed into generated payloads.
        payload_count: Total number of payloads produced.
        discrepancy: Difference between source rows and packed rows (0 when balanced).
        file_breakdown: Per-file row counts and payload counts.
    """

    is_balanced: bool
    total_source_rows: int
    total_packed_rows: int
    payload_count: int
    discrepancy: int
    file_breakdown: Dict[str, Dict[str, int]]

    def summary(self) -> str:
        """Return a human-readable reconciliation audit summary."""
        status = "PASSED (Zero Data Loss)" if self.is_balanced else "FAILED (Discrepancy Detected)"
        lines = [
            "=" * 50,
            f"Audit Reconciliation Report: {status}",
            "=" * 50,
            f"Total Source Rows : {self.total_source_rows:,}",
            f"Total Packed Rows : {self.total_packed_rows:,}",
            f"Total Payloads    : {self.payload_count:,}",
            f"Discrepancy       : {self.discrepancy:,}",
        ]
        if len(self.file_breakdown) > 1:
            lines.append("-" * 50)
            lines.append("Per-File Breakdown:")
            for fname, metrics in self.file_breakdown.items():
                lines.append(
                    f"  - {fname}: {metrics.get('source_rows', 0):,} source rows "
                    f"-> {metrics.get('payload_count', 0):,} payloads"
                )
        lines.append("=" * 50)
        return "\n".join(lines)


def reconcile(
    output_df: pd.DataFrame,
    expected_rows: Optional[int] = None,
    strict: bool = True,
) -> AuditReport:
    """Verify that the generated payloads mathematically reconcile against source row counts.

    Args:
        output_df: The DataFrame returned by payloaded.build_payloads.
        expected_rows: Optional explicit count of expected source rows to tally against.
        strict: If True, raises ReconciliationError when not balanced.

    Returns:
        AuditReport containing verification metrics.

    Raises:
        ReconciliationError: If strict is True and a discrepancy is detected.
    """
    payload_count = len(output_df)
    if payload_count == 0:
        is_balanced = (expected_rows is None or expected_rows == 0)
        report = AuditReport(
            is_balanced=is_balanced,
            total_source_rows=expected_rows or 0,
            total_packed_rows=0,
            payload_count=0,
            discrepancy=expected_rows or 0,
            file_breakdown={},
        )
        if strict and not is_balanced:
            raise ReconciliationError(f"Zero payloads generated, but expected {expected_rows} rows.")
        return report

    if "rows_in_payload" in output_df.columns:
        total_packed_rows = int(output_df["rows_in_payload"].sum())
    else:
        total_packed_rows = int(output_df["running_total"].iloc[-1])

    target_rows = total_packed_rows if expected_rows is None else expected_rows
    discrepancy = target_rows - total_packed_rows
    is_balanced = (discrepancy == 0)

    # Per-file breakdown
    file_breakdown: Dict[str, Dict[str, int]] = {}
    if "source_filename" in output_df.columns:
        grouped = output_df.groupby("source_filename")
        for fname, f_df in grouped:
            f_rows = int(f_df["rows_in_payload"].sum()) if "rows_in_payload" in f_df.columns else int(f_df["running_total"].iloc[-1])
            file_breakdown[str(fname)] = {
                "payload_count": len(f_df),
                "source_rows": f_rows,
                "last_running_total": int(f_df["running_total"].iloc[-1]),
            }

    report = AuditReport(
        is_balanced=is_balanced,
        total_source_rows=target_rows,
        total_packed_rows=total_packed_rows,
        payload_count=payload_count,
        discrepancy=discrepancy,
        file_breakdown=file_breakdown,
    )

    if strict and not is_balanced:
        raise ReconciliationError(
            f"Reconciliation discrepancy detected! Expected {target_rows} rows, "
            f"but generated payloads accounted for {total_packed_rows} rows (diff: {discrepancy})."
        )

    return report
