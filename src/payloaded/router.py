"""Conditional routing and rule evaluation engine."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple
import pandas as pd

from payloaded.engine import HierarchyEngine, _match_column_name
from payloaded.expressions import GeneratorContext
from payloaded.models import ConditionConfig, PayloadConfig
from payloaded.template import PayloadTemplate


def _eval_rule(cell_val: str, rules: List[str]) -> bool:
    """Evaluate whether a stripped cell value matches a condition rule list.

    Rules:
    - Empty rules list matches all rows unconditionally.
    - Entries starting with '~' are negation rules (e.g. '~Terminated' matches cell_val != 'Terminated').
    - Non-negated entries are exact matches (e.g. 'New' matches cell_val == 'New').
    - Preserves exact case, trims whitespace.
    """
    if not rules:
        return True

    negations = [r[1:].strip() for r in rules if r.startswith("~")]
    positives = [r for r in rules if not r.startswith("~")]

    # Check negations: cell_val must NOT equal any negation target
    for neg in negations:
        if cell_val == neg:
            return False

    # Check positives: if positive rules exist, cell_val must match at least one
    if positives:
        return cell_val in positives

    return True


class ConditionRouter:
    """Routes DataFrame rows to appropriate condition engines and partitions by value."""

    def __init__(self, config: PayloadConfig, default_template: Optional[Any] = None):
        self.config = config
        self.default_template = default_template
        self.gen_context = GeneratorContext()

        # Validate that conditions exist
        if not self.config.conditions:
            raise ValueError(
                "PayloadConfig contains no conditions or entities. "
                "Specify at least one condition in 'conditions'."
            )

    def process_dataframe(
        self,
        df: pd.DataFrame,
    ) -> List[Tuple[Any, int, str, str]]:
        """Evaluate conditions and process DataFrame into payload tuples.

        Returns:
            List of tuples: (payload_data, rows_in_payload, condition_rule, condition_value)
        """
        if df.empty:
            return []

        df_cols = list(df.columns)
        cond_source_key = self.config.condition_source_key

        # ---------------------------------------------------------
        # Case 1: Unconditional execution (condition_source_key is blank)
        # ---------------------------------------------------------
        if cond_source_key is None or str(cond_source_key).strip() == "":
            primary_condition = self.config.conditions[0]
            template_src = primary_condition.payload_template or self.default_template
            if template_src is None:
                raise ValueError("No payload_template provided in condition or call.")
            template = PayloadTemplate(template_src)
            engine = HierarchyEngine(primary_condition.entities, template, gen_context=self.gen_context)
            results = engine.process_dataframe(df)
            rule_str = ", ".join(primary_condition.condition_rule)
            return [(data, count, rule_str, "") for data, count in results]

        # ---------------------------------------------------------
        # Case 2: Conditional execution
        # ---------------------------------------------------------
        matched_col = _match_column_name(cond_source_key, df_cols)
        if matched_col is None:
            raise KeyError(
                f"condition_source_key '{cond_source_key}' was not found in DataFrame columns: {df_cols}"
            )

        claimed_indices: Set[Any] = set()
        payload_records: List[Tuple[Any, int, str, str]] = []

        for cond_idx, condition in enumerate(self.config.conditions):
            rules = condition.condition_rule
            rule_label = ", ".join(rules) if rules else ""

            # Filter remaining unclaimed rows that match this condition's rule
            unclaimed_mask = ~df.index.isin(claimed_indices)
            candidate_df = df[unclaimed_mask]
            if candidate_df.empty:
                continue

            matching_row_indices = []
            for idx, val in candidate_df[matched_col].items():
                cell_val = "" if pd.isna(val) else str(val).strip()
                if _eval_rule(cell_val, rules):
                    matching_row_indices.append(idx)

            if not matching_row_indices:
                continue

            # Claim these rows
            claimed_indices.update(matching_row_indices)
            matched_df = df.loc[matching_row_indices]

            # Sub-partition matched rows by their distinct condition values
            # (e.g. partition 'New' rows separately from 'Update' rows)
            template_src = condition.payload_template or self.default_template
            if template_src is None:
                raise ValueError(
                    f"Condition {cond_idx + 1} ({rule_label or 'unconditional'}) has no payload_template."
                )
            template = PayloadTemplate(template_src)
            engine = HierarchyEngine(condition.entities, template, gen_context=self.gen_context)

            # Preserve source order of distinct values
            distinct_values = matched_df[matched_col].dropna().unique().tolist()
            # If all were null/NaN, handle empty
            if not distinct_values:
                distinct_values = [""]

            for dist_val in distinct_values:
                dist_str = str(dist_val).strip()
                # Slice rows with this exact value
                sub_mask = matched_df[matched_col].astype(str).str.strip() == dist_str
                sub_df = matched_df[sub_mask]
                if sub_df.empty:
                    continue

                engine_results = engine.process_dataframe(sub_df)
                for data, count in engine_results:
                    payload_records.append((data, count, rule_label, dist_str))

        # Check for unrouted/unclaimed rows
        unclaimed_count = len(df) - len(claimed_indices)
        if unclaimed_count > 0:
            unclaimed_mask = ~df.index.isin(claimed_indices)
            unmatched_sample = df.loc[unclaimed_mask, matched_col].dropna().unique().tolist()
            raise ValueError(
                f"{unclaimed_count} row(s) did not match any condition in 'conditions'. "
                f"Unmatched values found in column '{matched_col}': {unmatched_sample}. "
                "Ensure your conditions cover all rows (e.g. using a negation '~' or catch-all rule)."
            )

        return payload_records
