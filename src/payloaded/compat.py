"""Dataframe compatibility and lazy abstraction layer for pandas and polars."""

from __future__ import annotations

import json
import math
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union

# Lazy cache for polars and pandas module availability
_PANDAS_MOD: Any = None
_POLARS_MOD: Any = None


def get_pandas() -> Any:
    """Lazily import and return the pandas module, or None if not installed."""
    global _PANDAS_MOD
    if _PANDAS_MOD is None:
        try:
            import pandas as pd
            _PANDAS_MOD = pd
        except ImportError:
            _PANDAS_MOD = False
    return _PANDAS_MOD if _PANDAS_MOD is not False else None


def get_polars() -> Any:
    """Lazily import and return the polars module, or None if not installed."""
    global _POLARS_MOD
    if _POLARS_MOD is None:
        try:
            import polars as pl
            _POLARS_MOD = pl
        except ImportError:
            _POLARS_MOD = False
    return _POLARS_MOD if _POLARS_MOD is not False else None


def is_pandas_df(obj: Any) -> bool:
    """Check if an object is an instance of pandas DataFrame without hard dependency."""
    pd = get_pandas()
    return pd is not None and isinstance(obj, pd.DataFrame)


def is_polars_df(obj: Any) -> bool:
    """Check if an object is an instance of polars DataFrame without hard dependency."""
    pl = get_polars()
    return pl is not None and isinstance(obj, pl.DataFrame)


def polars_to_pandas_safe(pl_df: Any) -> Any:
    """Convert a polars DataFrame to pandas DataFrame without requiring pyarrow."""
    pd = get_pandas()
    if pd is None:
        raise ImportError("pandas is required for engine execution.")
    try:
        # Fast path if pyarrow is present
        return pl_df.to_pandas()
    except Exception:
        # Pure Python dict fallback: zero pyarrow dependency!
        return pd.DataFrame(pl_df.to_dict(as_series=False))


def is_null_or_nan(v: Any) -> bool:
    """Check if a scalar value is None, NaN, or pandas NA without throwing collection errors."""
    if v is None:
        return True
    if isinstance(v, (list, dict, tuple, set)):
        return False
    if isinstance(v, float) and math.isnan(v):
        return True
    pd = get_pandas()
    if pd is not None:
        try:
            return bool(pd.isna(v))
        except (ValueError, TypeError):
            return False
    return False


def is_blank(v: Any) -> bool:
    """Check if a value is None, NaN, empty string, or whitespace-only."""
    if is_null_or_nan(v):
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def create_output_dataframe(records: List[Dict[str, Any]], columns: List[str], target_type: str = "pandas") -> Any:
    """Build an output DataFrame matching target_type ('pandas' or 'polars')."""
    if target_type == "polars":
        pl = get_polars()
        if pl is None:
            raise ImportError(
                "polars is not installed. To return a polars DataFrame, install polars (pip install polars)."
            )
        if not records:
            # Construct empty polars DataFrame with expected column schema
            schema = {
                "index": pl.Int64,
                "condition_rule": pl.Utf8,
                "condition_value": pl.Utf8,
                "rows_in_payload": pl.Int64,
                "running_total": pl.Int64,
                "payload": pl.Utf8 if any(isinstance(r.get("payload"), str) for r in records) else pl.Object,
                "source_filename": pl.Utf8,
            }
            return pl.DataFrame(schema=schema)
        return pl.DataFrame(records)

    pd = get_pandas()
    if pd is None:
        raise ImportError(
            "pandas is not installed. To return a pandas DataFrame, install pandas (pip install pandas)."
        )
    return pd.DataFrame(records, columns=columns)


def slice_dataframe_chunks(
    df: Any,
    chunksize: int,
    group_columns: Optional[List[str]] = None,
) -> Iterator[Any]:
    """Slice a pandas or polars DataFrame into chunks of approximately chunksize rows.

    If group_columns is provided and non-empty, chunks will not split groups across boundaries.
    """
    if chunksize <= 0:
        yield df
        return

    n_rows = len(df)
    if n_rows <= chunksize:
        yield df
        return

    is_pol = is_polars_df(df)

    if not group_columns:
        # Simple row-count slicing
        start = 0
        while start < n_rows:
            end = min(start + chunksize, n_rows)
            yield df[start:end]
            start = end
        return

    # Group-boundary aware slicing
    if is_pol:
        # Polars group boundary slicing
        # Identify group changes
        # Find clean slice indices
        # We can extract the group columns as a list of tuples or distinct values
        cols = [c for c in group_columns if c in df.columns]
        if not cols:
            start = 0
            while start < n_rows:
                end = min(start + chunksize, n_rows)
                yield df[start:end]
                start = end
            return

        # Slicing by finding partition rows
        start = 0
        while start < n_rows:
            target_end = min(start + chunksize, n_rows)
            if target_end >= n_rows:
                yield df[start:n_rows]
                break

            # Check if target_end splits the same group as target_end - 1
            # Walk forward until group changes so we don't sever a group
            last_group_val = df.select(cols)[target_end - 1].to_dicts()[0]
            curr_end = target_end
            while curr_end < n_rows:
                curr_group_val = df.select(cols)[curr_end].to_dicts()[0]
                if curr_group_val != last_group_val:
                    break
                curr_end += 1

            yield df[start:curr_end]
            start = curr_end
    else:
        # Pandas group boundary slicing
        cols = [c for c in group_columns if c in df.columns]
        if not cols:
            start = 0
            while start < n_rows:
                end = min(start + chunksize, n_rows)
                yield df.iloc[start:end]
                start = end
            return

        start = 0
        while start < n_rows:
            target_end = min(start + chunksize, n_rows)
            if target_end >= n_rows:
                yield df.iloc[start:n_rows]
                break

            last_group = tuple(df.iloc[target_end - 1][c] for c in cols)
            curr_end = target_end
            while curr_end < n_rows:
                curr_group = tuple(df.iloc[curr_end][c] for c in cols)
                if curr_group != last_group:
                    break
                curr_end += 1

            yield df.iloc[start:curr_end]
            start = curr_end


def _extract_unique_nodes(payload_obj: Any) -> Dict[str, Set[Any]]:
    """Recursively extract sets of unique identifiers or distinct items for each entity node in a payload.

    For dict elements inside a list (e.g. orders: [{...}, {...}]), looks for common ID keys
    (e.g. 'order_id', 'id', 'key', 'code', 'number') to distinguish unique entities.
    If no ID key exists, hashes/serializes the item to count unique items without double-counting.
    """
    node_sets: Dict[str, Set[Any]] = {}

    def _traverse(node: Any, current_path: str = "") -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                child_path = f"{current_path}.{k}" if current_path else k
                if isinstance(v, list):
                    if child_path not in node_sets:
                        node_sets[child_path] = set()
                    for elem in v:
                        if isinstance(elem, dict):
                            # Try to identify an ID field in this dict
                            id_val = None
                            for id_candidate in ("id", f"{k}_id", f"{k[:-1]}_id" if k.endswith("s") else f"{k}_id", "code", "key", "number"):
                                if id_candidate in elem and not is_blank(elem[id_candidate]):
                                    id_val = (id_candidate, str(elem[id_candidate]))
                                    break
                            if id_val is not None:
                                node_sets[child_path].add(id_val)
                            else:
                                # Fallback to deterministic serialization of the dict keys/values
                                try:
                                    node_sets[child_path].add(json.dumps(elem, sort_keys=True))
                                except Exception:
                                    node_sets[child_path].add(id(elem))
                            _traverse(elem, child_path)
                        else:
                            node_sets[child_path].add(elem)
                elif isinstance(v, dict):
                    _traverse(v, child_path)
        elif isinstance(node, list):
            for elem in node:
                _traverse(elem, current_path)

    _traverse(payload_obj)
    return node_sets


def compute_meta_summary(payloads_df: Any) -> Any:
    """Compute an aggregated metadata summary DataFrame from a payloads DataFrame.

    Aggregates by ('condition_rule', 'condition_value') with metrics:
    - payload_count: total payloads produced for this condition
    - total_rows: total tabular source rows packed
    - avg_rows_per_payload: average rows per payload batch (rounded to 1 decimal)
    - min_rows: minimum batch size
    - max_rows: maximum batch size
    - node_totals: dictionary mapping each entity node name to its unique count

    Includes an 'ALL / TOTAL' row at the end.
    Returns a pandas.DataFrame or polars.DataFrame matching the input type.
    """
    meta_cols = [
        "condition_rule",
        "condition_value",
        "payload_count",
        "total_rows",
        "avg_rows_per_payload",
        "min_rows",
        "max_rows",
        "node_totals",
    ]

    if len(payloads_df) == 0:
        target_type = "polars" if is_polars_df(payloads_df) else "pandas"
        return create_output_dataframe([], meta_cols, target_type=target_type)

    # Standardize data to Python records for clean aggregation
    has_payload_col = "payload" in payloads_df.columns
    cols_to_select = ["condition_rule", "condition_value", "rows_in_payload"]
    if has_payload_col:
        cols_to_select.append("payload")

    if is_polars_df(payloads_df):
        rows = payloads_df.select(cols_to_select).to_dicts()
        target_type = "polars"
    else:
        rows = payloads_df[cols_to_select].to_dict(orient="records")
        target_type = "pandas"

    # Group metrics and track unique entities per condition key
    grouped: Dict[Tuple[str, str], List[int]] = {}
    condition_nodes: Dict[Tuple[str, str], Dict[str, Set[Any]]] = {}
    all_nodes: Dict[str, Set[Any]] = {}

    for r in rows:
        key = (str(r.get("condition_rule", "") or ""), str(r.get("condition_value", "") or ""))
        cnt = int(r.get("rows_in_payload", 0) or 0)
        if key not in grouped:
            grouped[key] = []
            condition_nodes[key] = {}
        grouped[key].append(cnt)

        # Parse and extract nodes if payload is present
        raw_p = r.get("payload")
        if raw_p is not None:
            if isinstance(raw_p, str):
                try:
                    p_obj = json.loads(raw_p)
                except Exception:
                    p_obj = None
            else:
                p_obj = raw_p

            if p_obj is not None:
                p_nodes = _extract_unique_nodes(p_obj)
                for node_name, node_set in p_nodes.items():
                    if node_name not in condition_nodes[key]:
                        condition_nodes[key][node_name] = set()
                    condition_nodes[key][node_name].update(node_set)

                    if node_name not in all_nodes:
                        all_nodes[node_name] = set()
                    all_nodes[node_name].update(node_set)

    meta_records: List[Dict[str, Any]] = []
    total_payloads_all = 0
    total_rows_all = 0
    all_sizes: List[int] = []

    for (c_rule, c_val), sizes in grouped.items():
        p_count = len(sizes)
        t_rows = sum(sizes)
        avg_r = round(t_rows / p_count, 1) if p_count > 0 else 0.0
        min_r = min(sizes) if sizes else 0
        max_r = max(sizes) if sizes else 0

        total_payloads_all += p_count
        total_rows_all += t_rows
        all_sizes.extend(sizes)

        # Convert node sets to counts
        nodes_dict = {
            node_name: len(s) for node_name, s in condition_nodes.get((c_rule, c_val), {}).items()
        }

        meta_records.append({
            "condition_rule": c_rule,
            "condition_value": c_val,
            "payload_count": p_count,
            "total_rows": t_rows,
            "avg_rows_per_payload": avg_r,
            "min_rows": min_r,
            "max_rows": max_r,
            "node_totals": nodes_dict,
        })

    # Add TOTAL row if there's at least one group
    if meta_records:
        avg_all = round(total_rows_all / total_payloads_all, 1) if total_payloads_all > 0 else 0.0
        min_all = min(all_sizes) if all_sizes else 0
        max_all = max(all_sizes) if all_sizes else 0
        all_nodes_dict = {node_name: len(s) for node_name, s in all_nodes.items()}

        meta_records.append({
            "condition_rule": "ALL",
            "condition_value": "TOTAL",
            "payload_count": total_payloads_all,
            "total_rows": total_rows_all,
            "avg_rows_per_payload": avg_all,
            "min_rows": min_all,
            "max_rows": max_all,
            "node_totals": all_nodes_dict,
        })

    return create_output_dataframe(meta_records, meta_cols, target_type=target_type)

