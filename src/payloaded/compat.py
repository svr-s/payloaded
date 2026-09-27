"""Dataframe compatibility and lazy abstraction layer for pandas and polars."""

from __future__ import annotations

import math
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

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
