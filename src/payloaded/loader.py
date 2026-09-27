"""Dataframe ingestion module for payloaded."""

from __future__ import annotations

from typing import Any, List, Tuple

from payloaded.compat import is_pandas_df, is_polars_df


def load_sources(
    source: Any,
    default_name: str = "source_data",
) -> List[Tuple[str, Any]]:
    """Standardize input DataFrames into a list of (name, DataFrame) tuples.

    Supports:
    - pandas.DataFrame
    - polars.DataFrame
    - List of pandas or polars DataFrames
    """
    if is_polars_df(source) or is_pandas_df(source):
        return [(default_name, source)]

    if isinstance(source, (list, tuple)):
        results: List[Tuple[str, Any]] = []
        for idx, item in enumerate(source):
            if is_polars_df(item) or is_pandas_df(item):
                name = f"{default_name}_{idx + 1}"
                results.append((name, item))
            else:
                raise TypeError(
                    f"Unsupported item type in sources list at index {idx}: {type(item).__name__}. "
                    "Sources must be in-memory pandas.DataFrame or polars.DataFrame objects."
                )
        return results

    raise TypeError(
        f"Unsupported source type: {type(source).__name__}. "
        "Source must be an in-memory pandas.DataFrame or polars.DataFrame (or a list of DataFrames)."
    )

