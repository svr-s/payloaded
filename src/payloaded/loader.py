"""Data ingestion module for loading tabular sources."""

from __future__ import annotations

import glob
from pathlib import Path
from typing import Any, List, Tuple, Union

from payloaded.compat import get_pandas, is_pandas_df, is_polars_df


def load_sources(
    source: Any,
    default_name: str = "source_data",
) -> List[Tuple[str, Any]]:
    """Standardize disparate input sources into a list of (filename, DataFrame) tuples.

    Supports:
    - pandas.DataFrame
    - polars.DataFrame (converted to pandas representation for engine routing, while preserving type info)
    - CSV filepath or glob pattern
    - List of paths or DataFrames.
    """
    results: List[Tuple[str, Any]] = []

    if is_polars_df(source) or is_pandas_df(source):
        return [(default_name, source)]

    if isinstance(source, (str, Path)):
        source_str = str(source)
        # Check if it's a glob pattern
        if any(char in source_str for char in ["*", "?", "["]):
            matched_files = sorted(glob.glob(source_str))
            if not matched_files:
                raise FileNotFoundError(f"No files matched glob pattern: {source_str}")
            for file_path in matched_files:
                p = Path(file_path)
                df = pd.read_csv(p)
                results.append((p.name, df))
            return results

        pd = get_pandas()
        if pd is None:
            raise ImportError("pandas is required to read CSV files directly. Install pandas or pass a DataFrame.")
        # Single path
        p = Path(source)
        if not p.is_file():
            raise FileNotFoundError(f"Source file not found: {source}")
        df = pd.read_csv(p)
        return [(p.name, df)]

    if isinstance(source, (list, tuple)):
        pd = get_pandas()
        for idx, item in enumerate(source):
            if is_polars_df(item) or is_pandas_df(item):
                name = f"{default_name}_{idx + 1}"
                results.append((name, item))
            elif isinstance(item, (str, Path)):
                p = Path(item)
                if not p.is_file():
                    raise FileNotFoundError(f"Source file not found: {item}")
                if pd is None:
                    raise ImportError("pandas is required to read CSV files directly.")
                df = pd.read_csv(p)
                results.append((p.name, df))
            else:
                raise ValueError(f"Unsupported item type in sources list: {type(item)}")
        return results

    raise ValueError(f"Unsupported source type: {type(source)}")
