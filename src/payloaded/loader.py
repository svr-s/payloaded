"""Data ingestion module for loading tabular sources."""

from __future__ import annotations

import glob
from pathlib import Path
from typing import List, Tuple, Union
import pandas as pd


def load_sources(
    source: Union[str, Path, pd.DataFrame, List[Union[str, Path, pd.DataFrame]]],
    default_name: str = "source_data",
) -> List[Tuple[str, pd.DataFrame]]:
    """Standardize disparate input sources into a list of (filename, DataFrame) tuples.

    Args:
        source: Single DataFrame, CSV filepath, glob pattern (e.g. 'data/*.csv'),
                or list of paths/DataFrames.
        default_name: Fallback identifier when a direct DataFrame is passed.

    Returns:
        List of tuples: (source_filename, DataFrame).

    Raises:
        FileNotFoundError: If a specified file path does not exist.
        ValueError: If input format is invalid or no files match the glob pattern.
    """
    results: List[Tuple[str, pd.DataFrame]] = []

    if isinstance(source, pd.DataFrame):
        return [(default_name, source.copy())]

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

        # Single path
        p = Path(source)
        if not p.is_file():
            raise FileNotFoundError(f"Source file not found: {source}")
        df = pd.read_csv(p)
        return [(p.name, df)]

    if isinstance(source, (list, tuple)):
        for idx, item in enumerate(source):
            if isinstance(item, pd.DataFrame):
                name = f"{default_name}_{idx + 1}"
                results.append((name, item.copy()))
            elif isinstance(item, (str, Path)):
                p = Path(item)
                if not p.is_file():
                    raise FileNotFoundError(f"Source file not found: {item}")
                df = pd.read_csv(p)
                results.append((p.name, df))
            else:
                raise ValueError(f"Unsupported item type in sources list: {type(item)}")
        return results

    raise ValueError(f"Unsupported source type: {type(source)}")
