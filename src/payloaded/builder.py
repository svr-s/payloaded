"""Main orchestration pipeline and user-facing API for payloaded."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import pandas as pd

from payloaded.audit import AuditReport, reconcile
from payloaded.engine import HierarchyEngine
from payloaded.loader import load_sources
from payloaded.models import PayloadConfig
from payloaded.template import PayloadTemplate


class PayloadBuilder:
    """Builder class for transforming tabular datasets into nested API payloads.

    Example:
        >>> builder = PayloadBuilder(template="template.json", config="config.yaml")
        >>> df_payloads = builder.build("data/*.csv")
    """

    def __init__(
        self,
        template: Union[dict, list, str, Path],
        config: Union[dict, str, Path, PayloadConfig],
        output_format: str = "json_string",
        indent: Optional[int] = None,
    ):
        """Initialize PayloadBuilder.

        Args:
            template: JSON template dict, list, raw JSON string, or path to .json file.
            config: Configuration dict, path to .json/.yaml config file, or PayloadConfig instance.
            output_format: 'json_string' (default, ready for Postman/HTTP) or 'dict'.
            indent: Optional indentation for JSON serialization (e.g. 2 for pretty-printed).
        """
        if output_format not in ("json_string", "dict"):
            raise ValueError(f"output_format must be 'json_string' or 'dict', got: {output_format}")

        self.template = PayloadTemplate(template)
        if isinstance(config, PayloadConfig):
            self.config = config
        elif isinstance(config, (str, Path)):
            self.config = PayloadConfig.from_file(config)
        elif isinstance(config, dict):
            self.config = PayloadConfig.from_dict(config)
        else:
            raise TypeError(f"Unsupported config type: {type(config)}")

        self.output_format = output_format
        self.indent = indent
        self.engine = HierarchyEngine(self.config, self.template)

    def build(
        self,
        source: Union[str, Path, pd.DataFrame, List[Union[str, Path, pd.DataFrame]]],
    ) -> pd.DataFrame:
        """Process source dataset(s) and produce an auditable output DataFrame.

        Args:
            source: DataFrame, CSV filepath, glob pattern (e.g. 'data/*.csv'),
                    or list of paths/DataFrames.

        Returns:
            DataFrame with columns: ['index', 'running_total', 'payload', 'source_filename'].
        """
        source_pairs = load_sources(source)
        records: List[Dict[str, Any]] = []

        global_index = 1
        global_running_total = 0

        for filename, df in source_pairs:
            payload_tuples = self.engine.process_dataframe(df)

            for payload_data, rows_in_payload in payload_tuples:
                global_running_total += rows_in_payload

                if self.output_format == "json_string":
                    formatted_payload = json.dumps(
                        payload_data,
                        ensure_ascii=False,
                        indent=self.indent,
                    )
                else:
                    formatted_payload = payload_data

                records.append({
                    "index": global_index,
                    "running_total": global_running_total,
                    "payload": formatted_payload,
                    "source_filename": filename,
                })
                global_index += 1

        output_df = pd.DataFrame(
            records,
            columns=["index", "running_total", "payload", "source_filename"],
        )
        return output_df


def build_payloads(
    source: Union[str, Path, pd.DataFrame, List[Union[str, Path, pd.DataFrame]]],
    template: Union[dict, list, str, Path],
    config: Union[dict, str, Path, PayloadConfig],
    output_format: str = "json_string",
    indent: Optional[int] = None,
) -> pd.DataFrame:
    """Transform tabular source data into nested, batch-chunked API payloads.

    Args:
        source: Single DataFrame, CSV filepath, glob pattern, or list of sources.
        template: JSON template dictionary, list, string, or file path.
        config: Unified entity configuration dictionary, file path, or PayloadConfig.
        output_format: 'json_string' (default, ready for Postman) or 'dict'.
        indent: Optional indentation for JSON serialization.

    Returns:
        DataFrame with columns: ['index', 'running_total', 'payload', 'source_filename'].
    """
    builder = PayloadBuilder(
        template=template,
        config=config,
        output_format=output_format,
        indent=indent,
    )
    return builder.build(source)
