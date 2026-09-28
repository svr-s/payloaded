"""Test validation and output inspection for hierarchical batching:
1 Batch -> 4 Orders (15 lines each = 60 lines total)
Batching limits: max 5 orders, max 10 lines per order.
Slices 4 orders of 15 lines into 8 order chunks (4 * 2 = 8).
Packing up to 5 orders per batch produces exactly 2 Payloads:
- Payload 1: 5 orders (Order 1 A+B, Order 2 A+B, Order 3 A) = 40 lines
- Payload 2: 3 orders (Order 3 B, Order 4 A+B) = 20 lines
Total rows = 60, zero-loss running_total = 60.
"""

import json
import os
import pandas as pd
import pytest
import payloaded as pld


def test_hierarchical_cascading_batch_chunks_reconciliation():
    """Verify that 4 orders with 15 lines each (60 lines total) batch into 2 payloads."""
    # Build 60 normalized rows
    rows = []
    for ord_idx in range(1, 5):
        for line_idx in range(1, 16):
            rows.append({
                "batch_id": "BATCH_001",
                "order_id": f"ORD_{ord_idx:02d}",
                "order_val": 100 * ord_idx,
                "line_id": f"LINE_{ord_idx}_{line_idx:02d}",
                "line_name": f"Item {line_idx}",
            })
    df_long = pd.DataFrame(rows)
    assert len(df_long) == 60

    template = {
        "batch_id": "{batch_id}",
        "orders": [
            {
                "order_id": "{order_id}",
                "order_val": "{order_val}",
                "line_items": [
                    {
                        "line_id": "{line_id}",
                        "line_name": "{line_name}",
                    }
                ],
            }
        ],
    }

    config = {
        "entities": [
            {
                "path": "root",
                "group_by": ["batch_id"],
                "mappings": [
                    {"payload_key": "batch_id", "source_key": "batch_id"},
                ],
            },
            {
                "path": "orders",
                "repeat_limit": 5,
                "group_by": ["order_id"],
                "mappings": [
                    {"payload_key": "order_id", "source_key": "order_id"},
                    {"payload_key": "order_val", "formula": "int({order_val})"},
                ],
            },
            {
                "path": "orders.line_items",
                "repeat_limit": 10,
                "mappings": [
                    {"payload_key": "line_id", "source_key": "line_id"},
                    {"payload_key": "line_name", "source_key": "line_name"},
                ],
            },
        ]
    }

    # Execute build_payloads (always returns (df_payloads, meta_df) tuple)
    df_payloads, meta_df = pld.build_payloads(
        source=df_long,
        template=template,
        config=config,
        output_format="dict",
    )

    # 1. Assertions on Payloads count and zero-loss audit
    assert len(df_payloads) == 2, f"Expected 2 payloads, got {len(df_payloads)}"

    # Payload 1
    p1 = df_payloads.iloc[0]
    assert p1["index"] == 1
    assert p1["rows_in_payload"] == 40
    assert p1["running_total"] == 40
    payload_1_data = p1["payload"]
    assert payload_1_data["batch_id"] == "BATCH_001"
    assert len(payload_1_data["orders"]) == 5
    # Line item counts in Payload 1 orders: [10, 5, 10, 5, 10]
    p1_line_counts = [len(o["line_items"]) for o in payload_1_data["orders"]]
    assert p1_line_counts == [10, 5, 10, 5, 10]
    assert sum(p1_line_counts) == 40
    # node_counts on Payload 1: 3 unique orders (Order 1, 2, 3), 40 lines
    assert p1["node_counts"]["orders"] == 3
    assert p1["node_counts"]["orders.line_items"] == 40

    # Payload 2
    p2 = df_payloads.iloc[1]
    assert p2["index"] == 2
    assert p2["rows_in_payload"] == 20
    assert p2["running_total"] == 60
    payload_2_data = p2["payload"]
    assert payload_2_data["batch_id"] == "BATCH_001"
    assert len(payload_2_data["orders"]) == 3
    # Line item counts in Payload 2 orders: [5, 10, 5]
    p2_line_counts = [len(o["line_items"]) for o in payload_2_data["orders"]]
    assert p2_line_counts == [5, 10, 5]
    assert sum(p2_line_counts) == 20
    # node_counts on Payload 2: 2 unique orders (Order 3, 4), 20 lines
    assert p2["node_counts"]["orders"] == 2
    assert p2["node_counts"]["orders.line_items"] == 20

    # Total reconciliation
    assert p1["rows_in_payload"] + p2["rows_in_payload"] == 60

    # 2. Assertions on meta_df
    assert len(meta_df) == 2  # default rule + ALL total row
    total_row = meta_df[meta_df["condition_rule"] == "ALL"].iloc[0]
    assert total_row["payload_count"] == 2
    assert total_row["total_rows"] == 60
    assert total_row["avg_rows_per_payload"] == 30.0
    assert total_row["min_rows"] == 20
    assert total_row["max_rows"] == 40
    # node_totals: 4 unique orders and 60 line items, even though Order 3 was split across both payloads!
    assert total_row["node_totals"]["orders"] == 4
    assert total_row["node_totals"]["orders.line_items"] == 60

    # Retain the exact payload output and audit summary for user verification
    output_dir = os.path.dirname(__file__)
    output_json_path = os.path.join(output_dir, "hierarchical_batch_output.json")
    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "summary": {
                    "total_payloads": len(df_payloads),
                    "total_source_rows": len(df_long),
                    "payload_1_rows": int(p1["rows_in_payload"]),
                    "payload_2_rows": int(p2["rows_in_payload"]),
                },
                "payload_1": payload_1_data,
                "payload_2": payload_2_data,
            },
            f,
            indent=2,
        )


if __name__ == "__main__":
    test_hierarchical_cascading_batch_chunks_reconciliation()
    print("Test passed successfully! Output saved to hierarchical_batch_output.json")
