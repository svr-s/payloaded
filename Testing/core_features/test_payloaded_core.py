"""Comprehensive unit tests for payloaded core functionality.

Tests cover:
- Multi-level hierarchy (Root list -> Orders -> LineItems)
- Composite grouping keys (batch_id + group_id)
- Multi-level batch chunking (repeat limits at every level)
- Zero-loss mathematical running_total reconciliation
- Multi-source file ingestion and filename tagging
- Postman JSON string readiness
"""

import json
import pandas as pd
import pytest

import payloaded as pld


def test_three_level_hierarchy_with_composite_keys():
    """Verify 3-level hierarchy with composite keys and batch chunking.

    Scenario:
    - 2 Batches (Batch A with group G1, Batch B with group G2)
    - Batch A has Order 101 with 95 line items.
    - With line_items limit = 40, Order 101 must split into 3 chunks: [40, 40, 15].
    - Total source rows = 95.
    - Final running_total must be exactly 95.
    """
    # 1. Create synthetic dataset: 95 rows for Order 101
    rows = []
    for i in range(95):
        rows.append({
            "Batch_Number": "B001",
            "Group_Code": "GRP_ALPHA",
            "Order_ID": "ORD_101",
            "Item_SKU": f"SKU_{i+1:03d}",
            "Quantity": (i % 5) + 1,
            "Price": 19.99,
        })
    df_source = pd.DataFrame(rows)

    # 2. Template matching user's structure: [{batch_id, group_id, orders: [{order_id, line_items: [{sku, qty}]}]}]
    template = [
        {
            "batch_id": "{batch_id}",
            "group_id": "{group_id}",
            "orders": [
                {
                    "order_id": "{order_id}",
                    "line_items": [
                        {
                            "sku": "{sku}",
                            "qty": "{qty}",
                        }
                    ]
                }
            ]
        }
    ]

    # 3. Config with composite grouping keys and repeat limits
    config = {
        "entities": [
            {
                "path": "root",
                "repeat_limit": 20,
                "group_by": ["batch_id", "group_id"],
                "mappings": [
                    {"payload_key": "batch_id", "file_key": "Batch_Number"},
                    {"payload_key": "group_id", "file_key": "Group_Code"},
                ],
            },
            {
                "path": "orders",
                "repeat_limit": 30,
                "group_by": ["order_id"],
                "mappings": [
                    {"payload_key": "order_id", "file_key": "Order_ID"},
                ],
            },
            {
                "path": "orders.line_items",
                "repeat_limit": 40,
                "mappings": [
                    {"payload_key": "sku", "file_key": "Item_SKU"},
                    {"payload_key": "qty", "file_key": "Quantity", "formula": "int({Quantity})"},
                ],
            },
        ]
    }

    # 4. Generate payloads
    df_payloads, meta_df = pld.build_payloads(
        source=df_source,
        template=template,
        config=config,
        output_format="json_string")

    # 5. Assertions
    assert len(df_payloads) == 1, "95 items with root limit 20 fits inside 1 payload batch"
    first_payload_str = df_payloads["payload"].iloc[0]
    payload_json = json.loads(first_payload_str)

    # Verify JSON structure
    assert isinstance(payload_json, list)
    batch_item = payload_json[0]
    assert batch_item["batch_id"] == "B001"
    assert batch_item["group_id"] == "GRP_ALPHA"

    # Order 101 split into 3 order chunks [40, 40, 15]
    assert len(batch_item["orders"]) == 3
    assert len(batch_item["orders"][0]["line_items"]) == 40
    assert len(batch_item["orders"][1]["line_items"]) == 40
    assert len(batch_item["orders"][2]["line_items"]) == 15

    # Check typing preservation (qty should be integer, not string)
    assert isinstance(batch_item["orders"][0]["line_items"][0]["qty"], int)

    # Verify running total matches source rows exactly
    assert df_payloads["running_total"].iloc[-1] == 95

    # Run reconciliation audit
    report = pld.reconcile(df_payloads, expected_rows=len(df_source))
    assert report.is_balanced is True
    assert report.total_source_rows == 95
    assert report.total_packed_rows == 95
    assert report.discrepancy == 0


def test_root_limit_chunking():
    """Verify that when root parent limit is reached, multiple payloads are created."""
    # Create 5 distinct batches, each with 1 order and 1 item
    rows = []
    for b in range(5):
        rows.append({
            "Batch_Number": f"BATCH_{b+1}",
            "Group_Code": "G1",
            "Order_ID": f"ORD_{b+1}",
            "Item_SKU": f"SKU_{b+1}",
            "Quantity": 1,
        })
    df_source = pd.DataFrame(rows)

    template = [
        {
            "batch_id": "{batch_id}",
            "group_id": "{group_id}",
            "orders": [
                {
                    "order_id": "{order_id}",
                    "line_items": [{"sku": "{sku}", "qty": "{qty}"}],
                }
            ],
        }
    ]

    # Limit root to 2 batches per payload (so 5 batches yields 3 payloads: 2, 2, 1)
    config = {
        "entities": [
            {
                "path": "root",
                "repeat_limit": 2,
                "group_by": ["batch_id", "group_id"],
                "mappings": [
                    {"payload_key": "batch_id", "file_key": "Batch_Number"},
                    {"payload_key": "group_id", "file_key": "Group_Code"},
                ],
            },
            {
                "path": "orders",
                "repeat_limit": 10,
                "group_by": ["order_id"],
                "mappings": [{"payload_key": "order_id", "file_key": "Order_ID"}],
            },
            {
                "path": "orders.line_items",
                "repeat_limit": 10,
                "mappings": [
                    {"payload_key": "sku", "file_key": "Item_SKU"},
                    {"payload_key": "qty", "file_key": "Quantity"},
                ],
            },
        ]
    }

    df_payloads, meta_df = pld.build_payloads(
        source=df_source,
        template=template,
        config=config)

    # 5 batches chunked into max 2 per payload -> 3 payloads
    assert len(df_payloads) == 3
    # Check running totals: 2 -> 4 -> 5
    assert list(df_payloads["running_total"]) == [2, 4, 5]
    assert df_payloads["running_total"].iloc[-1] == 5

    # Reconcile audit
    report = pld.reconcile(df_payloads, expected_rows=5)
    assert report.is_balanced is True


def test_multi_source_traceability():
    """Verify that multiple input DataFrames or CSV files are tracked with source_filename."""
    df1 = pd.DataFrame({
        "order_id": ["O1", "O2"],
        "sku": ["S1", "S2"],
    })
    df2 = pd.DataFrame({
        "order_id": ["O3", "O4", "O5"],
        "sku": ["S3", "S4", "S5"],
    })

    template = {"orders": [{"id": "{order_id}", "sku": "{sku}"}]}
    config = {
        "entities": [
            {
                "path": "root",
                "mappings": [],
            },
            {
                "path": "orders",
                "repeat_limit": 2,
                "group_by": ["order_id"],
                "mappings": [
                    {"payload_key": "order_id", "file_key": "order_id"},
                    {"payload_key": "sku", "file_key": "sku"},
                ],
            },
        ]
    }

    df_payloads, meta_df = pld.build_payloads(
        source=[df1, df2],
        template=template,
        config=config)

    assert "source_filename" in df_payloads.columns
    assert len(df_payloads) == 3  # df1: 2 items (1 payload); df2: 3 items (2 payloads: 2, 1)
    assert df_payloads["rows_in_payload"].sum() == 5
    # File 1 running total reconciles to 2
    assert df_payloads[df_payloads["source_filename"] == "source_data_1"]["running_total"].iloc[-1] == 2
    # File 2 running total reconciles to 3
    assert df_payloads[df_payloads["source_filename"] == "source_data_2"]["running_total"].iloc[-1] == 3
    assert df_payloads["source_filename"].tolist() == ["source_data_1", "source_data_2", "source_data_2"]


def test_missing_column_raises_clear_error():
    """Verify that a missing source column raises a descriptive KeyError."""
    df = pd.DataFrame({"col_a": [1, 2]})
    template = {"key": "{missing_key}"}
    config = {
        "entities": [
            {
                "path": "root",
                "mappings": [{"payload_key": "missing_key", "file_key": "non_existent_column"}],
            }
        ]
    }
    with pytest.raises(KeyError) as exc_info:
        pld.build_payloads(source=df, template=template, config=config)
    assert "non_existent_column" in str(exc_info.value)


def test_dict_output_format():
    """Verify that output_format='dict' returns Python dictionaries rather than strings."""
    df = pd.DataFrame({"val": [10, 20]})
    template = [{"v": "{v}"}]
    config = {
        "entities": [
            {
                "path": "root",
                "repeat_limit": 2,
                "mappings": [{"payload_key": "v", "file_key": "val", "formula": "int({val})"}],
            }
        ]
    }
    df_payloads, meta_df = pld.build_payloads(source=df, template=template, config=config, output_format="dict")
    assert isinstance(df_payloads["payload"].iloc[0], list)
    assert df_payloads["payload"].iloc[0][0]["v"] == 10


def test_reconciliation_failure_raises_error():
    """Verify that pld.reconcile with strict=True raises ReconciliationError on discrepancy."""
    df_out = pd.DataFrame({
        "index": [1],
        "running_total": [50],
        "payload": ["{}"],
        "source_filename": ["test.csv"],
    })
    with pytest.raises(pld.ReconciliationError) as exc_info:
        pld.reconcile(df_out, expected_rows=100, strict=True)
    assert "Expected 100 rows, but generated payloads accounted for 50 rows" in str(exc_info.value)


def test_integer_column_index_mapping():
    """Verify mapping by 0-based column indices (e.g. source_key: 0, 1)."""
    # Source dataframe without explicit header names
    df = pd.DataFrame([
        ["BATCH_X", "ORD_1", "SKU_100", 5],
        ["BATCH_X", "ORD_1", "SKU_200", 10],
    ])

    template = [
        {
            "batch": "{batch}",
            "orders": [
                {
                    "order": "{order}",
                    "items": [{"sku": "{sku}", "qty": "{qty}"}],
                }
            ],
        }
    ]

    config = {
        "entities": [
            {
                "path": "root",
                "group_by": ["batch"],
                "mappings": [{"payload_key": "batch", "source_key": 0}],
            },
            {
                "path": "orders",
                "group_by": ["order"],
                "mappings": [{"payload_key": "order", "source_key": 1}],
            },
            {
                "path": "orders.items",
                "mappings": [
                    {"payload_key": "sku", "source_key": 2},
                    {"payload_key": "qty", "source_key": 3, "formula": "int({3})"},
                ],
            },
        ]
    }

    df_payloads, meta_df = pld.build_payloads(source=df, template=template, config=config, output_format="dict")
    assert len(df_payloads) == 1
    payload = df_payloads["payload"].iloc[0]
    assert payload[0]["batch"] == "BATCH_X"
    assert payload[0]["orders"][0]["order"] == "ORD_1"
    assert len(payload[0]["orders"][0]["items"]) == 2
    assert payload[0]["orders"][0]["items"][0]["sku"] == "SKU_100"
    assert payload[0]["orders"][0]["items"][0]["qty"] == 5


def test_whitespace_tolerance_in_keys_and_columns():
    """Verify that leading/trailing whitespaces in mappings, group_by, and columns are handled gracefully."""
    # Column has accidental spaces in CSV
    df = pd.DataFrame({
        " Batch_ID ": ["B_01"],
        " Item_SKU": ["SKU_99"],
    })

    template = [{"batch": "{batch}", "sku": "{sku}"}]
    config = {
        "entities": [
            {
                "path": " root ",
                "group_by": [" batch "],
                "mappings": [
                    {"payload_key": " batch ", "source_key": " Batch_ID "},
                    {"payload_key": " sku ", "source_key": " Item_SKU "},
                ],
            }
        ]
    }

    df_payloads, meta_df = pld.build_payloads(source=df, template=template, config=config, output_format="dict")
    payload = df_payloads["payload"].iloc[0]
    assert payload[0]["batch"] == "B_01"
    assert payload[0]["sku"] == "SKU_99"


def test_mapping_deduplication_first_occurrence():
    """Verify that repeating payload_key or source_key retains the first occurrence."""
    config_dict = {
        "entities": [
            {
                "path": "root",
                "mappings": [
                    {"payload_key": "batch_id", "source_key": "Col_1"},
                    {"payload_key": "batch_id", "source_key": "Col_Duplicate"},  # duplicate payload_key
                    {"payload_key": "alt_key", "source_key": "Col_1"},          # duplicate source_key
                    {"payload_key": "order_id", "source_key": "Col_2"},
                ],
            }
        ]
    }
    cfg = pld.PayloadConfig.from_dict(config_dict)
    root_entity = cfg.get_entity("root")
    assert len(root_entity.mappings) == 2
    assert root_entity.mappings[0].payload_key == "batch_id"
    assert root_entity.mappings[0].source_key == "Col_1"
    assert root_entity.mappings[1].payload_key == "order_id"
    assert root_entity.mappings[1].source_key == "Col_2"


def test_default_value_when_column_omitted():
    """Verify that a mapping with default value populates cleanly even if column is missing from CSV."""
    df = pd.DataFrame({"Batch_ID": ["B100"]})
    template = [{"batch": "{batch}", "location": "{location}", "date": "{date}"}]
    config = {
        "entities": [
            {
                "path": "root",
                "group_by": ["batch"],
                "mappings": [
                    {"payload_key": "batch", "source_key": "Batch_ID"},
                    {"payload_key": "location", "default": "WAREHOUSE_01"},
                    {"payload_key": "date", "default": ""},
                ],
            }
        ]
    }
    df_payloads, meta_df = pld.build_payloads(source=df, template=template, config=config, output_format="dict")
    item = df_payloads["payload"].iloc[0][0]
    assert item["batch"] == "B100"
    assert item["location"] == "WAREHOUSE_01"
    assert item["date"] == ""


def test_canonical_conditional_routing_with_negation_and_partitioning():
    """Verify conditional routing with negation (~Terminated), value partitioning, and 7-column output."""
    # Source dataset with New (3 rows), Update (2 rows), Terminated (1 row) -> total 6 rows
    df = pd.DataFrame([
        {"status": "New", "batch": "B1", "order": "O1", "sku": "S1", "qty": 1},
        {"status": "New", "batch": "B1", "order": "O1", "sku": "S2", "qty": 2},
        {"status": "New", "batch": "B1", "order": "O2", "sku": "S3", "qty": 1},
        {"status": "Update", "batch": "B1", "order": "O3", "sku": "S4", "qty": 4},
        {"status": "Update", "batch": "B1", "order": "O3", "sku": "S5", "qty": 5},
        {"status": "Terminated", "batch": "B1", "order": "O4", "sku": "S6", "qty": 0},
    ])

    template_active = [
        {
            "batch_id": "{batch_id}",
            "orders": [
                {
                    "order_id": "{order_id}",
                    "items": [{"sku": "{sku}", "qty": "{qty}"}],
                }
            ],
        }
    ]

    template_cancel = [
        {
            "batch_id": "{batch_id}",
            "cancelled_orders": [{"order_id": "{order_id}"}],
        }
    ]

    config = {
        "condition_source_key": "status",
        "conditions": [
            {
                "condition_rule": ["~Terminated"],
                "payload_template": template_active,
                "entities": [
                    {
                        "path": "root",
                        "repeat_limit": 10,
                        "group_by": ["batch_id"],
                        "mappings": [{"payload_key": "batch_id", "source_key": "batch"}],
                    },
                    {
                        "path": "orders",
                        "repeat_limit": 10,
                        "group_by": ["order_id"],
                        "mappings": [{"payload_key": "order_id", "source_key": "order"}],
                    },
                    {
                        "path": "orders.items",
                        "repeat_limit": 10,
                        "mappings": [
                            {"payload_key": "sku", "source_key": "sku"},
                            {"payload_key": "qty", "source_key": "qty", "formula": "int({qty})"},
                        ],
                    },
                ],
            },
            {
                "condition_rule": ["Terminated"],
                "payload_template": template_cancel,
                "entities": [
                    {
                        "path": "root",
                        "group_by": ["batch_id"],
                        "mappings": [{"payload_key": "batch_id", "source_key": "batch"}],
                    },
                    {
                        "path": "cancelled_orders",
                        "group_by": ["order_id"],
                        "mappings": [{"payload_key": "order_id", "source_key": "order"}],
                    },
                ],
            },
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")

    # Verify exact 8 columns
    expected_cols = [
        "index",
        "condition_rule",
        "condition_value",
        "rows_in_payload",
        "running_total",
        "node_counts",
        "payload",
        "source_filename",
    ]
    assert list(df_payloads.columns) == expected_cols

    # Should yield 3 payloads: New (3 rows), Update (2 rows), Terminated (1 row)
    assert len(df_payloads) == 3

    # Check Payload 1: New
    p1 = df_payloads.iloc[0]
    assert p1["condition_rule"] == "~Terminated"
    assert p1["condition_value"] == "New"
    assert p1["rows_in_payload"] == 3
    assert p1["running_total"] == 3
    assert len(p1["payload"][0]["orders"]) == 2  # O1 and O2

    # Check Payload 2: Update
    p2 = df_payloads.iloc[1]
    assert p2["condition_rule"] == "~Terminated"
    assert p2["condition_value"] == "Update"
    assert p2["rows_in_payload"] == 2
    assert p2["running_total"] == 5

    # Check Payload 3: Terminated
    p3 = df_payloads.iloc[2]
    assert p3["condition_rule"] == "Terminated"
    assert p3["condition_value"] == "Terminated"
    assert p3["rows_in_payload"] == 1
    assert p3["running_total"] == 6
    assert "cancelled_orders" in p3["payload"][0]

    # Reconcile audit
    report = pld.reconcile(df_payloads, expected_rows=len(df))
    assert report.is_balanced is True
    assert report.total_source_rows == 6
    assert report.total_packed_rows == 6


def test_canonical_unconditional_blank_schema():
    """Verify that blank condition_source_key and condition_rule processes all rows unconditionally."""
    df = pd.DataFrame([
        {"id": "1", "val": "A"},
        {"id": "2", "val": "B"},
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [{"id": "{id}", "val": "{val}"}],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [
                            {"payload_key": "id", "source_key": "id"},
                            {"payload_key": "val", "source_key": "val"},
                        ],
                    }
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    assert len(df_payloads) == 1
    assert df_payloads["condition_rule"].iloc[0] == ""
    assert df_payloads["condition_value"].iloc[0] == ""
    assert df_payloads["rows_in_payload"].iloc[0] == 2
    assert df_payloads["running_total"].iloc[0] == 2
    assert len(df_payloads["payload"].iloc[0]) == 2


def test_column_index_as_condition_source_key():
    """Verify that condition_source_key can be an integer column index."""
    df = pd.DataFrame([
        ["BATCH_1", "ACTIVE", "O1"],
        ["BATCH_1", "CANCELLED", "O2"],
    ])

    config = {
        "condition_source_key": 1,  # column index 1 has status
        "conditions": [
            {
                "condition_rule": ["ACTIVE"],
                "payload_template": [{"batch": "{batch}", "order": "{order}"}],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [
                            {"payload_key": "batch", "source_key": 0},
                            {"payload_key": "order", "source_key": 2},
                        ],
                    }
                ],
            },
            {
                "condition_rule": ["CANCELLED"],
                "payload_template": [{"cancel": "{order}"}],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [{"payload_key": "order", "source_key": 2}],
                    }
                ],
            },
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    assert len(df_payloads) == 2
    assert df_payloads["condition_value"].tolist() == ["ACTIVE", "CANCELLED"]
    assert df_payloads["rows_in_payload"].tolist() == [1, 1]


def test_user_formula_expression_and_transforms():
    """Verify user's exact formula expression with case, strip, slice, and replace."""
    # User's exact formula:
    # lower(strip({location}))[1:5] & "-" & replace(strip({batch_number}), ":", "")
    df = pd.DataFrame([
        {"Location": "  NEWYORK  ", "Batch_Num": "B:001:X", "Item": "ITEM_A"},
        {"Location": "  LONDON   ", "Batch_Num": "B:002:Y", "Item": "ITEM_B"},
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "tracking_code": "{tracking_code}",
                        "item": "{item}",
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [
                            {
                                "payload_key": "tracking_code",
                                "formula": 'lower(strip({Location}))[1:5] & "-" & replace(strip({Batch_Num}), ":", "")',
                            },
                            {
                                "payload_key": "item",
                                "source_key": "Item",
                            },
                        ],
                    }
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    assert len(df_payloads) == 1
    items = df_payloads["payload"].iloc[0]
    assert len(items) == 2

    # "NEWYORK" -> lower stripped is "newyork" -> [1:5] is "ewyo"
    # "B:001:X" -> replace ":" with "" is "B001X"
    # Combined: "ewyo-B001X"
    assert items[0]["tracking_code"] == "ewyo-B001X"
    assert items[0]["item"] == "ITEM_A"

    # "LONDON" -> lower stripped is "london" -> [1:5] is "ondo"
    # "B:002:Y" -> replace ":" is "B002Y"
    # Combined: "ondo-B002Y"
    assert items[1]["tracking_code"] == "ondo-B002Y"
    assert items[1]["item"] == "ITEM_B"


def test_sequence_generator_parent_scoping():
    """Verify that sequence(start=1, scope='parent') resets per parent entity (e.g. per order)."""
    df = pd.DataFrame([
        {"Order": "ORD_1", "SKU": "SKU_01"},
        {"Order": "ORD_1", "SKU": "SKU_02"},
        {"Order": "ORD_2", "SKU": "SKU_03"},
        {"Order": "ORD_2", "SKU": "SKU_04"},
        {"Order": "ORD_2", "SKU": "SKU_05"},
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "order_id": "{order_id}",
                        "line_items": [
                            {"line_num": "{line_num}", "sku": "{sku}"}
                        ],
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "group_by": ["order_id"],
                        "mappings": [{"payload_key": "order_id", "source_key": "Order"}],
                    },
                    {
                        "path": "line_items",
                        "mappings": [
                            {"payload_key": "line_num", "formula": "sequence(start=1, scope='parent')"},
                            {"payload_key": "sku", "source_key": "SKU"},
                        ],
                    },
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    assert len(df_payloads) == 1
    orders = df_payloads["payload"].iloc[0]
    assert len(orders) == 2

    # Order 1 lines: 1, 2
    o1_lines = [item["line_num"] for item in orders[0]["line_items"]]
    assert o1_lines == [1, 2]

    # Order 2 lines: resets to 1, 2, 3
    o2_lines = [item["line_num"] for item in orders[1]["line_items"]]
    assert o2_lines == [1, 2, 3]


def test_sequence_generator_global_scoping():
    """Verify that sequence(start=100, scope='global') counts continuously across all entities."""
    df = pd.DataFrame([
        {"Order": "ORD_1", "SKU": "SKU_01"},
        {"Order": "ORD_1", "SKU": "SKU_02"},
        {"Order": "ORD_2", "SKU": "SKU_03"},
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "order_id": "{order_id}",
                        "items": [{"global_idx": "{global_idx}", "sku": "{sku}"}],
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "group_by": ["order_id"],
                        "mappings": [{"payload_key": "order_id", "source_key": "Order"}],
                    },
                    {
                        "path": "items",
                        "mappings": [
                            {"payload_key": "global_idx", "formula": "sequence(start=100, scope='global')"},
                            {"payload_key": "sku", "source_key": "SKU"},
                        ],
                    },
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    orders = df_payloads["payload"].iloc[0]

    # Order 1 items: 100, 101
    o1_indices = [item["global_idx"] for item in orders[0]["items"]]
    assert o1_indices == [100, 101]

    # Order 2 item: continues at 102
    o2_indices = [item["global_idx"] for item in orders[1]["items"]]
    assert o2_indices == [102]


def test_formula_security_sandboxing():
    """Verify that dangerous constructs (private attributes, imports) are rejected at config parse time."""
    with pytest.raises(pld.FormulaSecurityError):
        pld.CompiledFormula('"".__class__.__bases__[0]')

    with pytest.raises(pld.FormulaSecurityError):
        pld.CompiledFormula('import os')


def test_formula_utility_functions():
    """Verify lpad, coalesce, date_format, and now utility functions in formulas."""
    df = pd.DataFrame([
        {"ID": 5, "Alt": "", "Date": "2026-09-25"},
        {"ID": 123, "Alt": "DirectAlt", "Date": "2026-10-01"},
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "padded_id": "{padded_id}",
                        "label": "{label}",
                        "formatted_date": "{formatted_date}",
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [
                            {"payload_key": "padded_id", "formula": 'lpad({ID}, 5, "0")'},
                            {"payload_key": "label", "formula": 'coalesce({Alt}, "DEFAULT_LABEL")'},
                            {"payload_key": "formatted_date", "formula": 'date_format({Date}, "%Y-%m-%d", "%d/%m/%Y")'},
                        ],
                    }
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    rows = df_payloads["payload"].iloc[0]

    assert rows[0]["padded_id"] == "00005"
    assert rows[0]["label"] == "DEFAULT_LABEL"
    assert rows[0]["formatted_date"] == "25/09/2026"

    assert rows[1]["padded_id"] == "00123"
    assert rows[1]["label"] == "DirectAlt"
    assert rows[1]["formatted_date"] == "01/10/2026"


def test_sequence_across_chunked_batches_and_reset_on_next_order():
    """Verify that parent-scoped sequence continues across chunked payloads for the same order and resets on the next order.

    Scenario:
    - Order 1 has 95 items (chunked by limit 40 into 40, 40, 15).
    - Sequence for Order 1 must run 1..40, 41..80, 81..95.
    - Order 2 has 5 items.
    - Sequence for Order 2 must reset back to 1..5.
    """
    rows = []
    # 95 items for Order 1
    for i in range(95):
        rows.append({"Order": "ORD_1", "SKU": f"SKU_1_{i+1:03d}"})
    # 5 items for Order 2
    for i in range(5):
        rows.append({"Order": "ORD_2", "SKU": f"SKU_2_{i+1:03d}"})

    df = pd.DataFrame(rows)

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "order_id": "{order_id}",
                        "items": [
                            {"line_num": "{line_num}", "sku": "{sku}"}
                        ],
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "repeat_limit": 1,
                        "group_by": ["order_id"],
                        "mappings": [{"payload_key": "order_id", "source_key": "Order"}],
                    },
                    {
                        "path": "items",
                        "repeat_limit": 40,
                        "mappings": [
                            {"payload_key": "line_num", "formula": "sequence(start=1, scope='parent')"},
                            {"payload_key": "sku", "source_key": "SKU"},
                        ],
                    },
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    # Order 1 produces 3 chunks (40, 40, 15), Order 2 produces 1 chunk (5) -> Total 4 payloads
    assert len(df_payloads) == 4

    p1 = df_payloads["payload"].iloc[0]  # Order 1, Chunk 1
    p2 = df_payloads["payload"].iloc[1]  # Order 1, Chunk 2
    p3 = df_payloads["payload"].iloc[2]  # Order 1, Chunk 3
    p4 = df_payloads["payload"].iloc[3]  # Order 2, Chunk 1

    # Chunk 1: lines 1..40
    p1_lines = [item["line_num"] for item in p1[0]["items"]]
    assert p1_lines == list(range(1, 41))

    # Chunk 2: lines 41..80
    p2_lines = [item["line_num"] for item in p2[0]["items"]]
    assert p2_lines == list(range(41, 81))

    # Chunk 3: lines 81..95
    p3_lines = [item["line_num"] for item in p3[0]["items"]]
    assert p3_lines == list(range(81, 96))

    # Order 2: resets to 1..5
    p4_lines = [item["line_num"] for item in p4[0]["items"]]
    assert p4_lines == list(range(1, 6))

    # Reconcile audit
    report = pld.reconcile(df_payloads, expected_rows=100)
    assert report.is_balanced is True
    assert report.total_source_rows == 100
    assert report.total_packed_rows == 100


def test_omit_if_blank_field_level():
    """Verify that omit_if_blank removes the key when value is None, NaN, '', or whitespace-only."""
    df = pd.DataFrame([
        {"id": "ORD_1", "discount": "SAVE20", "qty": 0, "notes": "Urgent"},
        {"id": "ORD_2", "discount": None, "qty": 5, "notes": "  "},
        {"id": "ORD_3", "discount": "", "qty": 1, "notes": "Standard"},
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "order_id": "{order_id}",
                        "discount_code": "{discount_code}",
                        "quantity": "{quantity}",
                        "notes": "{notes}",
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [
                            {"payload_key": "order_id", "source_key": "id"},
                            {"payload_key": "discount_code", "source_key": "discount", "omit_if_blank": True},
                            {"payload_key": "quantity", "source_key": "qty", "omit_if_blank": True},
                            {"payload_key": "notes", "source_key": "notes", "omit_if_blank": True},
                        ],
                    }
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    records = df_payloads["payload"].iloc[0]

    # Row 1: all values present (qty is 0, which is valid and NOT blank)
    assert records[0]["order_id"] == "ORD_1"
    assert records[0]["discount_code"] == "SAVE20"
    assert records[0]["quantity"] == 0
    assert records[0]["notes"] == "Urgent"

    # Row 2: discount is None, notes is whitespace -> both omitted!
    assert records[1]["order_id"] == "ORD_2"
    assert "discount_code" not in records[1]
    assert records[1]["quantity"] == 5
    assert "notes" not in records[1]

    # Row 3: discount is "" -> omitted! notes is "Standard" -> present!
    assert records[2]["order_id"] == "ORD_3"
    assert "discount_code" not in records[2]
    assert records[2]["quantity"] == 1
    assert records[2]["notes"] == "Standard"


def test_omit_if_blank_default_is_false_preserving_null():
    """Verify that omit_if_blank defaults to False and emits null for missing cells."""
    df = pd.DataFrame([
        {"id": "ORD_1", "opt": None},
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [{"id": "{id}", "optional_field": "{optional_field}"}],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [
                            {"payload_key": "id", "source_key": "id"},
                            {"payload_key": "optional_field", "source_key": "opt"},  # omit_if_blank default False
                        ],
                    }
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    record = df_payloads["payload"].iloc[0][0]
    assert "optional_field" in record
    assert record["optional_field"] is None


def test_omit_if_blank_entity_level_inheritance():
    """Verify that omit_if_blank on EntityConfig is inherited by all child mappings."""
    df = pd.DataFrame([
        {"id": "ORD_1", "opt1": None, "opt2": "Present", "opt3": ""},
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "id": "{id}",
                        "opt1": "{opt1}",
                        "opt2": "{opt2}",
                        "opt3": "{opt3}",
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "omit_if_blank": True,  # entity-level setting
                        "mappings": [
                            {"payload_key": "id", "source_key": "id"},
                            {"payload_key": "opt1", "source_key": "opt1"},
                            {"payload_key": "opt2", "source_key": "opt2"},
                            {"payload_key": "opt3", "source_key": "opt3"},
                        ],
                    }
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    record = df_payloads["payload"].iloc[0][0]

    assert record["id"] == "ORD_1"
    assert "opt1" not in record  # None -> omitted
    assert record["opt2"] == "Present"
    assert "opt3" not in record  # "" -> omitted


def test_wildcard_unpivot_wide_to_nested_child_entities():
    """Verify that wildcard source_key mappings (e.g. orderid*) convert wide format to nested child arrays."""
    # Wide dataset with 3 order slots across 2 batches
    df = pd.DataFrame([
        {
            "location": "Warehouse_East",
            "batchnumber": "B100",
            "orderid": "ORD_01", "ordername": "Alpha", "ordervalue": 100,
            "orderid2": "ORD_02", "ordername2": "Beta", "ordervalue2": 200,
            "orderid3": "ORD_03", "ordername3": "Gamma", "ordervalue3": 300,
        },
        {
            "location": "Warehouse_West",
            "batchnumber": "B200",
            "orderid": "ORD_04", "ordername": "Delta", "ordervalue": 400,
            "orderid2": None, "ordername2": None, "ordervalue2": None,  # Slot 2 empty
            "orderid3": "ORD_05", "ordername3": "Epsilon", "ordervalue3": 500,
        },
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "location": "{loc}",
                        "batch": "{batch_no}",
                        "orders": [
                            {
                                "id": "{id}",
                                "name": "{name}",
                                "value": "{val}",
                            }
                        ],
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "repeat_limit": 1,
                        "group_by": ["batch_no"],
                        "mappings": [
                            {"payload_key": "loc", "source_key": "location"},
                            {"payload_key": "batch_no", "source_key": "batchnumber"},
                        ],
                    },
                    {
                        "path": "orders",
                        "mappings": [
                            {"payload_key": "id", "source_key": "orderid*"},
                            {"payload_key": "name", "source_key": "ordername*"},
                            {"payload_key": "val", "source_key": "ordervalue*"},
                        ],
                    },
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    assert len(df_payloads) == 2

    # Batch 1 (all 3 orders present)
    p1 = df_payloads["payload"].iloc[0][0]
    assert p1["batch"] == "B100"
    assert len(p1["orders"]) == 3
    assert p1["orders"][0] == {"id": "ORD_01", "name": "Alpha", "value": 100}
    assert p1["orders"][1] == {"id": "ORD_02", "name": "Beta", "value": 200.0}
    assert p1["orders"][2] == {"id": "ORD_03", "name": "Gamma", "value": 300}

    # Batch 2 (slot 2 empty, so only slot 1 and 3 are present)
    p2 = df_payloads["payload"].iloc[1][0]
    assert p2["batch"] == "B200"
    assert len(p2["orders"]) == 2
    assert p2["orders"][0] == {"id": "ORD_04", "name": "Delta", "value": 400}
    assert p2["orders"][1] == {"id": "ORD_05", "name": "Epsilon", "value": 500}


def test_wildcard_unpivot_with_missing_columns_and_omit_if_blank():
    """Verify that if a column like ordername3 is completely absent from the DataFrame, it does not fail."""
    # Notice: ordername3 does NOT exist in headers!
    df = pd.DataFrame([
        {
            "batch": "B300",
            "orderid": "ORD_01", "ordername": "Widget A",
            "orderid2": "ORD_02", "ordername2": "Widget B",
            "orderid3": "ORD_03",  # ordername3 missing from columns
        }
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "batch": "{batch}",
                        "orders": [
                            {
                                "id": "{id}",
                                "name": "{name}",
                            }
                        ],
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [{"payload_key": "batch", "source_key": "batch"}],
                    },
                    {
                        "path": "orders",
                        "mappings": [
                            {"payload_key": "id", "source_key": "orderid*"},
                            {"payload_key": "name", "source_key": "ordername*", "omit_if_blank": True},
                        ],
                    },
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    p = df_payloads["payload"].iloc[0][0]
    assert len(p["orders"]) == 3
    assert p["orders"][0] == {"id": "ORD_01", "name": "Widget A"}
    assert p["orders"][1] == {"id": "ORD_02", "name": "Widget B"}
    # For slot 3, 'name' is omitted without error!
    assert p["orders"][2] == {"id": "ORD_03"}
    assert "name" not in p["orders"][2]


def test_source_column_list_of_dicts():
    """Verify exploding a cell containing a list of dictionaries via source_column."""
    df = pd.DataFrame([
        {
            "batch": "B100",
            "orders": [
                {"orderid": "ord1", "ordervalue": 100},
                {"orderid": "ord2", "ordervalue": 200},
            ],
        }
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "batch": "{batch}",
                        "orders": [
                            {
                                "id": "{id}",
                                "val": "{val}",
                            }
                        ],
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [{"payload_key": "batch", "source_key": "batch"}],
                    },
                    {
                        "path": "orders",
                        "source_column": "orders",
                        "mappings": [
                            {"payload_key": "id", "source_key": "orderid"},
                            {"payload_key": "val", "source_key": "ordervalue"},
                        ],
                    },
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    p = df_payloads["payload"].iloc[0][0]
    assert p["batch"] == "B100"
    assert len(p["orders"]) == 2
    assert p["orders"][0] == {"id": "ord1", "val": 100}
    assert p["orders"][1] == {"id": "ord2", "val": 200}


def test_source_column_sparse_dicts_with_omit_if_blank():
    """Verify cell explode with sparse dictionaries and omit_if_blank."""
    df = pd.DataFrame([
        {
            "batch": "B101",
            "orders": '[{"orderid": "ord1", "ordervalue": 100, "ordername": "order1"}, {"orderid": "ord2", "ordervalue": 200}]',
        }
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "batch": "{batch}",
                        "orders": [
                            {
                                "id": "{id}",
                                "val": "{val}",
                                "name": "{name}",
                            }
                        ],
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [{"payload_key": "batch", "source_key": "batch"}],
                    },
                    {
                        "path": "orders",
                        "source_column": "orders",
                        "mappings": [
                            {"payload_key": "id", "source_key": "orderid"},
                            {"payload_key": "val", "source_key": "ordervalue"},
                            {"payload_key": "name", "source_key": "ordername", "omit_if_blank": True},
                        ],
                    },
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    p = df_payloads["payload"].iloc[0][0]
    assert len(p["orders"]) == 2
    assert p["orders"][0] == {"id": "ord1", "val": 100, "name": "order1"}
    assert p["orders"][1] == {"id": "ord2", "val": 200}
    assert "name" not in p["orders"][1]


def test_source_column_inconsistent_keys_with_wildcards():
    """Verify wildcard matching within exploded dictionary items (e.g. orderid* matches orderid or orderid2)."""
    df = pd.DataFrame([
        {
            "batch": "B102",
            "orders": [
                {"orderid": "ord1", "ordervalue": 100, "ordername": "order1"},
                {"orderid2": "ord2", "ordervalue2": 200},
            ],
        }
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "batch": "{batch}",
                        "orders": [
                            {
                                "id": "{id}",
                                "val": "{val}",
                                "name": "{name}",
                            }
                        ],
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [{"payload_key": "batch", "source_key": "batch"}],
                    },
                    {
                        "path": "orders",
                        "source_column": "orders",
                        "mappings": [
                            {"payload_key": "id", "source_key": "orderid*"},
                            {"payload_key": "val", "source_key": "ordervalue*"},
                            {"payload_key": "name", "source_key": "ordername*", "omit_if_blank": True},
                        ],
                    },
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    p = df_payloads["payload"].iloc[0][0]
    assert len(p["orders"]) == 2
    assert p["orders"][0] == {"id": "ord1", "val": 100, "name": "order1"}
    assert p["orders"][1] == {"id": "ord2", "val": 200}
    assert "name" not in p["orders"][1]


def test_source_column_key_value_map_unfurl():
    """Verify key-value dictionary unfurling into entries via __key__ and __value__."""
    df = pd.DataFrame([
        {
            "batch": "B103",
            "orders": {"ord1": "100", "ord2": "200", "ord3": ""},
        }
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "batch": "{batch}",
                        "items": [
                            {
                                "order_key": "{order_key}",
                                "amount": "{amount}",
                            }
                        ],
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [{"payload_key": "batch", "source_key": "batch"}],
                    },
                    {
                        "path": "items",
                        "source_column": "orders",
                        "mappings": [
                            {"payload_key": "order_key", "source_key": "__key__"},
                            {"payload_key": "amount", "source_key": "__value__", "omit_if_blank": True},
                        ],
                    },
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    p = df_payloads["payload"].iloc[0][0]
    assert len(p["items"]) == 3
    assert p["items"][0] == {"order_key": "ord1", "amount": "100"}
    assert p["items"][1] == {"order_key": "ord2", "amount": "200"}
    assert p["items"][2] == {"order_key": "ord3"}
    assert "amount" not in p["items"][2]


def test_formula_json_passthrough():
    """Verify formula 'json({col})' preserves nested JSON structure as parsed object/list."""
    df = pd.DataFrame([
        {
            "batch": "B104",
            "orders_json": '[{"orderid": "ord1", "amt": 50}, {"orderid": "ord2", "amt": 75}]',
        }
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "batch": "{batch}",
                        "raw_orders": "{raw_orders}",
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [
                            {"payload_key": "batch", "source_key": "batch"},
                            {"payload_key": "raw_orders", "formula": "json({orders_json})"},
                        ],
                    }
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    p = df_payloads["payload"].iloc[0][0]
    assert p["batch"] == "B104"
    assert isinstance(p["raw_orders"], list)
    assert len(p["raw_orders"]) == 2
    assert p["raw_orders"][0] == {"orderid": "ord1", "amt": 50}
    assert p["raw_orders"][1] == {"orderid": "ord2", "amt": 75}


def test_nested_subdictionary_block_within_child_items():
    """Verify that a child entity item (e.g. lineitems) can contain nested sub-dictionaries (e.g. namecode)

    without requiring an extra entity path, hydrating both lineid and codevalue/shortname cleanly.
    """
    df = pd.DataFrame([
        {
            "batchnumber": "BATCH-001",
            "ordernumber": "ORD-101",
            "ordertotal": 250.0,
            "linenumber": "L1",
            "productid": "PRD-99",
        },
        {
            "batchnumber": "BATCH-001",
            "ordernumber": "ORD-101",
            "ordertotal": 250.0,
            "linenumber": "L2",
            "productid": "PRD-100",
        },
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "batchid": "{batchnumber}",
                        "location": "loc01",
                        "orders": [
                            {
                                "orderid": "{ordernumber}",
                                "ordertotal": "{ordertotal}",
                                "lineitems": [
                                    {
                                        "lineid": "{linenumber}",
                                        "namecode": {
                                            "codevalue": "{productid}",
                                            "shortname": "toy01",
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [
                            {"payload_key": "batchnumber", "source_key": "batchnumber"},
                        ],
                    },
                    {
                        "path": "orders",
                        "mappings": [
                            {"payload_key": "ordernumber", "source_key": "ordernumber"},
                            {"payload_key": "ordertotal", "source_key": "ordertotal"},
                        ],
                    },
                    {
                        "path": "orders.lineitems",
                        "mappings": [
                            {"payload_key": "linenumber", "source_key": "linenumber"},
                            {"payload_key": "productid", "source_key": "productid"},
                        ],
                    },
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    assert len(df_payloads) == 1
    p = df_payloads["payload"].iloc[0][0]

    assert p["batchid"] == "BATCH-001"
    assert p["location"] == "loc01"
    assert len(p["orders"]) == 1
    order = p["orders"][0]
    assert order["orderid"] == "ORD-101"
    assert order["ordertotal"] == 250.0

    lineitems = order["lineitems"]
    assert len(lineitems) == 2
    assert lineitems[0] == {
        "lineid": "L1",
        "namecode": {
            "codevalue": "PRD-99",
            "shortname": "toy01",
        },
    }
    assert lineitems[1] == {
        "lineid": "L2",
        "namecode": {
            "codevalue": "PRD-100",
            "shortname": "toy01",
        },
    }


def test_polars_dataframe_support_and_return_type():
    """Verify that passing a polars.DataFrame returns a polars.DataFrame with 7 columns and reconciles."""
    import polars as pl

    pldf = pl.DataFrame({
        "Batch": ["B1", "B1", "B2"],
        "Order": ["O1", "O2", "O3"],
        "SKU": ["SKU1", "SKU2", "SKU3"],
    })

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [
                    {
                        "batch": "{batch}",
                        "orders": [{"order": "{order}", "items": [{"sku": "{sku}"}]}],
                    }
                ],
                "entities": [
                    {"path": "root", "repeat_limit": 1, "group_by": ["batch"], "mappings": [{"payload_key": "batch", "source_key": "Batch"}]},
                    {"path": "orders", "group_by": ["order"], "mappings": [{"payload_key": "order", "source_key": "Order"}]},
                    {"path": "orders.items", "mappings": [{"payload_key": "sku", "source_key": "SKU"}]},
                ],
            }
        ],
    }

    result, _ = pld.build_payloads(source=pldf, config=config, output_format="dict")
    assert isinstance(result, pl.DataFrame)
    assert len(result) == 2  # 2 payloads: B1 and B2
    assert "rows_in_payload" in result.columns
    assert "running_total" in result.columns
    assert "payload" in result.columns

    report = pld.reconcile(result, expected_rows=3)
    assert report.is_balanced is True
    assert report.total_source_rows == 3


def test_chunksize_in_memory_batching():
    """Verify that chunksize splits large input into sequential slices and produces a single unified DataFrame."""
    rows = [{"id": f"ID_{i}", "val": i} for i in range(25)]
    df = pd.DataFrame(rows)

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [{"id": "{id}", "val": "{val}"}],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [
                            {"payload_key": "id", "source_key": "id"},
                            {"payload_key": "val", "source_key": "val"},
                        ],
                    }
                ],
            }
        ],
    }

    # Process with chunksize=10 (chunks: 10, 10, 5)
    result, _ = pld.build_payloads(source=df, config=config, chunksize=10, output_format="dict")
    assert isinstance(result, pd.DataFrame)
    # Each chunk of rows is packaged into a payload, yielding 3 payloads
    assert len(result) == 3
    assert result["rows_in_payload"].iloc[0] == 10
    assert result["rows_in_payload"].iloc[1] == 10
    assert result["rows_in_payload"].iloc[2] == 5
    assert result["running_total"].iloc[-1] == 25

    report = pld.reconcile(result, expected_rows=25)
    assert report.is_balanced is True


def test_chunksize_stream_generator_mode():
    """Verify stream=True returns an iterator yielding mini-DataFrames matching input type."""
    import polars as pl

    pldf = pl.DataFrame({
        "id": [f"ID_{i}" for i in range(15)],
        "val": list(range(15)),
    })

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "condition_rule": [],
                "payload_template": [{"id": "{id}", "val": "{val}"}],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [
                            {"payload_key": "id", "source_key": "id"},
                            {"payload_key": "val", "source_key": "val"},
                        ],
                    }
                ],
            }
        ],
    }

    # Stream in chunks of 5 rows (yields 3 mini-DataFrames, each containing 1 payload of 5 rows)
    stream_gen = pld.build_payloads(source=pldf, config=config, chunksize=5, stream=True, output_format="dict")
    chunks = list(stream_gen)
    assert len(chunks) == 3
    for chunk in chunks:
        assert isinstance(chunk, pl.DataFrame)
        assert len(chunk) == 1
        assert chunk["rows_in_payload"][0] == 5

    # Concat and reconcile
    combined = pl.concat(chunks)
    report = pld.reconcile(combined, expected_rows=15)
    assert report.is_balanced is True


def test_meta_summary_and_return_meta_pandas():
    """Verify that  returns (payloads_df, meta_df) with accurate condition aggregations in pandas."""
    df = pd.DataFrame([
        {"status": "Create", "val": 10},
        {"status": "Create", "val": 20},
        {"status": "Create", "val": 30},
        {"status": "Update", "val": 40},
    ])

    config = {
        "condition_source_key": "status",
        "conditions": [
            {
                "condition_rule": ["Create"],
                "payload_template": [{"status": "{status}", "val": "{val}"}],
                "entities": [
                    {
                        "path": "root",
                        "repeat_limit": 2,  # 3 rows -> 2 payloads (2, 1)
                        "mappings": [
                            {"payload_key": "status", "source_key": "status"},
                            {"payload_key": "val", "source_key": "val"},
                        ],
                    }
                ],
            },
            {
                "condition_rule": ["Update"],
                "payload_template": [{"status": "{status}", "val": "{val}"}],
                "entities": [
                    {
                        "path": "root",
                        "mappings": [
                            {"payload_key": "status", "source_key": "status"},
                            {"payload_key": "val", "source_key": "val"},
                        ],
                    }
                ],
            },
        ],
    }

    payloads_df, meta_df = pld.build_payloads(df, config=config)
    assert isinstance(payloads_df, pd.DataFrame)
    assert isinstance(meta_df, pd.DataFrame)
    assert len(payloads_df) == 3  # 2 for Create, 1 for Update

    # Verify meta_df rows: Create, Update, and TOTAL
    assert len(meta_df) == 3
    create_meta = meta_df[meta_df["condition_value"] == "Create"].iloc[0]
    assert create_meta["payload_count"] == 2
    assert create_meta["total_rows"] == 3
    assert create_meta["avg_rows_per_payload"] == 1.5
    assert create_meta["min_rows"] == 1
    assert create_meta["max_rows"] == 2

    update_meta = meta_df[meta_df["condition_value"] == "Update"].iloc[0]
    assert update_meta["payload_count"] == 1
    assert update_meta["total_rows"] == 1
    assert update_meta["avg_rows_per_payload"] == 1.0

    total_meta = meta_df[meta_df["condition_rule"] == "ALL"].iloc[0]
    assert total_meta["condition_value"] == "TOTAL"
    assert total_meta["payload_count"] == 3
    assert total_meta["total_rows"] == 4

    # Standalone summarize() check
    standalone_meta = pld.summarize(payloads_df)
    assert len(standalone_meta) == 3


def test_meta_summary_and_return_meta_polars():
    """Verify that  returns a polars.DataFrame meta summary when given polars input."""
    import polars as pl

    pldf = pl.DataFrame({
        "status": ["A", "A", "B"],
        "num": [1, 2, 3],
    })

    config = {
        "condition_source_key": "status",
        "conditions": [
            {
                "condition_rule": ["A"],
                "payload_template": [{"status": "{status}", "num": "{num}"}],
                "entities": [
                    {"path": "root", "mappings": [{"payload_key": "status", "source_key": "status"}, {"payload_key": "num", "source_key": "num"}]},
                ],
            },
            {
                "condition_rule": ["B"],
                "payload_template": [{"status": "{status}", "num": "{num}"}],
                "entities": [
                    {"path": "root", "mappings": [{"payload_key": "status", "source_key": "status"}, {"payload_key": "num", "source_key": "num"}]},
                ],
            },
        ],
    }

    payloads_df, meta_df = pld.build_payloads(pldf, config=config)
    assert isinstance(payloads_df, pl.DataFrame)
    assert isinstance(meta_df, pl.DataFrame)
    assert len(meta_df) == 3  # A, B, and TOTAL
    total_row = meta_df.filter(pl.col("condition_rule") == "ALL")
    assert total_row["payload_count"][0] == 2
    assert total_row["total_rows"][0] == 3


def test_sibling_direct_children_zip_longest_and_empty_padding():
    """Verify that multiple sibling direct children (e.g. orders and returns) chunk independently and pair with zip-longest."""
    df = pd.DataFrame([
        {
            "batch_id": "BATCH_001",
            "orders_data": [{"id": f"ord_{i}", "val": i * 10} for i in range(1, 71)],
            "returns_data": [{"id": f"ret_{j}", "reason": "defective"} for j in range(1, 11)],
        }
    ])

    template = {
        "batch_id": "{batch_id}",
        "orders": [{"order_id": "{id}", "order_val": "{val}"}],
        "returns": [{"return_id": "{id}", "reason": "{reason}"}],
    }

    config = {
        "entities": [
            {
                "path": "root",
                "group_by": ["batch_id"],
                "mappings": [{"payload_key": "batch_id", "source_key": "batch_id"}],
            },
            {
                "path": "orders",
                "source_column": "orders_data",
                "repeat_limit": 40,
                "mappings": [
                    {"payload_key": "id", "source_key": "id"},
                    {"payload_key": "val", "source_key": "val"},
                ],
            },
            {
                "path": "returns",
                "source_column": "returns_data",
                "repeat_limit": 40,
                "omit_if_blank": False,
                "mappings": [
                    {"payload_key": "id", "source_key": "id"},
                    {"payload_key": "reason", "source_key": "reason"},
                ],
            },
        ]
    }

    payloads_df, meta_df = pld.build_payloads(df, config=config, template=template, output_format="dict")
    assert len(payloads_df) == 2

    # Payload 1 has 40 orders and 10 returns
    p1 = payloads_df["payload"].iloc[0]
    assert len(p1["orders"]) == 40
    assert len(p1["returns"]) == 10

    # Payload 2 has remaining 30 orders and empty [] returns
    p2 = payloads_df["payload"].iloc[1]
    assert len(p2["orders"]) == 30
    assert p2["returns"] == []


def test_sibling_direct_children_with_omit_if_blank():
    """Verify that sibling direct children with omit_if_blank=True are completely omitted when empty."""
    df = pd.DataFrame([
        {
            "batch_id": "BATCH_001",
            "orders_data": [{"id": f"ord_{i}", "val": i * 10} for i in range(1, 71)],
            "returns_data": [{"id": f"ret_{j}", "reason": "defective"} for j in range(1, 11)],
        }
    ])

    template = {
        "batch_id": "{batch_id}",
        "orders": [{"order_id": "{id}", "order_val": "{val}"}],
        "returns": [{"return_id": "{id}", "reason": "{reason}"}],
    }

    config = {
        "entities": [
            {
                "path": "root",
                "group_by": ["batch_id"],
                "mappings": [{"payload_key": "batch_id", "source_key": "batch_id"}],
            },
            {
                "path": "orders",
                "source_column": "orders_data",
                "repeat_limit": 40,
                "mappings": [
                    {"payload_key": "id", "source_key": "id"},
                    {"payload_key": "val", "source_key": "val"},
                ],
            },
            {
                "path": "returns",
                "source_column": "returns_data",
                "repeat_limit": 40,
                "omit_if_blank": True,
                "mappings": [
                    {"payload_key": "id", "source_key": "id"},
                    {"payload_key": "reason", "source_key": "reason"},
                ],
            },
        ]
    }

    payloads_df, meta_df = pld.build_payloads(df, config=config, template=template, output_format="dict")
    assert len(payloads_df) == 2

    # Payload 1 has both
    p1 = payloads_df["payload"].iloc[0]
    assert len(p1["orders"]) == 40
    assert len(p1["returns"]) == 10

    # Payload 2 has orders, returns key is completely omitted
    p2 = payloads_df["payload"].iloc[1]
    assert len(p2["orders"]) == 30
    assert "returns" not in p2


def test_batch_3_children_with_1_2_3_grandchildren():
    """Verify arbitrary tree scenario: 1 Batch (parent) with 3 children:
    - Child 1 (orders): has 1 child (items)
    - Child 2 (returns): has 2 children (items, notes)
    - Child 3 (shipments): has 3 children (packages, routes, customs)
    Validates accurate hydration, chunking, and node_totals reconciliation.
    """
    df = pd.DataFrame([
        {
            "batch_id": "BATCH_2026",
            "orders_data": [
                {
                    "order_id": "ORD_01",
                    "items": [{"item_id": "ITM_1", "qty": 5}, {"item_id": "ITM_2", "qty": 10}],
                },
                {
                    "order_id": "ORD_02",
                    "items": [{"item_id": "ITM_3", "qty": 2}],
                },
            ],
            "returns_data": [
                {
                    "return_id": "RET_01",
                    "items": [{"item_id": "R_ITM_1", "reason": "damaged"}],
                    "notes": [{"note_id": "NOTE_1", "text": "Customer called"}],
                }
            ],
            "shipments_data": [
                {
                    "shipment_id": "SHIP_01",
                    "packages": [{"pkg_id": "PKG_1", "weight": 2.5}],
                    "routes": [{"stop_id": "STP_1", "city": "New York"}],
                    "customs": [{"duty_id": "DUTY_1", "code": "HS8471"}],
                }
            ],
        }
    ])

    template = {
        "batch_id": "{batch_id}",
        "orders": [
            {
                "order_id": "{order_id}",
                "items": [{"item_id": "{item_id}", "qty": "{qty}"}],
            }
        ],
        "returns": [
            {
                "return_id": "{return_id}",
                "items": [{"item_id": "{item_id}", "reason": "{reason}"}],
                "notes": [{"note_id": "{note_id}", "text": "{text}"}],
            }
        ],
        "shipments": [
            {
                "shipment_id": "{shipment_id}",
                "packages": [{"pkg_id": "{pkg_id}", "weight": "{weight}"}],
                "routes": [{"stop_id": "{stop_id}", "city": "{city}"}],
                "customs": [{"duty_id": "{duty_id}", "code": "{code}"}],
            }
        ],
    }

    config = {
        "entities": [
            {
                "path": "root",
                "group_by": ["batch_id"],
                "mappings": [{"payload_key": "batch_id", "source_key": "batch_id"}],
            },
            # Child 1: orders -> 1 grandchild: items
            {
                "path": "orders",
                "source_column": "orders_data",
                "mappings": [{"payload_key": "order_id", "source_key": "order_id"}],
            },
            {
                "path": "orders.items",
                "source_column": "items",
                "mappings": [
                    {"payload_key": "item_id", "source_key": "item_id"},
                    {"payload_key": "qty", "formula": "int({qty})"},
                ],
            },
            # Child 2: returns -> 2 grandchildren: items, notes
            {
                "path": "returns",
                "source_column": "returns_data",
                "mappings": [{"payload_key": "return_id", "source_key": "return_id"}],
            },
            {
                "path": "returns.items",
                "source_column": "items",
                "mappings": [
                    {"payload_key": "item_id", "source_key": "item_id"},
                    {"payload_key": "reason", "source_key": "reason"},
                ],
            },
            {
                "path": "returns.notes",
                "source_column": "notes",
                "mappings": [
                    {"payload_key": "note_id", "source_key": "note_id"},
                    {"payload_key": "text", "source_key": "text"},
                ],
            },
            # Child 3: shipments -> 3 grandchildren: packages, routes, customs
            {
                "path": "shipments",
                "source_column": "shipments_data",
                "mappings": [{"payload_key": "shipment_id", "source_key": "shipment_id"}],
            },
            {
                "path": "shipments.packages",
                "source_column": "packages",
                "mappings": [
                    {"payload_key": "pkg_id", "source_key": "pkg_id"},
                    {"payload_key": "weight", "formula": "float({weight})"},
                ],
            },
            {
                "path": "shipments.routes",
                "source_column": "routes",
                "mappings": [
                    {"payload_key": "stop_id", "source_key": "stop_id"},
                    {"payload_key": "city", "source_key": "city"},
                ],
            },
            {
                "path": "shipments.customs",
                "source_column": "customs",
                "mappings": [
                    {"payload_key": "duty_id", "source_key": "duty_id"},
                    {"payload_key": "code", "source_key": "code"},
                ],
            },
        ]
    }

    payloads_df, meta_df = pld.build_payloads(df, config=config, template=template, output_format="dict")
    assert len(payloads_df) == 1

    p = payloads_df["payload"].iloc[0]
    assert p["batch_id"] == "BATCH_2026"

    # Child 1: orders (2) -> items (3 total across both orders)
    assert len(p["orders"]) == 2
    assert len(p["orders"][0]["items"]) == 2
    assert p["orders"][0]["items"][0] == {"item_id": "ITM_1", "qty": 5}
    assert len(p["orders"][1]["items"]) == 1

    # Child 2: returns (1) -> 2 grandchildren: items (1), notes (1)
    assert len(p["returns"]) == 1
    assert len(p["returns"][0]["items"]) == 1
    assert p["returns"][0]["items"][0] == {"item_id": "R_ITM_1", "reason": "damaged"}
    assert len(p["returns"][0]["notes"]) == 1
    assert p["returns"][0]["notes"][0] == {"note_id": "NOTE_1", "text": "Customer called"}

    # Child 3: shipments (1) -> 3 grandchildren: packages (1), routes (1), customs (1)
    assert len(p["shipments"]) == 1
    assert len(p["shipments"][0]["packages"]) == 1
    assert p["shipments"][0]["packages"][0] == {"pkg_id": "PKG_1", "weight": 2.5}
    assert len(p["shipments"][0]["routes"]) == 1
    assert p["shipments"][0]["routes"][0] == {"stop_id": "STP_1", "city": "New York"}
    assert len(p["shipments"][0]["customs"]) == 1
    assert p["shipments"][0]["customs"][0] == {"duty_id": "DUTY_1", "code": "HS8471"}

    # Check node_totals in meta_df
    total_row = meta_df[meta_df["condition_rule"] == "ALL"].iloc[0]
    node_totals = total_row["node_totals"]
    assert node_totals["orders"] == 2
    assert node_totals["orders.items"] == 3
    assert node_totals["returns"] == 1
    assert node_totals["returns.items"] == 1
    assert node_totals["returns.notes"] == 1
    assert node_totals["shipments"] == 1
    assert node_totals["shipments.packages"] == 1
    assert node_totals["shipments.routes"] == 1
    assert node_totals["shipments.customs"] == 1


def test_wrapper_envelope_injection():
    """Verify that wrapper_envelope embeds generated payloads and hydrates header fields."""
    df = pd.DataFrame([
        {
            "batch_id": "BATCH_99",
            "terminal": "CLOCK1",
            "badge": "B101",
            "action": "lunchout",
        },
        {
            "batch_id": "BATCH_99",
            "terminal": "CLOCK1",
            "badge": "B102",
            "action": "lunchin",
        },
    ])

    config = {
        "condition_source_key": "",
        "conditions": [
            {
                "wrapper_envelope": {
                    "batchID": "{batch_id}",
                    "events": [
                        {
                            "serviceCategoryCode": {"codeValue": "core"},
                            "data": {
                                "transform": {
                                    "dataCollectionEntries": "{payload_template}"
                                }
                            }
                        }
                    ]
                },
                "payload_template": [
                    {
                        "terminalName": "{terminal}",
                        "workerEntries": [
                            {
                                "badgeID": "{badge}",
                                "actionCode": {"codeValue": "{action}"}
                            }
                        ]
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "group_by": ["terminal"],
                        "mappings": [
                            {"payload_key": "batch_id", "source_key": "batch_id"},
                            {"payload_key": "terminal", "source_key": "terminal"},
                        ],
                    },
                    {
                        "path": "workerEntries",
                        "mappings": [
                            {"payload_key": "badge", "source_key": "badge"},
                            {"payload_key": "action", "source_key": "action"},
                        ],
                    },
                ],
            }
        ],
    }

    df_payloads, meta_df = pld.build_payloads(source=df, config=config, output_format="dict")
    assert len(df_payloads) == 1
    p = df_payloads["payload"].iloc[0]

    assert p["batchID"] == "BATCH_99"
    assert "events" in p
    assert p["events"][0]["serviceCategoryCode"]["codeValue"] == "core"
    entries = p["events"][0]["data"]["transform"]["dataCollectionEntries"]
    assert len(entries) == 1
    assert entries[0]["terminalName"] == "CLOCK1"
    workers = entries[0]["workerEntries"]
    assert len(workers) == 2
    assert workers[0]["badgeID"] == "B101"
    assert workers[0]["actionCode"]["codeValue"] == "lunchout"
    assert workers[1]["badgeID"] == "B102"
    assert workers[1]["actionCode"]["codeValue"] == "lunchin"

