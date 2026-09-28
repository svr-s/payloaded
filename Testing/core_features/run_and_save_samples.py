"""Script to run test scenarios and export generated payloads and DataFrames to disk."""

import json
from pathlib import Path
import pandas as pd
import payloaded as pld


def main():
    output_dir = Path(__file__).parent / "sample_outputs"
    output_dir.mkdir(exist_ok=True)

    print("Running 3-level hierarchy scenario (95 items across composite keys)...")
    # 1. 3-Level hierarchy with composite keys (Batch -> Orders -> LineItems)
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
                    {"payload_key": "qty", "file_key": "Quantity", "type_cast": "int"},
                ],
            },
        ]
    }

    # Generate payloads with indent=2 for human-readable inspection
    df_payloads, meta_df = pld.build_payloads(
        source=df_source,
        template=template,
        config=config,
        output_format="json_string",
        indent=2,
    )

    # Save DataFrame to CSV
    csv_path = output_dir / "three_level_payload_df.csv"
    df_payloads.to_csv(csv_path, index=False)
    print(f"Saved output DataFrame to: {csv_path}")

    # Save individual JSON payload for Postman
    json_path = output_dir / "three_level_payload_postman.json"
    raw_payload_str = df_payloads["payload"].iloc[0]
    json_path.write_text(raw_payload_str, encoding="utf-8")
    print(f"Saved Postman payload to: {json_path}")

    # Reconcile audit
    report = pld.reconcile(df_payloads, expected_rows=len(df_source))
    audit_path = output_dir / "three_level_reconciliation_report.txt"
    audit_path.write_text(report.summary(), encoding="utf-8")
    print(f"Saved reconciliation report to: {audit_path}")

    # 2. Multi-payload scenario (Root limit chunking)
    print("\nRunning root limit scenario (5 batches with limit=2)...")
    batch_rows = []
    for b in range(5):
        batch_rows.append({
            "Batch_Number": f"BATCH_{b+1}",
            "Group_Code": "GRP_BETA",
            "Order_ID": f"ORD_{b+1}",
            "Item_SKU": f"SKU_{b+1}",
            "Quantity": 1,
        })
    df_batches = pd.DataFrame(batch_rows)

    root_config = {
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

    df_multi_payloads, multi_meta = pld.build_payloads(
        source=df_batches,
        template=template,
        config=root_config,
        indent=2,
    )

    multi_csv_path = output_dir / "root_limit_payloads_df.csv"
    df_multi_payloads.to_csv(multi_csv_path, index=False)
    print(f"Saved multi-payload DataFrame to: {multi_csv_path}")

    for idx, row in df_multi_payloads.iterrows():
        p_path = output_dir / f"root_limit_payload_{row['index']}_of_{len(df_multi_payloads)}.json"
        p_path.write_text(row["payload"], encoding="utf-8")
        print(f"Saved payload #{row['index']} to: {p_path}")

    # 3. Conditional routing scenario (New, Update, Terminated)
    print("\nRunning conditional routing scenario (New, Update, Terminated)...")
    cond_rows = [
        {"Status": "New", "Batch": "B001", "Order": "ORD_101", "SKU": "SKU_01", "Qty": 2},
        {"Status": "New", "Batch": "B001", "Order": "ORD_101", "SKU": "SKU_02", "Qty": 4},
        {"Status": "New", "Batch": "B001", "Order": "ORD_102", "SKU": "SKU_03", "Qty": 1},
        {"Status": "Update", "Batch": "B001", "Order": "ORD_103", "SKU": "SKU_04", "Qty": 5},
        {"Status": "Update", "Batch": "B001", "Order": "ORD_103", "SKU": "SKU_05", "Qty": 3},
        {"Status": "Terminated", "Batch": "B001", "Order": "ORD_104", "SKU": "SKU_06", "Qty": 0},
    ]
    df_cond = pd.DataFrame(cond_rows)

    cond_config = {
        "condition_source_key": "Status",
        "conditions": [
            {
                "condition_rule": ["~Terminated"],
                "payload_template": [
                    {
                        "batch_id": "{batch_id}",
                        "orders": [
                            {
                                "order_id": "{order_id}",
                                "line_items": [{"sku": "{sku}", "qty": "{qty}"}],
                            }
                        ],
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "group_by": ["batch_id"],
                        "mappings": [{"payload_key": "batch_id", "source_key": "Batch"}],
                    },
                    {
                        "path": "orders",
                        "group_by": ["order_id"],
                        "mappings": [{"payload_key": "order_id", "source_key": "Order"}],
                    },
                    {
                        "path": "orders.line_items",
                        "mappings": [
                            {"payload_key": "sku", "source_key": "SKU"},
                            {"payload_key": "qty", "source_key": "Qty", "type_cast": "int"},
                        ],
                    },
                ],
            },
            {
                "condition_rule": ["Terminated"],
                "payload_template": [
                    {
                        "batch_id": "{batch_id}",
                        "cancellations": [{"order_id": "{order_id}"}],
                    }
                ],
                "entities": [
                    {
                        "path": "root",
                        "group_by": ["batch_id"],
                        "mappings": [{"payload_key": "batch_id", "source_key": "Batch"}],
                    },
                    {
                        "path": "cancellations",
                        "group_by": ["order_id"],
                        "mappings": [{"payload_key": "order_id", "source_key": "Order"}],
                    },
                ],
            },
        ],
    }

    df_cond_payloads, cond_meta = pld.build_payloads(source=df_cond, config=cond_config, indent=2)
    cond_csv_path = output_dir / "conditional_payloads_df.csv"
    df_cond_payloads.to_csv(cond_csv_path, index=False)
    print(f"Saved conditional DataFrame to: {cond_csv_path}")

    for idx, row in df_cond_payloads.iterrows():
        p_path = output_dir / f"conditional_payload_{row['index']}_{row['condition_value']}.json"
        p_path.write_text(row["payload"], encoding="utf-8")
        print(f"Saved payload #{row['index']} ({row['condition_value']}) to: {p_path}")

    # Reconcile conditional
    cond_report = pld.reconcile(df_cond_payloads, expected_rows=len(df_cond))
    cond_audit_path = output_dir / "conditional_reconciliation_report.txt"
    cond_audit_path.write_text(cond_report.summary(), encoding="utf-8")
    # 4. Nested sub-dictionary block within lineitems (namecode block)
    print("\nRunning nested sub-dictionary (namecode block within lineitems) scenario...")
    df_nested = pd.DataFrame([
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

    nested_config = {
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

    df_nested_payloads, nested_meta = pld.build_payloads(source=df_nested, config=nested_config, indent=2)
    nested_csv_path = output_dir / "nested_subdictionary_payloads_df.csv"
    df_nested_payloads.to_csv(nested_csv_path, index=False)
    print(f"Saved nested sub-dictionary DataFrame to: {nested_csv_path}")

    nested_json_path = output_dir / "nested_subdictionary_payload.json"
    nested_json_path.write_text(df_nested_payloads["payload"].iloc[0], encoding="utf-8")
    print(f"Saved nested sub-dictionary JSON payload to: {nested_json_path}")

    print("\nAll outputs generated successfully!")


if __name__ == "__main__":
    main()
