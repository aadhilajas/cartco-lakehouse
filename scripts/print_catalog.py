#!/usr/bin/env python3
"""CartCo Lakehouse - Live Iceberg REST Data Catalog Printer

Connects to the Iceberg REST catalog and prints all namespaces, tables,
partition specifications, and column-level schemas.
"""

from __future__ import annotations

import os
import sys
from pyiceberg.catalog import load_catalog

ICEBERG_CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")


def get_catalog():
    """Load PyIceberg REST catalog."""
    return load_catalog(
        "cartco",
        **{
            "type": "rest",
            "uri": ICEBERG_CATALOG_URI,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


def print_catalog():
    """Traverse and print all namespaces, tables, and schemas."""
    catalog = get_catalog()
    print("================================================================================")
    print("                       CARTCO LAKEHOUSE DATA CATALOG                            ")
    print("================================================================================")
    print(f"Catalog URI: {ICEBERG_CATALOG_URI}")
    print(f"S3 Endpoint: {S3_ENDPOINT}")
    print("--------------------------------------------------------------------------------\n")

    namespaces = catalog.list_namespaces()
    if not namespaces:
        print("No namespaces found in catalog.")
        return

    for ns_tuple in namespaces:
        ns = ns_tuple[0] if isinstance(ns_tuple, tuple) else ns_tuple
        print(f"📁 Namespace: {ns}")
        tables = catalog.list_tables(ns)
        if not tables:
            print("  (no tables found)")
            continue

        for table_identifier in tables:
            full_table_name = ".".join(table_identifier) if isinstance(table_identifier, tuple) else str(table_identifier)
            try:
                table = catalog.load_table(full_table_name)
                schema = table.schema()
                spec = table.spec()
                
                print(f"\n  📊 Table: {full_table_name}")
                print(f"     Owner: Data Platform Team (single-person for this project)")
                print(f"     Partition Spec: {spec}")
                print("     Columns:")
                for field in schema.fields:
                    nullable_str = "NULLABLE" if field.optional else "NOT NULL"
                    print(f"       - {field.name:<24} | {str(field.field_type):<18} | {nullable_str}")
            except Exception as exc:
                print(f"     ❌ Error loading table {full_table_name}: {exc}")
        print("\n" + "-" * 80 + "\n")


if __name__ == "__main__":
    print_catalog()
