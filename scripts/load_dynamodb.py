"""
load_dynamodb.py
----------------
Script to load sample e-commerce orders from sample_dataset.json
into the DynamoDB table 'orders-table'.

Usage:
    pip install boto3
    python scripts/load_dynamodb.py

Prerequisites:
    - AWS CLI configured (aws configure) with valid credentials
    - DynamoDB table 'orders-table' already created (via CloudFormation template)
"""

import boto3
import json
import os
from decimal import Decimal
from botocore.exceptions import ClientError

# ── Configuration ──────────────────────────────────────────────────────────────
TABLE_NAME   = "orders-table"
REGION       = "us-east-1"                     # Change to your AWS region
DATA_FILE    = os.path.join(os.path.dirname(__file__), "../data/sample_dataset.json")
# ───────────────────────────────────────────────────────────────────────────────


def float_to_decimal(obj):
    """
    DynamoDB does not accept Python float; convert all floats to Decimal.
    This function walks the JSON object recursively.
    """
    if isinstance(obj, list):
        return [float_to_decimal(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: float_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))
    return obj


def load_data(table, records: list) -> None:
    """Batch-write records into DynamoDB using batch_writer (max 25 per call)."""
    with table.batch_writer() as batch:
        for record in records:
            item = float_to_decimal(record)
            batch.put_item(Item=item)
            print(f"  ✔  Inserted order_id={item['order_id']}")


def main():
    # Load JSON data
    with open(DATA_FILE, "r") as f:
        records = json.load(f)

    print(f"\n🚀  Connecting to DynamoDB table '{TABLE_NAME}' in region '{REGION}' …\n")
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table    = dynamodb.Table(TABLE_NAME)

    # Quick sanity-check: verify table exists
    try:
        status = table.table_status
        print(f"   Table status : {status}\n")
    except ClientError as e:
        print(f"❌  Cannot find table '{TABLE_NAME}': {e.response['Error']['Message']}")
        raise SystemExit(1)

    print(f"📦  Loading {len(records)} records …\n")
    load_data(table, records)

    print(f"\n✅  Finished loading {len(records)} records into '{TABLE_NAME}'.")
    print("    Next step → trigger the Lambda function or wait for DynamoDB Streams to fire.\n")


if __name__ == "__main__":
    main()
