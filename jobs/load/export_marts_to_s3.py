"""Export dbt mart tables from Snowflake to local CSV, then upload to Floci's S3.
 
Run this on the HOST (not inside a Docker container) — Floci publishes its S3
endpoint to the host, avoiding the sibling-container networking issue
entirely (see conversation notes).
 
Prerequisites:
  floci start
  eval "$(floci env)"          # not required by this script directly, but
                                # confirms Floci is up before running
  pip install boto3
 
Reads the same SNOWFLAKE_* env vars load_to_snowflake.py uses.
"""
import csv
import os
from pathlib import Path
 
import boto3
import snowflake.connector
from cryptography.hazmat.primitives import serialization
 
MART_TABLES = [
    "dim_drug",
    "dim_patient",
    "dim_reporter",
    "dim_date",
    "bridge_report_drug",
    "fact_adverse_event_reaction",
]
 
EXPORT_DIR = Path(__file__).resolve().parent / "_export"
S3_BUCKET = os.environ.get("FLOCI_S3_BUCKET", "fda-capstone-landing")
FLOCI_ENDPOINT_URL = os.environ.get("FLOCI_ENDPOINT_URL", "http://localhost.floci.io:4566")
 
 
def _load_private_key() -> bytes:
    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None
    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(),
            password=passphrase.encode() if passphrase else None,
        )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
 
 
def _connect_snowflake():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=_load_private_key(),
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema="MARTS",
    )
 
 
def _export_table_to_csv(cur, table_name: str, dest: Path) -> int:
    cur.execute(f"select * from marts.{table_name}")
    columns = [col[0] for col in cur.description]
    rows = cur.fetchall()
    with open(dest, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    return len(rows)
 
 
def main() -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
 
    conn = _connect_snowflake()
    cur = conn.cursor()
 
    s3 = boto3.client(
        "s3",
        endpoint_url=FLOCI_ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
 
    for table_name in MART_TABLES:
        dest = EXPORT_DIR / f"{table_name}.csv"
        row_count = _export_table_to_csv(cur, table_name, dest)
        s3_key = f"marts/{table_name}.csv"
        s3.upload_file(str(dest), S3_BUCKET, s3_key)
        print(f"{table_name}: {row_count} rows -> s3://{S3_BUCKET}/{s3_key}")
 
    cur.close()
    conn.close()
 
 
if __name__ == "__main__":
    main()