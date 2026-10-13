"""PUT the Spark-cleaned Parquet output to an internal stage and COPY INTO the
raw Snowflake tables. Run after both clean_drug_ndc.py and
clean_drug_adverse_events.py have produced their output.

Reads the same SNOWFLAKE_* env vars dbt's profiles.yml uses (key-pair auth).
"""
import os
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization

_CONTAINER_LANDED_ROOT = Path("/opt/landed")
LANDED_ROOT = (
    _CONTAINER_LANDED_ROOT
    if _CONTAINER_LANDED_ROOT.is_dir()
    else Path(__file__).resolve().parents[1] / "landed"
)

TABLES = {
    "drug_ndc_products": LANDED_ROOT / "drug_ndc" / "cleaned" / "products",
    "drug_adverse_events_reports": LANDED_ROOT / "drug_adverse_events" / "cleaned" / "reports",
}


def get_ddl(schema: str) -> dict[str, str]:
    """Generate table DDL using the configured Snowflake schema."""
    return {
        "drug_ndc_products": f"""
            create table if not exists {schema}.drug_ndc_products (
                product_ndc string,
                product_id string,
                spl_id string,
                generic_name string,
                brand_name string,
                brand_name_base string,
                labeler_name string,
                dosage_form string,
                product_type string,
                marketing_category string,
                application_number string,
                route variant,
                finished boolean,
                marketing_start_date date,
                listing_expiration_date date,
                dea_schedule string,
                manufacturer_name string,
                rxcui_list variant,
                spl_set_id string,
                unii string,
                pharm_class_epc string,
                pharm_class_moa string,
                pharm_class_pe string,
                pharm_class_cs string,
                active_ingredients variant,
                packaging variant
            )
        """,
        "drug_adverse_events_reports": f"""
            create table if not exists {schema}.drug_adverse_events_reports (
                safetyreportid string,
                safetyreportversion integer,
                primarysourcecountry string,
                occurcountry string,
                reporttype string,
                authoritynumb string,
                companynumb string,
                transmissiondate date,
                receivedate date,
                receiptdate date,
                serious boolean,
                seriousnessdeath boolean,
                seriousnesslifethreatening boolean,
                seriousnesshospitalization boolean,
                seriousnessdisabling boolean,
                seriousnesscongenitalanomali boolean,
                seriousnessother boolean,
                duplicate boolean,
                duplicate_source string,
                duplicate_numb string,
                reporter_country string,
                reporter_qualification string,
                reporter_literaturereference string,
                sender_type string,
                sender_organization string,
                receiver_type string,
                receiver_organization string,
                patient_onsetage string,
                patient_onsetage_unit string,
                patient_weight string,
                patient_sex string,
                patient_agegroup string,
                narrative string,
                reaction variant,
                drug variant
            )
        """,
    }



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


def _connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=_load_private_key(),
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
    )



def main() -> None:
    # Read the target schema from airflow.env
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "RAW").upper()

    # Use the same schema consistently for tables and stage
    ddl = get_ddl(schema)

    conn = _connect()

    try:
        with conn.cursor() as cur:
            cur.execute(f"create schema if not exists {schema}")
            cur.execute(f"create stage if not exists {schema}.raw_stage")

            for table_name, parquet_dir in TABLES.items():
                if not parquet_dir.is_dir():
                    print(
                        f"Skipping {table_name}: {parquet_dir} not found "
                        "(run the Spark job first)"
                    )
                    continue

                print(f"Loading {table_name} from {parquet_dir} ...")
                cur.execute(ddl[table_name])

                stage_path = f"@{schema}.raw_stage/{table_name}/"

                cur.execute(f"remove {stage_path}")
                cur.execute(
                    f"put file://{parquet_dir}/*.parquet {stage_path} "
                    "auto_compress=false overwrite=true"
                )

                cur.execute(f"truncate table {schema}.{table_name}")

                cur.execute(
                    f"""
                    copy into {schema}.{table_name}
                    from {stage_path}
                    file_format = (type = parquet)
                    match_by_column_name = case_insensitive
                    """
                )

                count = cur.execute(
                    f"select count(*) from {schema}.{table_name}"
                ).fetchone()[0]

                print(f"  -> {count} rows loaded into {schema}.{table_name}")
    finally:
        conn.close()



if __name__ == "__main__":
    main()