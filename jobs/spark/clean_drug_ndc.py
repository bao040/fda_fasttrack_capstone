"""Dedup the landed NDC Directory data into a SINGLE tabular output:
  cleaned/products/ -- 1 row per product_ndc; active_ingredients and packaging
                       are kept as ARRAY<STRUCT<...>> columns (no cross-join
                       between the two, no row explosion) — Snowflake loads
                       these natively and they can be LATERAL FLATTEN'd later
                       only if/when a query actually needs per-ingredient or
                       per-package rows.

Submit against the real cluster, e.g. from inside the airflow container:
  spark-submit --master spark://spark-master:7077 /opt/airflow/jobs/spark/clean_drug_ndc.py

Reads/writes under /opt/landed, which docker-compose mounts at the SAME path
for both the airflow and spark services — this matters because in client
deploy mode the driver (running wherever spark-submit was invoked) and the
executors (running inside spark-worker) must resolve the same local path.
"""
from pathlib import Path
import json

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

_CONTAINER_LANDED_ROOT = Path("/opt/landed")
LANDED_ROOT = (
    _CONTAINER_LANDED_ROOT
    if _CONTAINER_LANDED_ROOT.is_dir()
    else Path(__file__).resolve().parents[2] / "landed"
)

RAW_PATH = str(LANDED_ROOT / "drug_ndc" / "raw.json")
PRODUCTS_PATH = str(LANDED_ROOT / "drug_ndc" / "cleaned" / "products")


def _pharm_class_tag(tag: str):
    """Pull the first `pharm_class` entry ending in " [TAG]", stripped of the tag suffix."""
    return F.expr(
        f"""
        element_at(
            transform(
                filter(pharm_class, x -> x rlike '\\\\[{tag}\\\\]$'),
                x -> regexp_replace(x, ' \\\\[{tag}\\\\]$', '')
            ),
            1
        )
        """
    )


def main():
    spark = (
        SparkSession.builder.appName("clean_drug_ndc")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    with open(RAW_PATH, "r", encoding="utf-8") as f:
        payload = json.load(f)

    results = payload.get("results", [])

    if not results:
        raise ValueError(f"No results found in {RAW_PATH}")

    raw = spark.read.json(
        spark.sparkContext.parallelize(
            [json.dumps(record) for record in results]
        )
    )

    # Dedup by product_ndc. listing_expiration_date is often an identical
    # administrative cutoff across many products (not a reliable tiebreaker
    # on its own), so order by marketing_start_date first for a deterministic pick.
    window = Window.partitionBy("product_ndc").orderBy(
        F.col("marketing_start_date").desc(),
        F.col("listing_expiration_date").desc(),
    )
    deduped = (
        raw.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Rebuild packaging as the same array-of-struct shape, just with dates cast
    packaging_casted = F.expr(
        """
        transform(packaging, x -> named_struct(
            'package_ndc', x.package_ndc,
            'description', x.description,
            'marketing_start_date', to_date(x.marketing_start_date, 'yyyyMMdd'),
            'marketing_end_date', to_date(x.marketing_end_date, 'yyyyMMdd'),
            'sample', boolean(x.sample)
        ))
        """
    )

    products = deduped.select(
        "product_ndc",
        "product_id",
        "spl_id",
        "generic_name",
        "brand_name",
        "brand_name_base",
        "labeler_name",
        "dosage_form",
        "product_type",
        "marketing_category",
        "application_number",
        "route",
        F.col("finished").cast("boolean").alias("finished"),
        F.to_date("marketing_start_date", "yyyyMMdd").alias("marketing_start_date"),
        F.to_date("listing_expiration_date", "yyyyMMdd").alias("listing_expiration_date"),
        "dea_schedule",
        F.element_at("openfda.manufacturer_name", 1).alias("manufacturer_name"),
        F.col("openfda.rxcui").alias("rxcui_list"),
        F.element_at("openfda.spl_set_id", 1).alias("spl_set_id"),
        F.element_at("openfda.unii", 1).alias("unii"),
        F.coalesce(F.element_at("openfda.pharm_class_epc", 1), _pharm_class_tag("EPC")).alias("pharm_class_epc"),
        F.coalesce(F.element_at("openfda.pharm_class_moa", 1), _pharm_class_tag("MoA")).alias("pharm_class_moa"),
        F.coalesce(F.element_at("openfda.pharm_class_pe", 1), _pharm_class_tag("PE")).alias("pharm_class_pe"),
        F.coalesce(F.element_at("openfda.pharm_class_cs", 1), _pharm_class_tag("CS")).alias("pharm_class_cs"),
        "active_ingredients",
        packaging_casted.alias("packaging"),
    )

    products.write.mode("overwrite").parquet(PRODUCTS_PATH)
    print(f"Wrote single table output to {PRODUCTS_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()


