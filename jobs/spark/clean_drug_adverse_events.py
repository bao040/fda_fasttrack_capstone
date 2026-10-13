"""Dedup the landed drug adverse event reports into a SINGLE tabular output:
  cleaned/reports/ -- 1 row per safetyreportid; reaction and drug are kept
                      as ARRAY<STRUCT<...>> columns (no cross-join between the
                      two — openFDA's own docs confirm no drug is linked to any
                      specific reaction within a report). LATERAL FLATTEN these
                      later in dbt only when building fact_adverse_event_reaction.

Submit against the real cluster, e.g. from inside the airflow container:
  spark-submit --master spark://spark-master:7077 /opt/airflow/jobs/spark/clean_drug_adverse_events.py
"""
from pathlib import Path
import json

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

_CONTAINER_LANDED_ROOT = Path("/opt/landed")
LANDED_ROOT = (
    _CONTAINER_LANDED_ROOT
    if _CONTAINER_LANDED_ROOT.is_dir()
    else Path(__file__).resolve().parents[2] / "landed"
)

RAW_PATH = str(LANDED_ROOT / "drug_adverse_events" / "raw.json")
REPORTS_PATH = str(LANDED_ROOT / "drug_adverse_events" / "cleaned" / "reports")

# openFDA represents "no" either by an explicit '2' or by omitting the field
# entirely — this works for both data vintages (see conversation notes).
_SERIOUSNESS_FIELDS = [
    "seriousnessdeath",
    "seriousnesslifethreatening",
    "seriousnesshospitalization",
    "seriousnessdisabling",
    "seriousnesscongenitalanomali",
    "seriousnessother",
]


def _bool_flag(col: str):
    return F.coalesce(F.col(col) == "1", F.lit(False)).alias(col)


def _safe_struct_col(df, path: str, alias: str):
    """Extract a dotted nested field, defaulting to NULL if any ancestor along
    the path isn't actually a STRUCT in the inferred schema. FAERS data is
    inconsistent across report submitters — an optional nested object
    (reportduplicate, primarysource, sender...) sometimes shows up as a bare
    string in some records, which collapses the whole column's inferred type
    and breaks dot-notation access at the analysis stage (not something a
    runtime null-check like coalesce() can work around)."""
    parts = path.split(".")
    current_type = df.schema
    for part in parts:
        field = (
            next((f for f in current_type.fields if f.name == part), None)
            if isinstance(current_type, StructType)
            else None
        )
        if field is None:
            return F.lit(None).cast("string").alias(alias)
        current_type = field.dataType
        if part != parts[-1] and not isinstance(current_type, StructType):
            return F.lit(None).cast("string").alias(alias)
    return F.col(path).alias(alias)


def main():
    spark = (
        SparkSession.builder.appName("clean_drug_adverse_events")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    # openFDA raw.json is a response object containing a "results" array.
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

    # Dedup by safetyreportid, keeping the highest safetyreportversion
    window = Window.partitionBy("safetyreportid").orderBy(
        F.col("safetyreportversion").cast("int").desc()
    )
    deduped = (
        raw.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # drug[] keeps its nested openfda sub-object; cast its date strings and
    # pull activesubstancename/openfda fields up a level for easier querying later
    drug_casted = F.expr(
        """
        transform(patient.drug, x -> named_struct(
            'drugcharacterization', x.drugcharacterization,
            'medicinalproduct', x.medicinalproduct,
            'drugindication', x.drugindication,
            'drugadministrationroute', x.drugadministrationroute,
            'drugdosagetext', x.drugdosagetext,
            'drugstartdate', to_date(x.drugstartdate, 'yyyyMMdd'),
            'drugenddate', to_date(x.drugenddate, 'yyyyMMdd'),
            'activesubstancename', x.activesubstance.activesubstancename,
            'openfda_product_ndc', x.openfda.product_ndc,
            'openfda_package_ndc', x.openfda.package_ndc,
            'openfda_brand_name', x.openfda.brand_name,
            'openfda_generic_name', x.openfda.generic_name,
            'openfda_manufacturer_name', x.openfda.manufacturer_name,
            'openfda_pharm_class_epc', x.openfda.pharm_class_epc,
            'openfda_rxcui', x.openfda.rxcui
        ))
        """
    )

    reports = deduped.select(
        "safetyreportid",
        F.col("safetyreportversion").cast("int").alias("safetyreportversion"),
        "primarysourcecountry",
        "occurcountry",
        "reporttype",
        "authoritynumb",
        "companynumb",
        F.to_date("transmissiondate", "yyyyMMdd").alias("transmissiondate"),
        F.to_date("receivedate", "yyyyMMdd").alias("receivedate"),
        F.to_date("receiptdate", "yyyyMMdd").alias("receiptdate"),
        _bool_flag("serious").alias("serious"),
        *[_bool_flag(c) for c in _SERIOUSNESS_FIELDS],
        F.coalesce(F.col("duplicate") == "1", F.lit(False)).alias("duplicate"),
        _safe_struct_col(deduped, "reportduplicate.duplicatesource", "duplicate_source"),
        _safe_struct_col(deduped, "reportduplicate.duplicatenumb", "duplicate_numb"),
        _safe_struct_col(deduped, "primarysource.reportercountry", "reporter_country"),
        _safe_struct_col(deduped, "primarysource.qualification", "reporter_qualification"),
        _safe_struct_col(deduped, "primarysource.literaturereference", "reporter_literaturereference"),
        _safe_struct_col(deduped, "sender.sendertype", "sender_type"),
        _safe_struct_col(deduped, "sender.senderorganization", "sender_organization"),
        _safe_struct_col(deduped, "receiver.receivertype", "receiver_type"),
        _safe_struct_col(deduped, "receiver.receiverorganization", "receiver_organization"),
        F.col("patient.patientonsetage").alias("patient_onsetage"),
        F.col("patient.patientonsetageunit").alias("patient_onsetage_unit"),
        F.col("patient.patientweight").alias("patient_weight"),
        F.col("patient.patientsex").alias("patient_sex"),
        F.col("patient.patientagegroup").alias("patient_agegroup"),
        _safe_struct_col(deduped, "patient.summary.narrativeincludeclinical", "narrative"),
        F.col("patient.reaction").alias("reaction"),
        drug_casted.alias("drug"),
    )

    reports.write.mode("overwrite").parquet(REPORTS_PATH)
    print(f"Wrote single table output to {REPORTS_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()