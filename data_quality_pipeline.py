import dlt 
from pyspark.sql.functions import col, to_date 

volume = "/Volumes/dataentrepreneurssynthetic/dataentrepreneurssynthetic/dataentrepreneurssynthetic"


@dlt.table(
    name = "beneficiary_dlt"
)
@dlt.expect("valid_birth_date","birth_date_clean is NOT NULL")
def beneficiary():
    df = spark.read.csv(f"{volume}/Beneficiary_Summary_File.csv", header=True, inferSchema=True)
    df = df.withColumn("birth_date_clean",to_date(col("BENE_BIRTH_DT").cast("string"),"yyyyMMdd"))
    return df 

@dlt.table(
    name="inpatient_dlt"
)
@dlt.expect("valid_claim_id","CLM_ID IS NOT NULL")
def inpatient():
    df = spark.read.csv(f"{volume}/Inpatient_Claims.csv", header=True, inferSchema=True)
    df = df.withColumn("date_missing", col("CLM_FROM_DT").isNull())
    df = df.withColumn("clean_from_date", to_date(col("CLM_FROM_DT").cast("string"), "yyyyMMdd"))
    return df

@dlt.table(
    name="outpatient_dlt"
)
@dlt.expect("valid_claim_id","CLM_ID IS NOT NULL")
def outpatient():
    df = spark.read.csv(f"{volume}/Outpatient_Claims.csv", header=True,inferSchema=True)
    df = df.withColumn("date_missing", col("CLM_FROM_DT").isNull())
    df = df.withColumn("clean_from_date", to_date(col("CLM_FROM_DT").cast("string"), "yyyyMMdd"))
    return df 
