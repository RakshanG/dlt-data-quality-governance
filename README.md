Data Quality & Governance Pipeline (Delta Live Tables + Unity Catalog)

A declarative data pipeline built with Delta Live Tables (DLT) on CMS Medicare synthetic claims data, incorporating automated data quality validation and leveraging Unity Catalog's built-in lineage tracking for full data traceability.

Overview

This project extends a standard batch ETL pipeline into a governed, production-style pipeline by replacing manual, imperative cell-by-cell processing with declarative table definitions that include built-in data quality rules, and by verifying automatic column-level lineage tracking end-to-end.

Tables & Data Quality Expectations

TableSourceData Quality Expectationbeneficiary_dltBeneficiary Summary Filebirth_date_clean IS NOT NULLinpatient_dltInpatient ClaimsCLM_ID IS NOT NULLoutpatient_dltOutpatient ClaimsCLM_ID IS NOT NULL

Each table applies transformations consistent with the equivalent batch pipeline (date parsing, missing-value flagging), but validation is now enforced declaratively via @dlt.expect(...) rather than checked manually with ad hoc queries.

Why Delta Live Tables

Rather than manually sequencing "load → clean → save" operations across separate notebook cells, DLT allows the rules to be declared once:

python@dlt.table(name="beneficiary_dlt")
@dlt.expect("valid_birth_date", "birth_date_clean IS NOT NULL")
def beneficiary():
    ...

Databricks then handles execution ordering, dependency resolution, and continuously reports whether each declared expectation is being met — surfaced directly in the pipeline's run results (e.g., "1 met" per table, with zero errors or warnings across all three tables in this project).

Data Lineage

Unity Catalog automatically captures lineage for all DLT-managed tables without any additional configuration. Verified lineage for beneficiary_dlt confirms:

Raw CSV (Volume) → Data Quality & Governance Pipeline → beneficiary_dlt

Lineage is tracked down to the column level — all 33 output columns, including derived columns like birth_date_clean, are traceable back to their source.

Tech Stack

Databricks · Delta Live Tables (Lakeflow Declarative Pipelines) · PySpark · Unity Catalog · Python

Key Engineering Decisions


Declarative over imperative pipeline definition: encoding transformation and validation logic as declarative table definitions reduces the risk of the manual sequencing errors (e.g., stale variable references, incorrect run order) common in traditional notebook-based ETL.
Verifying lineage rather than assuming it: rather than simply asserting that governance features exist, this project includes direct verification of the lineage graph to confirm accurate, column-level traceability from raw source to final table.


Future Work


Add access control policies restricting table access by role (e.g., clinician vs. analyst views)
Add expectations with @dlt.expect_or_drop to enforce stricter data quality on downstream Gold-layer aggregates
Extend lineage verification to include Carrier Claims and Prescription Drug Event tables
