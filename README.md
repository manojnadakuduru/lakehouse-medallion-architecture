# lakehouse-medallion-architecture
End-to-end lakehouse pipeline using Databricks-style medallion architecture (Bronze-Silver-Gold) with incremental ingestion, merge based upserts, dimensional modeling, and Slowly Changing Dimensions using Spark SQL and PySpark.

## 1)Architecture Overview
![Dashboard Preview](https://github.com/manojnadakuduru/lakehouse-medallion-architecture/blob/main/ArchitectureDiagram.png)
This project follows the Databricks-style Lakehouse Medallion Architecture:

- Bronze layer stores raw, immutable copies of source data.
- Silver layer applies transformations, business logic, and incremental upserts.
- Gold layer contains analytics-ready fact and dimension tables.
- Slowly Changing Dimensions (SCDs) are implemented to preserve historical changes.

The design mirrors real-world lakehouse patterns used in modern data platforms.
## 2)Data Flow (Bronze → Silver → Gold)
![Dashboard Preview](https://github.com/manojnadakuduru/lakehouse-medallion-architecture/blob/main/Dataflow.png)
- Source data is ingested incrementally based on last processed timestamps.
- Bronze layer maintains a raw 1:1 copy of the source.
- Silver layer applies cleansing, transformations, and merge-based upserts.
- Gold layer models the data into fact and dimension tables using a star schema.
- SCD logic tracks historical changes in dimensional attributes.
## 3)Incremental Load Strategy
- Tracks the last successfully processed timestamp.
- Extracts only new or updated records from the source.
- Uses merge-based upserts to handle updates and late-arriving data.
- Prevents full table reprocessing and improves scalability.
## 4)Dimensional Modeling (Gold Layer)
- The Gold layer contains analytics-ready datasets modeled using a star schema to support efficient querying and BI consumption.
- Store descriptive attributes used for slicing and filtering analytical queries. Dimensions are designed to be reusable across multiple facts.
Surrogate keys
- Generated for all dimension tables to ensure stable joins, decouple analytics from source system keys, and support Slowly Changing Dimensions.
Star schema design
- Optimized for analytical workloads and BI tools by minimizing joins and enabling fast aggregations.
This layer is intentionally free of heavy transformation logic and is curated specifically for reporting, dashboards, and downstream analytical use cases.
## 5)Slowly Changing Dimensions (SCD)
This project implements Slowly Changing Dimension (SCD) logic to track historical changes in dimensional attributes over time without losing data integrity.

**Change detection** - Compares incoming records with current dimension data to identify updates.

**Historical versioning** - Instead of overwriting records, new versions of dimension rows are created to preserve historical states.

**Validity tracking** - Each dimension record includes effective start and end timestamps (or flags) to enable accurate point-in-time analysis.

**Surrogate key management** - New surrogate keys are assigned for changed dimension records to maintain consistent joins with fact tables.

## 6)Technologies Used
**PySpark** 
– Core data processing engine used for transformations, incremental ingestion, merge-based upserts, and dimensional modeling.

**Spark SQL**
– Used for declarative transformations, joins, window functions, and analytical logic.

**Lakehouse Medallion Architecture** 
– Bronze–Silver–Gold layered design aligned with Databricks lakehouse best practices.

**Dimensional Modeling** 
– Star schema design with fact and dimension tables optimized for analytics.

**Slowly Changing Dimensions (SCD Type 2)**
– Implemented to preserve historical changes in dimensional attributes.

**Databricks-style Notebooks** 
– Notebook-based development pattern following Databricks execution semantics.

