```markdown
# Steam Market Intelligence Lakehouse

Production-oriented Medallion Architecture built on Databricks to analyze Steam's global video game marketplace using PySpark, Delta Lake, and AWS S3.

---

## Academic & Business Context

This platform was developed as part of the **CDSD RNCP Level 6 certification pathway (RNCP35288)**, specifically for *Bloc 2 (Large-scale data processing, analytical modeling, and cloud-native data architectures)*.

Instead of relying on traditional, isolated exploratory notebooks, this project intentionally embraces enterprise-grade **Analytics Engineering** practices. It positions Steam as a massive, macroeconomic digital marketplace to help publishers, analysts, and product teams answer business-critical questions:

* **Market Concentration:** Which publishers dominate the marketplace, and where are the power-law effects/revenue concentrations?
* **Commercial Traction:** Which genres and pricing/discount strategies generate the highest user engagement?
* **Data-Driven Dynamics:** How do review ratios directly impact visibility, popularity, and estimated ownership?
* **Ecosystem Distribution:** What do localization (languages) and platform adoption (Windows, Mac, Linux) trends look like across market cycles?

---

## Technology Stack

* **Data Platform:** Databricks, Apache Spark (PySpark / Spark SQL), Delta Lake, AWS S3
* **Analytics Engineering:** Python, Pandas, Databricks SQL Engine
* **Visualization:** Databricks Visualization Engine & Dashboards

---

## System Architecture & Data Flow

The platform implements a classic **Medallion Architecture** using AWS S3 as the object store and Delta Lake as the transactional storage format. 

```text
 s3://full-stack-bigdata-datasets/.../steam_game_output.json
                            │
                            ▼
               ┌──────────────────────────┐
               │    steam.bronze (S3)     │ ◄── [Step 2: Raw Ingestion & Schema Lock]
               └────────────┬─────────────┘
                            │
                            ▼
               ┌──────────────────────────┐
               │    steam.silver (S3)     │ ◄── [Step 3: Cleaning & Array Flattening]
               └────────────┬─────────────┘
                            │
                            ▼
               ┌──────────────────────────┐
               │  steam.gold (Core Whse)  │ ◄── [Step 4: Dimension/Fact Star Schema]
               └────────────┬─────────────┘
                            │
                            ▼
               ┌──────────────────────────┐
               │  steam.gold (BI Marts)   │ ◄── [Step 5: Pre-Aggregated Serving Layer]
               └──────────────────────────┘

```

### Repository Structure & Execution Order

The pipeline is modularized into dedicated notebooks matching architectural responsibilities. They must be executed sequentially:

```text
project/
├── steam_eda.ipynb                    # 1. Structural exploration & missing value analysis
├── bronze_ingestion.ipynb              # 2. Immutable raw ingestion from S3 to Delta
├── silver_preprocessing.ipynb          # 3. Type normalization, flattening, & feature prep
├── gold_core_and_serving_marts.ipynb  # 4. Dimensional modeling & BI materialization
├── gold_analytics.ipynb               # 5. Market intelligence & dashboard metrics
├── requirements.txt
└── README.md

```

---

## Layer-by-Layer Engineering

### Step 1: Exploratory Data Analysis (steam_eda.ipynb)

Before industrialization, structural analysis was performed to map the data terrain: missing value distributions, nested array depths (genres, categories), pricing anomalies, and language coverage anomalies.

### Step 2: Bronze Layer (bronze_ingestion.ipynb)

Acts as the immutable, schema-enforced source of truth. It reads raw semi-structured JSON directly from S3 with minimal cleaning.

* **Schema:** `CREATE SCHEMA IF NOT EXISTS steam.bronze`
* **Primary Asset:** `steam.bronze.steam_games_bronze`

### Step 3: Silver Layer (silver_preprocessing.ipynb)

Performs rigorous data cleaning, type casting, date standardization, and handles nested array layouts. To support clean downstream analytics without data distortion, the data was decoupled into two tables:

1. **Canonical Games:** `steam.silver.steam_games` — The definitive representation of a game grain.
2. **Genre Breakdown:** `steam.silver.steam_games_genres` — An exploded analytical table isolated to prevent metric multiplication during early aggregations.

### Step 4: Gold Core Layer & Dimensional Modeling

Transforms Silver datasets into a robust, conformed **Star Schema Warehouse** to support consistent cross-domain analytics.

```text
       ┌──────────────────┐             ┌────────────────┐
       │  dim_publishers  │             │   dim_genres   │
       └────────┬─────────┘             └───────┬────────┘
                │                               │
                ▼                               ▼
     ┌──────────────────────┐        ┌─────────────────────┐
     │bridge_game_publishers│        │ bridge_game_genres  │
     └──────────┬───────────┘        └──────────┬──────────┘
                │                               │
                └───────────────┬───────────────┘
                                │
                                ▼
                      ┌──────────────────┐      ┌─────────────┐
                      │    fact_games    │─────►│  dim_dates  │
                      └──────────────────┘      └─────────────┘

```

* **Conformed Dimensions:** `dim_publishers` (with fallback logic for missing values), `dim_genres`, and a calendar-based `dim_dates`.
* **Central Fact Table:** `fact_games` — Houses granular business metrics (pricing, reviews, estimated ownership, platform support indices).
* **Many-to-Many (M2M) Resolution:** Games on Steam frequently have multiple genres and publishers. Joining directly against them introduces dangerous **fan-out duplication effects** that artificially multiply revenue/ownership metrics. This project cleanly resolves M2M constraints using dedicated **Bridge Tables**:
* `steam.gold.bridge_game_genres`
* `steam.gold.bridge_game_publishers`



### Step 5: Gold Serving Layer (Data Marts)

To optimize BI query response times and lower dashboard engine workloads, wide, pre-aggregated serving data marts are materialized directly in Delta:

* `steam.gold.mart_publisher_performance`: Tracks publisher KPIs, average pricing, market share, and review success.
* `steam.gold.market_trends`: Time-series aggregates monitoring market dynamics and ecosystem expansion across years.

---

## Delta Lake & Performance Engineering

The architecture integrates native Lakehouse optimizations to handle large-scale analytics efficiently:

### Z-Ordering & File Compaction

To minimize scan amplification and speed up critical join paths, tables are compacted and optimized based on frequent query predicates:

```sql
OPTIMIZE steam.gold.fact_games 
ZORDER BY (release_date);

```

### PySpark & SQL Snippets

The pipeline relies heavily on high-throughput distributed PySpark patterns for extraction and storage:

```python
# Flattening complex structures safely
from pyspark.sql.functions import explode
exploded_df = games_df.withColumn("genre", explode("genres"))

# ACID-compliant Delta persistence
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("steam.silver.steam_games")

```

---

## Analytical Themes & Insights

The consumption layer (`gold_analytics.ipynb`) translates warehouse assets into business intelligence dashboards focused on three main pillars:

* **Ecosystem Concentration:** Uncovering the steep long-tail distribution where a minor fraction of AAA operators dominate ownership volume.
* **Genre Economics:** Benchmarking genre profitability versus genre saturation to identify market entry gaps.
* **Cross-Platform Asymmetries:** Quantitative analysis mapping genre specializations against Linux and Mac ecosystem adoptions.

---

## Future Roadmap

* **Data Governance:** Transition metadata architecture to Unity Catalog for column-level lineage and fine-grained access.
* **Orchestration:** Implement Apache Airflow or Databricks Workflows for automated, scheduled pipeline runs.
* **Data Quality Guardrails:** Embed **Great Expectations** or Delta Live Tables (DLT) expectations for programmatic data testing.
* **CI/CD:** Introduce GitHub Actions for automated notebook deployment to staging and production clusters.

---

## Conclusion

This platform demonstrates a production-grade implementation of a Databricks Lakehouse. By separating raw data immutability, structural isolation, and normalized dimensional modeling from pre-aggregated serving layers, the project avoids the pitfalls of unstructured notebook code and delivers a maintainable, high-performance analytical engine fit for enterprise BI.

```
