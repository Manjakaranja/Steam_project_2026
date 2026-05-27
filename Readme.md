# Steam market analysis Lakehouse

Databricks Lakehouse project implementing a Medallion Architecture to analyze Steam's global video game marketplace using PySpark, Delta Lake, and AWS S3.

---

## Academic & Business Context

This platform was developed as part of the **CDSD RNCP Level 6 certification pathway (RNCP35288)**, specifically for *Bloc 2 (Large-scale data processing, analytical modeling, and cloud-native data architectures)*.

Instead of relying on traditional, isolated exploratory notebooks, this project intentionally embraces enterprise-grade **Analytics Engineering** practices. It positions Steam as an important, macroeconomic digital marketplace to help publishers, analysts, and product teams answer business-critical questions:

* **Market Concentration:** Which publishers dominate the marketplace, and where are the power-law effects/revenue concentrations?
* **Commercial Traction:** Which genres and pricing/discount strategies generate the highest user engagement?
* **Data-Driven Dynamics:** How do review ratios directly impact visibility, popularity, and estimated ownership?
* **Ecosystem Distribution:** What do localization (languages) and platform adoption (Windows, Mac, Linux) trends look like across market cycles?

---

## Technology Stack

* **Data Platform:** Databricks, Apache Spark (PySpark / Spark SQL), Delta Lake, AWS S3
* **Analytics Engineering:** Python, Pandas, Databricks SQL Engine
* **Visualization:** Seaborn, Matplotlib

---

## System Architecture & Data Flow

The platform implements a classic **Medallion Architecture** using AWS S3 as the object store and Delta Lake as the transactional storage format. 

```text
s3://full-stack-bigdata-datasets/.../steam_game_output.json
                            │
                            ▼
               ┌──────────────────────────┐
               │   steam.bronze (Delta)   │ ◄── Step 1: Raw ingestion & schema
               └────────────┬─────────────┘
                            │
                            ▼
               ┌──────────────────────────┐
               │   steam.silver (Delta)   │ ◄── Step 2: Cleaning & normalization
               └────────────┬─────────────┘
                            │
                            ▼
               ┌──────────────────────────┐
               │    steam.gold (Delta)    │ ◄── Step 3: Facts, dimensions, marts
               └────────────┬─────────────┘
                            │
                            ▼
               ┌──────────────────────────┐
               │ Analytics & Visualization│ ◄── Step 4: Seaborn / Matplotlib
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
├── gold_analytics.ipynb               # 5. market analysis & dashboard metrics
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
     ┌──────────────────────┐        ┌─────────────────────┐
     │bridge_game_publishers│        │ bridge_game_genres  │
     └──────────┬───────────┘        └──────────┬──────────┘
                │                               │
                └───────────────┬───────────────┘
                                │
                      ┌──────────────────┐      ┌─────────────┐
                      │    fact_games    │──────│  dim_dates  │
                      └──────────────────┘      └─────────────┘

```

* **Conformed Dimensions:** `dim_publishers` (standardized handling of missing publisher values), `dim_genres`, and a calendar-based `dim_dates`.
* **Central Fact Table:** `fact_games` — Houses granular business metrics (pricing, reviews, estimated ownership, platform support indices).
* **Many-to-Many (M2M) Resolution:** Games on Steam frequently have multiple genres and publishers. Joining directly against them introduces dangerous **fan-out duplication effects** that artificially multiply revenue/ownership metrics. This project cleanly resolves M2M constraints using dedicated **Bridge Tables**:
* `steam.gold.bridge_game_genres`
* `steam.gold.bridge_game_publishers`



### Step 5: Gold Serving Layer (Data Marts)

To simplify analytics queries and reduce repeated aggregations, several serving marts are materialized directly in Delta tables.

#### Serving Marts

- `steam.gold.mart_publisher_performance`
  Publisher-level KPIs including catalog size, pricing metrics, ownership estimates, and review ratios.

- `steam.gold.market_trends`
  Yearly market trends such as release volume, pricing evolution, and marketplace growth.

- `steam.gold.genre_platform_summary`
  Cross-platform genre distribution across Windows, MacOS, and Linux.

- `steam.gold.publisher_genre_affinity`
  Publisher specialization and genre concentration across catalogs.

These marts provide pre-aggregated datasets optimized for downstream analytics and visualization workloads.

---

## Delta Lake & Performance Engineering

The architecture integrates native Lakehouse optimizations to handle large-scale analytics efficiently:

### Z-Ordering & File Compaction

Delta tables are optimized to improve query performance during analytics and visualization workloads :

```sql
OPTIMIZE steam.gold.fact_games 
ZORDER BY (release_date);

```

### PySpark & SQL Snippets

The pipeline relies on PySpark for data cleaning, transformation, and table generation :

```python
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

The analytics layer (`gold_analytics.ipynb`) translates the warehouse into a set of market-level analyses focused on three core areas:

* **Market Concentration:** Analysis of ownership and engagement concentration across top-performing games and publishers.

* **Genre Economics:** Comparison of genre-level pricing, review performance, and ownership trends across the marketplace.

* **Platform Ecosystems:** Evaluation of Windows, macOS, and Linux support to identify cross-platform trends and genre-specific adoption patterns.


---

## Key Findings

The analysis highlights several recurring patterns across the Steam marketplace:

* **Genre Positioning Matters More Than Genre Volume:** While genres such as Action, Adventure, Casual, and Indie dominate the marketplace in terms of release volume, high saturation alone does not guarantee stronger engagement or commercial success.

* **Cross-Platform Accessibility Increases Reach:** Windows remains the dominant platform across the Steam ecosystem, but broader compatibility with macOS, Linux, and handheld environments such as the Steam Deck expands potential audience reach.

* **Localization Plays a Major Role in Visibility:** English remains the primary language across the marketplace, while additional support for major international languages significantly improves global accessibility.

* **Pricing Remains Concentrated Around Accessible Ranges:** Most games are positioned around accessible low-to-mid pricing ranges, suggesting that extreme premium pricing remains limited to a smaller subset of established titles.

* **Market Concentration Remains Strong:** Ownership and engagement remain heavily concentrated around a relatively small number of games and publishers, reflecting strong long-tail dynamics across the ecosystem.


---

## What Drives Visibility & Engagement on Steam?

The analysis suggests that successful games on Steam tend to combine several recurring characteristics:

* Strong positioning within highly demanded genres such as Action, Adventure, Casual, and Multiplayer experiences.

* Broad technical accessibility through Windows compatibility, often extended to Linux and handheld ecosystems through Steam Deck and Proton support.

* Wider international reach enabled by English-first localization combined with support for major secondary languages.

* Accessible pricing strategies positioned around low-to-mid market ranges rather than extreme premium pricing.

* Strong differentiation inside highly saturated categories, where visibility depends not only on genre selection but also on discoverability and player reception.

---

## Future Roadmap

* **Data Governance:**  Integrate Unity Catalog for centralized schema management, access control, and data lineage.
* **Orchestration:** Implement Apache Airflow or Databricks Workflows for automated, scheduled pipeline runs.
* **Data Quality Guardrails:** Embed **Great Expectations** or Delta Live Tables (DLT) expectations for programmatic data testing.
* **CI/CD:** Introduce GitHub Actions for automated notebook deployment to staging and production clusters.

## Link to Databricks ressources

Link : https://dbc-d6f92ac6-02fe.cloud.databricks.com/editor/notebooks/691053708266143?o=7474654066156834

## Repository Structure

.
├─ data/
│  └─ Readme.md                  # dataset source and storage information
│
├─ notebooks/                    # Databricks notebooks
│  ├─ steam_eda.ipynb            # exploratory data analysis
│  ├─ bronze_ingestion.ipynb     # raw ingestion into Bronze Delta tables
│  ├─ silver_preprocessing.ipynb # cleaning, normalization, and preprocessing
│  ├─ gold_core_and_marts.ipynb  # dimensional modeling and serving marts
│  └─ gold_analytics.ipynb       # analytics and visualization layer
│
├─ .gitignore
└─ Readme.md                     # project documentation