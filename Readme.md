# Steam Market Intelligence Lakehouse

Production-oriented Medallion Architecture built on Databricks to analyze Steam's global video game marketplace using PySpark, Delta Lake and AWS S3.

Cloud-scale Medallion Architecture project designed to analyze Steam's global video game marketplace using Databricks, PySpark, Delta Lake and AWS S3.

---

# Academic Context

This project was developed as part of the CDSD RNCP Level 6 certification pathway (RNCP35288), specifically for Bloc 2 focused on large-scale data processing, analytical modeling and cloud-native data architectures.

The project was intentionally designed using production-oriented engineering practices rather than a traditional academic notebook workflow.

The objective was not only to explore the Steam dataset, but to build a complete analytical platform capable of:

* Ingesting semi-structured large-scale datasets
* Designing a Medallion Architecture on Databricks
* Building scalable Bronze, Silver and Gold layers
* Modeling analytical dimensions and fact tables
* Resolving many-to-many relationships through bridge tables
* Producing serving marts for business intelligence
* Conducting market-level analytics on the global video game ecosystem

The analysis focuses on Steam's marketplace and aims to identify the commercial, behavioral and structural factors influencing:

* Video game popularity
* Market visibility
* User engagement
* Genre performance
* Platform distribution
* Pricing strategies
* Publisher dominance

---

# Project Overview

This project implements a complete Medallion Architecture pipeline on Databricks to transform raw Steam marketplace data into a scalable analytical warehouse optimized for business intelligence and market analytics.

The platform combines:

* Semi-structured JSON ingestion
* Delta Lake storage
* Bronze/Silver/Gold layered modeling
* PySpark transformations
* SQL-based analytical modeling
* Data marts materialization
* Market intelligence analytics
* Databricks visualizations

The architecture follows modern Lakehouse engineering principles using AWS S3 as the storage layer and Delta Lake as the transactional storage format.

Instead of relying on a single exploratory notebook, the project separates ingestion, preprocessing, dimensional modeling and analytical consumption into dedicated layers.

---

# Business Objective

The objective is to help publishers, analysts and product teams better understand the structure and economics of the Steam ecosystem.

The platform enables analysis of:

* Which publishers dominate Steam's marketplace
* Which genres generate the highest commercial traction
* How review ratios impact visibility and popularity
* How pricing and discount strategies are distributed
* Platform adoption across Windows, Mac and Linux
* Release trends across years and market cycles
* Localization and language availability patterns
* Age restriction distribution across the catalog
* Power-law effects in game popularity and revenue concentration

The project positions Steam as a large-scale commercial marketplace rather than simply a gaming platform.

---

# Technology Stack

## Data Platform

* Databricks
* Apache Spark
* PySpark
* Spark SQL
* Delta Lake
* AWS S3

## Analytics & Processing

* Python
* Pandas
* Databricks SQL
* Delta Optimizations

## Visualization

* Databricks Visualization Engine
* Databricks Dashboards

---

# Architecture Overview

The platform follows a Medallion Architecture design:

```text
Raw JSON Dataset
        ↓
Bronze Layer
        ↓
Silver Layer
        ↓
Gold Core Warehouse
        ↓
Serving Data Marts
        ↓
Business Analytics
```

All layers are materialized as Delta tables directly inside Databricks schemas:

```text
steam.bronze.*
steam.silver.*
steam.gold.*
```

This design provides:

* Incremental scalability
* Layer isolation
* Query optimization
* Data quality control
* BI-ready serving tables
* Production-grade analytical governance

---

# Storage Architecture

AWS S3 acts as the raw storage layer.

The original Steam dataset is loaded from:

```text
s3://full-stack-bigdata-datasets/Big_Data/Project_Steam/steam_game_output.json
```

Delta Lake tables are then materialized directly inside Databricks catalogs and schemas.

The project uses:

* ACID-compliant Delta tables
* Columnar storage
* Optimized analytical reads
* Lakehouse-style architecture

---

# Repository Structure

```text
project/
│
├── steam_eda.ipynb
├── bronze_ingestion.ipynb
├── silver_preprocessing.ipynb
├── gold_core_and_serving_marts.ipynb
├── gold_analytics.ipynb
│
├── README.md
└── requirements.txt
```

Each notebook corresponds to a distinct architectural responsibility inside the platform.

---

# Data Flow Overview

```text
Steam Raw JSON
        ↓
Bronze Ingestion
        ↓
Schema Normalization
        ↓
Silver Preprocessing
        ↓
Analytical Feature Engineering
        ↓
Gold Dimensional Modeling
        ↓
Bridge Table Resolution
        ↓
Serving Mart Materialization
        ↓
Business Analytics
```

The pipeline progressively transforms semi-structured marketplace data into business-oriented analytical assets.

---

# Step 1 — Exploratory Data Analysis

Notebook:

```text
steam_eda.ipynb
```

The EDA layer performs a complete structural exploration of the Steam dataset before industrialization.

The notebook investigates:

* Dataset dimensions
* Missing value distribution
* Platform availability
* Languages coverage
* Categories structure
* Genre distribution
* Pricing and discounts
* Required age restrictions
* Estimated owners distribution
* Release date trends
* Review metrics
* Publisher and developer concentration

This stage validates the dataset structure and identifies the transformations required for downstream modeling.

---

# Step 2 — Bronze Layer Ingestion

Notebook:

```text
bronze_ingestion.ipynb
```

The Bronze layer acts as the raw ingestion zone of the Lakehouse.

Main responsibilities:

* Raw JSON ingestion from S3
* Minimal cleaning
* Schema standardization
* Delta table persistence
* Initial validation checks

The Bronze table preserves the original dataset structure as closely as possible while ensuring ingestion stability.

Created schema:

```sql
CREATE SCHEMA IF NOT EXISTS steam.bronze
```

Primary table:

```text
steam.bronze.steam_games_bronze
```

This layer serves as the immutable source of truth for downstream transformations.

---

# Step 3 — Silver Layer Engineering

Notebook:

```text
silver_preprocessing.ipynb
```

The Silver layer performs preprocessing, normalization and analytical restructuring.

Main transformations include:

* Type normalization
* Missing value handling
* Nested field flattening
* Date standardization
* Text normalization
* Analytical feature preparation
* Genre explosion handling

The Silver layer produces two curated analytical tables.

## Canonical Games Table

```text
steam.silver.steam_games
```

This table represents the canonical business representation of a Steam game.

Each row corresponds to a single game with cleaned and standardized attributes.

## Genre Analytical Table

```text
steam.silver.steam_games_genres
```

This exploded analytical table isolates the relationship between games and genres.

The table was intentionally separated from the canonical games table in order to:

* simplify genre aggregations
* avoid metric multiplication during joins
* support scalable segmented analytics
* preserve the canonical grain of the fact table

This modeling decision becomes especially important once ownership and revenue metrics are introduced into the Gold warehouse layer.

---

# Step 4 — Gold Core Warehouse Modeling

Notebook:

```text
gold_core_and_serving_marts.ipynb
```

The Gold layer transforms Silver datasets into a dimensional warehouse optimized for analytics and BI consumption.

The architecture separates:

* Conformed dimensions
* Core fact tables
* Bridge tables
* Analytical serving marts

---

# Gold Dimension Tables

## Publishers Dimension

```text
steam.gold.dim_publishers
```

Centralized publisher reference dimension.

Missing publishers are normalized using fallback logic to preserve analytical consistency.

## Genres Dimension

```text
steam.gold.dim_genres
```

Reference dimension containing normalized genre entities.

## Dates Dimension

```text
steam.gold.dim_dates
```

Calendar dimension enabling time-series and release trend analysis.

---

# Gold Fact Table

## Games Fact Table

```text
steam.gold.fact_games
```

Core analytical fact table containing standardized business metrics for Steam games.

The fact table centralizes:

* Pricing metrics
* Reviews
* Estimated ownership
* Platform availability
* Release information
* Commercial indicators
* Engagement signals

This table acts as the central warehouse entity powering downstream marts and dashboards.

---

# Bridge Tables

The Steam dataset contains multiple many-to-many relationships.

The project resolves these relationships through dedicated bridge tables to avoid fan-out duplication effects during aggregation.

## Game ↔ Genre Bridge

```text
steam.gold.bridge_game_genres
```

Allows a single game to be associated with multiple genres while preserving aggregation correctness.

## Game ↔ Publisher Bridge

```text
steam.gold.bridge_game_publishers
```

Allows multi-publisher attribution while maintaining warehouse normalization.

---

# Serving Layer & Data Marts

The serving layer materializes pre-aggregated analytical tables optimized for dashboards and business reporting.

Examples include:

## Publisher Performance Mart

```text
steam.gold.mart_publisher_performance
```

Provides pre-aggregated publisher KPIs including:

* number of published games
* estimated ownership metrics
* average review ratios
* average pricing
* publisher-level market concentration

## Market Trends Mart

```text
steam.gold.market_trends
```

Centralized time-series metrics for release dynamics and marketplace evolution.

The serving layer reduces dashboard query complexity and improves BI responsiveness.

---

# Gold Layer Engineering Notes

The Gold layer intentionally separates governance-oriented warehouse entities from dashboard-oriented marts.

Core entities such as:

* `fact_games`
* `dim_publishers`
* `dim_genres`
* `bridge_game_genres`

preserve analytical correctness and reusable business logic.

Serving marts such as:

* `mart_publisher_performance`
* `market_trends`
* `genre_platform_summary`

are intentionally pre-aggregated to reduce dashboard query complexity and improve BI responsiveness.

The architecture follows a hybrid Analytics Engineering approach inspired by both traditional dimensional warehousing and modern Lakehouse design.

---

# Delta Lake Optimizations

The platform includes Delta optimization operations for analytical performance.

Examples:

```sql
OPTIMIZE steam.gold.fact_games
```

These optimizations improve:

* Query latency
* File compaction
* Scan efficiency
* Dashboard responsiveness

The project follows Lakehouse performance engineering best practices.

---

# Analytical Themes

Notebook:

```text
gold_analytics.ipynb
```

The analytical layer explores the Steam ecosystem through multiple market-level perspectives.

---

# Market Overview Analytics

The project analyzes:

* Most popular games
* Best selling games
* Publisher concentration
* Best rated games
* Steam release trends
* Pricing distributions
* Discount strategies
* Supported languages
* Age restriction patterns

---

# Genre Analytics

The platform investigates:

* Most represented genres
* Genre profitability
* Genre review performance
* Genre-platform relationships
* Genre popularity concentration

---

# Platform Analytics

The project evaluates:

* Windows vs Mac vs Linux availability
* Cross-platform adoption
* Platform specialization by genre
* Distribution asymmetries across operating systems

---

# Key Engineering Concepts

The project intentionally implements production-grade warehouse concepts.

## Canonical Tables

The Silver canonical tables provide a stable and deduplicated representation of core business entities.

## Bridge Tables

Bridge tables resolve many-to-many relationships while avoiding aggregation distortions.

## Fan-Out Prevention

The modeling strategy explicitly prevents metric multiplication during joins.

## Conformed Dimensions

Dimensions are standardized to support consistent cross-domain analytics.

## Serving Marts

Dedicated marts provide BI-ready analytical datasets optimized for consumption.

---

# Data Modeling Strategy

The warehouse follows a hybrid dimensional approach combining:

* Star-schema principles
* Bridge-table normalization
* Lakehouse optimization
* Delta-based storage

This architecture balances:

* Scalability
* Analytical flexibility
* Query performance
* Data quality
* Maintainability

---

# Running the Project

## Environment Requirements

* Databricks Runtime
* AWS S3 access
* Spark cluster

## Main Workflow

The notebooks are executed sequentially:

```text
1. steam_eda.ipynb
2. bronze_ingestion.ipynb
3. silver_preprocessing.ipynb
4. gold_core_and_serving_marts.ipynb
5. gold_analytics.ipynb
```

---

# Final Analytical Outputs

| Layer     | Main Assets                        |
| --------- | ---------------------------------- |
| Bronze    | Raw Steam ingestion tables         |
| Silver    | Canonical cleaned datasets         |
| Gold Core | Dimensions, facts and bridges      |
| Serving   | Business marts and aggregated KPIs |
| Analytics | Market intelligence dashboards     |

---

# Example Warehouse Relationships

```text
fact_games
    │
    ├── dim_publishers
    ├── dim_genres
    ├── dim_dates
    │
    ├── bridge_game_genres
    └── bridge_game_publishers
```

This modeling strategy enables:

* Flexible many-to-many analysis
* Genre-level aggregation
* Publisher segmentation
* Cross-platform analytics
* Time-series exploration

while preventing fan-out duplication during joins.

---

# Example Analytical Questions

The warehouse was designed to answer complex business questions such as:

* Which publishers dominate Steam's marketplace?
* Which genres generate the strongest engagement?
* Are indie games outperforming AAA publishers?
* Which genres receive the best review ratios?
* How concentrated is the market around top-selling games?
* Do lower-priced games receive better engagement?
* Which operating systems are most represented?
* How did the Covid period affect release volume?
* Which genres are the most cross-platform friendly?
* Are discounts concentrated around specific categories?

---

# Example PySpark Operations

The project relies heavily on distributed PySpark transformations.

Examples include:

## Nested Array Explosion

```python
from pyspark.sql.functions import explode

exploded_df = games_df.withColumn(
    "genre",
    explode("genres")
)
```

## Delta Table Persistence

```python
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("steam.silver.steam_games")
```

## Analytical Aggregations

```python
genre_metrics = (
    games_df
    .groupBy("genre")
    .agg(avg("positive_ratio"))
)
```

---

# SQL Modeling Layer

Spark SQL is used extensively inside the Gold warehouse layer.

Example:

```sql
CREATE TABLE steam.gold.fact_games AS
SELECT
    game_id,
    publisher_id,
    release_date,
    price,
    positive_ratio,
    estimated_owners
FROM steam.silver.steam_games
```

The SQL layer enables:

* Warehouse materialization
* Dimensional modeling
* KPI generation
* Serving mart creation
* Analytical optimization

---

# Lakehouse Engineering Principles

The architecture intentionally follows modern Lakehouse engineering practices.

Key principles implemented:

* Separation between raw and curated layers
* Immutable Bronze ingestion
* Standardized Silver preprocessing
* Business-oriented Gold warehouse modeling
* Delta transactional reliability
* Query optimization through Delta operations
* BI-oriented serving marts
* Analytical scalability through Spark

The project was designed to emulate real-world analytical platform construction rather than isolated notebook experimentation.

---

# Performance Considerations

The project includes several optimization strategies.

## Delta Optimization

```sql
OPTIMIZE steam.gold.fact_games
```

## Storage Optimization

* Columnar Delta storage
* Reduced scan amplification
* Distributed Spark execution
* Optimized analytical reads

## Analytical Optimization

* Pre-aggregated marts
* Bridge-table normalization
* Reduced fan-out risk
* Dimensional filtering

These optimizations improve scalability and dashboard responsiveness when working with large analytical datasets.

---

# Databricks Integration

The project was fully developed inside Databricks.

The platform leverages:

* Databricks notebooks
* Spark clusters
* Delta Lake tables
* Databricks SQL
* Interactive visualizations
* Distributed processing

The notebooks can be published directly through Databricks public sharing for interactive review and dashboard exploration.

---

# Dataset Characteristics

The Steam dataset contains semi-structured marketplace information including:

* Game metadata
* Publishers and developers
* Pricing information
* Discounts
* Reviews
* Estimated owners
* Platforms
* Genres
* Languages
* Categories
* Age restrictions
* Release dates

The nested schema requires flattening and normalization strategies during the Silver transformation phase.

---

# Engineering Challenges

Several data engineering challenges were addressed during implementation.

## Semi-Structured Modeling

The Steam dataset contains nested arrays and complex fields requiring distributed flattening strategies.

## Many-to-Many Relationships

Games may belong to multiple genres, publishers and categories.

Bridge tables were implemented to preserve aggregation correctness.

## Large-Scale Aggregation

Distributed Spark transformations were required to compute market-wide metrics efficiently.

## Data Quality Normalization

The pipeline standardizes:

* Missing publishers
* Empty categorical values
* Nested array structures
* Date inconsistencies
* Numeric type conversions

---

# Future Improvements

* Airflow orchestration
* Incremental Delta ingestion
* Automated data quality testing
* CI/CD deployment pipelines
* Unity Catalog governance
* Advanced Delta partitioning
* Real-time ingestion pipelines
* Power BI or Tableau integration
* Dockerized orchestration stack

---

# Analytical Positioning

The project approaches Steam as a large-scale digital marketplace rather than simply a gaming catalog.

The analytical focus is therefore centered around:

* ecosystem concentration
* commercial asymmetries
* publisher scale effects
* pricing dynamics
* platform dominance
* long-tail distribution patterns
* genre saturation
* engagement concentration

Several analyses reveal strong power-law effects where a very small subset of games captures a disproportionate share of player attention and commercial traction.

This positioning transforms the dataset into a genuine market intelligence use case rather than a simple exploratory gaming project.

---

# Conclusion

This project demonstrates the implementation of a production-oriented Medallion Architecture on Databricks for large-scale marketplace analytics.

The platform combines:

* Cloud-native Lakehouse engineering
* Delta-based warehousing
* Dimensional modeling
* Many-to-many bridge resolution
* Scalable PySpark transformations
* Business intelligence serving layers
* Advanced Steam marketplace analytics

Rather than limiting the project to exploratory notebook analysis, the architecture was designed as a modular analytical platform aligned with modern Data Engineering and Analytics Engineering practices.

