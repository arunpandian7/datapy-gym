---
status: active
last_reviewed: 2026-06-04
tags: [job-search, skills, practice, gym]
related: ["[[master_skills]]", "[[target_companies]]"]
---

# Practice Roadmap — datapy-gym Tracks to Build

> Derived from master_skills.md + target_companies.md on 2026-06-04.
> Sorted by interview impact across Tier S and Tier 1 targets.

---

## Already Built (PySpark Track)

- `01_aggregations`
- `02_window_functions`
- `03_joins`
- `04_partitioning`
- `05_salting`

---

## Tier 1 — Critical (blocks the most Tier S / Tier 1 companies)

| Rank | Technology | Why urgent |
|------|-----------|------------|
| **1** | **SQL (advanced)** | Every target company tests SQL — GS, MS, JPM, Uber, Amazon, all of them. Window functions, CTEs, recursive queries, query plans. Use SparkSQL or DuckDB against the existing dataset. |
| **2** | **Kafka / Streaming** | Top self-identified gap. Directly blocks: PhonePe, Uber, Flipkart, Swiggy, Confluent, Stripe, Visa, Mastercard, PayPal, Meesho, Adobe. Producer/consumer patterns, partition strategy, consumer groups, offset management. |

---

## Tier 2 — High Priority

| Rank | Technology | Why it matters |
|------|-----------|----------------|
| **3** | **pandas** | Most Python-first coding screens use pandas, not PySpark. Same dataset — near-zero setup. "pandas vs PySpark trade-offs" is a universal interview question. |
| **4** | **Airflow (DAG design)** | Production experience exists but interview-depth questions on complex DAG patterns (dynamic tasks, XCom, sensors, SLA hooks) come up at Uber, Amazon, Swiggy, Intuit, McKinsey QB. Depth gap, not a knowledge gap. |

---

## Tier 3 — Medium Priority (adds differentiation)

| Rank | Technology | Why it matters |
|------|-----------|----------------|
| **5** | **dbt** | Snowflake, Intuit, SAP Labs, Chargebee, McKinsey QB, ThoughtWorks stacks. Runs locally on DuckDB — easy to gym. |
| **6** | **Great Expectations (data quality)** | Self-identified gap. Finance GCCs (GS, JPM, MS, BlackRock) care about data quality. Easy to integrate with existing dataset. |
| **7** | **Delta Lake** | Databricks is Tier S. Delta vs Iceberg trade-off questions are guaranteed there. Iceberg depth covers ~80% — Delta is the delta. |

---

## Tier 4 — Lower Priority

| Rank | Technology | Why it matters |
|------|-----------|----------------|
| **8** | **Apache Hudi** | Flipkart specifically. Rounds out the Iceberg/Delta/Hudi lakehouse format picture. Mostly conceptual. |
| **9** | **Polars** | Not yet widely tested but growing fast. Low effort given a pandas track is already planned. |
| **10** | **Flink** | Swiggy uses it; broader real-time streaming. Defer until Kafka is solid — complex infra to gym. |

---

## Skip for Now

| Technology | Reason |
|-----------|--------|
| Kubernetes | DE interviews almost never test this hands-on |
| Terraform | Infra provisioning not interview-tested for DE roles |
| LoRA/PEFT | Only relevant for AI-native startups (Sarvam, Krutrim), not Tier S/1 targets |
| RAGAS | RAG angle is secondary to DE core story at most targets |

---

## Suggested Build Order

```
SQL track (DuckDB or SparkSQL)     ← same dataset, 2–3 days
pandas track                       ← same dataset, 1–2 days
Kafka track (Docker Compose)       ← new infra, ~1 week
dbt track (DuckDB adapter)         ← 2–3 days
Great Expectations                 ← 1 day
Delta Lake                         ← PySpark + Delta, 1–2 days
```

SQL and pandas reuse the existing e-commerce CSVs directly — near-zero setup cost.
Kafka is the hardest infrastructure-wise but closes the single biggest gap in the target company list.
