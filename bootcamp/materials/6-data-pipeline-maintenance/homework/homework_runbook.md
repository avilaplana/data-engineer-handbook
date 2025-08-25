## 1. Ownership of Pipelines

Since you’re in a group of 4 engineers, you need a **primary owner** (responsible day-to-day) and a **secondary owner** (backup person) for each pipeline. The idea is to balance load and avoid single points of failure.

| Pipeline                         | Primary Owner | Secondary Owner |
| -------------------------------- | ------------- | --------------- |
| Unit-level profit (experiments)  | Engineer A    | Engineer B      |
| Aggregate profit (investors)     | Engineer B    | Engineer C      |
| Aggregate growth (investors)     | Engineer C    | Engineer D      |
| Daily growth (experiments)       | Engineer D    | Engineer A      |
| Aggregate engagement (investors) | Engineer A    | Engineer D      |

Rationale:

* Investor-facing pipelines (aggregate profit, growth, engagement) are spread across different people so no one person owns all investor reporting.
* Experiment pipelines rotate through everyone for fairness.

---

## 2. On-Call Schedule (Fair Rotation)

Since there are 4 engineers, the simplest system is **weekly rotation**.

* **Week 1:** Engineer A
* **Week 2:** Engineer B
* **Week 3:** Engineer C
* **Week 4:** Engineer D
* Then repeat.

### Holiday coverage

* If someone’s on holiday during their assigned week, they swap with another teammate in advance.
* Each person must be secondary at least once per cycle, so if primary is unavailable, secondary gets paged.

---

## 3. Runbooks (for investor-facing pipelines)

Runbooks give **step-by-step instructions** so whoever is on-call can act quickly. For each pipeline that reports to investors:

### Example Runbook Template

1. **Name:** Aggregate Profit Pipeline
2. **Purpose:** Generates daily/weekly/monthly profit metrics reported to investors.
3. **Data Sources:**

   * Sales database (transactions)
   * Costs database (logistics, ops)
   * Experiment data warehouse (unit-level profit → rolled up)
4. **Expected Run Schedule:** 3 AM UTC daily
5. **Common Failures & Symptoms:**

   * Upstream data not available (sales DB late) → empty report
   * ETL job timeout → missing tables
   * Schema changes in source DB → pipeline errors
6. **Troubleshooting Steps:**

   * Check Airflow/DBT logs for failed tasks
   * Validate data arrival in raw layer (e.g., sales table not empty)
   * Retry failed job if upstream data has arrived
7. **Escalation:** If unresolved within 2 hours, escalate to secondary owner and notify stakeholders.

You’d repeat this for **Aggregate Growth** and **Aggregate Engagement**.

---

## 4. What Could Go Wrong?

### Common failure modes for these pipelines:

* **Data freshness issues**

  * Upstream data sources delayed (DB extract runs late).
  * Broken external API integration.

* **Data quality issues**

  * Schema drift (new column, datatype change).
  * Null/missing values in critical fields.
  * Duplicated records (double-counting profit).

* **Infrastructure issues**

  * Out of memory or storage in processing cluster.
  * Network connectivity between warehouse and source.

* **Operational issues**

  * Cron/Airflow job misconfigured or skipped.
  * Credentials expired (API keys, DB logins).

* **Human error**

  * Wrong SQL transformation committed.
  * Incorrect configuration in aggregation logic (e.g., mixing currencies).

* **Holiday/on-call gaps**

  * No one available to respond quickly → investor reports delayed.

ed notes style?
