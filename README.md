# 📊 HR Analytics & Employee Retention — SQL Portfolio Project

![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-336791?style=flat&logo=postgresql&logoColor=white)
![SQL](https://img.shields.io/badge/Language-SQL-orange?style=flat&logo=amazondynamodb&logoColor=white)
![Domain](https://img.shields.io/badge/Domain-HR%20Analytics-blue?style=flat)
![Queries](https://img.shields.io/badge/Queries-50-brightgreen?style=flat)
![Author](https://img.shields.io/badge/Author-Indrajith-lightgrey?style=flat)

---

## 📖 Overview

This project analyzes employee attrition, performance, and retention using a structured SQL data warehouse built on top of raw IBM HR Analytics data. It demonstrates end-to-end data engineering and business intelligence skills — from schema design and data cleaning to 50 analytical queries covering real HR use cases.

---

## 📁 Project Structure

```
hr_analytics/
├── schema/
│   ├── dim_employee         # Employee dimension table
│   ├── dim_time             # Date dimension table
│   ├── performance_fact     # Performance scores over time
│   ├── retention_fact       # Hire and exit events
│   └── staging_hr_employee  # Raw staging table (source data)
├── data_preparation/
│   ├── Data loading & inserts
│   ├── hire_date estimation from YearsAtCompany
│   └── Data validation checks
└── analysis/
    └── Q1–Q50 Business Analysis Queries
```

---

## 🗂️ Data Model

The project follows a **star schema** design:

| Table | Type | Description |
|---|---|---|
| `dim_employee` | Dimension | Employee profile: gender, department, role, hire date |
| `dim_time` | Dimension | Calendar date dimension for 2025 |
| `performance_fact` | Fact | Daily performance ratings per employee |
| `retention_fact` | Fact | Hire and exit events with timestamps |
| `staging_hr_employee` | Staging | Raw source data with 35 HR attributes |

---

## 🔧 Data Preparation

- Raw data is loaded into `staging_hr_employee` and transformed into dimension and fact tables
- Missing `hire_date` values are estimated using the `YearsAtCompany` field
- Data validation queries check for nulls in critical fields (gender, department, hire date)
- Retention events are derived from the `Attrition` flag in staging data

---

## 📊 Business Analysis Queries (Q1–Q50)

The 50 queries are organized around five HR themes:

<details>
<summary><strong>🔴 Attrition & Retention</strong></summary>

| Query | Description |
|---|---|
| Q1 | Monthly attrition count with rolling 12-month sum |
| Q3 | Cohort retention rate by hire year |
| Q4 | Attrition risk scoring using weighted factors |
| Q7 | Attrition rate by department and gender |
| Q12 | Overtime vs. attrition analysis |
| Q15 | Attrition rate by marital status |
| Q23 | Attrition rate by education field |
| Q46 | Attrition rate by business travel frequency |
| Q48 | Attrition rate by years in current role |
| Q49 | Correlation between commute distance and attrition |
| Q50 | Rolling 3-month average attrition count |

</details>

<details>
<summary><strong>🟡 Performance</strong></summary>

| Query | Description |
|---|---|
| Q2 | Employee performance ranking within roles |
| Q5 | Month-over-month performance trend (LAG window) |
| Q6 | Avg performance by department and role |
| Q8 | Top 5 departments by average performance |
| Q11 | Avg performance by tenure bucket |
| Q14 | Performance score distribution |
| Q47 | Avg performance rating by department |

</details>

<details>
<summary><strong>🟢 Compensation & Incentives</strong></summary>

| Query | Description |
|---|---|
| Q13 | Avg salary by role and department |
| Q20 | Avg monthly income by marital status |
| Q24 | Avg hourly rate by job role |
| Q25 | Total stock options by department |
| Q27 | Avg percent salary hike by department |
| Q40 | Avg daily rate by department |
| Q41 | Avg hourly rate by marital status |
| Q45 | Avg salary hike by education field |

</details>

<details>
<summary><strong>🔵 Workforce Composition</strong></summary>

| Query | Description |
|---|---|
| Q9 | Employees with longest tenure |
| Q10 | Monthly new hire counts |
| Q17 | Employee count by education field |
| Q21 | Employee count by job level and department |
| Q28 | Employee count by business travel frequency |
| Q36 | Employee count by stock option level |
| Q39 | Gender distribution by job role |

</details>

<details>
<summary><strong>🟣 Engagement & Satisfaction</strong></summary>

| Query | Description |
|---|---|
| Q16 | Avg work-life balance score by department |
| Q19 | Job satisfaction score distribution |
| Q22 | Avg job involvement by department |
| Q26 | Avg training frequency by department |
| Q29 | Avg relationship satisfaction by department |
| Q30 | Employee count by environment satisfaction level |
| Q31 | Commute distance for overtime vs. non-overtime workers |
| Q32 | Avg years since last promotion by department |
| Q35 | Avg years in current role by department |
| Q37 | Avg years with current manager by department |
| Q38 | Training frequency for attrited vs. retained employees |
| Q43 | Avg job satisfaction by department |

</details>

---

## 🧠 Key SQL Techniques Used

| Technique | Details |
|---|---|
| **Window Functions** | `RANK()`, `LAG()`, rolling sums/averages with `ROWS BETWEEN` |
| **CTEs** | Multi-step logic for cohort analysis, risk scoring, and trends |
| **CASE Expressions** | Tenure bucketing, risk scoring, event derivation |
| **Aggregate Functions** | `AVG()`, `COUNT()`, `SUM()`, `STDDEV_POP()` |
| **Statistical Correlation** | Manual Pearson correlation (commute distance vs. attrition) |
| **Data Cleaning** | `COALESCE`, `NULLIF`, `ON CONFLICT DO NOTHING` |
| **Date Arithmetic** | `INTERVAL`, `DATE_TRUNC`, `generate_series` |

---

## 🚀 How to Run

1. Create a PostgreSQL database and run the schema definitions from **Section 1**
2. Load your HR CSV data into `staging_hr_employee`:

```sql
\COPY staging_hr_employee FROM 'hr_data.csv' WITH (FORMAT csv, HEADER true);
```

3. Run the data preparation scripts in **Section 2** to populate dimension and fact tables
4. Execute any of the **Q1–Q50** queries from Section 3 for business insights

---

## 📦 Dataset

Compatible with the publicly available **[IBM HR Analytics Employee Attrition & Performance](https://www.kaggle.com/datasets/pavansubhasht/ibm-hr-analytics-attrition-dataset)** dataset — 1,470 employee records across 35 attributes including demographics, job details, satisfaction scores, and attrition status.

---

## ✅ Skills Demonstrated

- Dimensional data modeling (star schema)
- ETL pipeline design in SQL
- Advanced analytical SQL (window functions, CTEs, correlation)
- HR domain knowledge and KPI design
- Data quality validation
