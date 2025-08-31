/* =========================================================
   PORTFOLIO PROJECT: HR Analytics & Employee Retention
   Author: Indrajith
   Purpose: Analyze employee attrition, performance, and retention
   Sections:
     1) Data Preparation & Transformation
     2) Business Analysis Queries (Q1-Q50)
   ========================================================= */

/* ===================
   SECTION 1: SCHEMA DEFINITION
   =================== */

/* Employee Dimension Table */
CREATE TABLE dim_employee (
  employee_id SERIAL PRIMARY KEY,
  first_name VARCHAR(50),                 -- Currently NULL, consider enriching data if possible
  last_name VARCHAR(50),                  -- Optional: enrich if data available
  gender VARCHAR(10) NOT NULL,
  date_of_birth DATE,                     -- Optional field, to be added if data available
  hire_date DATE,                         -- Optional field; estimate from YearsAtCompany if missing
  department VARCHAR(50) NOT NULL,
  role VARCHAR(50) NOT NULL,
  location VARCHAR(50)                    -- To be enriched later if data added
);

/* Date Dimension Table */
CREATE TABLE dim_time (
  date_key DATE PRIMARY KEY,
  year INT NOT NULL,
  quarter INT NOT NULL,
  month INT NOT NULL,
  day INT NOT NULL,
  day_of_week INT NOT NULL
);

/* Performance Fact Table */
CREATE TABLE performance_fact (
  perf_id SERIAL PRIMARY KEY,
  employee_id INT NOT NULL REFERENCES dim_employee(employee_id),
  date_key DATE NOT NULL REFERENCES dim_time(date_key),
  performance_score INT NOT NULL CHECK (performance_score BETWEEN 1 AND 5)
);

/* Retention Fact Table */
CREATE TABLE retention_fact (
  event_id SERIAL PRIMARY KEY,
  employee_id INT NOT NULL REFERENCES dim_employee(employee_id),
  event_date DATE NOT NULL REFERENCES dim_time(date_key),
  event_type VARCHAR(10) NOT NULL  -- Values like 'hire' or 'exit'
);

/* Staging Table for Raw HR Data */
CREATE TABLE staging_hr_employee (
  "Age" INT,
  "Attrition" VARCHAR(10),
  "BusinessTravel" VARCHAR(50),
  "DailyRate" INT,
  "Department" VARCHAR(50),
  "DistanceFromHome" INT,
  "Education" INT,
  "EducationField" VARCHAR(50),
  "EmployeeCount" INT,
  "EmployeeNumber" INT PRIMARY KEY,
  "EnvironmentSatisfaction" INT,
  "Gender" VARCHAR(10),
  "HourlyRate" INT,
  "JobInvolvement" INT,
  "JobLevel" INT,
  "JobRole" VARCHAR(100),
  "JobSatisfaction" INT,
  "MaritalStatus" VARCHAR(50),
  "MonthlyIncome" INT,
  "MonthlyRate" INT,
  "NumCompaniesWorked" INT,
  "Over18" VARCHAR(10),
  "OverTime" VARCHAR(10),
  "PercentSalaryHike" INT,
  "PerformanceRating" INT,
  "RelationshipSatisfaction" INT,
  "StandardHours" INT,
  "StockOptionLevel" INT,
  "TotalWorkingYears" INT,
  "TrainingTimesLastYear" INT,
  "WorkLifeBalance" INT,
  "YearsAtCompany" INT,
  "YearsInCurrentRole" INT,
  "YearsSinceLastPromotion" INT,
  "YearsWithCurrManager" INT
);

/* ===================
   SECTION 2: DATA PREPARATION & LOADING
   =================== */

/* Insert data into dim_employee */
INSERT INTO dim_employee (employee_id, first_name, last_name, gender, date_of_birth, hire_date, department, role, location)
SELECT 
  "EmployeeNumber" AS employee_id,
  NULL AS first_name,
  NULL AS last_name,
  "Gender" AS gender,
  NULL::DATE AS date_of_birth,
  NULL::DATE AS hire_date,
  "Department" AS department,
  "JobRole" AS role,
  NULL AS location
FROM staging_hr_employee
ON CONFLICT (employee_id) DO NOTHING;

/* Populate dim_time for 2025 calendar year */
INSERT INTO dim_time (date_key, year, quarter, month, day, day_of_week)
SELECT date::date,
       EXTRACT(YEAR FROM date) AS year,
       EXTRACT(QUARTER FROM date) AS quarter,
       EXTRACT(MONTH FROM date) AS month,
       EXTRACT(DAY FROM date) AS day,
       EXTRACT(DOW FROM date) AS day_of_week
FROM generate_series('2025-01-01'::date, '2025-12-31'::date, interval '1 day') AS date
ON CONFLICT (date_key) DO NOTHING;

/* Insert today's performance ratings from staging */
INSERT INTO performance_fact (employee_id, date_key, performance_score)
SELECT 
  "EmployeeNumber" AS employee_id,
  CURRENT_DATE AS date_key,
  "PerformanceRating" AS performance_score
FROM staging_hr_employee;

/* Insert retention events (hire or exit) */
INSERT INTO retention_fact (employee_id, event_date, event_type)
SELECT 
  "EmployeeNumber" AS employee_id,
  CURRENT_DATE AS event_date,
  CASE WHEN "Attrition" = 'Yes' THEN 'exit' ELSE 'hire' END AS event_type
FROM staging_hr_employee;

/* Data Cleaning: Estimate missing hire_date using YearsAtCompany */
UPDATE dim_employee de
SET hire_date = CURRENT_DATE - (se."YearsAtCompany" * INTERVAL '1 year')
FROM staging_hr_employee se
WHERE de.employee_id = se."EmployeeNumber"
  AND de.hire_date IS NULL
  AND se."YearsAtCompany" IS NOT NULL;

/* Data Validation: Check number of missing hire_dates */
SELECT COUNT(*) AS missing_hire_dates FROM dim_employee WHERE hire_date IS NULL;

/* Additional data validation examples */
-- Check for missing gender
SELECT COUNT(*) AS missing_gender FROM dim_employee WHERE gender IS NULL OR gender = '';

-- Check for missing or invalid departments
SELECT department, COUNT(*) FROM dim_employee GROUP BY department HAVING department IS NULL OR department = '';


/* ===================
   SECTION 3: BUSINESS ANALYSIS QUERIES
   =================== */

/* Q1: Monthly Employee Attrition Count with Rolling 12-Month Sum
   Business Insight: Monitor attrition trends and cumulative impact over time for retention strategies. */
WITH monthly_exits AS (
  SELECT DATE_TRUNC('month', event_date) AS month, COUNT(*) AS attrition_count
  FROM retention_fact
  WHERE event_type = 'exit'
  GROUP BY month
)
SELECT month,
       attrition_count,
       SUM(attrition_count) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_12_month_attrition
FROM monthly_exits
ORDER BY month;

/* Q2: Employee Performance Ranking Within Roles on Current Date
   Business Insight: Identify performance leaders and laggers within roles for targeted management. */
SELECT de.employee_id, de.role, pf.performance_score,
       RANK() OVER (PARTITION BY de.role ORDER BY pf.performance_score DESC) AS role_performance_rank
FROM performance_fact pf
JOIN dim_employee de ON pf.employee_id = de.employee_id
WHERE pf.date_key = CURRENT_DATE;

/* Q3: Cohort Retention Rate by Hire Year
   Business Insight: Analyze retention trends by hire cohorts to improve onboarding and retention programs. */
WITH hires AS (
  SELECT employee_id, EXTRACT(YEAR FROM COALESCE(hire_date, CURRENT_DATE)) AS hire_year
  FROM dim_employee
),
exits AS (
  SELECT employee_id, EXTRACT(YEAR FROM event_date) AS exit_year
  FROM retention_fact
  WHERE event_type = 'exit'
)
SELECT h.hire_year,
       COUNT(DISTINCT h.employee_id) AS total_hired,
       COUNT(DISTINCT CASE WHEN e.exit_year = h.hire_year THEN e.employee_id END) AS exited_in_hire_year,
       ROUND(100.0 * (COUNT(DISTINCT h.employee_id) - COUNT(DISTINCT CASE WHEN e.exit_year = h.hire_year THEN e.employee_id END)) / NULLIF(COUNT(DISTINCT h.employee_id), 0), 2) AS retention_rate_percent
FROM hires h
LEFT JOIN exits e ON h.employee_id = e.employee_id
GROUP BY h.hire_year
ORDER BY h.hire_year;

/* Q4: Attrition Risk Score Using Weighted Factors
   Business Insight: Score employees by risk of attrition based on performance and tenure to prioritize interventions. */
SELECT de.employee_id,
       (CASE WHEN pf.performance_score < 3 THEN 50 ELSE 0 END) +
       (CASE WHEN se."YearsAtCompany" < 2 THEN 30 ELSE 0 END) +
       (CASE WHEN se."Attrition" = 'Yes' THEN 20 ELSE 0 END) AS churn_risk_score
FROM dim_employee de
LEFT JOIN performance_fact pf ON de.employee_id = pf.employee_id AND pf.date_key = CURRENT_DATE
LEFT JOIN staging_hr_employee se ON de.employee_id = se."EmployeeNumber"
WHERE pf.performance_score IS NOT NULL;

/* Q5: Average Performance Trend Over Last 6 Months Using Window LAG
   Business Insight: Track month-over-month performance changes to identify improving or declining employees. */
WITH recent_performance AS (
  SELECT pf.employee_id, pf.date_key, pf.performance_score
  FROM performance_fact pf
  WHERE pf.date_key >= CURRENT_DATE - INTERVAL '6 months'
)
SELECT employee_id, date_key, performance_score,
       LAG(performance_score, 1) OVER (PARTITION BY employee_id ORDER BY date_key) AS prev_month_score,
       performance_score - LAG(performance_score, 1) OVER (PARTITION BY employee_id ORDER BY date_key) AS month_score_diff
FROM recent_performance
ORDER BY employee_id, date_key;

/* Q6: Average Performance Score by Department and Job Role
   Business Insight: Assess performance across departments and roles for resource allocation and recognition. */
SELECT de.department, de.role,
       ROUND(AVG(pf.performance_score), 2) AS avg_performance_score,
       COUNT(DISTINCT pf.employee_id) AS employee_count
FROM performance_fact pf
JOIN dim_employee de ON pf.employee_id = de.employee_id
GROUP BY de.department, de.role
ORDER BY avg_performance_score DESC, employee_count DESC;

/* Q7: Attrition Rate by Department and Gender
   Business Insight: Detect demographic or departmental attrition issues to inform diversity and retention policies. */
WITH exits AS (
  SELECT employee_id FROM retention_fact WHERE event_type = 'exit'
),
dept_gender AS (
  SELECT employee_id, department, gender
  FROM dim_employee
)
SELECT dg.department, dg.gender,
       COUNT(*) AS total_employees,
       COUNT(e.employee_id) AS total_exits,
       ROUND(100.0 * COUNT(e.employee_id) / NULLIF(COUNT(*), 0), 2) AS attrition_rate_percent
FROM dept_gender dg
LEFT JOIN exits e ON dg.employee_id = e.employee_id
GROUP BY dg.department, dg.gender
ORDER BY dg.department, dg.gender;

/* Q8: Top 5 Departments by Average Performance Score
   Business Insight: Highlight high-performing departments for best practices sharing. */
SELECT de.department,
       ROUND(AVG(pf.performance_score), 2) AS avg_performance_score
FROM performance_fact pf
JOIN dim_employee de ON pf.employee_id = de.employee_id
GROUP BY de.department
ORDER BY avg_performance_score DESC
LIMIT 5;

/* Q9: Employees with Longest Tenure
   Business Insight: Identify veteran employees who can be mentors or key to knowledge retention. */
SELECT "EmployeeNumber" AS employee_id, "Department" AS department, "JobRole" AS role, "YearsAtCompany"
FROM staging_hr_employee
ORDER BY "YearsAtCompany" DESC
LIMIT 10;

/* Q10: Monthly New Hires Count
   Business Insight: Understand hiring trends and workforce growth patterns over time. */
WITH estimated_hires AS (
  SELECT 
    "EmployeeNumber" AS employee_id,
    (CURRENT_DATE - ("YearsAtCompany" * INTERVAL '1 year'))::date AS estimated_hire_date
  FROM staging_hr_employee
  WHERE "YearsAtCompany" IS NOT NULL
)
SELECT DATE_TRUNC('month', estimated_hire_date) AS month,
       COUNT(*) AS new_hires
FROM estimated_hires
GROUP BY month
ORDER BY month;

/* Q11: Average Performance Score by Years at Company Bucket
   Business Insight: Analyze how tenure impacts employee performance for retention strategies. */
SELECT
  CASE
    WHEN se."YearsAtCompany" < 1 THEN '< 1 year'
    WHEN se."YearsAtCompany" BETWEEN 1 AND 3 THEN '1-3 years'
    WHEN se."YearsAtCompany" BETWEEN 4 AND 6 THEN '4-6 years'
    WHEN se."YearsAtCompany" BETWEEN 7 AND 10 THEN '7-10 years'
    ELSE '10+ years'
  END AS tenure_bucket,
  ROUND(AVG(pf.performance_score), 2) AS avg_performance_score
FROM performance_fact pf
JOIN staging_hr_employee se ON pf.employee_id = se."EmployeeNumber"
GROUP BY tenure_bucket
ORDER BY tenure_bucket;

/* Q12: Employees with Overtime and Attrition Status
   Business Insight: Analyze whether overtime work impacts attrition, informing workload policies. */
SELECT se."OverTime",
       COUNT(*) AS total_employees,
       SUM(CASE WHEN se."Attrition" = 'Yes' THEN 1 ELSE 0 END) AS attrition_count,
       ROUND(100.0 * SUM(CASE WHEN se."Attrition" = 'Yes' THEN 1 ELSE 0 END)/COUNT(*), 2) AS attrition_rate_percent
FROM staging_hr_employee se
GROUP BY se."OverTime"
ORDER BY attrition_rate_percent DESC;

/* Q13: Average Salary by Role and Department
   Business Insight: Identify compensation differences and align pay equity initiatives. */
SELECT "Department" AS department, "JobRole",
       ROUND(AVG("MonthlyIncome"), 2) AS avg_monthly_income
FROM staging_hr_employee
GROUP BY "Department", "JobRole"
ORDER BY avg_monthly_income DESC
LIMIT 15;

/* Q14: Performance Score Distribution
   Business Insight: Understand overall performance ratings distribution to identify gaps. */
SELECT performance_score,
       COUNT(*) AS employee_count
FROM performance_fact
GROUP BY performance_score
ORDER BY performance_score DESC;

/* Q15: Attrition Count by Marital Status
   Business Insight: Explore attrition risk variations by marital status to tailor retention programs. */
SELECT "MaritalStatus",
       COUNT(*) AS total_employees,
       SUM(CASE WHEN "Attrition" = 'Yes' THEN 1 ELSE 0 END) AS attritions,
       ROUND(100.0 * SUM(CASE WHEN "Attrition" = 'Yes' THEN 1 ELSE 0 END)/COUNT(*), 2) AS attrition_rate_percent
FROM staging_hr_employee
GROUP BY "MaritalStatus"
ORDER BY attrition_rate_percent DESC;

/* Q16: Average WorkLifeBalance Score by Department
   Business Insight: Correlate work-life balance perception with department performance and retention. */
SELECT "Department" AS department,
       ROUND(AVG("WorkLifeBalance"), 2) AS avg_worklife_balance
FROM staging_hr_employee
GROUP BY "Department"
ORDER BY avg_worklife_balance DESC;

/* Q17: Count of Employees by Education Field
   Business Insight: Understand education field distribution to align training and development. */
SELECT "EducationField", COUNT(*) AS employee_count
FROM staging_hr_employee
GROUP BY "EducationField"
ORDER BY employee_count DESC;

/* Q18: Average Distance From Home by Department
   Business Insight: Measure commute impact on departments to inform flexible work policies. */
SELECT "Department" AS department,
       ROUND(AVG("DistanceFromHome"), 2) AS avg_distance_from_home
FROM staging_hr_employee
GROUP BY "Department"
ORDER BY avg_distance_from_home DESC;

/* Q19: Distribution of Job Satisfaction Scores
   Business Insight: Understand overall job satisfaction and identify groups needing attention. */
SELECT "JobSatisfaction" AS satisfaction_score,
       COUNT(*) AS employee_count
FROM staging_hr_employee
GROUP BY satisfaction_score
ORDER BY satisfaction_score DESC;

/* Q20: Average Monthly Income by Marital Status
   Business Insight: Analyze compensation trends by marital status for equity assessment. */
SELECT "MaritalStatus",
       ROUND(AVG("MonthlyIncome"), 2) AS avg_monthly_income
FROM staging_hr_employee
GROUP BY "MaritalStatus"
ORDER BY avg_monthly_income DESC;

/* Q21: Count of Employees by Job Level and Department
   Business Insight: Know employee distribution by seniority within departments for succession planning. */
SELECT "Department",
       "JobLevel",
       COUNT(*) AS employee_count
FROM staging_hr_employee
GROUP BY "Department", "JobLevel"
ORDER BY "Department", "JobLevel";

/* Q22: Average Job Involvement Score by Department
   Business Insight: Identify departments with high employee engagement to replicate successes. */
SELECT "Department",
       ROUND(AVG("JobInvolvement"), 2) AS avg_job_involvement
FROM staging_hr_employee
GROUP BY "Department"
ORDER BY avg_job_involvement DESC;

/* Q23: Attrition Rate by Education Field
   Business Insight: Detect if certain education fields have higher attrition for targeted retention. */
SELECT "EducationField",
       COUNT(*) AS total_employees,
       SUM(CASE WHEN "Attrition" = 'Yes' THEN 1 ELSE 0 END) AS attrited_employees,
       ROUND(100.0 * SUM(CASE WHEN "Attrition" = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS attrition_rate_percent
FROM staging_hr_employee
GROUP BY "EducationField"
ORDER BY attrition_rate_percent DESC;

/* Q24: Average Hourly Rate by Job Role
   Business Insight: Investigate wage differences by role for compensation benchmarking. */
SELECT "JobRole",
       ROUND(AVG("HourlyRate"), 2) AS avg_hourly_rate
FROM staging_hr_employee
GROUP BY "JobRole"
ORDER BY avg_hourly_rate DESC
LIMIT 15;

/* Q25: Total Stock Option Levels by Department
   Business Insight: Analyze given stock options to evaluate incentives distribution. */
SELECT "Department",
       SUM("StockOptionLevel") AS total_stock_options
FROM staging_hr_employee
GROUP BY "Department"
ORDER BY total_stock_options DESC;

/* Q26: Average Training Times Last Year by Department
   Business Insight: See which departments invest more in employee training for development strategies. */
SELECT "Department",
       ROUND(AVG("TrainingTimesLastYear"), 2) AS avg_training_times
FROM staging_hr_employee
GROUP BY "Department"
ORDER BY avg_training_times DESC;

/* Q27: Average Percent Salary Hike by Department
   Business Insight: Measure salary adjustments across departments to identify compensation trends. */
SELECT "Department",
       ROUND(AVG("PercentSalaryHike"), 2) AS avg_salary_hike_percent
FROM staging_hr_employee
GROUP BY "Department"
ORDER BY avg_salary_hike_percent DESC;

/* Q28: Count of Employees by Business Travel Frequency
   Business Insight: Understand travel patterns for work-life balance analysis. */
SELECT "BusinessTravel",
       COUNT(*) AS employee_count
FROM staging_hr_employee
GROUP BY "BusinessTravel"
ORDER BY employee_count DESC;

/* Q29: Average Relationship Satisfaction by Department
   Business Insight: Explore employee relationship satisfaction to improve manager-employee dynamics. */
SELECT "Department",
       ROUND(AVG("RelationshipSatisfaction"), 2) AS avg_relationship_satisfaction
FROM staging_hr_employee
GROUP BY "Department"
ORDER BY avg_relationship_satisfaction DESC;

/* Q30: Count of Employees by Environment Satisfaction Level
   Business Insight: Identify environment satisfaction to enhance workplace conditions. */
SELECT "EnvironmentSatisfaction",
       COUNT(*) AS employee_count
FROM staging_hr_employee
GROUP BY "EnvironmentSatisfaction"
ORDER BY "EnvironmentSatisfaction" DESC;

/* Q31: Average Distance From Home for Employees Who Work Overtime vs Not
   Business Insight: Test impact of commute distance on overtime work pattern and work-life balance. */
SELECT "OverTime",
       ROUND(AVG("DistanceFromHome"), 2) AS avg_distance_from_home
FROM staging_hr_employee
GROUP BY "OverTime"
ORDER BY avg_distance_from_home DESC;

/* Q32: Average Years Since Last Promotion by Department
   Business Insight: Review promotion frequency to assess career growth opportunities. */
SELECT "Department",
       ROUND(AVG("YearsSinceLastPromotion"), 2) AS avg_years_since_promotion
FROM staging_hr_employee
GROUP BY "Department"
ORDER BY avg_years_since_promotion DESC;

/* Q33: Count of Employees by Marital Status and Attrition Status
   Business Insight: Understand attrition trends across marital statuses for tailored retention. */
SELECT "MaritalStatus",
       "Attrition",
       COUNT(*) AS employee_count
FROM staging_hr_employee
GROUP BY "MaritalStatus", "Attrition"
ORDER BY "MaritalStatus", "Attrition";

/* Q34: Average Total Working Years by Department
   Business Insight: Identify departments with experienced workforce. */
SELECT "Department",
       ROUND(AVG("TotalWorkingYears"), 2) AS avg_total_working_years
FROM staging_hr_employee
GROUP BY "Department"
ORDER BY avg_total_working_years DESC;

/* Q35: Average Years in Current Role by Department
   Business Insight: Assess time spent in roles for succession planning. */
SELECT "Department",
       ROUND(AVG("YearsInCurrentRole"), 2) AS avg_years_in_current_role
FROM staging_hr_employee
GROUP BY "Department"
ORDER BY avg_years_in_current_role DESC;

/* Q36: Count of Employees by Stock Option Level
   Business Insight: Explore distribution of stock incentives. */
SELECT "StockOptionLevel",
       COUNT(*) AS employee_count
FROM staging_hr_employee
GROUP BY "StockOptionLevel"
ORDER BY "StockOptionLevel" DESC;

/* Q37: Average Years With Current Manager by Department
   Business Insight: Understand manager tenure impact on teams. */
SELECT "Department",
       ROUND(AVG("YearsWithCurrManager"), 2) AS avg_years_with_manager
FROM staging_hr_employee
GROUP BY "Department"
ORDER BY avg_years_with_manager DESC;

/* Q38: Average Training Times Last Year by Attrition Status
   Business Insight: Compare training frequency between attrited and retained employees. */
SELECT "Attrition",
       ROUND(AVG("TrainingTimesLastYear"), 2) AS avg_training_times
FROM staging_hr_employee
GROUP BY "Attrition"
ORDER BY avg_training_times DESC;

/* Q39: Count of Employees by Gender and Job Role
   Business Insight: Analyze gender distribution across roles for diversity management. */
SELECT "Gender", 
       "JobRole",
       COUNT(*) AS employee_count
FROM staging_hr_employee
GROUP BY "Gender", "JobRole"
ORDER BY "Gender", employee_count DESC;

/* Q40: Average Daily Rate by Department
   Business Insight: Review daily pay rates for budget planning. */
SELECT "Department",
       ROUND(AVG("DailyRate"), 2) AS avg_daily_rate
FROM staging_hr_employee
GROUP BY "Department"
ORDER BY avg_daily_rate DESC;

/* Q41: Average Hourly Rate by Marital Status
   Business Insight: Analyze hourly pay variation by marital status. */
SELECT "MaritalStatus",
       ROUND(AVG("HourlyRate"), 2) AS avg_hourly_rate
FROM staging_hr_employee
GROUP BY "MaritalStatus"
ORDER BY avg_hourly_rate DESC;

/* Q42: Count of Employees by Education Level and Attrition
   Business Insight: Identify attrition patterns by education level. */
SELECT "Education",
       "Attrition",
       COUNT(*) AS employee_count
FROM staging_hr_employee
GROUP BY "Education", "Attrition"
ORDER BY "Education", "Attrition";

/* Q43: Average Job Satisfaction by Department
   Business Insight: Pinpoint departments with high/low job satisfaction for HR focus. */
SELECT "Department",
       ROUND(AVG("JobSatisfaction"), 2) AS avg_job_satisfaction
FROM staging_hr_employee
GROUP BY "Department"
ORDER BY avg_job_satisfaction DESC;

/* Q44: Count of Employees by Stock Option Level and Attrition
   Business Insight: Study attrition in relation to stock incentives. */
SELECT "StockOptionLevel",
       "Attrition",
       COUNT(*) AS employee_count
FROM staging_hr_employee
GROUP BY "StockOptionLevel", "Attrition"
ORDER BY "StockOptionLevel", "Attrition";

/* Q45: Average Percent Salary Hike by Education Field
   Business Insight: Analyze salary hikes by education for equity assessment. */
SELECT "EducationField",
       ROUND(AVG("PercentSalaryHike"), 2) AS avg_salary_hike
FROM staging_hr_employee
GROUP BY "EducationField"
ORDER BY avg_salary_hike DESC;

/* Q46: Count of Employees by Business Travel and Attrition Status
   Business Insight: Understand attrition among travel frequency groups. */
SELECT "BusinessTravel",
       "Attrition",
       COUNT(*) AS employee_count
FROM staging_hr_employee
GROUP BY "BusinessTravel", "Attrition"
ORDER BY "BusinessTravel", "Attrition";

/* Q47: Average Performance Rating by Department
   Business Insight: Department-level performance insights for leadership focus. */
SELECT "Department",
       ROUND(AVG("PerformanceRating"), 2) AS avg_performance_rating
FROM staging_hr_employee
GROUP BY "Department"
ORDER BY avg_performance_rating DESC;

/* Q48: Attrition Rate by Years in Current Role Bucket
   Business Insight: Determine if role tenure impacts attrition risk. */
SELECT
  CASE
    WHEN "YearsInCurrentRole" < 1 THEN '< 1 year'
    WHEN "YearsInCurrentRole" BETWEEN 1 AND 3 THEN '1-3 years'
    WHEN "YearsInCurrentRole" BETWEEN 4 AND 6 THEN '4-6 years'
    ELSE '7+ years'
  END AS role_tenure_bucket,
  COUNT(*) AS total_employees,
  SUM(CASE WHEN "Attrition" = 'Yes' THEN 1 ELSE 0 END) AS total_exits,
  ROUND(100.0 * SUM(CASE WHEN "Attrition" = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS attrition_rate_percent
FROM staging_hr_employee
GROUP BY role_tenure_bucket
ORDER BY role_tenure_bucket;

/* Q49: Correlation Between Distance From Home and Attrition
   Business Insight: Measure how commute length correlates with attrition risk. */
WITH stats AS (
  SELECT
    AVG("DistanceFromHome") AS avg_dist,
    AVG(CASE WHEN "Attrition" = 'Yes' THEN 1 ELSE 0 END) AS avg_attrition
  FROM staging_hr_employee
),
cov AS (
  SELECT
    AVG(("DistanceFromHome" - s.avg_dist) * (CASE WHEN "Attrition" = 'Yes' THEN 1 ELSE 0 END - s.avg_attrition)) AS covariance,
    STDDEV_POP("DistanceFromHome") AS sd_dist,
    STDDEV_POP(CASE WHEN "Attrition" = 'Yes' THEN 1 ELSE 0 END) AS sd_attr
  FROM staging_hr_employee, stats s
)
SELECT ROUND(covariance / NULLIF(sd_dist * sd_attr, 0), 2) AS corr_distance_attrition
FROM cov;

/* Q50: Rolling 3-Month Average Attrition Count
   Business Insight: Smooth short-term attrition fluctuations to identify emerging trends. */
WITH monthly_exits AS (
  SELECT DATE_TRUNC('month', event_date) AS month, COUNT(*) AS exits
  FROM retention_fact
  WHERE event_type = 'exit'
  GROUP BY month
)
SELECT month,
       exits,
       ROUND(AVG(exits) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rolling_3m_avg_exits
FROM monthly_exits
ORDER BY month;
   
