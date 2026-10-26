-- Annualised salary ranges by job category.
-- Uses annualised figures (hourly x 2080, daily x 260, weekly x 52,
-- monthly x 12) so categories are comparable across pay frequencies.
-- SQLite has no MEDIAN aggregate, so centre is shown as AVG of the
-- range midpoint; MIN/MAX show the spread of range midpoints.
SELECT
    f.job_category,
    COUNT(*) AS postings_with_salary,
    ROUND(AVG((f.salary_min_annual + f.salary_max_annual) / 2.0)) AS avg_midpoint_annual,
    ROUND(MIN((f.salary_min_annual + f.salary_max_annual) / 2.0)) AS min_midpoint_annual,
    ROUND(MAX((f.salary_min_annual + f.salary_max_annual) / 2.0)) AS max_midpoint_annual
FROM fact_postings f
WHERE f.salary_min_annual IS NOT NULL
  AND f.salary_max_annual IS NOT NULL
  AND f.job_category <> ''
GROUP BY f.job_category
ORDER BY postings_with_salary DESC
LIMIT 15;
