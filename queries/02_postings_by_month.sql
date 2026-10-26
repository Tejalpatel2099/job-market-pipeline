-- Posting volume by month (last 12 months present in the data)
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(*) AS postings
FROM fact_postings f
JOIN dim_date d ON d.date_key = f.posting_date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year DESC, d.month DESC
LIMIT 12;
