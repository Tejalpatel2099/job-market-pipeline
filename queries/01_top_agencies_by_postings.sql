-- Top hiring agencies (companies) by number of live postings
SELECT
    c.agency,
    COUNT(*) AS postings,
    SUM(f.number_of_positions) AS total_positions
FROM fact_postings f
JOIN dim_company c ON c.company_key = f.company_key
GROUP BY c.agency
ORDER BY postings DESC
LIMIT 15;
