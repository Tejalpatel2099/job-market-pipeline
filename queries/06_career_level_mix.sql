-- Career-level mix of live postings, with the share that is entry-level
SELECT
    f.career_level,
    COUNT(*) AS postings,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM fact_postings), 1) AS pct
FROM fact_postings f
WHERE f.career_level <> ''
GROUP BY f.career_level
ORDER BY postings DESC;
