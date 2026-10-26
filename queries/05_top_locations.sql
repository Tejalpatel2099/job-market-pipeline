-- Top work locations by posting count.
-- Source locations are free-text street addresses (not geocoded); the
-- transform stage normalises whitespace only, so each distinct address
-- string is one location row.
SELECT
    l.work_location,
    COUNT(*) AS postings
FROM fact_postings f
JOIN dim_location l ON l.location_key = f.location_key
WHERE l.work_location <> 'Unknown'
GROUP BY l.work_location
ORDER BY postings DESC
LIMIT 15;
