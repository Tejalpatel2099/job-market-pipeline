-- Most demanded skills/keywords across postings.
-- Skills come from posting_skills, a bridge table built in the transform
-- stage by keyword-matching the posting's preferred_skills + description
-- text against a documented keyword list (see src/transform.py).
SELECT
    s.skill,
    COUNT(DISTINCT s.posting_key) AS postings,
    ROUND(100.0 * COUNT(DISTINCT s.posting_key) /
          (SELECT COUNT(*) FROM fact_postings), 1) AS pct_of_postings
FROM posting_skills s
GROUP BY s.skill
ORDER BY postings DESC
LIMIT 20;
