SET search_path = affairs;

SELECT * FROM gender ORDER BY respondent_id;
SELECT * FROM relationships;

SELECT * FROM gender
LEFT JOIN relationships
ON gender.respondent_id = relationships.respondent_id;

DROP TABLE IF EXISTS gender;
DROP TABLE IF EXISTS relationships;