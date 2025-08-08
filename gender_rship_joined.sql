SET search_path = affairs;

SELECT * FROM gender ORDER BY respondent_id;
SELECT * FROM relationships;

SELECT * FROM gender
LEFT JOIN relationships
ON gender.respondent_id = relationships.respondent_id;

--drop columns that are double.
CREATE TABLE IF NOT EXISTS
gender_rship AS
SELECT
	g.respondent_id,
	g.affairs,
	g.gender,
	g.age,
	g.yearsmarried,
	r.children,
	g.religiousness,
	g.rating,
	r.relationship_type,
	r.education,
	r.occupation,
	r.narrative_text
FROM gender g
LEFT JOIN relationships r
ON g.respondent_id = r.respondent_id;

SELECT * FROM gender_rship;

DROP TABLE IF EXISTS gender_rship
