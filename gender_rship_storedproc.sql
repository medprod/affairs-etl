CREATE OR REPLACE PROCEDURE join_gender_rship()
LANGUAGE plpgsql
AS $$
BEGIN
    SET search_path = affairs;
	DROP TABLE IF EXISTS gender_rship;

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

END;
$$;

CALL join_gender_rship();