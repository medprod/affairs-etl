CREATE OR REPLACE PROCEDURE normalize_gender_rship()
LANGUAGE plpgsql
AS $$
BEGIN
    SET search_path = affairs;

    -- A. Gender lookup
    DROP TABLE IF EXISTS sex;
    CREATE TABLE sex AS
    SELECT ROW_NUMBER() OVER (ORDER BY gender) AS gender_id, gender
    FROM (SELECT DISTINCT gender FROM gender_rship) sub;

    -- B. Education lookup
    DROP TABLE IF EXISTS education;
    CREATE TABLE education AS
    SELECT ROW_NUMBER() OVER (ORDER BY education) AS education_id, education
    FROM (SELECT DISTINCT education FROM gender_rship) sub;

    -- C. Occupation lookup
    DROP TABLE IF EXISTS occupation;
    CREATE TABLE occupation AS
    SELECT ROW_NUMBER() OVER (ORDER BY occupation) AS occupation_id, occupation
    FROM (SELECT DISTINCT occupation FROM gender_rship) sub;

    -- D. Rating lookup
    DROP TABLE IF EXISTS rating;
    CREATE TABLE rating AS
    SELECT rating AS rating_id,
        CASE rating
            WHEN 1 THEN 'Very Dissatisfied'
            WHEN 2 THEN 'Dissatisfied'
            WHEN 3 THEN 'Neutral'
            WHEN 4 THEN 'Satisfied'
            WHEN 5 THEN 'Very Satisfied'
            ELSE 'Unknown'
        END AS rating_desc
    FROM (SELECT DISTINCT rating FROM gender_rship) sub;

    -- E. Religiousness lookup
    DROP TABLE IF EXISTS religiousness;
    CREATE TABLE religiousness AS
    SELECT religiousness AS religiousness_id,
        CASE religiousness
            WHEN 1 THEN 'Not Religious'
            WHEN 2 THEN 'Slightly Religious'
            WHEN 3 THEN 'Moderately Religious'
            WHEN 4 THEN 'Religious'
            WHEN 5 THEN 'Very Religious'
            ELSE 'Unknown'
        END AS religiousness_desc
    FROM (SELECT DISTINCT religiousness FROM gender_rship) sub;

    -- F. Relationship type lookup
    DROP TABLE IF EXISTS relationship_type;
    CREATE TABLE relationship_type AS
    SELECT ROW_NUMBER() OVER (ORDER BY relationship_type) AS relationship_type_id,
           relationship_type
    FROM (SELECT DISTINCT relationship_type FROM gender_rship) sub;

    -- G. Respondent table
    DROP TABLE IF EXISTS respondent;
    CREATE TABLE respondent AS
    SELECT g.respondent_id,
           s.gender_id,
           g.age,
           e.education_id,
           o.occupation_id,
           reg.religiousness_id
    FROM gender_rship g
    JOIN sex s ON g.gender = s.gender
    JOIN education e ON g.education = e.education
    JOIN occupation o ON g.occupation = o.occupation
    JOIN religiousness reg ON g.religiousness = reg.religiousness_id;

    -- H. Relationship details table
    DROP TABLE IF EXISTS relationship_details;
    CREATE TABLE relationship_details AS
    SELECT g.respondent_id,
           rt.relationship_type_id,
           g.yearsmarried,
           g.children,
           r.rating_id,
           g.affairs AS affairs_num,
           g.narrative_text
    FROM gender_rship g
    JOIN rating r ON g.rating = r.rating_id
    JOIN relationship_type rt ON g.relationship_type = rt.relationship_type;

END;
$$;

CALL normalize_gender_rship();