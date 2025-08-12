SET search_path = affairs;

--4. normalizing the gender_rship table
SELECT * FROM gender_rship;

--A. creating a gender lookup table for male/female
DROP TABLE sex;

CREATE TABLE IF NOT EXISTS sex AS
SELECT ROW_NUMBER() OVER (ORDER BY gender) AS gender_id, gender
FROM (SELECT DISTINCT gender FROM gender_rship) sub;

SELECT * FROM sex;

--B. creating an education lookup table
DROP TABLE education;

CREATE TABLE IF NOT EXISTS education AS
SELECT ROW_NUMBER() OVER (ORDER BY education) AS education_id, education
FROM (SELECT DISTINCT education FROM gender_rship) sub;

SELECT * FROM education;

--C. creating an occupation lookup table
DROP TABLE education;

CREATE TABLE IF NOT EXISTS occupation AS
SELECT ROW_NUMBER() OVER (ORDER BY occupation) AS occupation_id, occupation
FROM (SELECT DISTINCT occupation FROM gender_rship) sub;

SELECT * FROM occupation;

--D. creating a rating lookup and adding rating in text
DROP TABLE rating;

CREATE TABLE IF NOT EXISTS rating AS
SELECT rating AS rating_id,
    CASE rating WHEN 1 THEN 'Very Dissatisfied'
	WHEN 2 THEN 'Dissatisfied'
	WHEN 3 THEN 'Neutral'
    WHEN 4 THEN 'Satisfied'
    WHEN 5 THEN 'Very Satisfied'
    ELSE 'Unknown'
    END AS rating_desc
FROM (SELECT DISTINCT rating FROM gender_rship) sub;

SELECT * FROM rating;

--E. creating a religiousness lookup and adding rating in text
DROP TABLE religiousness;

CREATE TABLE religiousness AS
SELECT religiousness AS religiousness_id,  -- use existing value as PK
    CASE religiousness WHEN 1 THEN 'Not Religious'
    WHEN 2 THEN 'Slightly Religious'
    WHEN 3 THEN 'Moderately Religious'
    WHEN 4 THEN 'Religious'
    WHEN 5 THEN 'Very Religious'
    ELSE 'Unknown'
    END AS religiousness_desc
FROM (SELECT DISTINCT religiousness FROM gender_rship) sub;

SELECT * FROM religiousness;

--F. Creating a relationship type lookup
DROP TABLE IF EXISTS relationship_type_lookup;

CREATE TABLE relationship_type AS
SELECT ROW_NUMBER() OVER (ORDER BY relationship_type) AS relationship_type_id,
       relationship_type
FROM (SELECT DISTINCT relationship_type FROM gender_rship) sub;

SELECT * FROM relationship_type;


--G. Creating respondent table
DROP TABLE IF EXISTS respondent;

CREATE TABLE IF NOT EXISTS respondent AS 
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

SELECT * FROM respondent ORDER BY respondent_id;

--H. Creating relationship_type table
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


SELECT * FROM relationship_details;