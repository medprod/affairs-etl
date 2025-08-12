--creating dimension tables

CREATE SCHEMA affairs_olap;

SET search_path = affairs_olap;

--1. Gender DIM
DROP TABLE IF EXISTS gender_dim;

CREATE TABLE gender_dim(
	gender_dim_id SERIAL PRIMARY KEY,
	gender TEXT
);

INSERT INTO gender_dim(gender)
SELECT DISTINCT gender
FROM affairs.sex;

SELECT * FROM gender_dim;
SELECT * FROM affairs.sex;

--2.rship details dim
DROP TABLE IF EXISTS rship_details_dim;

CREATE TABLE rship_details_dim (
    rship_details_dim_id SERIAL PRIMARY KEY,
	respondent_id NUMERIC,
	relationship_type_id NUMERIC,
	rating_id NUMERIC,
    years_married NUMERIC,
    children INT,
    num_affairs NUMERIC
);

INSERT INTO rship_details_dim(respondent_id, relationship_type_id, rating_id, years_married, children, num_affairs)
SELECT DISTINCT respondent_id, relationship_type_id, rating_id, yearsmarried, children, affairs_num
FROM affairs.relationship_details;

SELECT * FROM affairs.relationship_details;
SELECT * FROM rship_details_dim;

--3. respondent dim
DROP TABLE IF EXISTS respondent_dim;

CREATE TABLE respondent_dim (
    respondent_dim_id SERIAL PRIMARY KEY,
	respondent_id NUMERIC,
	gender_id NUMERIC,
	occupation_id NUMERIC, 
	education_id NUMERIC, 
	religiousness_id NUMERIC,
    age INT
);

INSERT INTO respondent_dim(respondent_id, gender_id, occupation_id, education_id, religiousness_id, age)
SELECT DISTINCT respondent_id,gender_id, occupation_id, education_id, religiousness_id, age
FROM affairs.respondent;

SELECT * FROM respondent_dim;
SELECT * FROM affairs.respondent;


--4. Rship_Type Dim
DROP TABLE IF EXISTS rship_type_dim;

CREATE TABLE rship_type_dim (
    rship_type_dim_id SERIAL PRIMARY KEY,
    relationship_type_id NUMERIC,
	relationship_type TEXT,
    survey_start_date DATE,
    survey_end_date DATE
);

INSERT INTO rship_type_dim(relationship_type_id, relationship_type, survey_start_date)
SELECT DISTINCT relationship_type_id, relationship_type, CURRENT_DATE
FROM affairs.relationship_type;

SELECT * FROM affairs.relationship_type;
SELECT * FROM rship_type_dim;

--5. survey date dim
DROP TABLE IF EXIST survey_date_dim;
CREATE TABLE survey_date_dim (
    survey_date_dim_id SERIAL PRIMARY KEY,
    Survey_Name TEXT,
    Year INT,
    Month INT,
    Day INT,
    Quarter INT
);

INSERT INTO survey_date_dim(Survey_Name, Year, Month, Day, Quarter)
SELECT DISTINCT
    'Affairs Survey' AS Survey_Name,
    EXTRACT(YEAR FROM CURRENT_DATE),
    EXTRACT(MONTH FROM CURRENT_DATE),
    EXTRACT(DAY FROM CURRENT_DATE),
    EXTRACT(QUARTER FROM CURRENT_DATE);

SELECT * FROM survey_date_dim;