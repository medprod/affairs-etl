SET search_path = affairs_olap;


--1. Education DIM
DROP TABLE IF EXISTS education_dim;

CREATE TABLE education_dim(
    education_dim_id SERIAL PRIMARY KEY,
	education_id NUMERIC,
    education_level_2025 TEXT,
    education_level_2024 TEXT,
	education_level_2023 TEXT
);

INSERT INTO education_dim(education_id, education_level_2025)
SELECT DISTINCT education_id, education
FROM affairs.education;

SELECT * FROM education_dim;


--2.occupation dim
DROP TABLE IF EXISTS occupation_dim;
CREATE TABLE occupation_dim(
    occupation_dim_id SERIAL PRIMARY KEY,
	occupation_id NUMERIC,
    occupation_2025 TEXT,
    occupation_2024 TEXT,
    occupation_2023 TEXT
);

INSERT INTO occupation_dim(occupation_id, occupation_2025)
SELECT DISTINCT occupation_id, occupation
FROM affairs.occupation;

SELECT * FROM occupation_dim;