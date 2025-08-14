--Stored Procedure to call for creating all dimensions.
CREATE OR REPLACE PROCEDURE affairs_olap_dimensions()
LANGUAGE plpgsql
AS $$
BEGIN
    SET search_path = affairs_olap;

    -- 1. Gender DIM
    DROP TABLE IF EXISTS gender_dim;
    CREATE TABLE gender_dim(
        gender_dim_id SERIAL PRIMARY KEY,
        gender TEXT
    );
    INSERT INTO gender_dim(gender)
    SELECT DISTINCT gender
    FROM affairs.sex;

    -- 2. Rship Details DIM
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

    -- 3. Respondent DIM
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
    SELECT DISTINCT respondent_id, gender_id, occupation_id, education_id, religiousness_id, age
    FROM affairs.respondent;

    -- 4. Rship Type DIM
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

    -- 5. Survey Date DIM
    DROP TABLE IF EXISTS survey_date_dim;
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

    -- 6. Education DIM
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

    -- 7. Occupation DIM
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

    -- 8. Rating DIM
    DROP TABLE IF EXISTS rating_dim;
    CREATE TABLE rating_dim (
        rating_dim_id NUMERIC PRIMARY KEY,
        rating_2025 NUMERIC,
        rating_2024 NUMERIC,
        rating_2023 NUMERIC,
        rating_desc TEXT
    );
    INSERT INTO rating_dim (rating_dim_id, rating_2025, rating_desc)
    SELECT DISTINCT rating_id, rating_id, rating_desc
    FROM affairs.rating;

    -- 9. Religiousness DIM
    DROP TABLE IF EXISTS religiousness_dim;
    CREATE TABLE religiousness_dim (
        religiousness_dim_id SERIAL PRIMARY KEY,
        religiousness_id NUMERIC,
        religiousness_desc TEXT,
        survey_start_date DATE,
        survey_end_date DATE
    );
    INSERT INTO religiousness_dim(religiousness_id, religiousness_desc, survey_start_date)
    SELECT DISTINCT religiousness_id, religiousness_desc, CURRENT_DATE
    FROM affairs.religiousness;

    RAISE NOTICE 'All dimension tables created and populated successfully.';
END;
$$;

CALL affairs_olap_dimensions();