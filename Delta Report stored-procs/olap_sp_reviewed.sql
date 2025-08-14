--Stored Procedure to call for creating all dimensions.
CREATE OR REPLACE PROCEDURE affairs_olap_dimensions()
LANGUAGE plpgsql
AS $$
BEGIN
    SET search_path = affairs_olap;

    -- 1. Rship Details DIM
    CREATE TABLE IF NOT EXISTS rship_details_dim (
        rship_details_dim_id SERIAL PRIMARY KEY,
        respondent_id NUMERIC,
        relationship_type_id NUMERIC,
        rating_id NUMERIC,
        years_married NUMERIC,
        children INT,
        num_affairs NUMERIC
    );
	MERGE INTO rship_details_dim tgt
	USING (
	    SELECT DISTINCT  respondent_id, relationship_type_id, rating_id, yearsmarried AS years_married, children, affairs_num AS num_affairs
	    FROM affairs.relationship_details
	) src
	ON tgt.respondent_id = src.respondent_id
	WHEN MATCHED THEN
	    UPDATE SET relationship_type_id = src.relationship_type_id,
	        rating_id = src.rating_id,
	        years_married = src.years_married,
	        children = src.children,
	        num_affairs = src.num_affairs
	WHEN NOT MATCHED THEN
	    INSERT (respondent_id, relationship_type_id, rating_id, years_married, children, num_affairs)
	    VALUES (src.respondent_id, src.relationship_type_id, src.rating_id, src.years_married, src.children, src.num_affairs);

    -- 2. Respondent DIM
    CREATE TABLE IF NOT EXISTS respondent_dim (
        respondent_dim_id SERIAL PRIMARY KEY,
        respondent_id NUMERIC,
        gender TEXT,
		religiousness_desc TEXT,
        occupation_id NUMERIC, 
        education_id NUMERIC, 
        age INT
    );
	MERGE INTO respondent_dim tgt
	USING (
	    SELECT DISTINCT r.respondent_id, s.gender, rel.religiousness_desc, r.occupation_id, r.education_id, r.age
	    FROM affairs.respondent r
	    LEFT JOIN affairs.sex s ON r.gender_id = s.gender_id
		LEFT JOIN affairs.religiousness rel ON r.religiousness_id = rel.religiousness_id
	) src
	ON tgt.respondent_id = src.respondent_id
	WHEN MATCHED THEN
	    UPDATE SET gender = src.gender,
	        religiousness_desc = src.religiousness_desc,
	        occupation_id = src.occupation_id,
	        education_id = src.education_id,
	        age = src.age
	WHEN NOT MATCHED THEN
	    INSERT (respondent_id, gender, religiousness_desc, occupation_id, education_id, age)
	    VALUES (src.respondent_id, src.gender, src.religiousness_desc, src.occupation_id, src.education_id, src.age);

    -- 3. Rship Type DIM
    CREATE TABLE IF NOT EXISTS rship_type_dim (
        rship_type_dim_id SERIAL PRIMARY KEY,
        relationship_type_id NUMERIC,
        relationship_type TEXT
    );
	MERGE INTO rship_type_dim tgt
	USING (
		SELECT DISTINCT relationship_type_id, relationship_type
		FROM affairs.relationship_type
	) src
	ON tgt.relationship_type_id = src.relationship_type_id
	WHEN MATCHED THEN UPDATE SET relationship_type = src.relationship_type
	WHEN NOT MATCHED THEN
		INSERT (relationship_type_id, relationship_type)
		VALUES (src.relationship_type_id, src.relationship_type);

    -- 4. Survey Date DIM
    CREATE TABLE IF NOT EXISTS survey_date_dim (
        survey_date_dim_id SERIAL PRIMARY KEY,
        Survey_Name TEXT,
        Year INT,
        Month INT,
        Day INT,
        Quarter INT
    );
	MERGE INTO survey_date_dim tgt
	USING (
		SELECT DISTINCT
		'Affairs Survey' AS survey_name,
		EXTRACT(YEAR FROM CURRENT_DATE) AS year,
		EXTRACT(MONTH FROM CURRENT_DATE) AS month,
		EXTRACT(DAY FROM CURRENT_DATE) AS day,
		EXTRACT(QUARTER FROM CURRENT_DATE) AS quarter
	) src
	ON tgt.survey_name = src.survey_name
	WHEN MATCHED THEN
		UPDATE SET year = src.year, month = src.month, day = src.day, quarter = src.quarter
	WHEN NOT MATCHED THEN
		INSERT (survey_name, year, month, day, quarter)
		VALUES (src.survey_name, src.year, src.month, src.day, src.quarter);

    -- 5. Education DIM
    CREATE TABLE IF NOT EXISTS education_dim(
        education_dim_id SERIAL PRIMARY KEY,
        education_id NUMERIC,
        education_level_2025 TEXT,
        education_level_2024 TEXT,
        education_level_2023 TEXT
    );
	MERGE INTO education_dim tgt
	USING (
		SELECT DISTINCT education_id, education AS education_level_2025
		FROM affairs.education
	) src
	ON tgt.education_id = src.education_id
	WHEN MATCHED THEN
		UPDATE SET education_level_2025 = src.education_level_2025
	WHEN NOT MATCHED THEN
		INSERT (education_id, education_level_2025)
		VALUES (src.education_id, src.education_level_2025);

    -- 6. Occupation DIM
    DROP TABLE IF EXISTS occupation_dim;
    CREATE TABLE occupation_dim(
        occupation_dim_id SERIAL PRIMARY KEY,
        occupation_id NUMERIC,
        occupation_2025 TEXT,
        occupation_2024 TEXT,
        occupation_2023 TEXT
    );
	MERGE INTO occupation_dim tgt
	USING (
		SELECT DISTINCT occupation_id, occupation AS occupation_2025
		FROM affairs.occupation
	) src
	ON tgt.occupation_id = src.occupation_id
	WHEN MATCHED THEN
		UPDATE SET occupation_2025 = src.occupation_2025
	WHEN NOT MATCHED THEN
		INSERT (occupation_id, occupation_2025)
		VALUES (src.occupation_id, src.occupation_2025);

    --7. Rating DIM
    DROP TABLE IF EXISTS rating_dim;
    CREATE TABLE rating_dim (
        rating_dim_id NUMERIC PRIMARY KEY,
        rating_2025 NUMERIC,
        rating_2024 NUMERIC,
        rating_2023 NUMERIC,
        rating_desc TEXT
    );
	MERGE INTO rating_dim tgt
	USING (
		SELECT DISTINCT rating_id AS rating_dim_id, rating_id AS rating_2025, rating_desc
		FROM affairs.rating
	) src
	ON tgt.rating_dim_id = src.rating_dim_id
	WHEN MATCHED THEN
		UPDATE SET rating_2025 = src.rating_2025, rating_desc = src.rating_desc
	WHEN NOT MATCHED THEN
		INSERT (rating_dim_id, rating_2025, rating_desc)
		VALUES (src.rating_dim_id, src.rating_2025, src.rating_desc);

    RAISE NOTICE 'All dimension tables created and populated successfully.';
END;
$$;

CALL affairs_olap_dimensions();