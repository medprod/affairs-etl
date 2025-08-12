CREATE OR REPLACE PROCEDURE affairs_olap_facts()
LANGUAGE plpgsql
AS $$
BEGIN
	SET search_path = affairs_olap;
    -- =========================
    -- Demographics Fact
    -- =========================
    DROP TABLE IF EXISTS demographics_fact;

    CREATE TABLE demographics_fact (
        demographics_fact_id SERIAL PRIMARY KEY,
        respondent_dim_id INT NOT NULL,
        gender_dim_id INT NOT NULL,
        rship_type_dim_id INT NOT NULL,
        rship_details_dim_id INT NOT NULL, 
        survey_date_dim_id INT NOT NULL,
        affair_count INT NOT NULL,
        cheated_flag BOOLEAN NOT NULL,
        FOREIGN KEY (respondent_dim_id) REFERENCES respondent_dim(respondent_dim_id),
        FOREIGN KEY (gender_dim_id) REFERENCES gender_dim(gender_dim_id),
        FOREIGN KEY (rship_type_dim_id) REFERENCES rship_type_dim(rship_type_dim_id),
        FOREIGN KEY (rship_details_dim_id) REFERENCES rship_details_dim(rship_details_dim_id),
        FOREIGN KEY (survey_date_dim_id) REFERENCES survey_date_dim(survey_date_dim_id)
    );

    INSERT INTO demographics_fact(
        respondent_dim_id,
        gender_dim_id,
        rship_type_dim_id,
        rship_details_dim_id,
        survey_date_dim_id,
        affair_count,
        cheated_flag
    )
    SELECT res.respondent_dim_id,
           g.gender_dim_id,
           rt.rship_type_dim_id,
           rd.rship_details_dim_id,
           s.survey_date_dim_id,
           rd.num_affairs AS affairs_count,
           CASE WHEN rd.num_affairs > 0 THEN TRUE ELSE FALSE END
    FROM respondent_dim res
    JOIN gender_dim g ON res.gender_id = g.gender_dim_id
    JOIN rship_details_dim rd ON res.respondent_id = rd.respondent_id
    JOIN rship_type_dim rt ON rd.relationship_type_id = rt.relationship_type_id
    JOIN survey_date_dim s 
         ON s.year = EXTRACT(YEAR FROM CURRENT_DATE) 
        AND s.month = EXTRACT(MONTH FROM CURRENT_DATE)
        AND s.day = EXTRACT(DAY FROM CURRENT_DATE);

    -- =========================
    -- Work_Edu Fact
    -- =========================
    DROP TABLE IF EXISTS work_edu_fact;

    CREATE TABLE work_edu_fact (
        work_edu_fact_id SERIAL PRIMARY KEY,
        respondent_dim_id INT NOT NULL,
        occupation_dim_id INT NOT NULL,
        education_dim_id INT NOT NULL,
        rship_details_dim_id INT NOT NULL,
        survey_date_dim_id INT NOT NULL,
        affair_count INT NOT NULL,
        cheated_flag BOOLEAN NOT NULL,
        FOREIGN KEY (respondent_dim_id) REFERENCES respondent_dim(respondent_dim_id),
        FOREIGN KEY (occupation_dim_id) REFERENCES occupation_dim(occupation_dim_id),
        FOREIGN KEY (education_dim_id) REFERENCES education_dim(education_dim_id),
        FOREIGN KEY (rship_details_dim_id) REFERENCES rship_details_dim(rship_details_dim_id),
        FOREIGN KEY (survey_date_dim_id) REFERENCES survey_date_dim(survey_date_dim_id)
    );

    INSERT INTO work_edu_fact (
        respondent_dim_id,
        occupation_dim_id,
        education_dim_id,
        rship_details_dim_id,
        survey_date_dim_id,
        affair_count,
        cheated_flag
    )
    SELECT res.respondent_dim_id,
           o.occupation_dim_id,
           e.education_dim_id,
           rd.rship_details_dim_id,
           s.survey_date_dim_id,
           rd.num_affairs,
           CASE WHEN rd.num_affairs > 0 THEN TRUE ELSE FALSE END
    FROM respondent_dim res
    JOIN occupation_dim o ON res.occupation_id = o.occupation_id
    JOIN education_dim e ON res.education_id = e.education_id
    JOIN rship_details_dim rd ON res.respondent_id = rd.respondent_id
    JOIN survey_date_dim s 
         ON s.year = EXTRACT(YEAR FROM CURRENT_DATE) 
        AND s.month = EXTRACT(MONTH FROM CURRENT_DATE)
        AND s.day = EXTRACT(DAY FROM CURRENT_DATE);

    -- =========================
    -- Marital_Duration Fact
    -- =========================
    DROP TABLE IF EXISTS marital_duration_fact;

    CREATE TABLE marital_duration_fact (
        marital_duration_fact_id SERIAL PRIMARY KEY,
        respondent_dim_id INT NOT NULL,
        rship_details_dim_id INT NOT NULL,
        rating_dim_id INT NOT NULL,
        survey_date_dim_id INT NOT NULL,
        years_married INT NOT NULL,
        affair_count INT NOT NULL,
        cheated_flag BOOLEAN NOT NULL,
        FOREIGN KEY (respondent_dim_id) REFERENCES respondent_dim(respondent_dim_id),
        FOREIGN KEY (rship_details_dim_id) REFERENCES rship_details_dim(rship_details_dim_id),
        FOREIGN KEY (rating_dim_id) REFERENCES rating_dim(rating_dim_id),
        FOREIGN KEY (survey_date_dim_id) REFERENCES survey_date_dim(survey_date_dim_id)
    );

    INSERT INTO marital_duration_fact (
        respondent_dim_id,
        rship_details_dim_id,
        rating_dim_id,
        survey_date_dim_id,
        years_married,
        affair_count,
        cheated_flag
    )
    SELECT res.respondent_dim_id,
           rd.rship_details_dim_id,
           rat.rating_dim_id,
           s.survey_date_dim_id,
           rd.years_married,
           rd.num_affairs,
           CASE WHEN rd.num_affairs > 0 THEN TRUE ELSE FALSE END
    FROM respondent_dim res
    JOIN rship_details_dim rd ON res.respondent_id = rd.respondent_id
    JOIN rating_dim rat ON rd.rating_id = rat.rating_dim_id
    JOIN survey_date_dim s 
         ON s.year = EXTRACT(YEAR FROM CURRENT_DATE) 
        AND s.month = EXTRACT(MONTH FROM CURRENT_DATE)
        AND s.day = EXTRACT(DAY FROM CURRENT_DATE);

    -- =========================
    -- Religion Impact Fact
    -- =========================
    DROP TABLE IF EXISTS religion_impact_fact;

    CREATE TABLE religion_impact_fact (
        religion_impact_fact_id SERIAL PRIMARY KEY,
        respondent_dim_id INT NOT NULL,
        religiousness_dim_id INT NOT NULL,
        rship_details_dim_id INT NOT NULL,
        survey_date_dim_id INT NOT NULL,
        affair_count INT NOT NULL,
        cheated_flag BOOLEAN NOT NULL,
        FOREIGN KEY (respondent_dim_id) REFERENCES respondent_dim(respondent_dim_id),
        FOREIGN KEY (religiousness_dim_id) REFERENCES religiousness_dim(religiousness_dim_id),
        FOREIGN KEY (rship_details_dim_id) REFERENCES rship_details_dim(rship_details_dim_id),
        FOREIGN KEY (survey_date_dim_id) REFERENCES survey_date_dim(survey_date_dim_id)
    );

    INSERT INTO religion_impact_fact (
        respondent_dim_id,
        religiousness_dim_id,
        rship_details_dim_id,
        survey_date_dim_id,
        affair_count,
        cheated_flag
    )
    SELECT res.respondent_dim_id,
           rel.religiousness_dim_id,
           rd.rship_details_dim_id,
           s.survey_date_dim_id,
           rd.num_affairs,
           CASE WHEN rd.num_affairs > 0 THEN TRUE ELSE FALSE END
    FROM respondent_dim res
    JOIN religiousness_dim rel ON res.religiousness_id = rel.religiousness_id
    JOIN rship_details_dim rd ON res.respondent_id = rd.respondent_id
    JOIN survey_date_dim s 
         ON s.year = EXTRACT(YEAR FROM CURRENT_DATE) 
        AND s.month = EXTRACT(MONTH FROM CURRENT_DATE)
        AND s.day = EXTRACT(DAY FROM CURRENT_DATE);

END;
$$;

CALL affairs_olap_facts();
