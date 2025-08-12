SET search_path = affairs_olap;

--Martial_Duration_Fact

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
SELECT
res.respondent_dim_id,
rd.rship_details_dim_id,
rat.rating_dim_id,
s.survey_date_dim_id,
rd.years_married AS total_years_married,
rd.num_affairs AS affair_count,
CASE WHEN rd.num_affairs > 0 THEN TRUE ELSE FALSE END AS cheated_flag
FROM respondent_dim res
JOIN rship_details_dim rd ON res.respondent_id = rd.respondent_id
JOIN rating_dim rat ON rd.rating_id = rat.rating_2025
JOIN survey_date_dim s ON s.year = EXTRACT(YEAR FROM CURRENT_DATE) AND s.month = EXTRACT(MONTH FROM CURRENT_DATE)
AND s.day = EXTRACT(DAY FROM CURRENT_DATE);


SELECT * FROM marital_duration_fact;