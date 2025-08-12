--Demographics Fact
SET search_path = affairs_olap;

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
CASE WHEN rd.num_affairs > 0 THEN TRUE ELSE FALSE END AS cheated_flag
FROM respondent_dim res
JOIN gender_dim g ON res.gender_id = g.gender_dim_id
JOIN rship_details_dim rd ON res.respondent_id = rd.respondent_id
JOIN rship_type_dim rt ON rd.relationship_type_id = rt.relationship_type_id
JOIN survey_date_dim s on s.year = EXTRACT(YEAR FROM CURRENT_DATE) AND s.month = EXTRACT(MONTH FROM CURRENT_DATE)
AND s.day = EXTRACT(DAY FROM CURRENT_DATE);

SELECT * FROM demographics_fact;
SELECT * FROM respondent_dim;
SELECT * FROM gender_dim;
SELECT * FROM rship_type_dim;
SELECT * FROM rship_details_dim;

