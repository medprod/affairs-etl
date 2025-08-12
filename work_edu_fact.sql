SET search_path = affairs_olap;

--Work_Edu_Fact
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
rd.num_affairs AS affair_count,
CASE WHEN rd.num_affairs > 0 THEN TRUE ELSE FALSE END AS cheated_flag
FROM respondent_dim res
JOIN occupation_dim o ON res.occupation_id = o.occupation_id
JOIN education_dim e ON res.education_id = e.education_id
JOIN rship_details_dim rd ON res.respondent_id = rd.respondent_id
JOIN survey_date_dim s ON s.year = EXTRACT(YEAR FROM CURRENT_DATE) AND s.month = EXTRACT(MONTH FROM CURRENT_DATE) AND s.day = EXTRACT(DAY FROM CURRENT_DATE);

SELECT * FROM respondent_dim;
SELECT * FROM occupation_dim;
SELECT * FROM education_dim;
SELECT * FROM work_edu_fact;