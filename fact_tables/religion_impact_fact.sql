SET search_path = affairs_olap;

--Religion Impact Fact
DROP TABLE IF EXISTS religion_impact_fact;

CREATE TABLE religion_impact_fact (
    religion_impact_fact_id SERIAL PRIMARY KEY,
    respondent_dim_id INT NOT NULL,
    religiousness_dim_id INT NOT NULL,
    rship_details_dim_id INT NOT NULL,
	rating_dim_id INT NOT NULL,
    survey_date_dim_id INT NOT NULL,
    affair_count INT NOT NULL,
    cheated_flag BOOLEAN NOT NULL,
    FOREIGN KEY (respondent_dim_id) REFERENCES respondent_dim(respondent_dim_id),
    FOREIGN KEY (religiousness_dim_id) REFERENCES religiousness_dim(religiousness_dim_id),
    FOREIGN KEY (rship_details_dim_id) REFERENCES rship_details_dim(rship_details_dim_id),
	FOREIGN KEY (rating_dim_id) REFERENCES rating_dim(rating_dim_id),
    FOREIGN KEY (survey_date_dim_id) REFERENCES survey_date_dim(survey_date_dim_id)
);

INSERT INTO religion_impact_fact (
    respondent_dim_id,
    religiousness_dim_id,
    rship_details_dim_id,
	rating_dim_id,
    survey_date_dim_id,
    affair_count,
    cheated_flag
)
SELECT res.respondent_dim_id,
rel.religiousness_dim_id,
rd.rship_details_dim_id,
rat.rating_dim_id,
s.survey_date_dim_id,
rd.num_affairs AS affair_count,
CASE WHEN rd.num_affairs > 0 THEN TRUE ELSE FALSE END AS cheated_flag
FROM respondent_dim res
JOIN religiousness_dim rel ON res.religiousness_id = rel.religiousness_id
JOIN rship_details_dim rd ON res.respondent_id = rd.respondent_id
JOIN rating_dim rat ON rd.rating_id = rat.rating_2025
JOIN survey_date_dim s ON s.survey_date_dim_id = 1;


SELECT * FROM survey_date_dim;
SELECT * FROM religion_impact_fact;
SELECT * FROM rating_dim;
SELECT * FROM rship_details_dim
