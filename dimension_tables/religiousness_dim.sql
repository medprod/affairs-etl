SET search_path = affairs_olap;

--1.religiousness dim
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

SELECT * FROM religiousness_dim;