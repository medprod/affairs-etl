SET search_path = affairs_olap;

--1. Percentage of men vs women who cheated
SELECT * FROM demographics_fact;

SELECT g.gender,
SUM((CASE WHEN df.cheated_flag THEN 1 ELSE 0 END)::NUMERIC) AS each_gender,
COUNT(*) AS total
FROM demographics_fact df
JOIN gender_dim g ON df.gender_dim_id = g.gender_dim_id
GROUP BY g.gender;

SELECT g.gender,
ROUND((SUM(CASE WHEN df.cheated_flag THEN 1 ELSE 0 END)::NUMERIC/COUNT(*)) * 100, 2) AS percent_cheated
FROM demographics_fact df
JOIN gender_dim g ON df.gender_dim_id = g.gender_dim_id
GROUP BY g.gender;


--2. Average Number of Affairs: Same-Sex vs Opposite-Sex
SELECT * FROM demographics_fact;
SELECT * FROM rship_type_dim;

SELECT rt.relationship_type,
COUNT(*) AS number_of_couples,
SUM(df.affair_count) AS total_affairs,
ROUND(AVG(df.affair_count),2) AS avg_num_of_affairs
FROM demographics_fact df
JOIN rship_type_dim rt ON df.rship_type_dim_id = rt.rship_type_dim_id
GROUP BY rt.relationship_type

