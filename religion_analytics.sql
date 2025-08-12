SET search_path = affairs_olap;

--1. Does higher religiousness correlate to fewer affairs?
SELECT * FROM religiousness_dim;
SELECT * FROM religion_impact_fact;

SELECT rel.religiousness_desc,
SUM(CASE WHEN ri.cheated_flag THEN 1 ELSE 0 END) AS num_cheated,
COUNT(*) AS total_respondents,
 ROUND(100.0 * SUM(CASE WHEN ri.cheated_flag THEN 1 ELSE 0 END) / COUNT(*), 2) AS percent_cheated
FROM religion_impact_fact ri
JOIN religiousness_dim rel ON ri.religiousness_dim_id = rel.religiousness_dim_id
GROUP BY rel.religiousness_desc
ORDER BY percent_cheated DESC;

--2. Correlation between religiousness and marriage satisfaction
SELECT * FROM religiousness_dim;
SELECT * FROM rating_dim;
SELECT * FROM religion_impact_fact;

SELECT rel.religiousness_desc,
AVG(rat.rating_2025) AS avg_marriage_rating,
AVG(rimp.affair_count) AS avg_num_affairs,
COUNT(*) AS respondent_count
FROM religion_impact_fact rimp
JOIN religiousness_dim rel ON rimp.religiousness_dim_id = rel.religiousness_dim_id
JOIN rating_dim rat ON rimp.rating_dim_id = rat.rating_dim_id
GROUP BY rel.religiousness_desc
ORDER BY rel.religiousness_desc;

