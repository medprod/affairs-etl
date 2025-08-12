SET search_path = affairs_olap;


SELECT * FROM occupation_dim;
SELECT * FROM education_dim;
SELECT * FROM work_edu_fact;


--Are certain occupations more likely to cheat?
SELECT o.occupation_2025 AS occupation,
ROUND(AVG(we.affair_count),2) AS avg_affairs,
COUNT(*) AS respondent_count
FROM work_edu_fact we
JOIN occupation_dim o ON  we.occupation_dim_id = o.occupation_dim_id
GROUP BY o.occupation_2025
ORDER BY avg_affairs DESC;


--Are certain education-levels more likely to cheat?
SELECT e.education_level_2025 AS education_level,
ROUND(AVG(we.affair_count),2) AS avg_affairs,
COUNT(*) AS respondent_count
FROM work_edu_fact we
JOIN education_dim e ON  we.education_dim_id = e.education_dim_id
GROUP BY e.education_level_2025
ORDER BY avg_affairs DESC;