SET search_path = affairs_olap;


--Does the number of years a couple is married correlate to the number of affairs/overall satisfaction?
SELECT * FROM marital_duration_fact;
SELECT * FROM rating_dim;
SELECT * FROM rship_details_dim;
SELECT * FROM respondent_dim;

SELECT mdf.years_married, 
ROUND(AVG(mdf.affair_count),2) AS avg_affairs,
ROUND(AVG(rat.rating_2025),2) AS avg_rating
FROM marital_duration_fact mdf
JOIN rating_dim rat ON mdf.rating_dim_id = rat.rating_dim_id
GROUP BY mdf.years_married
ORDER BY mdf.years_married

