SET search_path = affairs_olap;



--1. rating dim
DROP TABLE IF EXISTS rating_dim;
CREATE TABLE rating_dim (
    rating_dim_id SERIAL PRIMARY KEY,
    rating_2025 NUMERIC,
    rating_2024 NUMERIC,
    rating_2023 NUMERIC,
    rating_desc TEXT
);

INSERT INTO rating_dim (rating_2025, rating_desc)
SELECT DISTINCT rating_id, rating_desc
FROM affairs.rating;

SELECT * FROM rating_dim;