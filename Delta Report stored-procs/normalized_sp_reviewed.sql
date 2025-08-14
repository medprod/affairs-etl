--using merge instead of dropping tables each time
CREATE OR REPLACE PROCEDURE normalize_gender_rship()
LANGUAGE plpgsql
AS $$
BEGIN
    SET search_path = affairs;

    -- A. Gender lookup
    CREATE TABLE IF NOT EXISTS sex (
        gender_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        gender text UNIQUE NOT NULL
    );

    MERGE INTO sex AS t
    USING (SELECT DISTINCT gender FROM gender_rship) AS s
    ON t.gender = s.gender
    WHEN NOT MATCHED THEN
        INSERT (gender) VALUES (s.gender);

    -- B. Education lookup
    CREATE TABLE IF NOT EXISTS education (
        education_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        education text UNIQUE NOT NULL
    );

    MERGE INTO education AS t
    USING (SELECT DISTINCT education FROM gender_rship) AS s
    ON t.education = s.education
    WHEN NOT MATCHED THEN
        INSERT (education) VALUES (s.education);

    -- C. Occupation lookup
    CREATE TABLE IF NOT EXISTS occupation (
        occupation_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        occupation text UNIQUE NOT NULL
    );

    MERGE INTO occupation AS t
    USING (SELECT DISTINCT occupation FROM gender_rship) AS s
    ON t.occupation = s.occupation
    WHEN NOT MATCHED THEN
        INSERT (occupation) VALUES (s.occupation);

    -- D. Rating lookup
    CREATE TABLE IF NOT EXISTS rating (
        rating_id integer PRIMARY KEY,
        rating_desc text
    );

    MERGE INTO rating AS t
    USING (
        SELECT DISTINCT rating FROM gender_rship
    ) AS s
    ON t.rating_id = s.rating
    WHEN NOT MATCHED THEN
        INSERT (rating_id, rating_desc) VALUES (
            s.rating,
            CASE s.rating
                WHEN 1 THEN 'Very Dissatisfied'
                WHEN 2 THEN 'Dissatisfied'
                WHEN 3 THEN 'Neutral'
                WHEN 4 THEN 'Satisfied'
                WHEN 5 THEN 'Very Satisfied'
                ELSE 'Unknown'
            END
        );

    -- E. Religiousness lookup
    CREATE TABLE IF NOT EXISTS religiousness (
        religiousness_id integer PRIMARY KEY,
        religiousness_desc text
    );

    MERGE INTO religiousness AS t
    USING (
        SELECT DISTINCT religiousness FROM gender_rship
    ) AS s
    ON t.religiousness_id = s.religiousness
    WHEN NOT MATCHED THEN
        INSERT (religiousness_id, religiousness_desc) VALUES (
            s.religiousness,
            CASE s.religiousness
                WHEN 1 THEN 'Not Religious'
                WHEN 2 THEN 'Slightly Religious'
                WHEN 3 THEN 'Moderately Religious'
                WHEN 4 THEN 'Religious'
                WHEN 5 THEN 'Very Religious'
                ELSE 'Unknown'
            END
        );

    -- F. Relationship type lookup
    CREATE TABLE IF NOT EXISTS relationship_type (
        relationship_type_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        relationship_type text UNIQUE NOT NULL
    );

    MERGE INTO relationship_type AS t
    USING (SELECT DISTINCT relationship_type FROM gender_rship) AS s
    ON t.relationship_type = s.relationship_type
    WHEN NOT MATCHED THEN
        INSERT (relationship_type) VALUES (s.relationship_type);

    -- G. Respondent table
    CREATE TABLE IF NOT EXISTS respondent (
        respondent_id integer PRIMARY KEY,
        gender_id integer REFERENCES sex(gender_id),
        age integer,
        education_id integer REFERENCES education(education_id),
        occupation_id integer REFERENCES occupation(occupation_id),
        religiousness_id integer REFERENCES religiousness(religiousness_id)
    );

    MERGE INTO respondent AS t
    USING (
        SELECT 
            g.respondent_id,
            s.gender_id,
            g.age,
            e.education_id,
            o.occupation_id,
            reg.religiousness_id
        FROM gender_rship g
        JOIN sex s ON g.gender = s.gender
        JOIN education e ON g.education = e.education
        JOIN occupation o ON g.occupation = o.occupation
        JOIN religiousness reg ON g.religiousness = reg.religiousness_id
    ) AS s
    ON t.respondent_id = s.respondent_id
    WHEN NOT MATCHED THEN
        INSERT (respondent_id, gender_id, age, education_id, occupation_id, religiousness_id)
        VALUES (s.respondent_id, s.gender_id, s.age, s.education_id, s.occupation_id, s.religiousness_id);

    -- H. Relationship details table
    CREATE TABLE IF NOT EXISTS relationship_details (
        respondent_id integer REFERENCES respondent(respondent_id),
        relationship_type_id integer REFERENCES relationship_type(relationship_type_id),
        yearsmarried integer,
        children integer,
        rating_id integer REFERENCES rating(rating_id),
        affairs_num integer,
        narrative_text text,
        PRIMARY KEY (respondent_id, relationship_type_id)
    );

    MERGE INTO relationship_details AS t
    USING (
        SELECT 
            g.respondent_id,
            rt.relationship_type_id,
            g.yearsmarried,
            g.children,
            r.rating_id,
            g.affairs AS affairs_num,
            g.narrative_text
        FROM gender_rship g
        JOIN rating r ON g.rating = r.rating_id
        JOIN relationship_type rt ON g.relationship_type = rt.relationship_type
    ) AS s
    ON t.respondent_id = s.respondent_id
       AND t.relationship_type_id = s.relationship_type_id
    WHEN NOT MATCHED THEN
        INSERT (respondent_id, relationship_type_id, yearsmarried, children, rating_id, affairs_num, narrative_text)
        VALUES (s.respondent_id, s.relationship_type_id, s.yearsmarried, s.children, s.rating_id, s.affairs_num, s.narrative_text);

END;
$$;

CALL normalize_gender_rship();


--testing
SELECT * FROM sex;
SELECT * FROM education;
SELECT * FROM occupation;

SELECT * FROM gender_rship ORDER BY respondent_id;


INSERT INTO gender_rship (
    respondent_id, affairs, gender, age, yearsmarried, children, religiousness, rating, relationship_type, education, occupation, narrative_text
)
VALUES
  (8888, 5, 'other', 40, 4, 4, 5, 3, 'Opposite-Sex Marriage', 'Bachelor', 'Fashion',  'No narrative provided.'),
  (4444, 8, 'bisexual', 45, 6, 4, 4, 4, 'Opposite-Sex Marriage', 'Associates', 'Unemployed',  'No narrative provided.');

INSERT INTO gender_rship (
    respondent_id, affairs, gender, age, yearsmarried, children, religiousness, rating, relationship_type, education, occupation, narrative_text
)
VALUES
  (8888, 5, 'other', 40, 4, 4, 5, 3, 'Opposite-Sex Marriage', 'Bachelor', 'Unemployed',  'No narrative provided.'),
  (4444, 8, 'bisexual', 45, 6, 4, 4, 4, 'Opposite-Sex Marriage', 'Masters', 'Unemployed',  'No narrative provided.');


DELETE FROM gender_rship WHERE respondent_id = 4444;

DELETE FROM gender_rship WHERE respondent_id = 8888;

DROP TABLE sex