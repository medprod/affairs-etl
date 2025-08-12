SET search_path = affairs;

SELECT * FROM gender_rship;
--3. filling null values up with some conditions I designed.

--A. CHILDREN column NULL values filled based on years married
UPDATE gender_rship
SET children = 
CASE WHEN yearsmarried >= 15 THEN 4
WHEN yearsmarried >= 10 THEN 3
WHEN yearsmarried >= 5 THEN 2
WHEN yearsmarried >= 1 THEN 1
ELSE 0 END 
WHERE children IS NULL;

--B. fill relationsip_type RANDOMLY (asked AI how to do this)
UPDATE gender_rship
SET relationship_type =
CASE WHEN RANDOM() > 0.5 THEN 'Opposite-Sex Marriage'
ELSE 'Same-Sex Marriage'
END 
WHERE relationship_type IS NULL;

--C. filling education based on age
UPDATE gender_rship
SET education = 
CASE WHEN age <= 18 THEN 'High School'
WHEN age <= 25 THEN 'Bachelor'
WHEN age <= 35 THEN 'Master'
ELSE 'PhD'
END WHERE education IS NULL;


--D. fill occupation column RANDOMLY (similar to relationship_type)
UPDATE gender_rship
SET occupation = CASE FLOOR(RANDOM() * 5)
    WHEN 0 THEN 'Unemployed'
    WHEN 1 THEN 'Education'
    WHEN 2 THEN 'Finance'
    WHEN 3 THEN 'Tech'
    ELSE 'Healthcare'
END
WHERE occupation IS NULL;


--E. filling narrative_test as None provided
UPDATE gender_rship
SET narrative_text = 'No narrative provided'
WHERE narrative_text IS NULL OR narrative_text = 'NaN';