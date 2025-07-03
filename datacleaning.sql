-- remove as no data
SELECT *
FROM perfumes
WHERE "Main Accords" = '[]'
ORDER BY "Name";

DELETE FROM perfumes
WHERE "Main Accords" = '[]';

-- combine "th" and "is" into "this"
SELECT *
FROM perfumes
WHERE "Description" ILIKE '%th is%';

UPDATE perfumes
SET "Description" = regexp_replace("Description", 't[hH]\s+i[sS]', 'this', 'gi')
WHERE "Description" ~* 't[hH]\s+i[sS]';

-- combine "Ir is" to "Iris"
SELECT *
FROM perfumes
WHERE "Description" LIKE '%Ir is%';

UPDATE perfumes
SET "Description" = regexp_replace("Description", 'Ir\s*is', 'Iris', 'g')
WHERE "Description" ~ 'Ir\s*is';

-- combine "Ambergr" and "is" to "Ambregris"
SELECT *
FROM perfumes
WHERE "Description" LIKE '%Ambergr is%';

UPDATE perfumes
SET "Description" = regexp_replace("Description", 'Ambergr\s*is', 'Ambergris', 'g')
WHERE "Description" ~ 'Ambergr\s*is';

-- combine "Lou" and "is" to "Louis"
SELECT *
FROM perfumes
WHERE "Description" LIKE '%Lou is%';

UPDATE perfumes
SET "Description" = regexp_replace("Description", 'Lou\s*is', 'Louis', 'g')
WHERE "Description" ~ 'Lou\s*is';

-- no space after "."
SELECT *
FROM perfumes
WHERE "Description" ~ '\.[^\s]';

UPDATE perfumes
SET "Description" = regexp_replace("Description", '\.([^\s])', '. \1', 'g')
WHERE "Description" ~ '\.[^\s]';

-- no space after "?"
SELECT *
FROM perfumes
WHERE "Description" ~ '\?[^\s]';

UPDATE perfumes
SET "Description" = regexp_replace("Description", '\?([^\s])', '? \1', 'g')
WHERE "Description" ~ '\?[^\s]';

-- "? ?" fix to "??"
SELECT *
FROM perfumes
WHERE "Description" LIKE '%? ?%';

UPDATE perfumes
SET "Description" = replace("Description", '? ?', '??')
WHERE "Description" LIKE '%? ?%';

-- "So. .. ?" fix to "So...?"
SELECT *
FROM perfumes
WHERE "Description" LIKE '%So. .. ?%';

UPDATE perfumes
SET "Description" = replace("Description", 'So. .. ?', 'So...?')
WHERE "Description" LIKE '%So. .. ?%';

-- "was" stuck with the word before
SELECT *
FROM perfumes
WHERE "Description" ~ '[a-zA-Z]was';

UPDATE perfumes
SET "Description" = regexp_replace("Description", '([a-zA-Z])was', '\1 was', 'g')
WHERE "Description" ~ '[a-zA-Z]was';

-- "was" stuck with the number before
SELECT *
FROM perfumes
WHERE "Description" ~ '[0-9]was';

UPDATE perfumes
SET "Description" = regexp_replace("Description", '([0-9])was', '\1 was', 'g')
WHERE "Description" ~ '[0-9]was';

-- lower case middle notes and base notes fix to upper case
SELECT *
FROM perfumes
WHERE "Description" LIKE '%middle notes%'
   OR "Description" LIKE '%base notes%';

UPDATE perfumes
SET "Description" = regexp_replace("Description", 'middle notes', 'Middle notes', 'gi')
WHERE "Description" ILIKE '%middle notes%';

UPDATE perfumes
SET "Description" = regexp_replace("Description", 'base notes', 'Base notes', 'gi')
WHERE "Description" ILIKE '%base notes%';

-- lower case middle note and base note fix to upper case
SELECT *
FROM perfumes
WHERE "Description" LIKE '%middle note%'
   OR "Description" LIKE '%base note%';

UPDATE perfumes
SET "Description" = regexp_replace("Description", 'middle note', 'Middle note', 'gi')
WHERE "Description" ILIKE '%middle note%';

UPDATE perfumes
SET "Description" = regexp_replace("Description", 'base note', 'Base note', 'gi')
WHERE "Description" ILIKE '%base note%';

-- lower case letter after "."
SELECT *
FROM perfumes
WHERE "Description" ~ '\.[ ]*[a-z]';

UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?a', '. A', 'g') WHERE "Description" ~ '\. ?a';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?b', '. B', 'g') WHERE "Description" ~ '\. ?b';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?c', '. C', 'g') WHERE "Description" ~ '\. ?c';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?d', '. D', 'g') WHERE "Description" ~ '\. ?d';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?e', '. E', 'g') WHERE "Description" ~ '\. ?e';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?f', '. F', 'g') WHERE "Description" ~ '\. ?f';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?g', '. G', 'g') WHERE "Description" ~ '\. ?g';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?h', '. H', 'g') WHERE "Description" ~ '\. ?h';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?i', '. I', 'g') WHERE "Description" ~ '\. ?i';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?j', '. J', 'g') WHERE "Description" ~ '\. ?j';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?k', '. K', 'g') WHERE "Description" ~ '\. ?k';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?l', '. L', 'g') WHERE "Description" ~ '\. ?l';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?m', '. M', 'g') WHERE "Description" ~ '\. ?m';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?n', '. N', 'g') WHERE "Description" ~ '\. ?n';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?o', '. O', 'g') WHERE "Description" ~ '\. ?o';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?p', '. P', 'g') WHERE "Description" ~ '\. ?p';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?q', '. Q', 'g') WHERE "Description" ~ '\. ?q';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?r', '. R', 'g') WHERE "Description" ~ '\. ?r';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?s', '. S', 'g') WHERE "Description" ~ '\. ?s';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?t', '. T', 'g') WHERE "Description" ~ '\. ?t';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?u', '. U', 'g') WHERE "Description" ~ '\. ?u';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?v', '. V', 'g') WHERE "Description" ~ '\. ?v';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?w', '. W', 'g') WHERE "Description" ~ '\. ?w';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?x', '. X', 'g') WHERE "Description" ~ '\. ?x';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?y', '. Y', 'g') WHERE "Description" ~ '\. ?y';
UPDATE perfumes SET "Description" = regexp_replace("Description", '\. ?z', '. Z', 'g') WHERE "Description" ~ '\. ?z';

-- add "Brand" column since it does not exist
ALTER TABLE perfumes
ADD COLUMN "Brand" TEXT;

SELECT 
  "Description",
  regexp_replace("Description", '^.*? by (.*?) is .*$', '\1') AS extracted_brand
FROM perfumes;

UPDATE perfumes
SET "Brand" = regexp_replace("Description", '^.*? by (.*?) is .*$', '\1');

SELECT *
FROM perfumes
WHERE "Brand" = "Description";

UPDATE perfumes
SET "Brand" = NULL;


-- copy broken results to new table for better analysis
CREATE TABLE broken_brands AS
SELECT *
FROM perfumes
WHERE false;

INSERT INTO broken_brands
SELECT *
FROM perfumes
WHERE "Brand" = "Description";

-- 1. capital "B" in "By"
SELECT *
FROM perfumes
WHERE "Description" ~* '^.*by .*?\.';

UPDATE perfumes
SET "Description" = regexp_replace("Description", 'By', 'by', 'gi')
WHERE "Description" ILIKE '%by%';

-- 2. "is" stuck with the number before ("777is")
SELECT *
FROM perfumes
WHERE "Description" ~ '\d+is';

UPDATE perfumes
SET "Description" = regexp_replace("Description", '([0-9])is', '\1 is', 'g')
WHERE "Description" ~ '[0-9]is';

-- 3. capital "W" in "Was"
SELECT *
FROM perfumes
WHERE "Description" LIKE '%Was launched%';

UPDATE perfumes
SET "Description" = regexp_replace("Description", 'Was', 'was', 'gi')
WHERE "Description" ILIKE '%was%';

--4. "is" and "was" stuck with chinese character before
SELECT *
FROM perfumes
WHERE "Description" ~ '[\u4e00-\u9fff]+is';

SELECT *
FROM perfumes
WHERE "Description" ~ '[\u4e00-\u9fff]+was';

UPDATE perfumes
SET "Description" = regexp_replace("Description", '([\u4e00-\u9fff]+)(is)', '\1 \2', 'g')
WHERE "Description" ~ '[\u4e00-\u9fff]+is';

UPDATE perfumes
SET "Description" = regexp_replace("Description", '([\u4e00-\u9fff]+)(was)', '\1 \2', 'g')
WHERE "Description" ~ '[\u4e00-\u9fff]+was';

--5. capital "I" in "Is"
SELECT *
FROM perfumes
WHERE "Description" LIKE '%Is a%';

UPDATE perfumes
SET "Description" = regexp_replace("Description", 'Is', 'is', 'gi')
WHERE "Description" ILIKE '%is%';

--6. space after "®"
UPDATE perfumes
SET "Description" = regexp_replace("Description", '(®)(\S)', '\1 \2', 'g')
WHERE "Description" ~ '®\S';

--7. "is" stuck with "Tooth" ("Toothis")
SELECT *
FROM perfumes
WHERE "Description" LIKE '%Toothis%';

UPDATE perfumes
SET "Description" = replace("Description", 'Toothis', 'Tooth is')
WHERE "Description" LIKE '%Toothis%';

--7. "is" stuck with "Mith" ("Mithis")
SELECT *
FROM perfumes
WHERE "Description" LIKE '%Mithis%';

UPDATE perfumes
SET "Description" = replace("Description", 'Mithis', 'Mith is')
WHERE "Description" LIKE '%Mithis%';

--7. "is" stuck with "Beyoncé" ("Beyoncéis")
SELECT *
FROM perfumes
WHERE "Description" LIKE '%Beyoncéis%';

UPDATE perfumes
SET "Description" = replace("Description", 'Beyoncéis', 'Beyoncé is')
WHERE "Description" LIKE '%Beyoncéis%';

--8. space after ")"
UPDATE perfumes
SET "Description" = regexp_replace("Description", '(\))(\S)', '\1 \2', 'g')
WHERE "Description" ~ '\)\S';

-- 9. Smithis, Chloéis, розаis, Joop!is, Sauzéis, Fathis, Bathis, Aladéis, DSQUARED²is, Samouraïis, Ikiryōis, Doréis

-- "for" stuck with number before (Name)
SELECT *
FROM perfumes
WHERE "Name" ~ '\d+for';

UPDATE perfumes
SET "Name" = regexp_replace("Name", '([0-9])for', '\1 for', 'g')
WHERE "Name" ~ '[0-9]for';

-- combine "Lou" and "is" to "Louis"
SELECT *
FROM perfumes
WHERE "Description" LIKE '%Ru by%';

UPDATE perfumes
SET "Description" = regexp_replace("Description", 'Ru\s*by', 'Ruby', 'g')
WHERE "Description" ~ 'Ru\s*by';

-- combine "Ba" and "by" to "Baby"
SELECT *
FROM perfumes
WHERE "Description" LIKE '%Ba by%';

UPDATE perfumes
SET "Description" = regexp_replace("Description", 'Ba\s*by', 'Baby', 'g')
WHERE "Description" ~ 'Ba\s*by';

-- combine "Orr" and "is" to "Orris"
SELECT *
FROM perfumes
WHERE "Description" LIKE '%Orr is%';

UPDATE perfumes
SET "Description" = regexp_replace("Description", 'Orr\s*is', 'Orris', 'g')
WHERE "Description" ~ 'Orr\s*is';

-- combine "Cannab" and "is" to "Cannabis" 
SELECT *
FROM perfumes
WHERE "Description" ILIKE '%cannab is%';

UPDATE perfumes
SET "Description" = regexp_replace("Description", '\bcannab\s+is\b', 'Cannabis', 'gi')
WHERE "Description" ~* '\bcannab\s+is\b';

-- add top notes column
ALTER TABLE perfumes
ADD COLUMN "Top Notes" text[];

UPDATE perfumes 
SET "Top Notes" = regexp_split_to_array(
    regexp_replace(
        regexp_replace("Description", '.*Top note(?:s)? (?:is|are) (.*?);.*', '\1', 'gi'),
        '\s+and\s+', ', ',
        'gi'
    ),
    '\s*,\s*'
)
WHERE "Description" ~* 'Top note(?:s)? (?:is|are)';

-- add middle notes column
ALTER TABLE perfumes
ADD COLUMN "Middle Notes" text[];

UPDATE perfumes 
SET "Middle Notes" = regexp_split_to_array(
    regexp_replace(
        regexp_replace("Description", '.*Middle note(?:s)? (?:is|are) (.*?);.*', '\1', 'gi'),
        '\s+and\s+', ', ',
        'gi'
    ),
    '\s*,\s*'
)
WHERE "Description" ~* 'Middle note(?:s)? (?:is|are)';

-- add base notes column
ALTER TABLE perfumes
ADD COLUMN "Base Notes" text[];

UPDATE perfumes 
SET "Base Notes" = regexp_split_to_array(
    regexp_replace(
        regexp_replace("Description", '.*Base note(?:s)? (?:is|are) (.*?)[.]', '\1', 'gi'),
        '\s+and\s+', ', ',
        'gi'
    ),
    '\s*,\s*'
)
WHERE "Description" ~* 'Base note(?:s)? (?:is|are)';

-- add notes column
ALTER TABLE perfumes
ADD COLUMN "Notes" text[];

-- add empty_notes table 
CREATE TABLE empty_notes AS
SELECT *
FROM perfumes
WHERE false;

INSERT INTO empty_notes
SELECT *
FROM perfumes
WHERE 
  ( "Top Notes" IS NULL OR array_length("Top Notes", 1) = 0 ) AND
  ( "Middle Notes" IS NULL OR array_length("Middle Notes", 1) = 0 ) AND
  ( "Base Notes" IS NULL OR array_length("Base Notes", 1) = 0 );
