-- CREATE TABLE similar_pairs (
--     id INTEGER PRIMARY KEY AUTOINCREMENT,
--     table1_id INTEGER,
--     table2_id INTEGER,
--     table1_text TEXT,
--     table2_text TEXT,
--     similarity REAL,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- CREATE TABLE scripts (
--     id INTEGER PRIMARY KEY AUTOINCREMENT,
--     scene_id INTEGER,
--     type TEXT NOT NULL,
--     speaker TEXT,
--     description TEXT NOT NULL, embedding BLOB,
--     FOREIGN KEY (scene_id) REFERENCES scene(id)
-- );

-- sqlite> .schema subtitles
-- CREATE TABLE subtitles (
--     id INTEGER PRIMARY KEY AUTOINCREMENT,
--     subtitle_index INTEGER,
--     start_time TEXT,
--     end_time TEXT,
--     text TEXT
-- , embedding BLOB);

-- table1='subtitles',
-- table2='scripts',

-- CREATE VIEW high_similar_subtitles as 
-- SELECT *
-- FROM (
--     SELECT s.id as script_id,s.speaker,p.table2_text as description, b.id as subtitle_id, p.table1_text as subtitle,
--     b.start_time, b.end_time,
--     p.similarity,    
--     row_number() over (partition by s.id order by p.similarity desc) as rank
--     FROM scripts s JOIN similar_pairs p ON s.id = p.table2_id
--          JOIN subtitles b on b.id = p.table1_id
--     WHERE s.type = 'dialogue' 
--     ORDER BY s.id, p.similarity DESC
-- )
-- WHERE rank = 1;

-- DROP VIEW dialogue;
-- CREATE VIEW dialogue AS 
-- SELECT *, ROW_NUMBER() OVER (ORDER BY id) as dialog_id, LAG(id,1) OVER (ORDER BY id) as prev_id
-- FROM (
--     SELECT *
--     FROM scripts
--     WHERE type = 'dialogue'
--     ORDER BY id) sc



-- SELECT s.*, strftime('%s',start_time)- 
--       strftime('%s',prev_start_time) as diff
-- FROM (
--   SELECT id, 
--     LAG(start_time,1) 
--       OVER (ORDER BY id) as prev_start_time, 
--     start_time, 
--     description as current_text,
--     subtitle
--   FROM (
--     SELECT * 
--     FROM high_similar_subtitles 
--     WHERE rank=1 ORDER BY id)
-- ) s JOIN dialogue d1 ON s.id = d1.id JOIN dialogue d2 ON d1.prev_id = d2.id
-- WHERE diff between 0 and 60*5
-- limit 10;

-- update scripts set type = 'innervoice'
-- where id in (29,40,46,101,120,124,237,252,254,292,372,399,403,448,466,467,469,582,593,838,905,909,944,956,966,986,989,990,1037,1038,1083);

-- update scripts set type = 'speachless'
-- where id in 
-- (7,15,44,81,96,130,152,160,174,177,179,194,201,207,214,233,258,296,304,312,314,346,350,362,365,370,395,400,402,407,422,468,471,474,477,478,496,501,503,512,531,563,571,614,623,722,757,832,874,876,880,885,911,914,924,926,928,939,946,954,968,973,979,980,1001);

-- .mode csv
-- .header on
-- .output subtitle_script_mapping.csv
-- DROP VIEW subtitle_script_mapping;
-- CREATE VIEW subtitle_script_mapping as     
-- SELECT sm.id as script_id, sm.speaker, sm.description, sb.id as subtitle_id, sb.text as subtitle, sb.start_time, sb.end_time
-- FROM 
--     (SELECT s.id, s.speaker, s.description, h.subtitle_id
--      FROM dialogue s 
--      LEFT OUTER JOIN high_similar_subtitles h ON s.id = h.script_id) sm
--     LEFT OUTER JOIN subtitles sb
--     ON sm.subtitle_id = sb.id
-- ORDER BY script_id;

-- DROP VIEW subtitle_script_similar_matching;
-- CREATE VIEW subtitle_script_similar_matching as 
-- SELECT table2_id as script_id, table1_id as subtitle_id, 
--     table2_text as script_text,table1_text as subtitle_text, similarity
-- FROM similar_pairs
-- ORDER BY script_id;
-------------------対応表を確認-----------------
-- SELECT sm.id as script_id, sm.speaker, sm.description, sb.id as subtitle_id, sb.text as subtitle, sb.start_time, sb.end_time
-- FROM 
--     subtitles sb FULL OUTER JOIN 
--     (SELECT s.id, s.speaker, s.description, h.subtitle_id
--      FROM dialogue s 
--      LEFT OUTER JOIN high_similar_subtitles h ON s.id = h.script_id) sm
--     ON sb.id = sm.subtitle_id
-- ORDER BY script_id;
-------------------

-- WITH DuplicateMapping AS (
-- SELECT subtitle_id, script_id FROM (
-- SELECT m1.subtitle_id, m1.prev_id, m2.script_id as prev_script_id ,m3.script_id as script_id,
--   m3.script_id - m2.script_id as diff,
--   row_number() OVER (
--     PARTITION BY m1.subtitle_id
--     ORDER BY m3.script_id-m2.script_id ASC
--   ) AS ROW_NUM  
-- FROM (
--   SELECT distinct subtitle_id, prev_id
--   FROM (SELECT distinct subtitle_id, LAG(subtitle_id) over (order by subtitle_id) as prev_id
--       FROM mappings WHERE script_id IS NOT NULL ORDER BY subtitle_id)
--   WHERE subtitle_id in (
--       SELECT subtitle_id
--       FROM mappings 
--       WHERE subtitle_id IN  (SELECT subtitle_id FROM mappings GROUP BY subtitle_id HAVING count(*)>1)
--  ) and subtitle_id > prev_id) m1 JOIN mappings m2 ON m1.prev_id = m2.subtitle_id
--  JOIN mappings m3 ON m1.subtitle_id = m3.subtitle_id )
-- WHERE row_num > 1) 

-- DELETE FROM mappings
-- WHERE (subtitle_id,script_id) IN DuplicateMapping;

-- SELECT s.id, s.text, d.dialog_id, d.speaker, d.description,s.start_time, s.end_time
--  FROM dialogue d LEFT OUTER JOIN mappings m ON d.id = m.script_id
--  FULL OUTER JOIN subtitles s ON s.id = m.subtitle_id
-- ORDER BY s.id

-- CREATE VIEW SubtitleDialogMapping AS 
-- SELECT s.id, d.dialog_id, s.start_time, s.end_time
--  FROM dialogue d LEFT OUTER JOIN mappings m ON d.id = m.script_id
--  FULL OUTER JOIN subtitles s ON s.id = m.subtitle_id
-- ORDER BY s.id;


-- SELECT dialog_id, start_time
-- FROM SubtitleDialogMapping
-- WHERE dialog_id IS NOT NULL
-- ORDER BY dialog_id
-- LIMIT 20;


CREATE VIEW ScriptSubtitleMapping AS 
WITH OrderedData AS (
    SELECT 
        s.id AS subtitle_id, 
        s.text AS subtitle_text, 
        d.dialog_id, 
        d.speaker AS script_speaker, 
        d.description AS script_dialog,
        COALESCE(d.speaker, '?') AS speaker,
        COALESCE(d.description, s.text) AS subtitle,
        COALESCE(s.start_time, dt.start_time) AS start_time, 
        COALESCE(s.end_time, dt.end_time) AS end_time
    FROM dialogue d
    JOIN dialog_time dt ON d.dialog_id = dt.dialog_id
    LEFT OUTER JOIN mappings m ON d.id = m.script_id
    FULL OUTER JOIN subtitles s ON s.id = m.subtitle_id
),
AdjustedTimes AS (
    SELECT 
        subtitle_id,
        subtitle_text,
        dialog_id,
        script_speaker,
        script_dialog,
        speaker,
        subtitle,
        start_time,
        CASE 
            WHEN end_time >= LEAD(start_time) OVER (ORDER BY subtitle_id)
            THEN strftime('%H:%M:%f', datetime(LEAD(start_time) OVER (ORDER BY subtitle_id), '-0.1 seconds'))
            ELSE end_time
        END AS adjusted_end_time
    FROM OrderedData
)
SELECT 
    subtitle_id,
    subtitle_text,
    dialog_id,
    script_speaker,
    script_dialog,
    speaker,
    subtitle,
    start_time,
    adjusted_end_time AS end_time
FROM AdjustedTimes
ORDER BY start_time



-- SELECT 
--       s.id as subtitle_id, 
--       s.text as subtitle_text, 
--       d.dialog_id, 
--       d.speaker as script_spaeker, 
--       d.description script_dialog,
--       COALESCE(s.start_time, dt.start_time) AS start_time, 
--       COALESCE(s.end_time, dt.end_time) AS end_time
--   FROM dialogue d
--   JOIN dialog_time dt ON d.dialog_id = dt.dialog_id
--   LEFT OUTER JOIN mappings m ON d.id = m.script_id
--   FULL OUTER JOIN subtitles s ON s.id = m.subtitle_id
-- ORDER BY start_time;