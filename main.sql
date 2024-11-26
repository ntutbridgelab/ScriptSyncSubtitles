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


-- CREATE VIEW dialogue AS 
-- SELECT *, LAG(id,1) OVER (ORDER BY id) as prev_id
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
-------------------
-- SELECT sm.id as script_id, sm.speaker, sm.description, sb.id as subtitle_id, sb.text as subtitle, sb.start_time, sb.end_time
-- FROM 
--     subtitles sb FULL OUTER JOIN 
--     (SELECT s.id, s.speaker, s.description, h.subtitle_id
--      FROM dialogue s 
--      LEFT OUTER JOIN high_similar_subtitles h ON s.id = h.script_id) sm
--     ON sb.id = sm.subtitle_id
-- ORDER BY script_id;

.header on
.mode csv 
.output script_subtitle_mapping.csv

WITH dialogue_scriptid_mapping as 
  (select d.id, d.speaker, d.description, m.subtitle_id
  from dialogue d left outer join mappings m 
  on d.id = m.script_id)
  
SELECT d.id, d.speaker, d.description, s.id, s.text, s.start_time, s.end_time
FROM dialogue_scriptid_mapping d 
     FULL OUTER JOIN subtitles s ON d.subtitle_id = s.id;
