DROP TABLE if exists subtitles;
DROP TABLE if exists faiss_indexes;
DROP TABLE if exists similar_pairs;
DROP TABLE if exists scripts;
DROP TABLE if exists mappings;
DROP TABLE if exists dialog_time;
DROP TABLE if exists scene;
DROP VIEW if exists subtitle_script_similar_matching;
DROP VIEW if exists SubtitleSurroundingInfo;
DROP VIEW if exists dialogue;
DROP VIEW if exists SubtitleDialogMapping;
DROP VIEW if exists subtitle_script_mapping;
DROP VIEW if exists high_similar_subtitles;
DROP VIEW if exists AbekobeSubtitle;
DROP VIEW if exists AbekobeScript;
DROP VIEW if exists ScriptSubtitleMapping;
DROP VIEW if exists ScriptSurroundingInfo;

CREATE TABLE  scripts (
"index" INTEGER,
  "id" INTEGER,
  "scene_id" INTEGER,
  "type" TEXT,
  "speaker" TEXT,
  "contents" TEXT
, embedding BLOB);
CREATE INDEX "ix_scripts_index"ON "scripts" ("index");

CREATE TABLE subtitles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subtitle_index INTEGER,
    start_time TEXT,
    end_time TEXT,
    text TEXT
, embedding BLOB);

CREATE TABLE faiss_indexes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        index_name TEXT UNIQUE,
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL,
        index_data BLOB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(table_name, column_name)
    );
CREATE TABLE similar_pairs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        table1_id INTEGER,
        table2_id INTEGER,
        table1_text TEXT,
        table2_text TEXT,
        similarity REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
CREATE VIEW subtitle_script_similar_matching as 
SELECT table2_id as script_id, table1_id as subtitle_id, 
    table2_text as script_text,table1_text as subtitle_text, similarity
FROM similar_pairs
ORDER BY script_id
/* subtitle_script_similar_matching(script_id,subtitle_id,script_text,subtitle_text,similarity) */;
CREATE TABLE mappings (
    script_id int,
    subtitle_id int);
CREATE VIEW SubtitleSurroundingInfo as 
SELECT script_id,lag(subtitle_id) over (order by script_id) as prev_id,subtitle_id as current_id, lead(subtitle_id) over (order by script_id) as next_id, subtitle_id - avg(subtitle_id) over (order by script_id rows between 5 preceding and 5 following) as diff
FROM mappings
WHERE subtitle_id IS NOT NULL
/* SubtitleSurroundingInfo(script_id,prev_id,current_id,next_id,diff) */;
CREATE VIEW ScriptSurroundingInfo as 
SELECT subtitle_id,lag(script_id) over (order by subtitle_id) as prev_id,script_id as current_id, lead(script_id) over (order by subtitle_id) as next_id
FROM mappings
WHERE script_id IS NOT NULL and subtitle_id IS NOT NULL
/* ScriptSurroundingInfo(subtitle_id,prev_id,current_id,next_id) */;
CREATE VIEW dialogue AS 
SELECT *, ROW_NUMBER() OVER (ORDER BY id) as dialog_id, LAG(id,1) OVER (ORDER BY id) as prev_id
FROM (
    SELECT *
    FROM scripts
    WHERE type = 'dialogue'
    ORDER BY id) sc
/* dialogue("index",id,scene_id,type,speaker,contents,embedding,dialog_id,prev_id) */;
CREATE VIEW SubtitleDialogMapping AS 
SELECT s.id, d.dialog_id, s.start_time, s.end_time
 FROM dialogue d LEFT OUTER JOIN mappings m ON d.id = m.script_id
 FULL OUTER JOIN subtitles s ON s.id = m.subtitle_id
ORDER BY s.id
/* SubtitleDialogMapping(id,dialog_id,start_time,end_time) */;
CREATE TABLE dialog_time(dialog_id int, start_time text, end_time text);
CREATE TABLE scene (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL
);
CREATE VIEW subtitle_script_mapping as     
SELECT sm.id as script_id, sm.speaker, sm.contents, sb.id as subtitle_id, sb.text as subtitle, sb.start_time, sb.end_time
FROM 
    (SELECT s.id, s.speaker, s.contents, h.subtitle_id
     FROM dialogue s 
     LEFT OUTER JOIN high_similar_subtitles h ON s.id = h.script_id) sm
    LEFT OUTER JOIN subtitles sb
    ON sm.subtitle_id = sb.id
ORDER BY script_id
/* subtitle_script_mapping(script_id,speaker,contents,subtitle_id,subtitle,start_time,end_time) */;
CREATE VIEW high_similar_subtitles as 
SELECT *
FROM (
    SELECT s.id as script_id,s.speaker,p.table2_text as contents, b.id as subtitle_id, p.table1_text as subtitle,
    b.start_time, b.end_time,
    p.similarity,    
    row_number() over (partition by s.id order by p.similarity desc) as rank
    FROM scripts s JOIN similar_pairs p ON s.id = p.table2_id
         JOIN subtitles b on b.id = p.table1_id
    WHERE s.type = 'dialogue' 
    ORDER BY s.id, p.similarity DESC
)
WHERE rank<=5
/* high_similar_subtitles(script_id,speaker,contents,subtitle_id,subtitle,start_time,end_time,similarity,rank) */;
CREATE VIEW AbekobeSubtitle AS 
SELECT script_id, table1_id
FROM (
  SELECT 
    s.script_id,
    p.table1_id,
    rank() over (partition by s.script_id order by p.similarity desc, p.table1_id) as rank
  FROM (
    SELECT * 
    FROM SubtitleSurroundingInfo
    WHERE NOT (prev_id < current_id AND current_id < next_id)) s
  JOIN similar_pairs p 
     ON p.table2_id = s.script_id 
    AND p.table1_id > s.prev_id 
    AND p.table1_id < s.next_id
)
/* AbekobeSubtitle(script_id,table1_id) */;
CREATE VIEW AbekobeScript AS 
SELECT
    subtitle_id, table2_id
FROM(
    SELECT 
    s.subtitle_id,
    p.table2_id,
    rank() 
      over (partition by s.subtitle_id order by p.similarity desc, table2_id) as rank
  FROM (
    SELECT * 
    FROM ScriptSurroundingInfo
    WHERE NOT (prev_id < current_id AND current_id < next_id)
  ) s
JOIN similar_pairs p 
     ON p.table1_id = s.subtitle_id 
    AND p.table2_id > s.prev_id 
    AND p.table2_id < s.next_id
) 
WHERE rank = 1
/* AbekobeScript(subtitle_id,table2_id) */;
CREATE VIEW ScriptSubtitleMapping AS 
WITH OrderedData AS (
    SELECT distinct
        s.id AS subtitle_id, 
        s.text AS subtitle_text, 
        d.dialog_id, 
        d.speaker AS script_speaker, 
        d.contents AS script_dialog,
        COALESCE(d.speaker, '?') AS speaker,
        COALESCE(d.contents, s.text) AS subtitle,
        COALESCE(s.start_time, dt.start_time) AS start_time, 
        COALESCE(s.end_time, dt.end_time) AS end_time
    FROM dialogue d
    JOIN dialog_time dt ON d.dialog_id = dt.dialog_id
    LEFT OUTER JOIN (select distinct * from mappings) m ON d.id = m.script_id
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
/* ScriptSubtitleMapping(subtitle_id,subtitle_text,dialog_id,script_speaker,script_dialog,speaker,subtitle,start_time,end_time) */;