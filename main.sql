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

-- DROP VIEW high_similar_subtitles;
-- CREATE VIEW high_similar_subtitles as 
-- SELECT *
-- FROM (
--     SELECT s.id,s.speaker,p.table2_text as description, p.table1_text as subtitle,
--     b.start_time, b.end_time,
--     p.similarity,    
--     row_number() over (partition by s.id order by p.similarity desc) as rank
--     FROM scripts s JOIN similar_pairs p ON s.id = p.table2_id
--          JOIN subtitles b on b.id = p.table1_id
--     WHERE s.type = 'dialogue' 
--     ORDER BY s.id, p.similarity DESC
-- )
-- WHERE rank <= 5;


CREATE VIEW dialogue AS 
SELECT *, LAG(id,1) OVER (ORDER BY id) as prev_id
FROM (
    SELECT *
    FROM scripts
    WHERE type = 'dialogue'
    ORDER BY id) sc



SELECT s.*, strftime('%s',start_time)- 
      strftime('%s',prev_start_time) as diff
FROM (
  SELECT id, 
    LAG(start_time,1) 
      OVER (ORDER BY id) as prev_start_time, 
    start_time, 
    description as current_text,
    subtitle
  FROM (
    SELECT * 
    FROM high_similar_subtitles 
    WHERE rank=1 ORDER BY id)
) s JOIN dialogue d1 ON s.id = d1.id JOIN dialogue d2 ON d1.prev_id = d2.id
WHERE diff between 0 and 60*5