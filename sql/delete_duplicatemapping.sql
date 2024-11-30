WITH DuplicateMapping AS (
 SELECT subtitle_id, script_id FROM (
 SELECT m1.subtitle_id, m1.prev_id, m2.script_id as prev_script_id ,m3.script_id as script_id,
   m3.script_id - m2.script_id as diff,
   row_number() OVER (
     PARTITION BY m1.subtitle_id
     ORDER BY m3.script_id-m2.script_id ASC
   ) AS ROW_NUM  
 FROM (
   SELECT distinct subtitle_id, prev_id
   FROM (SELECT distinct subtitle_id, LAG(subtitle_id) over (order by subtitle_id) as prev_id
       FROM mappings WHERE script_id IS NOT NULL ORDER BY subtitle_id)
   WHERE subtitle_id in (
       SELECT subtitle_id
       FROM mappings 
       WHERE subtitle_id IN  (SELECT subtitle_id FROM mappings GROUP BY subtitle_id HAVING count(*)>1)
  ) and subtitle_id > prev_id) m1 JOIN mappings m2 ON m1.prev_id = m2.subtitle_id
  JOIN mappings m3 ON m1.subtitle_id = m3.subtitle_id )
 WHERE row_num > 1) 

 DELETE FROM mappings
 WHERE (subtitle_id,script_id) IN DuplicateMapping;