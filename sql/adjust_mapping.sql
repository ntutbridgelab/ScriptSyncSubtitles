UPDATE mappings
SET subtitle_id = a.table1_id
FROM AbekobeSubtitle a
WHERE mappings.script_id = a.script_id;

UPDATE mappings
SET script_id = a.table2_id
FROM AbekobeScript a
WHERE mappings.subtitle_id = a.subtitle_id;