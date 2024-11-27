## 台本を使って字幕修正

1. 台本をCSV形式にする
2. 台本をLLMで字幕に合うように長さ修正（LLMを使う）
3. 音声認識で作られた字幕を修正（LLMを使う）
4. 2と3で作ったデータをそれぞれデータベースに格納
    DONE
5. FAISSで字幕ごとにembedding(vector)を作成
   
```
% python3 add_faiss_index.py
```
6. vectorを使って字幕と台本の類似度を求めてtop5を保存

```
  % python3 scriptmatching.py
```
7. ベクトルを使って、字幕と台本の類似マッチングをする
> % python3 scriptmatching.py
  * 作られるテーブル
    * similar_pairs
8. 類似度が高いペアを見つける
  * 現時点のデータベースですでにviewを作っている。
    * high_similar_subtitle
    * subtitle_script_similar_matching (ほとんどsimilar_pairsと同じなんだけど、見やすくしてる？)
  * 字幕と台本のマッチング結果を出力
    > % python3 script_subtitle_mathing
   * 二つのビューを使って、マッチング結果（台本と字幕の流れを合わせたもの。不完全）を作る
   * 作成されるテーブル
  > mappings

9. 順番があべこべになっているところを修正
    あらかじめ定義されているview

```        SubtitleSurroundingInfo(script_id,prev_id,current_id,next_id,diff)
ScriptSurroundingInfo(subtitle_id,prev_id,current_id,next_id)

```
        
```SQL
-- 以下のSQLを使って中央のidが前後のidに挟まれていない場合、similar_pairsで前後のidで挟まれているものに置き換える。
     -- 該当するものがなければ空にする。
     WITH FilteredResults AS (
    SELECT 
      s.script_id,
      p.table1_id
    FROM (
      SELECT * 
      FROM SubtitleSurroundingInfo
      WHERE prev_id < next_id AND current_id NOT BETWEEN prev_id AND next_id
    ) s
    LEFT OUTER JOIN similar_pairs p 
       ON p.table2_id = s.script_id 
      AND p.table1_id > s.prev_id 
      AND p.table1_id < s.next_id
  )
  UPDATE mapping
  SET subtitle_id = f.table1_id
  FROM FilteredResults f
  WHERE mapping.script_id = f.script_id;

  -- 以下のSQLを使って中央のidが前後のidに挟まれていない場合、similar_pairsで前後のidで挟まれているものに置き換える。
  -- 該当するものがなければ空にする。
  WITH FilteredResults AS (
      SELECT 
        s.subtitle_id,
        p.table2_id
      FROM (
        SELECT * 
        FROM ScriptSurroundingInfo
        WHERE prev_id < next_id AND current_id NOT BETWEEN prev_id AND next_id
      ) s
      LEFT OUTER JOIN similar_pairs p 
         ON p.table1_id = s.subtitle_id 
        AND p.table2_id > s.prev_id 
        AND p.table2_id < s.next_id
    )
    UPDATE mapping
    SET script_id = f.table1_id
    FROM FilteredResults f
    WHERE mapping.subtitle_id = f.subtitle_id;
```

* 結果、mappingが更新される
* 重複マッピングを排除する

10. 一つのSubtitleにたいして複数のScript_idがマップされている時、前のSubtitleに対応しているScript_idとの差が一番小さなものだけを選択し、それ以外を削除する。

```sql
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
```

11. 字幕が対応づいていない台本（セリフ）に、前後のマッチングしているセリフの関係から時間情報を推測して追加する
> %python3 addstarttime.py
  * 字幕に直接starttimeをつけることができないので、dialog_timeというリレーションを用意している

12.以下の問い合わせで対応関係が出てくるぞ！
```sql
         SELECT 
         s.id, 
         s.text, 
         d.dialog_id, 
         d.speaker, 
         d.description,
         COALESCE(s.start_time, dt.start_time) AS start_time, 
         COALESCE(s.end_time, dt.end_time) AS end_time
     FROM dialogue d
     JOIN dialog_time dt ON d.dialog_id = dt.dialog_id
     LEFT OUTER JOIN mappings m ON d.id = m.script_id
     FULL OUTER JOIN subtitles s ON s.id = m.subtitle_id
   ORDER BY start_time;
```