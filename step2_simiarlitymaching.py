import sys
import csv
import pandas as pd
import pysqlite3
import sqlite3
sqlite3 = pysqlite3

from src.genscriptdb import exec_sql, fetch_data_by_query
from src.addvectorindex import add_embeddings
from src.scriptmatching import script_matching, script_matching_bylcs
from src.adjustmapping import adjust_mapping_pairs
from src.addstarttime import add_starttime

def parse_time(time_str):
    """SRTの時間形式(00:00:00,000)をパースする"""
    time_parts = time_str.replace(',', ':').split(':')
    hours = int(time_parts[0])
    minutes = int(time_parts[1])
    seconds = int(time_parts[2])
    milliseconds = int(time_parts[3])
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:03d}"

def create_table_sql():
    """CREATE TABLE文を生成"""
    return '''CREATE TABLE IF NOT EXISTS subtitles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subtitle_index INTEGER,
    start_time TEXT,
    end_time TEXT,
    text TEXT);
'''

def generate_insert_statements(srt_file_path):
    """SRTファイルからINSERT文を生成"""
    insert_statements = []

    with open(srt_file_path, 'r', encoding='utf-8') as file:
        content = file.read().strip()
        blocks = content.split('\n\n')

        for block in blocks:
            lines = block.split('\n')
            # print(lines)
            if len(lines) >= 3:  # 有効なブロックかチェック
                subtitle_index = int(lines[0])
                time_line = lines[1]
                text = '\n'.join(lines[2:])

                # 時間形式を解析
                start_time, end_time = time_line.split(' --> ')

                # SQLエスケープ処理
                escaped_text = text.replace("'", "''")

                # INSERT文を生成
                insert_sql = f"""INSERT INTO subtitles (subtitle_index, start_time, end_time, text)
VALUES ({subtitle_index}, '{parse_time(start_time)}', '{parse_time(end_time)}', '{escaped_text}');"""
                insert_statements.append(insert_sql)

    return insert_statements

def load_script_csv(script_path, db_path):
    df = pd.read_csv(script_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP TABLE scripts;")
        df.to_sql("scripts",con=conn)

def load_srt(srt_file_path,db_path):
    # 入力ファイルと出力ファイルのパス
    output_file = 'tmp/srt_tmp.sql'     # 出力するSQLファイルのパス

    # SQL文を生成
    create_sql = create_table_sql()
    insert_statements = generate_insert_statements(srt_file_path)

    # ファイルに出力
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(create_sql + '\n\n')
        f.write('\n'.join(insert_statements))
        f.write('\n')

    exec_sql(output_file,db_path)

if __name__ == "__main__":
  db_path = "scripts.db"
  if len(sys.argv) < 3:
      print(f"Usage: {sys.argv[0]} <srtfile> <scriptfile>")
  else:
      exec_sql("sql/setup.sql",db_path);
      print("loading SRT file...")
      load_srt(sys.argv[1],db_path)
      print("loading SCRIPT file...")
      load_script_csv(sys.argv[2],db_path)
      print("adding vector data...")
      add_embeddings(db_path)
      print("script matching...")
      script_matching_bylcs(db_path)
      # print("adjusting pairs...")
      # adjust_mapping_pairs(db_path)
      # for i in range(20):
      #    exec_sql("sql/adjust_mapping.sql",db_path)
      # exec_sql("sql/delete_duplicatemapping.sql",db_path)
      # print("adding starttime...")
      # add_starttime(db_path)
      # print("output mapping file...")
      # results = fetch_data_by_query(db_path,"SELECT * FROM ScriptSubtitleMapping")
      # csv_content = []
      # try:
      #     with open("mapping.csv", 'w', encoding='utf-8') as f:
      #         writer = csv.writer(f)
      #         for row in results:
      #             writer.writerow(row)
                  
      #     print("字幕とセリフの対応表を作りました")
      # except Exception as e:
      #     print(f"Error: ファイル書き込み中にエラーが発生しました: {e}")
