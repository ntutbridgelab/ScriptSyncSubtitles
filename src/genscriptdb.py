import re
import sys
from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path
import sqlite3


@dataclass
class ScriptLine:
    scene_id: int
    type: str
    speaker: Optional[str]
    description: str

def parse_script(text: str) -> tuple[List[tuple[int, str]], List[ScriptLine]]:
    """台本テキストを解析してシーン情報とスクリプト情報を返す"""
    scenes = []
    script_lines = []

    scene_pattern = r'(\d+) ([^\n]+)'
    dialogue_pattern = r'([^「」\s]+)「([^」]+)」'

    current_scene_id = None
    lines = text.split('\n')

    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        scene_match = re.match(scene_pattern, line)
        if scene_match:
            current_scene_id = int(scene_match.group(1))
            scene_title = scene_match.group(2).strip()
            scenes.append((current_scene_id, scene_title))
            continue

        if current_scene_id is None:
            continue

        dialogue_match = re.search(dialogue_pattern, line)
        if dialogue_match:
            speaker = dialogue_match.group(1)
            dialogue = dialogue_match.group(2)

            dialogue_del_dot = dialogue.replace('.', '').replace('．', '').replace('…', '').replace('!', '').replace('！', '').replace('?', '').replace('？', '').strip();

            if dialogue_del_dot == '':
                script_lines.append(ScriptLine(
                    scene_id=current_scene_id,
                    type='speachless',
                    speaker=speaker,
                    description=dialogue
                ))
            # セリフが（）で囲まれているかチェック
            elif dialogue_del_dot.startswith('（') and dialogue_del_dot.endswith('）'):
                script_lines.append(ScriptLine(
                    scene_id=current_scene_id,
                    type='innervoice',
                    speaker=speaker,
                    description=dialogue
                ))
            elif dialogue_del_dot.startswith('(') and dialogue_del_dot.endswith(')'):  # 半角括弧もチェック
                script_lines.append(ScriptLine(
                    scene_id=current_scene_id,
                    type='innervoice',
                    speaker=speaker,
                    description=dialogue
                ))
            else:    
                script_lines.append(ScriptLine(
                    scene_id=current_scene_id,
                    type='dialogue',
                    speaker=speaker,
                    description=dialogue
                ))
        else:
            script_lines.append(ScriptLine(
                scene_id=current_scene_id,
                type='description',
                speaker=None,
                description=line
            ))

    return scenes, script_lines

def escape_sql_string(s: str) -> str:
    """SQLの文字列をエスケープする"""
    return s.replace("'", "''")

def generate_sql(scenes: List[tuple], script_lines: List[ScriptLine]) -> str:
    """シーンとスクリプト情報からSQL文を生成する"""
    sql_lines = []

    # テーブル作成のSQL
    sql_lines.append("""-- テーブルの作成
CREATE TABLE IF NOT EXISTS scene (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scripts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scene_id INTEGER,
    type TEXT NOT NULL,
    speaker TEXT,
    description TEXT NOT NULL,
    FOREIGN KEY (scene_id) REFERENCES scene(id)
);

-- 既存のデータを削除
DELETE FROM scripts;
DELETE FROM scene;

-- シーケンスをリセット
DELETE FROM sqlite_sequence WHERE name='scripts';
""")

    # シーンのINSERT文
    sql_lines.append("\n-- シーンの挿入")
    for scene_id, title in scenes:
        escaped_title = escape_sql_string(title)
        sql = "INSERT INTO scene (id, title) VALUES ({}, '{}');".format(
            scene_id, escaped_title
        )
        sql_lines.append(sql)

    # スクリプトのINSERT文
    sql_lines.append("\n-- スクリプトの挿入")
    for line in script_lines:
        speaker = "'{}'".format(escape_sql_string(line.speaker)) if line.speaker else 'NULL'
        escaped_description = escape_sql_string(line.description)
        sql = "INSERT INTO scripts (scene_id, type, speaker, description) VALUES ({}, '{}', {}, '{}');".format(
            line.scene_id, 
            line.type,
            speaker,
            escaped_description
        )
        sql_lines.append(sql)

    return '\n'.join(sql_lines)

def generate_csv(scenes: List[tuple], script_lines: List[ScriptLine]) -> str:
    """シーンとスクリプト情報からSQL文を生成する"""
    csv_lines = []
    # ヘッダを追加
    csv_lines.append("id,scene_id,type,speaker,description")
    
    # スクリプトのINSERT文
    i=0
    for line in script_lines:
        speaker = "{}".format(escape_sql_string(line.speaker)) if line.speaker else 'NULL'
        escaped_description = escape_sql_string(line.description)
        csv = "{}, {}, {}, {}, {}".format(
            i,
            line.scene_id, 
            line.type,
            speaker,
            escaped_description
        )
        csv_lines.append(csv)
        i=i+1

    return '\n'.join(csv_lines)

def fetch_data_by_query(db_path,sql):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    results = []
    try:
        with conn:
            cursor.execute(sql)
            results = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"エラーが発生しました：{e}")
    finally:
        conn.close()
    return results;

def insert_script_data(output_file,db_path):
    # SQLite3データベースに接続
    conn = sqlite3.connect(db_path)

    # SQLファイルを読み込んで実行
    with open(output_file, "r") as file:
        sql_script = file.read()  # SQLファイルを文字列として読み込む

    try:
        # SQLスクリプトを実行
        with conn:
            conn.executescript(sql_script)
        print("SQLスクリプトの実行が完了しました。")
    except sqlite3.Error as e:
        print(f"エラーが発生しました: {e}")
    finally:
        # 接続を閉じる
        conn.close()

def exec_sql(sqlfile,db_path):
    return insert_script_data(sqlfile,db_path)

def generate_script_table(input_file,db_path):
    # 入力ファイルのパスをPathオブジェクトに変換
    input_path = Path(input_file)
    # 入力ファイル名と同じ名前で拡張子を変更
    output_file = f"./tmp/{input_path.with_suffix('.sql')}"
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            script_text = f.read()
    except FileNotFoundError:
        print("Error: input.txt が見つかりません")
        return
    except Exception as e:
        print(f"Error: ファイル読み込み中にエラーが発生しました: {e}")
        return

    # 台本を解析
    try:
        scenes, script_lines = parse_script(script_text)
    except Exception as e:
        print(f"Error: 台本の解析中にエラーが発生しました: {e}")
        return

    # SQL生成
    try:
        sql_content = generate_sql(scenes, script_lines)
    except Exception as e:
        print(f"Error: SQL生成中にエラーが発生しました: {e}")
        return

    # 出力ファイルに書き込む
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(sql_content)
        print(f"SQLファイル {output_file} を生成しました")
        insert_script_data(output_file,db_path)
    except Exception as e:
        print(f"Error: ファイル書き込み中にエラーが発生しました: {e}")
        return
        
    # CSV
    try:
        csv_content = generate_csv(scenes, script_lines)
    except Exception as e:
        print(f"Error: script.csv生成中にエラーが発生しました: {e}")
        return

    # csv出力ファイルに書き込む
    try:
        with open("script.csv", 'w', encoding='utf-8') as f:
            f.write(csv_content)
        print(f"台本のcsvファイルを生成しました")
    except Exception as e:
        print(f"Error: ファイル書き込み中にエラーが発生しました: {e}")
        return
