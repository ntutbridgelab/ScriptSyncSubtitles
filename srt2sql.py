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
    text TEXT
);'''

def generate_insert_statements(srt_file_path):
    """SRTファイルからINSERT文を生成"""
    insert_statements = []

    with open(srt_file_path, 'r', encoding='utf-8') as file:
        content = file.read().strip()
        blocks = content.split('\n\n')

        for block in blocks:
            lines = block.split('\n')
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

def main():
    # 入力ファイルと出力ファイルのパス
    srt_file_path = 'DorphinBlue_AWS.srt'  # SRTファイルのパス
    output_file = 'DorphinBlue_AWS.sql'     # 出力するSQLファイルのパス

    # SQL文を生成
    create_sql = create_table_sql()
    insert_statements = generate_insert_statements(srt_file_path)

    # ファイルに出力
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(create_sql + '\n\n')
        f.write('\n'.join(insert_statements))
        f.write('\n')

if __name__ == '__main__':
    main()