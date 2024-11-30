import sqlite3
import pandas as pd
from datetime import datetime, timedelta

# 日時形式に変換
def parse_time(time_str):
    return datetime.strptime(time_str, "%H:%M:%S.%f") if time_str else None

# 均等に埋める処理
def fill_missing_times(df):
    filled_data = []
    n = len(df)

    i = 0
    while i < n:
        if pd.notnull(df.loc[i, 'start_time']):
            # 現在の行を辞書型で追加
            filled_data.append({
                "dialog_id": df.loc[i, "dialog_id"],
                "start_time": df.loc[i, "start_time"],
                "end_time": df.loc[i, "end_time"]
            })
            i += 1
        else:
            # 空の`start_time`を処理
            start_idx = i - 1
            while i < n and pd.isnull(df.loc[i, 'start_time']):
                i += 1
            end_idx = i

            prev_time = df.loc[start_idx, 'start_time']
            next_time = df.loc[end_idx, 'start_time'] if end_idx < n else None

            if next_time:
                # 均等な間隔を計算
                delta = (next_time - prev_time) / (end_idx - start_idx)
                for j in range(start_idx + 1, end_idx):
                    filled_time = prev_time + delta * (j - start_idx)
                    filled_data.append({
                        "dialog_id": df.loc[j, "dialog_id"],
                        "start_time": filled_time,
                        "end_time": filled_time + delta*0.99
                    })
            else:
                # 次の時間がない場合
                for j in range(start_idx + 1, end_idx):
                    filled_data.append({
                        "dialog_id": df.loc[j, "dialog_id"],
                        "start_time": None,
                        "end_time": None
                    })

    if i < n:
        filled_data.append({
            "dialog_id": df.loc[i, "dialog_id"],
            "start_time": df.loc[i, "start_time"],
            "end_time": df.loc[i, "end_time"]
        })

    return pd.DataFrame(filled_data)
    
def add_starttime(db_path):
    conn = sqlite3.connect(db_path)

    # データを取得
    query = "SELECT dialog_id, start_time, end_time FROM SubtitleDialogMapping WHERE dialog_id IS NOT NULL ORDER BY dialog_id;"
    df = pd.read_sql_query(query, conn)
    
    df['start_time'] = df['start_time'].apply(parse_time)
    df['end_time'] = df['end_time'].apply(parse_time)
    
    # 処理実行
    filled_df = fill_missing_times(df)
    
    # 書式を元に戻す
    filled_df['start_time'] = filled_df['start_time'].apply(
        lambda x: x.strftime("%H:%M:%S.%f")[:-3] if pd.notnull(x) else None
    )
    
    filled_df['end_time'] = filled_df['end_time'].apply(
        lambda x: x.strftime("%H:%M:%S.%f")[:-3] if pd.notnull(x) else None
    )
    
    # データをSQLite3データベースに反映
    conn.execute("CREATE TABLE IF NOT EXISTS dialog_time(dialog_id int, start_time text, end_time text);");
    conn.execute("DELETE FROM dialog_time;");
    
    for _, row in filled_df.iterrows():
        if row['start_time']:
            # print(f"{row['dialog_id']} -> {row['start_time']},{row['end_time']}")
            conn.execute("INSERT INTO dialog_time VALUES (?,?,?)",
                        (row['dialog_id'],row['start_time'],row['end_time']))
    
    # 変更を保存
    conn.commit()
    
    # 接続を閉じる
    conn.close()
    
    print("start_timeの欠損を均等に埋めました。")
    