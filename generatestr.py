import pandas as pd
import sys

# SRTファイル形式に変換する関数
def create_srt(csv_file_path, output_file):
    df = pd.read_csv(csv_file_path)
    with open(output_file, 'w', encoding='utf-8') as f:
        for i, row in df.iterrows():
            # SRTの各セクションを構築
            srt_index = i + 1
            start_time = row['start_time'].replace('.', ',')  # ミリ秒をカンマに変換
            end_time = row['end_time'].replace('.', ',')  # ミリ秒をカンマに変換
            subtitle = f"{row['speaker']}: {row['subtitle']}" if row[
                'speaker'] != "?" else row['subtitle']

            # SRTフォーマットで書き込み
            f.write(f"{srt_index}\n")
            f.write(f"{start_time} --> {end_time}\n")
            f.write(f"{subtitle}\n\n")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <対応表.csv> <結果.srt>")
    else:
        create_srt(sys.argv[1], sys.argv[2])
