import pysqlite3
import sqlite3
sqlite3 = pysqlite3
import numpy as np
import faiss
from typing import List, Dict, Tuple, ValuesView, Optional

def load_embeddings(conn: sqlite3.Connection, table_name: str, id_column: str = 'id') -> Tuple[List[int], np.ndarray]:
    """テーブルからembeddingを読み込む"""
    cursor = conn.cursor()
    cursor.execute(f"SELECT {id_column}, embedding FROM {table_name} ORDER BY {id_column}")
    rows = cursor.fetchall()

    ids = []
    embeddings = []
    for row in rows:
        ids.append(row[0])
        embedding = np.frombuffer(row[1], dtype=np.float32)
        embeddings.append(embedding)

    return ids, np.vstack(embeddings)

def similarity_join_lcs(
    db_path: str,
    subtitles: str, # table1を文字起こしにしたので後で修正
    scripts: str, # table2を台本にしたので後で修正
    threshold: float = 0.9,
    top_k: int = 10,
    batch_size: int = 100
):

    valid_pairs = []
    
    conn = sqlite3.connect(db_path)
    results = []

    try:
        # 各テーブルのembeddingを読み込み
        ids_sub, emb_sub = load_embeddings(conn, subtitles)
        ids_scr, emb_scr = load_embeddings(conn, scripts)

        # FAISSインデックスを作成
        dimension = emb_scr.shape[1]
        # コサイン類似度用のインデックス
        index = faiss.IndexFlatIP(dimension)  
        index.add(emb_scr.astype(np.float32))

        # バッチでemb_subの1個1個に対してtop5かつ閾値以上のペアだけをsimilarity_pairsに入れる
        for i in range(0, len(emb_sub), batch_size):
            batch_end = min(i + batch_size, len(emb_sub))
            batch = emb_sub[i:batch_end]

            # 類似度検索(top_k)
            D, I = index.search(batch.astype(np.float32), top_k)
            # 結果の処理
            for batch_idx, (distances, indices) in enumerate(zip(D, I)):
                sub_idx = i + batch_idx
                for dist, scr_idx in zip(distances, indices):
                    similarity = float(dist)  # コサイン類似度
                    if similarity >= threshold:
                        valid_pairs.append((ids_sub[sub_idx],ids_scr[scr_idx],similarity))
            
    finally:
        conn.close()

    # 順序制約付き（クロスしない）最大重み二部マッチング問題を解く
    valid_pairs.sort(key=lambda x: x[2], reverse=True)
    # DPテーブルの初期化: 最初のk個のペアを考慮した場合の最大類似度
    dp = [0] * (len(valid_pairs))
    # バックトラックのための前任者記録
    prev: List[Optional[int]] = [None] * (len(valid_pairs)+1)

    #各ペアを検討
    for cur_idx, (cur_sub, cur_scr, cur_sim) in enumerate(valid_pairs):
        #ベースケース：このペアだけを選ぶ
        dp[cur_idx + 1] = cur_sim
        #このペアの前に選べるペアを探す
        for prev_idx, (prev_sub, prev_scr, _) in enumerate(valid_pairs[:cur_idx]):
           if prev_sub < cur_sub and prev_scr < prev_scr:
               if dp[prev_idx + 1] + cur_sim > dp[cur_idx + 1]:
                   dp[cur_idx + 1] = dp[prev_idx + 1] + cur_sim
                   prev[cur_idx + 1] = prev_idx + 1

    max_val = max(dp)
    max_idx = dp.index(max_val)

    matches = []
    cur = max_idx 

    while cur is not None and cur > 0:
        pair_idx = cur - 1
        sub_id,scr_id,_ = valid_pairs[pair_idx]
        matches.append((sub_id, scr_id))
        cur = prev[cur]

    matches.reverse()

    return matches


def similarity_join(
    db_path: str,
    table1: str,
    table2: str,
    threshold: float = 0.7,
    top_k: int = 5,
    batch_size: int = 100
) -> List[Dict]:
    """
    2つのテーブル間で類似結合を実行

    Parameters:
    - db_path: データベースファイルのパス
    - table1: 1つ目のテーブル名
    - table2: 2つ目のテーブル名
    - threshold: 類似度の閾値（0-1の範囲）
    - top_k: 各レコードに対して取得する最大の類似レコード数
    - batch_size: バッチ処理のサイズ

    Returns:
    - 類似ペアのリスト
    """
    conn = sqlite3.connect(db_path)

    try:
        # 各テーブルのembeddingを読み込み
        ids1, embeddings1 = load_embeddings(conn, table1)
        ids2, embeddings2 = load_embeddings(conn, table2)

        # FAISSインデックスを作成
        dimension = embeddings2.shape[1]
        # コサイン類似度用のインデックス
        index = faiss.IndexFlatIP(dimension)  
        index.add(embeddings2.astype(np.float32))

        # バッチ処理で類似度検索を実行
        results = []
        total_batches = (len(embeddings1) + batch_size - 1) // batch_size

        for i in range(0, len(embeddings1), batch_size):
            batch_end = min(i + batch_size, len(embeddings1))
            batch = embeddings1[i:batch_end]

            # 類似度検索
            D, I = index.search(batch.astype(np.float32), top_k)

            # 結果の処理
            for batch_idx, (distances, indices) in enumerate(zip(D, I)):
                orig_idx = i + batch_idx

                for dist, idx in zip(distances, indices):
                    similarity = float(dist)  # コサイン類似度

                    if similarity >= threshold:
                        # 元のテキストを取得
                        cursor = conn.cursor()

                        # table1のテキスト
                        text_col1 = 'text' if table1 == 'subtitles' else 'contents'
                        cursor.execute(f"SELECT {text_col1} FROM {table1} WHERE id = ?", (ids1[orig_idx],))
                        text1 = cursor.fetchone()[0]                    

                        # table2のテキスト
                        text_col2 = 'text' if table2 == 'subtitles' else 'contents'
                        cursor.execute(f"SELECT {text_col2} FROM {table2} WHERE id = ?", (ids2[idx],))
                        text2 = cursor.fetchone()[0]


                        results.append({
                            f"{table1}_id": ids1[orig_idx],
                            f"{table2}_id": ids2[idx],
                            f"{table1}_text": text1,
                            f"{table2}_text": text2,
                            "similarity": similarity
                        })                    

        return sorted(results, key=lambda x: x['similarity'], reverse=True)

    finally:
        conn.close()

def save_results_to_db(db_path: str, results: List[Dict], output_table: str = 'similar_pairs'):
    """類似ペアの結果をデータベースに保存"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 結果保存用のテーブルを作成
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {output_table} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        table1_id INTEGER,
        table2_id INTEGER,
        table1_text TEXT,
        table2_text TEXT,
        similarity REAL
    )''')

    cursor.execute(f'''DELETE FROM {output_table}''')
    
    # 結果を保存
    cursor.executemany(
        f'''INSERT INTO {output_table} 
        (table1_id, table2_id, table1_text, table2_text, similarity)
        VALUES (?, ?, ?, ?, ?)''',
        [(r['subtitles_id' if 'subtitles_id' in r else 'scripts_id'],
          r['scripts_id' if 'subtitles_id' in r else 'subtitles_id'],
          r['subtitles_text' if 'subtitles_text' in r else 'scripts_text'],
          r['scripts_text' if 'subtitles_text' in r else 'subtitles_text'],
          r['similarity']) for r in results]
    )

    conn.commit()
    conn.close()

def script_matching(db_path):

    # 全ての組み合わせで類似度を計算し、閾値以上のペアだけをresultsに出力
    print("Performing similarity join between subtitles and scripts...")
    results = similarity_join(
        db_path=db_path,
        table1='subtitles',
        table2='scripts',
        threshold=0.94,  # 類似度閾値
        top_k=10,        # 各レコードに対する最大マッチ数
        batch_size=100  # バッチサイズ
    )    

    # resultsをデータベースに保存
    print("\nSaving results to database...")
    save_results_to_db(db_path, results)
    print("Done!")

def script_matching_bylcs(db_path):
    # 全ての組み合わせで類似度を計算し、閾値以上のペアだけをresultsに出力
    print("Performing similarity join between subtitles and scripts...")
    results = similarity_join(
        db_path=db_path,
        table1='subtitles',
        table2='scripts',
        threshold=0.94,  # 類似度閾値
        top_k=10,        # 各レコードに対する最大マッチ数
        batch_size=100  # バッチサイズ
    )    
    return results


if __name__ == '__main__':
    main("scripts.db")