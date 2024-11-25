import sqlite3
import numpy as np
from transformers import AutoTokenizer, AutoModel
import torch
import faiss
import pickle

class E5Embedder:
    def __init__(self, model_name='intfloat/multilingual-e5-large'):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        self.device = 'cuda' if torch.cuda.is_available() else 'cpu'
        self.model.to(self.device)
        print(f"Using device: {self.device}")

    def average_pool(self, last_hidden_states, attention_mask):
        last_hidden = last_hidden_states.masked_fill(~attention_mask[..., None].bool(), 0.0)
        return last_hidden.sum(dim=1) / attention_mask.sum(dim=1)[..., None]

    def encode(self, texts, batch_size=32, show_progress=True):
        embeddings = []
        for i in range(0, len(texts), batch_size):
            if show_progress:
                print(f"Processing batch {i//batch_size + 1}/{len(texts)//batch_size + 1}")
            batch_texts = texts[i:i+batch_size]
            processed_texts = [f"passage: {text}" for text in batch_texts]

            encoded = self.tokenizer(
                processed_texts,
                padding=True,
                truncation=True,
                max_length=512,
                return_tensors='pt'
            ).to(self.device)

            with torch.no_grad():
                outputs = self.model(**encoded)
                embeddings_batch = self.average_pool(
                    outputs.last_hidden_state,
                    encoded['attention_mask']
                )
                embeddings_batch = torch.nn.functional.normalize(embeddings_batch, p=2, dim=1)
                embeddings.append(embeddings_batch.cpu().numpy())

        return np.vstack(embeddings)

def setup_database(conn):
    """データベースにembeddingカラムとFAISSインデックステーブルを追加"""
    cursor = conn.cursor()

    # 各テーブルにembeddingカラムを追加
    for table in ['subtitles', 'scripts']:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in cursor.fetchall()]
        if 'embedding' not in columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN embedding BLOB")

    # FAISSインデックステーブルの作成
    cursor.execute('''CREATE TABLE IF NOT EXISTS faiss_indexes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        index_name TEXT UNIQUE,
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL,
        index_data BLOB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(table_name, column_name)
    )''')

    conn.commit()

def create_and_save_faiss_index(embeddings):
    """FAISSインデックスを作成してバイト列に変換"""
    dimension = embeddings.shape[1]
    index = faiss.IndexFlatIP(dimension)
    index.add(embeddings.astype(np.float32))

    # インデックスをバイト列にシリアライズ
    return pickle.dumps(faiss.serialize_index(index))

def process_table(conn, embedder, table_name, text_column):
    """テーブルのテキストデータを処理してembeddingとインデックスを追加"""
    cursor = conn.cursor()

    # テキストデータの取得
    cursor.execute(f'SELECT id, {text_column} FROM {table_name} ORDER BY id')
    rows = cursor.fetchall()
    ids = [row[0] for row in rows]
    texts = [row[1] for row in rows]

    print(f"\nProcessing {len(texts)} texts from {table_name}.{text_column}")

    # embeddingの生成
    embeddings = embedder.encode(texts)

    # embeddingの保存
    for idx, embedding in enumerate(embeddings):
        cursor.execute(
            f"UPDATE {table_name} SET embedding = ? WHERE id = ?",
            (embedding.tobytes(), ids[idx])
        )

    # FAISSインデックスの作成と保存
    index_binary = create_and_save_faiss_index(embeddings)
    cursor.execute('''
        INSERT OR REPLACE INTO faiss_indexes 
        (index_name, table_name, column_name, index_data)
        VALUES (?, ?, ?, ?)
    ''', (f'{table_name}_{text_column}_index', table_name, text_column, index_binary))

    conn.commit()
    print(f"Completed processing {table_name}.{text_column}")

def main():
    db_path = 'scripts.db'

    # E5 embedderの初期化
    print("Initializing E5 embedder...")
    embedder = E5Embedder()

    # データベース接続
    print(f"Connecting to database: {db_path}")
    conn = sqlite3.connect(db_path)

    try:
        # データベースのセットアップ
        print("Setting up database tables...")
        setup_database(conn)

        # 各テーブルの処理
        tables_to_process = [
            ('subtitles', 'text'),
            ('scripts', 'description')
        ]

        for table_name, text_column in tables_to_process:
            process_table(conn, embedder, table_name, text_column)

        print("\nSuccessfully added FAISS indexes to all tables")

    finally:
        conn.close()

def search_similar_texts(db_path, query_text, table_name, column_name, top_k=5):
    """類似テキストを検索する関数"""
    # E5 embedderの初期化
    embedder = E5Embedder()
    query_embedding = embedder.encode([query_text])[0]

    # データベース接続
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # FAISSインデックスの取得
        cursor.execute('''
            SELECT index_data 
            FROM faiss_indexes 
            WHERE table_name = ? AND column_name = ?
        ''', (table_name, column_name))
        index_binary = cursor.fetchone()[0]

        # インデックスの復元
        index_data = pickle.loads(index_binary)
        index = faiss.deserialize_index(index_data)

        # 類似度検索
        D, I = index.search(query_embedding.reshape(1, -1).astype(np.float32), top_k)

        # 結果の取得
        results = []
        for i, (distance, idx) in enumerate(zip(D[0], I[0])):
            real_idx = int(idx)+1
            cursor.execute(f'''
                SELECT id, {column_name}
                FROM {table_name} 
                WHERE id = ?
            ''', (real_idx,))
            result = cursor.fetchone()
            print(results)
            results.append({
                'rank': i + 1,
                'id': result[0],
                'text': result[1],
                'similarity': float(distance)
            })

        return results

    finally:
        conn.close()

if __name__ == '__main__':
    main()

    # 検索例
    print("\nTesting search functionality...")
    results = search_similar_texts(
        'scripts.db',
        "おれ獣医なんだけど",
        table_name="scripts",
        column_name="description"
    )
    print("\nSearch results:")
    for result in results:
        print(f"Rank {result['rank']}: [{result['id']}] {result['text']} (similarity: {result['similarity']:.3f})")