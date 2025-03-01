import pysqlite3
import sqlite3
sqlite3 = pysqlite3
from typing import Dict, List, Optional, Tuple


class SubtitleMapper:

    def __init__(self, db_path):
        self.similar_matches: Dict[str, List[Tuple[str, float]]] = {}
        self.mapping_data: List[dict] = []
        self.db_path = db_path

    def load_similar_matches(self) -> None:
        """類似度マッチングデータをデータベースから読み込む"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT script_id, subtitle_id, similarity 
            FROM subtitle_script_similar_matching
        ''')

        for row in cursor.fetchall():
            script_id = row[0]
            subtitle_id = row[1]
            similarity = float(row[2])

            if script_id not in self.similar_matches:
                self.similar_matches[script_id] = []
            self.similar_matches[script_id].append((subtitle_id, similarity))

        conn.close()

    def load_mapping(self) -> None:
        """現在のマッピングデータをデータベースから読み込む"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT 
                script_id,
                subtitle_id
            FROM dialogue d LEFT OUTER JOIN high_similar_subtitles h
              ON d.id = h.script_id
            WHERE script_id IS NOT NULL
        ''')

        for row in cursor.fetchall():
            self.mapping_data.append({
                'script_id': row[0],
                'subtitle_id': row[1] if row[1] else ''
            })

        conn.close()

    def find_valid_subtitle_id(self, current_idx: int) -> Optional[str]:
        """与えられたインデックスの行に対して適切なsubtitle_idを見つける"""
        current_entry = self.mapping_data[current_idx]
        script_id = current_entry['script_id']

        # 前後のsubtitle_idを取得
        prev_id = None
        next_id = None

        # 前のsubtitle_idを探す
        for i in range(current_idx - 1, -1, -1):
            if self.mapping_data[i]['subtitle_id']:
                prev_id = int(self.mapping_data[i]['subtitle_id'])
                break

        # 次のsubtitle_idを探す
        for i in range(current_idx + 1, len(self.mapping_data)):
            if self.mapping_data[i]['subtitle_id']:
                next_id = int(self.mapping_data[i]['subtitle_id'])
                break

        # 現在のsubtitle_idがある場合
        if current_entry['subtitle_id']:
            current_id = int(current_entry['subtitle_id'])

            # 前後の番号との差が20以上ある場合は不適切とみなす
            if((prev_id and abs(current_id - prev_id) > 20) and \
              (next_id and abs(current_id - next_id) > 20)) :

                # 類似度マッチングから適切な候補を探す
                if script_id in self.similar_matches:
                    valid_range = range((prev_id or 0) + 1,
                                        (next_id or prev_id + 100))
                    for subtitle_id, _ in sorted(
                            self.similar_matches[script_id],
                            key=lambda x: x[1],
                            reverse=True):
                        if int(subtitle_id) in valid_range:
                            current_entry['subtitle_id'] = subtitle_id
                            return subtitle_id
                return None
            # print(f"prev:{prev_id},current:{current_id},next:{next_id}")

            return str(current_id)
        return None

    def process_mapping(self) -> None:
        """マッピングデータを処理"""
        for i in range(len(self.mapping_data)):
            valid_id = self.find_valid_subtitle_id(i)
            if valid_id is None:
                self.mapping_data[i]['subtitle_id'] = ''
                self.mapping_data[i]['subtitle'] = ''
                self.mapping_data[i]['start_time'] = ''
                self.mapping_data[i]['end_time'] = ''
            elif not self.mapping_data[i]['subtitle_id']:
                # 新しいsubtitle_idが見つかった場合、対応する字幕情報も更新
                self.mapping_data[i]['subtitle_id'] = valid_id
                self.mapping_data[i]

    def save_mapping(self) -> None:
        """処理したマッピングデータをデータベースに保存"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # マッピング結果を入れるリレーションを作る
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mappings (
                script_id int,
                subtitle_id int);
        ''')

        # まず既存のマッピングをクリアする（必要に応じて）
        cursor.execute('DELETE FROM mappings')
        conn.commit()

        # 新しいマッピングを挿入
        for entry in self.mapping_data:
            if (entry['script_id'] and entry['subtitle_id']):
                cursor.execute(
                    '''
                    INSERT INTO mappings (
                        script_id, subtitle_id
                    ) VALUES (?, ?)
                ''', (
                        entry['script_id'],
                        entry['subtitle_id'],
                    ))

        conn.commit()
        conn.close()


def adjust_mapping_pairs(db_path):
    mapper = SubtitleMapper(db_path)

    # データベースからデータの読み込み
    mapper.load_similar_matches()
    mapper.load_mapping()

    # マッピングの処理
    mapper.process_mapping()

    # データベースに結果を保存
    mapper.save_mapping()


if __name__ == "__main__":
    adjust_mapping_pairs("scripts.db")
