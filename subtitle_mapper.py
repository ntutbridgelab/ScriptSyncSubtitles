import csv
from typing import Dict, List, Optional, Tuple

class SubtitleMapper:
    def __init__(self):
        self.similar_matches: Dict[str, List[Tuple[str, float]]] = {}
        self.mapping_data: List[dict] = []

    def load_similar_matches(self, filename: str) -> None:
        """類似度マッチングデータを読み込む"""
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # ヘッダーをスキップ
            for row in reader:
                script_id = row[0]
                subtitle_id = row[1]
                similarity = float(row[4])

                if script_id not in self.similar_matches:
                    self.similar_matches[script_id] = []
                self.similar_matches[script_id].append((subtitle_id, similarity))

    def load_mapping(self, filename: str) -> None:
        """現在のマッピングデータを読み込む"""
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            for row in reader:
                if len(row) == 7:  # 完全なデータの行
                    self.mapping_data.append({
                        'script_id': row[0],
                        'speaker': row[1],
                        'description': row[2],
                        'subtitle_id': row[3],
                        'subtitle': row[4],
                        'start_time': row[5],
                        'end_time': row[6]
                    })
                else:  # subtitle_idがない行
                    self.mapping_data.append({
                        'script_id': row[0],
                        'speaker': row[1],
                        'description': row[2],
                        'subtitle_id': '',
                        'subtitle': '',
                        'start_time': '',
                        'end_time': ''
                    })
        
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

                    
            print(f"prev:{prev_id} current:{current_id} next:{next_id}")
            # 前後の番号との差が50以上ある場合は不適切とみなす
            if((prev_id and abs(current_id - prev_id) > 50) and \
              (next_id and abs(current_id - next_id) > 50)) :
            # if (prev_id and prev_id > current_id) or \
            #    (next_id and next_id < current_id):
                print(f"->prev:{prev_id} current:{current_id} next:{next_id}")
                # 類似度マッチングから適切な候補を探す
                if script_id in self.similar_matches:
                    valid_range = range(prev_id or 0, (next_id or prev_id + 100) + 1)
                    for subtitle_id, _ in sorted(self.similar_matches[script_id], 
                                              key=lambda x: x[1], reverse=True):
                        if int(subtitle_id) in valid_range:
                            print(subtitle_id)
                            current_entry['subtitle_id'] = subtitle_id
                            return subtitle_id
                    print("None")
                return None
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

    def save_mapping(self, filename: str) -> None:
        """処理したマッピングデータを保存"""
        headers = ['script_id', 'speaker', 'description', 'subtitle_id', 
                  'subtitle', 'start_time', 'end_time']

        with open(filename, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for entry in self.mapping_data:
                writer.writerow([
                    entry['script_id'],
                    entry['speaker'],
                    entry['description'],
                    entry['subtitle_id'],
                    entry['subtitle'],
                    entry['start_time'],
                    entry['end_time']
                ])

def main():
    mapper = SubtitleMapper()

    # データの読み込み
    mapper.load_similar_matches('subtitle_script_similar_matching.csv')
    mapper.load_mapping('subtitle_script_mapping.csv')

    # マッピングの処理
    mapper.process_mapping()

    # 結果の保存
    mapper.save_mapping('subtitle_script_mapping_processed.csv')

if __name__ == "__main__":
    main()