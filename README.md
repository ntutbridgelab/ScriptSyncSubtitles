## 台本を使って字幕修正

1. 台本をCSV形式にする
2. 台本をLLMで字幕に合うように長さ修正（LLMを使う）
3. 音声認識で作られた字幕を修正（LLMを使う）
4. 2と3で作ったデータをそれぞれデータベースに格納
    DONE（ただし現段階でembeddingが消えているが、いくつかのリレーションには前のバージョンが残っていて、一貫していない状況）
5. FAISSで字幕ごとにembedding(vector)を作成
　　```
   % python3 add_faiss_index.py
   ```
   DONE（enbeddingが作り直されている）
   * このバージョンでdialogをtableからviewに作り替えた
7. vectorを使って字幕と台本の類似検索をし、類似度が高いペアを見つける