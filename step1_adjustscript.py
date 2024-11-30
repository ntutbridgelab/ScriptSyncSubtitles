import sys
from src.genscriptdb import generate_script_table

db_path = "scripts.db"  # データベースのパス

if __name__ == "__main__":
  if len(sys.argv) < 2:
      print(f"Usage: {sys.argv[0]} <scriptfile.csv>")
  else:
      generate_script_table(sys.argv[1],db_path)