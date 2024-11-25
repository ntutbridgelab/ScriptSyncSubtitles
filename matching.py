import sqlite3

db_path = 'scripts.sql'

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute(f"SELECT {id_column}, embedding FROM {table_name} ORDER BY {id_column}")