import sqlite3
conn = sqlite3.connect('db.sqlite3')
cursor = conn.cursor()
cursor.execute('select ticker from acoes_asset limit 5')
print(cursor.fetchall())
conn.close()
