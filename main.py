from sqlite3 import connect, Row
from re import search, IGNORECASE
from dataparser import JSON, INTEGER, DATE, date

INNER_TABLES = 'sqlite_master', 'sqlite_sequence', 'sqlite_stat1', 'sqlite_stat2', 'sqlite_stat3', 'sqlite_stat4'


def extract_columns_from_create_table(sql):
    if (match := search(r'CREATE TABLE\s+\w+\s*\((.*)\)', sql, IGNORECASE)):
        return tuple(col.strip() for col in match.group(1).split(','))
    return None

# with connect('database.db') as conn:
#     conn.execute('UPDATE users SET name = ? WHERE id = ?', ('tilibom', 1))
    
    # tables = [
    #     Table(name, tuple(map(TableColumn, extract_columns_from_create_table(sql))))
    #     for name, sql in conn.execute(
    #         'SELECT name, sql FROM sqlite_master WHERE type = "table" AND name NOT IN (' + ', '.join('?' * len(INNER_TABLES)) + ')',
    #         INNER_TABLES
    #     ).fetchall()
    # ]
    # print(tables[0]._columns)
#     conn.execute(
#         f'INSERT INTO users (name, superstuff, regdate) VALUES (?, ?, ?)',
#         ('John',
#          JSON.dumps({'password': hash('qwerty123456')}),
#          DATE.dumps(date.today()))
#     )
#     print(DATE.loads(conn.execute('SELECT * FROM users WHERE id = 1').fetchone()[-1]))
