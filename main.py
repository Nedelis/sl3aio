from dataparser import init_inbuilt_parsers
from re import search, IGNORECASE
from table import TableColumn, MemoryTable, MemoizedTable, Executor
from os.path import abspath
from asyncio import run


async def main() -> None:
    init_inbuilt_parsers()
    database = abspath('./database.db')
    table = MemoizedTable(
        'users',
        (
            TableColumn('id INTEGER PRIMARY KEY UNIQUE NOT NULL'),
            TableColumn('name TEXT NOT NULL DEFAULT unknown', 'unknown'),
            TableColumn('jsondata JSON', {})
        ),
        database
    )
    await table.insert(id=20, name='John')
    await table.insert(id=3, name='Sussybaka')
    await table.insert(id=3, name='Abama', jsondata={"keyboard": ["language", "items", "profile"]})
    await table.insert(id=2, name='Niger')
    print(table._records)
    await Executor._instances[database].run()


run(main())


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
