from sqlite3 import connect


with connect('D:\\Code Projects\\archiver-bot\\resources\\usersdata.db') as conn:
    print(conn.execute('SELECT dflt_value FROM pragma_table_info("users")').fetchall())
