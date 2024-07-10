import unittest
from src.sl3aio import SolidTable, TableColumn, Parser
from sqlite3 import connect
from asyncio import run


class TestSQLTables(unittest.TestCase):
    def test_create_from_db(self):
        # self.assertEqual(
        #     [None, 'MAIN', 'en_us', '1024.0', '1024', '0'],
        #     [
        #         Parser.get_by_type(type).loads(default) if default is not None else default
        #         for type, default in connect('./test/usersdata.db').execute('SELECT type, dflt_value FROM pragma_table_info("users")')
        #     ]
        # )
        self.assertEqual(
            [column.default for column in run(SolidTable.from_database('users', './test/usersdata.db')).columns],
            [None, 'MAIN', 'en_us', 1024.0, 1024.0, 0]
        )


if __name__ == '__main__':
    unittest.main()
