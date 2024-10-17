import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
from typing import Any, Dict
from dataclasses import dataclass
from sqlite3 import PARSE_DECLTYPES
from sl3aio import SolidTable, TableColumnValueGenerator, Parser, BuiltinParser, ConnectionManager

TEST_DB = './src/test/usersdata.db'


class TestSQLTables(unittest.IsolatedAsyncioTestCase):
    # async def test_create_from_db(self):
    #     self.assertEqual(
    #         [column.default for column in (await SolidTable.from_database('users', './src/test/usersdata.db')).columns],
    #         [None, None, 'en_us', None]
    #     )

    # async def test_add_and_select(self):
    #     table = await SolidTable.from_database('users', TEST_DB)
    #     print(table.columns[0].sql)
    #     async with table:
    #         record = await table.make_record(id=3154135, name='John Smith')
    #         await table.insert(id=3154135, name='John Smith')
    #         self.assertEqual(
    #             record,
    #             await table.select_one(lambda record, _: record.id == 3154135)
    #         )
    #         await table.delete()

    async def test_parsable(self):
        BuiltinParser.init()
        TableColumnValueGenerator(lambda _, prev: prev + 1, -1, 'id_increment').register()

        @dataclass(slots=True)
        class UserData:
            bio: str | None = None
            email: str | None = None
            number: str | None = None

            @classmethod
            def from_data(cls, value: str):
                return cls(**BuiltinParser.DICT.loads(value))
            
            def to_data(self) -> str:
                return BuiltinParser.DICT.dumps({
                    'bio': self.bio,
                    'email': self.email,
                    'number': self.number
                })

        extra_data = UserData('I\'m the best actor in the world!')
        print(Parser.from_parsable(UserData), Parser.registry)
        print(Parser.get_by_typename('userdata'))
        async with ConnectionManager(TEST_DB, detect_types=PARSE_DECLTYPES) as conn:
            async with await SolidTable.from_database('users', conn) as table:
                await table.insert(
                    id=3154135,
                    name='John Smith',
                    extra_data=extra_data
                )
            print(await table.select_one(lambda record, _: record.id == 3154135))
            self.assertEqual(
                extra_data,
                (await table.select_one(lambda record, _: record.id == 3154135)).extra_data
            )
            await table.delete()

    # async def test_column_value_generator(self):
    #     TableColumnValueGenerator(lambda _, prev: prev + 1, -1, 'id_increment').register()
    #     async with await SolidTable.from_database('users', TEST_DB) as table:
    #         rec1 = await table.insert(name='Supersus')
    #         rec2 = await table.insert(name='Supersus2')
    #         print(rec1, rec2)
    #         print([rec async for rec in table.select()])
    #         self.assertEqual(
    #             0,
    #             (await table.select_one(lambda record, _: record.id == 0)).id
    #         )
    #         await table.delete()


if __name__ == '__main__':
    unittest.main()
