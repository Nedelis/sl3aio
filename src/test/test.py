from functools import partial
import os
from random import randint
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
from typing import Any, Dict
from dataclasses import dataclass
from sqlite3 import PARSE_DECLTYPES, adapters, converters, PrepareProtocol
from sl3aio import SolidTable, TableColumnValueGenerator, Parser, BuiltinParsers, ConnectionManager, EasyTable, allowed_typenames, Connector

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

    # async def test_parsable(self):
    #     BuiltinParsers.init()
    #     _id = -1
    #     TableColumnValueGenerator('id_increment', lambda: _id + 1).register()

    #     @dataclass(slots=True)
    #     class UserData:
    #         bio: str | None = None
    #         email: str | None = None
    #         number: str | None = None

    #         @classmethod
    #         def from_data(cls, value: str):
    #             return cls(**BuiltinParsers.DICT.loads(value))
            
    #         def to_data(self) -> str:
    #             return BuiltinParsers.DICT.dumps({
    #                 'bio': self.bio,
    #                 'email': self.email,
    #                 'number': self.number
    #             })

    #     extra_data = UserData('I\'m the best actor in the world!')
    #     print(Parser.from_parsable(UserData).register(), Parser.instances)
    #     print(allowed_typenames())
    #     print(Parser.get_by_typename('userdata'))
    #     async with ConnectionManager(Connector(TEST_DB, detect_types=PARSE_DECLTYPES)) as conn:
    #         table = EasyTable(await SolidTable.from_database('users', conn))
    #     await table.insert(
    #         id=3154135,
    #         name='John Smith',
    #         extra_data=extra_data
    #     )
    #     print((await (table.name[0] == 'J').select_one()).extra_data)
    #     self.assertEqual(
    #         extra_data,
    #         (await (table.name[0] == 'J').select_one()).extra_data
    #     )
    #     await table.delete()

    async def test_column_value_generator(self):
        TableColumnValueGenerator('id_increment', partial(randint, 0, 2 ** 32)).register()
        async with ConnectionManager(Connector(TEST_DB, detect_types=PARSE_DECLTYPES)) as conn:
            table: EasyTable[int | str] = EasyTable(await SolidTable.from_database('users', conn))
            rec1 = await table.insert(name='Supersus')
            rec2 = await table.insert(name='Supersus2')
            print(rec1, rec2)
            self.assertEqual(
                rec1,
                (await (table.id == rec1.id).select_one())
            )
            await table.delete()


if __name__ == '__main__':
    unittest.main()
