from typing import Any, Dict
import unittest
from dataclasses import dataclass
from src.sl3aio import parsable, MemoizedTable, SolidTable, TableColumnValueGenerator, Parser
from asyncio import run


class TestSQLTables(unittest.TestCase):
    def test_create_from_db(self):
        self.assertEqual(
            [column.default for column in run(SolidTable.from_database('users', './usersdata.db')).columns],
            [None, None, 'en_us', None]
        )

    def test_add_and_select(self):
        table = run(MemoizedTable.from_database('users', './usersdata.db'))
        record = table.record(id=3154135, name='John Smith')
        print(table.columns[0].sql)
        run(table.insert(id=3154135, name='John Smith'))
        self.assertEqual(
            record,
            run(table.select_one(lambda record, _: record.id == 3154135))
        )
        run(table.delete())

    def test_parsable(self):
        TableColumnValueGenerator(lambda _, prev: prev + 1, -1, 'id_increment').register()

        @parsable
        @dataclass(slots=True)
        class UserData:
            bio: str | None = None
            email: str | None = None
            number: str | None = None

            @classmethod
            def fromdict[T](cls: type[T], value: Dict[str, Any]) -> T:
                return cls(**value)
            
            def asdict(self) -> Dict[str, Any]:
                return {
                    'bio': self.bio,
                    'email': self.email,
                    'number': self.number
                }

        print(Parser.registry)
        table = run(SolidTable.from_database('users', './usersdata.db'))
        extra_data = UserData('I\'m the best actor in the world!')
        run(table.insert(
            id=3154135,
            name='John Smith',
            extra_data=extra_data
        ))
        self.assertEqual(
            extra_data,
            run(table.select_one(lambda record, _: record.id == 3154135)).extra_data
        )
        run(table.delete())

    def test_column_value_generator(self):
        tcvg = TableColumnValueGenerator(lambda _, prev: prev + 1, -1, 'id_increment').register()
        table = run(SolidTable.from_database('users', './usersdata.db'))
        run(table.insert(name='Supersus'))
        run(table.insert(name='Supersus2'))
        self.assertEqual(
            0,
            run(table.select_one(lambda record, _: record.id == 0)).id
        )
        run(table.delete())


if __name__ == '__main__':
    unittest.main()
