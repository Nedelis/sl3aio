from typing import Any, Dict
import unittest
from dataclasses import dataclass
from src.sl3aio import SolidTable, Parsable
from asyncio import run


class TestSQLTables(unittest.TestCase):
    def test_create_from_db(self):
        self.assertEqual(
            [column.default for column in run(SolidTable.from_database('users', './test/usersdata.db')).columns],
            [None, None, 'en_us', None]
        )

    def test_add_and_select(self):
        table = run(SolidTable.from_database('users', './test/usersdata.db'))
        record = table._record_factory(id=3154135, name='John Smith')
        run(table.insert(id=3154135, name='John Smith'))
        self.assertEqual(
            record,
            run(table.select_one(lambda record: record.id == 3154135))
        )
        run(table.delete())

    def test_parsable(self):
        @dataclass(slots=True)
        class UserData(Parsable):
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
        
        table = run(SolidTable.from_database('users', './test/usersdata.db'))
        extra_data = UserData('I\'m the best actor in the world!')
        run(table.insert(
            id=3154135,
            name='John Smith',
            extra_data=extra_data
        ))
        self.assertEqual(
            extra_data,
            run(table.select_one(lambda record: record.id == 3154135)).extra_data
        )
        run(table.delete())


if __name__ == '__main__':
    unittest.main()
