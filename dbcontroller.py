from dataclasses import dataclass, field, InitVar
from typing import Dict, Tuple, Literal, Hashable, List, Callable, NoReturn, Self, Coroutine, Any
from asyncio import Queue
from sqlite3 import connect as connect_to_db, Cursor as DBCursor, OperationalError

type DBDataType = str | int | float | None
type DBDataTypeAlias = Literal['INT', 'REAL', 'TEXT', 'NULL']
type TableRecord = Dict[str, DBDataType]
type TableChoicePredicate = Callable[[TableRecord], bool]


def data_to_str(value: DBDataType) -> str:
    if value is None:
        return 'NULL'
    elif isinstance(value, str):
        return f'"{value.replace('"', '""') if '"' in value else value}"'
    return str(value)


def record_set_str(record: TableRecord, separator: str = '') -> str:
    return separator.join(
        f'{col_name} = {data_to_str(col_value)}'
        for col_name, col_value in record.items()
    )


@dataclass(slots=True, frozen=True)
class SQLiteRequestQuery:
    dbpath: str
    _query: Queue[str] = field(init=False, default_factory=Queue)

    def execute(self, sql: str, exec_now: bool = False) -> DBCursor | None:
        if exec_now:
            with connect_to_db(self.dbpath) as conn:
                return conn.execute(sql)
        self._query.put_nowait(sql)

    async def update(self) -> Coroutine[Any, Any, None]:
        while True:
            request = await self._query.get()
            with connect_to_db(self.dbpath) as conn:
                try:
                    conn.execute(request)
                except OperationalError:
                    print(request)


@dataclass(slots=True, frozen=True)
class TableColumn:
    col_id: int
    name: str
    dtype: DBDataTypeAlias
    nullable: Hashable = 1
    default: DBDataType = None
    primary: Hashable = 0
    table_creation_str: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, 'table_creation_str', ' '.join(filter(None, (
            self.name,
            self.dtype,
            'PRIMARY KEY' if self.primary else '',
            '' if self.nullable else 'NOT NULL',
            '' if self.default is None else f'DEFAULT {data_to_str(self.default)}'
        ))))


@dataclass(slots=True, frozen=True)
class Table:
    name: str
    _request_query: SQLiteRequestQuery
    _columns: Dict[str, TableColumn]
    _records: List[TableRecord] = field(default_factory=list)
    primary_key: str | None = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, 'primary_key', next((column.name for column in self.columns if column.primary), None))
    
    @classmethod
    def from_query(cls, name: str, request_query: SQLiteRequestQuery) -> Self:
        columns = {column[1]: TableColumn(*column) for column in request_query.execute('''SELECT * FROM pragma_table_info("%s")''' % name, True)}
        return cls(
            name,
            request_query,
            columns,
            [dict(zip(columns, values)) for values in request_query.execute('''SELECT * FROM %s''' % name, True)]
        )

    @property
    def columns(self):
        return self._columns.values()
    
    @property
    def columns_names(self):
        return self._columns.keys()

    def contains_record(self, record: TableRecord) -> bool:
        return record in self._records \
               if self.primary_key is None else \
               record[self.primary_key] in (rec[self.primary_key] for rec in self._records)
    
    def get_first_record(self, predicate: TableChoicePredicate | None = None) -> TableRecord | None:
        return self._records[0] \
               if predicate is None and self._records else \
               next((value for value in self._records if predicate(value)), None)

    def get_records(self, predicate: TableChoicePredicate | None = None) -> List[TableRecord]:
        return self._records \
               if predicate is None else \
               [value for value in self._records if predicate(value)]
    
    def set_records(self, predicate: TableChoicePredicate | None = None, to_set: TableRecord | None = None) -> None | NoReturn:
        if to_set is None:
            return self.remove_records(predicate)
        elif self.primary_key in to_set:
            return self.add_record(to_set)
        elif not (selected := self.get_records(predicate)):
            return None
        for rec in selected:
            rec.update(to_set)
        if len(selected) == 1:
            rec = selected[0]
            self._request_query.execute(
                ('''UPDATE %s SET %s WHERE %s = %s''' % (
                    self.name,
                    record_set_str(to_set, ', '),
                    self.primary_key,
                    data_to_str(rec[self.primary_key])
                ))
                if self.primary_key is not None else
                ('''UPDATE %s SET %s WHERE %s''' % (
                    self.name,
                    record_set_str(to_set, ', '),
                    ' AND '.join(f'{col_name} = {rec[col_name]}' for col_name in self.columns_names)
                ))
            )
            return
        self._request_query.execute(
            ('''UPDATE %s SET %s WHERE %s IN (%s)''' % (
                self.name,
                record_set_str(to_set, ', '),
                self.primary_key,
                ', '.join(data_to_str(rec[self.primary_key]) for rec in selected)
            ))
            if self.primary_key is not None else
            ('''UPDATE %s SET %s WHERE %s''' % (
                self.name,
                record_set_str(to_set, ', '),
                ' AND '.join(
                    f'{col_name} IN ({", ".join(data_to_str(rec[col_name]) for rec in selected)})'
                    for col_name in self.columns_names
                )
            ))
        )

    def remove_records(self, predicate: TableChoicePredicate | None = None) -> None | NoReturn:
        if predicate is None:
            self._request_query.execute('''DELETE FROM %s''' % self.name)
            return self._records.clear()
        for record in (selected := self.get_records(predicate)):
            self._records.remove(record)
        self._request_query.execute(
            ('''DELETE FROM %s WHERE %s IN (%s)''' % (
                self.name,
                self.primary_key,
                ', '.join(data_to_str(rec[self.primary_key]) for rec in selected)
            ))
            if self.primary_key is not None else
            ('''DELETE FROM %s WHERE %s''' % (
                self.name,
                ' AND '.join(
                    f'{col_name} IN ({", ".join(rec[col_name] for rec in selected)})'
                    for col_name in self.columns_names
                )
            ))
        )
    
    def add_record(self, to_add: TableRecord) -> None | NoReturn:
        record = {
            name: to_add.get(name, column.default)
            for name, column in self._columns.items()
        }
        self._records.append(record)
        self._request_query.execute('''INSERT OR REPLACE INTO %s (%s) VALUES (%s)''' % (
            self.name,
            ', '.join(record),
            ', '.join(data_to_str(value) for value in record.values())
        ))

    def create(self) -> None | NoReturn:
        self._request_query.execute('''CREATE TABLE IF NOT EXISTS %s(%s)''' % (
            self.name,
            ', '.join(column.table_creation_str for column in self.columns)
        ))
        if not self._records:
            return
        self._request_query.execute('''INSERT OR REPLACE INTO %s (%s) VALUES %s''' % (
            self.name,
            ', '.join(self.columns_names),
            ', '.join('(%s)' % ', '.join(map(data_to_str, rec.values())) for rec in self._records)
        ))

    def drop(self) -> None | NoReturn:
        self._request_query.execute('''DROP TABLE IF EXISTS %s''' % self.name)


@dataclass(slots=True, frozen=True)
class DBController:
    dbpath: InitVar[str]
    request_query: SQLiteRequestQuery = field(init=False)
    _data: Dict[str, Table] = field(init=False, default_factory=dict)

    def __post_init__(self, dbpath: str) -> None:
        object.__setattr__(self,'request_query', SQLiteRequestQuery(dbpath))
        tables: Tuple[str] = sum(self.request_query.execute('''SELECT name FROM sqlite_master WHERE type="table"''', True), tuple())
        for table in tables:
            self._data[table] = Table.from_query(table, self.request_query)
    
    def get_table(self, name: str) -> Table | NoReturn:
        return self._data[name]
    
    def put_table(self, table: Table) -> None | NoReturn:
        table.create()
        self._data[table.name] = table

    def drop_table(self, name: str) -> None | NoReturn:
        self._data[name].drop()
        self._data.pop(name)

    def contains_record(self, table_name: str, record: TableRecord) -> bool:
        return self._data[table_name].contains_record(record)

    def get_first_record(self, table_name: str, predicate: TableChoicePredicate | None = None) -> TableRecord | None | NoReturn:
        return self._data[table_name].get_first_record(predicate)

    def get_records(self, table_name: str, predicate: TableChoicePredicate | None = None) -> List[TableRecord] | NoReturn:
        return self._data[table_name].get_records(predicate)
    
    def add_record(self, table_name, to_add: TableRecord) -> None | NoReturn:
        self._data[table_name].add_record(to_add)
    
    def set_records(self, table_name: str, predicate: TableChoicePredicate | None = None, to_set: TableRecord | None = None) -> None | NoReturn:
        self._data[table_name].set_records(predicate, to_set)

    def remove_records(self, table_name: str, predicate: TableChoicePredicate | None = None) -> None | NoReturn:
        self._data[table_name].remove_records(predicate)
