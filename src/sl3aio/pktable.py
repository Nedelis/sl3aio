from dataclasses import dataclass
from operator import attrgetter
from typing import Tuple, Self, Type, Callable, Any
from .table import Table, TableRecord, TableSelectPredicate


@dataclass(slots=True, frozen=True)
class PrimaryKeyTable[KT, VT, TT]:
    table: TT | Table[VT]
    primary_key: str
    primary_key_type: Type[KT] = Any

    def __predicate(self, key: KT) -> TableSelectPredicate:
        return lambda record: getattr(record, self.primary_key) == key
    
    async def contains(self, key: KT) -> bool:
        return await self.table.contains(self.table._record_factory(**{self.primary_key: key}))

    async def get(self, key: KT, *to_get: str) -> TableRecord[VT] | Tuple[VT, ...] | VT | None:
        selected = await self.table.select_one(self.__predicate(key))
        if not (to_get and selected):
            return selected
        return attrgetter(*to_get)(selected) if len(to_get) != 1 else getattr(selected, to_get[0])
    
    async def set(self, key: KT, **to_set: VT) -> Self:
        await self.table.update_one(self.__predicate(key), **to_set)
        return self

    async def add(self, key: KT, save_existing: bool = True, **values: VT) -> Self:
        await self.table.insert(save_existing, **({self.primary_key: key} | values))
        return self
    
    async def remove(self, key: KT) -> Self:
        await self.table.delete_one(self.__predicate(key))

    async def pop(self, key: KT) -> TableRecord[VT] | None:
        return await self.table.pop_one(self.__predicate(key))


def create[KT, VT, TT, T: PrimaryKeyTable](table: TT | Table[VT], primary_key: str, primary_key_type: Type[KT]) -> Callable[[Type[T]], PrimaryKeyTable[KT, VT, TT] | T]:
    def wrapper(cls: Type[T]) -> PrimaryKeyTable[KT, VT, TT]:
        return cls(table, primary_key, primary_key_type)
    return wrapper
