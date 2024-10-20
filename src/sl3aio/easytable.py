from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, Self
from .table import Table, TableRecord


@dataclass(slots=True)
class _EasySelector[T]:
    _table: Table[T]
    _selector: Callable[[bool, Any], tuple[bool, Any]]
    _select_first: bool = False

    def first(self) -> Self:
        return replace(self, _select_first=True)
