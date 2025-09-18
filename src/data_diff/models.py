from __future__ import annotations

import dataclasses
from typing import Any, Protocol


class DatabaseConnector(Protocol):
    def query(self, query: str) -> Any: ...


@dataclasses.dataclass
class Context:
    connection: DatabaseConnector
    dialect: str


@dataclasses.dataclass(frozen=True, slots=True)
class Column:
    name: str
    ordinal_position: int
    data_type: str


@dataclasses.dataclass(frozen=True, slots=True)
class Table:
    database: str
    schema: str
    name: str
    identifier: str

    @classmethod
    def from_identifier(cls, identifier: str) -> Table:
        parts = identifier.strip().split(".")
        if len(parts) != 3:  # noqa: PLR2004
            raise ValueError(f"Invalid table identifier: {identifier}")

        return cls(
            database=parts[0],
            schema=parts[1],
            name=parts[2],
            identifier=identifier,
        )
