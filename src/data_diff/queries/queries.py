import functools
import pathlib
from collections.abc import Callable
from typing import Any

import jinja2

HERE = pathlib.Path(__file__).parent
DEFAULT_DIALECT = "_default"
DIALECT = "bigquery"
COMPILED = ".compiled"  # relative to CWD

QueryCallable = Callable[..., str]


def save_to_file(*, filename: str) -> Callable:
    def decorator(func: QueryCallable) -> QueryCallable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> str:
            query = func(*args, **kwargs)
            filepath = pathlib.Path(COMPILED) / filename
            filepath.parent.mkdir(parents=True, exist_ok=True)
            filepath.write_text(query, "utf-8")
            return query

        return wrapper

    return decorator


def read_query(dialect: str, name: str, params: dict) -> str:
    try:
        query = (HERE / dialect / name).read_text("utf-8")
    except FileNotFoundError:
        query = (HERE / DEFAULT_DIALECT / name).read_text("utf-8")

    return jinja2.Template(query).render(params)


@save_to_file(filename="get-columns.sql")
def get_columns_query(
    dialect: str, database: str, schema: str, table: str
) -> str:
    return read_query(
        dialect=dialect,
        name="get-columns.sql",
        params={
            "database": database,
            "schema": schema,
            "table": table,
        },
    )


@save_to_file(filename="get-row-count.sql")
def get_row_count_query(dialect: str, identifier: str) -> str:
    return read_query(
        dialect=dialect,
        name="get-row-count.sql",
        params={
            "identifier": identifier,
        },
    )


@save_to_file(filename="create-temp-table.sql")
def create_temp_table_query(
    dialect: str,
    identifier_1: str,
    identifier_2: str,
    primary_keys: list[str],
    columns: list[str],
) -> str:
    return read_query(
        dialect=dialect,
        name="create-temp-table.sql",
        params={
            "identifier_1": identifier_1,
            "identifier_2": identifier_2,
            "primary_keys": primary_keys,
            "columns": columns,
        },
    )


@save_to_file(filename="compare-summary.sql")
def compare_summary_query(dialect: str, columns: list[str]) -> str:
    return read_query(
        dialect=dialect,
        name="compare-summary.sql",
        params={
            "columns": columns,
        },
    )


@save_to_file(filename="compare-detail.sql")
def compare_detail_query(
    dialect: str, primary_keys: list[str], columns: list[str]
) -> str:
    return read_query(
        dialect=dialect,
        name="compare-detail.sql",
        params={
            "primary_keys": primary_keys,
            "columns": columns,
        },
    )
