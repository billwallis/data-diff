import functools
import pathlib
from collections.abc import Callable
from typing import Any

import jinja2

HERE = pathlib.Path(__file__).parent
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


def read_query(name: str, params: dict) -> str:
    query = (HERE / DIALECT / name).read_text("utf-8")

    return jinja2.Template(query).render(params)


@save_to_file(filename="get-columns.sql")
def get_columns_query(database: str, schema: str, table: str) -> str:
    return read_query(
        "get-columns.sql",
        params={
            "database": database,
            "schema": schema,
            "table": table,
        },
    )


@save_to_file(filename="get-row-count.sql")
def get_row_count_query(identifier: str) -> str:
    return read_query(
        "get-row-count.sql",
        params={
            "identifier": identifier,
        },
    )


@save_to_file(filename="create-temp-table.sql")
def create_temp_table_query(
    identifier_1: str,
    identifier_2: str,
    primary_keys: list[str],
    columns: list[str],
) -> str:
    return read_query(
        "create-temp-table.sql",
        params={
            "identifier_1": identifier_1,
            "identifier_2": identifier_2,
            "primary_keys": primary_keys,
            "columns": columns,
        },
    )


@save_to_file(filename="compare-summary.sql")
def compare_summary_query(columns: list[str]) -> str:
    return read_query(
        "compare-summary.sql",
        params={
            "columns": columns,
        },
    )


@save_to_file(filename="compare-detail.sql")
def compare_detail_query(primary_keys: list[str], columns: list[str]) -> str:
    return read_query(
        "compare-detail.sql",
        params={
            "primary_keys": primary_keys,
            "columns": columns,
        },
    )
