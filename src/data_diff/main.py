from __future__ import annotations

import contextlib
import csv
import pathlib
from collections.abc import Generator
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from _typeshed.dbapi import DBAPICursor

from google.cloud.bigquery import dbapi

from data_diff import models, queries

SUCCESS = 0
FAILURE = 1
HERE = pathlib.Path(__file__).parent
QUERIES = HERE / "queries"


def _parse_columns(description: Any) -> dict:
    return {col.name: pos for pos, col in enumerate(description)}


def get_columns(
    ctx: models.Context,
    table: models.Table,
) -> dict[str, models.Column]:
    query = queries.get_columns_query(
        dialect=ctx.source,
        database=table.database,
        schema=table.schema,
        table=table.name,
    )
    ctx.cursor.execute(query)
    columns = _parse_columns(ctx.cursor.description)

    return {
        row[columns["column_name"]]: models.Column(
            name=row[columns["column_name"]],
            ordinal_position=row[columns["ordinal_position"]],
            data_type=row[columns["data_type"]],
        )
        for row in ctx.cursor.fetchall()
    }


def get_row_count(ctx: models.Context, table: models.Table) -> int:
    query = queries.get_row_count_query(
        dialect=ctx.source,
        identifier=table.identifier,
    )
    ctx.cursor.execute(query)
    (result,) = ctx.cursor.fetchall()

    return int(result[0])


def get_summary_mismatches(
    ctx: models.Context,
    identifier_1: str,
    identifier_2: str,
    primary_keys: list[str],
    columns: list[str],
) -> list:
    query = ";\n\n".join(
        [
            queries.create_temp_table_query(
                dialect=ctx.source,
                identifier_1=identifier_1,
                identifier_2=identifier_2,
                primary_keys=primary_keys,
                columns=columns,
            ),
            queries.compare_summary_query(
                dialect=ctx.source,
                columns=columns,
            ),
        ]
    )
    ctx.cursor.execute(query)

    return list(ctx.cursor.fetchall())


def get_detailed_mismatches(
    ctx: models.Context,
    identifier_1: str,
    identifier_2: str,
    primary_keys: list[str],
    columns: list[str],
) -> tuple[list, list]:
    query = ";\n\n".join(
        [
            queries.create_temp_table_query(
                dialect=ctx.source,
                identifier_1=identifier_1,
                identifier_2=identifier_2,
                primary_keys=primary_keys,
                columns=columns,
            ),
            queries.compare_detail_query(
                dialect=ctx.source,
                primary_keys=primary_keys,
                columns=columns,
            ),
        ]
    )
    ctx.cursor.execute(query)

    return (
        list(_parse_columns(ctx.cursor.description).keys()),
        list(ctx.cursor.fetchall()),
    )


@contextlib.contextmanager
def get_cursor() -> Generator[DBAPICursor]:
    connection = dbapi.Connection()
    cursor = connection.cursor()

    yield cursor  # type: ignore

    cursor.close()
    connection.close()


def main(
    source: str,
    table_1_id: str,
    table_2_id: str,
    primary_keys: list[str],
) -> int:
    assert source == "bigquery", "Only BigQuery is supported currently."  # noqa: S101

    with get_cursor() as conn:
        context = models.Context(
            cursor=conn,
            source="bigquery",
        )
        table_1 = models.Table.from_identifier(table_1_id)
        table_2 = models.Table.from_identifier(table_2_id)

        # Step 1: compare column schema
        table_1_columns = get_columns(context, table_1)
        table_2_columns = get_columns(context, table_2)
        if table_1_columns != table_2_columns:
            print("Column schemas do not match.")
            print(f"Table 1 columns: {table_1_columns}")
            print(f"Table 2 columns: {table_2_columns}")
            return FAILURE

        # Step 2: compare row counts
        table_1_row_count = get_row_count(context, table_1)
        table_2_row_count = get_row_count(context, table_2)
        if table_1_row_count != table_2_row_count:
            print("Row counts do not match.")
            print(f"Table 1 row count: {table_1_row_count}")
            print(f"Table 2 row count: {table_2_row_count}")
            return FAILURE
        print(f"Row count: {table_1_row_count:,}")

        # Step 3: compare high-level mismatches
        summary_mismatches = get_summary_mismatches(
            context,
            table_1.identifier,
            table_2.identifier,
            primary_keys,
            list(table_1_columns),
        )
        (summary,) = summary_mismatches
        print("\n\nMismatch summary:")
        width = 2 + max(len(col) for col in list(table_1_columns))
        for col in list(table_1_columns):
            print(f"{col:>{width}}: {getattr(summary, f'{col}__mismatches')}")

        # Step 4: compare detailed mismatches
        if all(col == 0 for col in summary_mismatches if col != "records"):
            return SUCCESS

        headers, detailed_mismatches = get_detailed_mismatches(
            context,
            table_1.identifier,
            table_2.identifier,
            primary_keys,
            list(table_1_columns),
        )
        details_file = pathlib.Path("mismatches.csv")
        with open(details_file, "w") as out:
            csv_out = csv.writer(out)
            csv_out.writerow(headers)
            for row in detailed_mismatches:
                csv_out.writerow(row)

        return FAILURE
