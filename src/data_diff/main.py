from __future__ import annotations

import dataclasses
import pathlib

from google.cloud import bigquery

from data_diff import queries

SUCCESS = 0
FAILURE = 1
HERE = pathlib.Path(__file__).parent
QUERIES = HERE / "queries"


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


def get_columns(conn: bigquery.Client, table: Table) -> dict[str, Column]:
    query = queries.get_columns_query(
        database=table.database,
        schema=table.schema,
        table=table.name,
    )

    return {
        row.column_name: Column(
            name=row.column_name,
            ordinal_position=row.ordinal_position,
            data_type=row.data_type,
        )
        for row in conn.query(query).result()
    }


def get_row_count(conn: bigquery.Client, table: Table) -> int:
    query = queries.get_row_count_query(identifier=table.identifier)
    (result,) = list(conn.query(query).result())

    return int(result.row_count)


def get_summary_mismatches(
    conn: bigquery.Client,
    identifier_1: str,
    identifier_2: str,
    primary_keys: list[str],
    columns: list[str],
) -> list:
    query = ";\n\n".join(
        [
            queries.create_temp_table_query(
                identifier_1=identifier_1,
                identifier_2=identifier_2,
                primary_keys=primary_keys,
                columns=columns,
            ),
            queries.compare_summary_query(
                columns=columns,
            ),
        ]
    )

    return list(conn.query(query).result())


def get_detailed_mismatches(
    conn: bigquery.Client,
    identifier_1: str,
    identifier_2: str,
    primary_keys: list[str],
    columns: list[str],
) -> list:
    query = ";\n\n".join(
        [
            queries.create_temp_table_query(
                identifier_1=identifier_1,
                identifier_2=identifier_2,
                primary_keys=primary_keys,
                columns=columns,
            ),
            queries.compare_detail_query(
                primary_keys=primary_keys,
                columns=columns,
            ),
        ]
    )

    return list(conn.query(query).result())


def main(
    table_1_id: str,
    table_2_id: str,
    primary_keys: list[str],
) -> int:
    client = bigquery.Client()
    table_1 = Table.from_identifier(table_1_id)
    table_2 = Table.from_identifier(table_2_id)

    # Step 1: compare column schema
    table_1_columns = get_columns(client, table_1)
    table_2_columns = get_columns(client, table_2)
    if table_1_columns != table_2_columns:
        print("Column schemas do not match.")
        print(f"Table 1 columns: {table_1_columns}")
        print(f"Table 2 columns: {table_2_columns}")
        return FAILURE

    # Step 2: compare row counts
    table_1_row_count = get_row_count(client, table_1)
    table_2_row_count = get_row_count(client, table_2)
    if table_1_row_count != table_2_row_count:
        print("Row counts do not match.")
        print(f"Table 1 row count: {table_1_row_count}")
        print(f"Table 2 row count: {table_2_row_count}")
        return FAILURE

    # Step 3: compare high-level mismatches
    summary_mismatches = get_summary_mismatches(
        client,
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

    detailed_mismatches = get_detailed_mismatches(
        client,
        table_1.identifier,
        table_2.identifier,
        primary_keys,
        list(table_1_columns),
    )
    details_file = pathlib.Path("mismatches.csv")
    details_file.write_text(
        "\n".join(detailed_mismatches),
        encoding="utf-8",
    )

    return FAILURE
