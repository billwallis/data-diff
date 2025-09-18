import pathlib

from google.cloud import bigquery

from data_diff import models, queries

SUCCESS = 0
FAILURE = 1
HERE = pathlib.Path(__file__).parent
QUERIES = HERE / "queries"


def get_columns(
    ctx: models.Context,
    table: models.Table,
) -> dict[str, models.Column]:
    query = queries.get_columns_query(
        dialect=ctx.dialect,
        database=table.database,
        schema=table.schema,
        table=table.name,
    )

    return {
        row.column_name: models.Column(
            name=row.column_name,
            ordinal_position=row.ordinal_position,
            data_type=row.data_type,
        )
        for row in ctx.connection.query(query).result()
    }


def get_row_count(ctx: models.Context, table: models.Table) -> int:
    query = queries.get_row_count_query(
        dialect=ctx.dialect,
        identifier=table.identifier,
    )
    (result,) = list(ctx.connection.query(query).result())

    return int(result.row_count)


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
                dialect=ctx.dialect,
                identifier_1=identifier_1,
                identifier_2=identifier_2,
                primary_keys=primary_keys,
                columns=columns,
            ),
            queries.compare_summary_query(
                dialect=ctx.dialect,
                columns=columns,
            ),
        ]
    )

    return list(ctx.connection.query(query).result())


def get_detailed_mismatches(
    ctx: models.Context,
    identifier_1: str,
    identifier_2: str,
    primary_keys: list[str],
    columns: list[str],
) -> list:
    query = ";\n\n".join(
        [
            queries.create_temp_table_query(
                dialect=ctx.dialect,
                identifier_1=identifier_1,
                identifier_2=identifier_2,
                primary_keys=primary_keys,
                columns=columns,
            ),
            queries.compare_detail_query(
                dialect=ctx.dialect,
                primary_keys=primary_keys,
                columns=columns,
            ),
        ]
    )

    return list(ctx.connection.query(query).result())


def main(
    dialect: str,
    table_1_id: str,
    table_2_id: str,
    primary_keys: list[str],
) -> int:
    assert dialect == "bigquery", "Only BigQuery is supported currently."  # noqa: S101

    context = models.Context(
        connection=bigquery.Client(),
        dialect="bigquery",
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

    detailed_mismatches = get_detailed_mismatches(
        context,
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
