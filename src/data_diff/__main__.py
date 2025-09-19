import arguably

import data_diff.main

NAME = "data-diff"


@arguably.command
def __root__() -> None:  # noqa: N807
    if arguably.is_target():
        print(f"Run '{NAME} --help' for help.")


@arguably.command
def compare(*, source: str, table: list[str], primary_key: list[str]) -> int:
    """
    Compare two database tables.

    :param source: [--source] The data source to use.
    :param table: [--table] The names of the tables to compare.
    :param primary_key: [--primary-key] The primary key columns to use for comparison.
    """

    if len(table) != 2:  # noqa: PLR2004
        raise ValueError("Exactly two table names must be provided.")

    return data_diff.main.main(
        source=source,
        table_1_id=table[0],
        table_2_id=table[1],
        primary_keys=primary_key,
    )


if __name__ == "__main__":
    arguably.run(name=NAME)
