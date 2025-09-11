import arguably

import data_diff.main

NAME = "data-diff"


@arguably.command
def __root__() -> None:  # noqa: N807
    if arguably.is_target():
        print(f"Run '{NAME} --help' for help.")


@arguably.command
def compare(*, table: list[str], primary_key: list[str]) -> int:
    """
    Compare two database tables.

    :param table: [--table] The names of the tables to compare.
    :param primary_key: [--primary-key] The primary key columns to use for comparison.
    """

    if len(table) != 2:  # noqa: PLR2004
        raise ValueError("Exactly two table names must be provided.")

    return data_diff.main.main(*table, primary_keys=primary_key)


if __name__ == "__main__":
    arguably.run(name=NAME)
