import uuid

from cs_data_utils.utils import normalise_column_names
import pandas as pd


def prepare_csps_data(
    df: pd.DataFrame,
    calculated_columns: list[str],
    column_renames: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Prepare a raw CSPS DataFrame: add IDs, drop calculated columns, and normalise column names.

    Args:
        df: Raw DataFrame read from a CSPS working file.
        calculated_columns: Column names (pre-normalisation) to drop.
        column_renames: Optional mapping of normalised column names to new names.

    Returns:
        DataFrame with a UUID id column, normalised snake_case column names,
        and any specified column renames applied.
    """
    df = df.copy()

    # Add UUID column
    df.insert(0, "id", [str(uuid.uuid4()) for _ in range(len(df))])

    # Drop calculated columns
    if calculated_columns:
        df = df.drop(columns=calculated_columns)

    # Normalise column names to snake_case
    df.columns = [normalise_column_names(col) for col in df.columns]

    # Rename columns
    if column_renames:
        df = df.rename(columns=column_renames)

    return df
