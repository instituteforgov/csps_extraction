import re


def normalise_column_names(col_name):
    """Normalises column names to snake_case and removes special characters.

    Args:
        col_name (str): The original column name.
    Returns:
        str: The normalised column name.
    """
    return re.sub(r"\s+", "_", re.sub(r"[^\w\s]", "", col_name.lower())).strip("_")
