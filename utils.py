import re
import pandas as pd


def strip_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from all column names in a DataFrame.

    Args:
        df: DataFrame whose column names should be stripped

    Returns:
        DataFrame with whitespace removed from column names
    """
    df.columns = df.columns.str.strip()
    return df


def remove_column_name_note_numbers(df_data: pd.DataFrame, df_notes: pd.DataFrame, note_number_column: str) -> pd.DataFrame:
    """Remove note numbers from column names in the data DataFrame.

    Removes various note formats from column names:
    - [1], [2], etc.
    - [note 1], [note 2], etc.
    - [1, 2], [notes 8 and 9], etc. (composite notes)

    Args:
        df_data: DataFrame whose column names should have note numbers removed
        df_notes: DataFrame containing the note numbers to remove
        note_number_column: Name of the column in df_notes containing note numbers

    Returns:
        DataFrame with note numbers removed from column names

    Raises:
        ValueError: If note_number_column is not in df_notes columns
    """
    if note_number_column not in df_notes.columns:
        raise ValueError(f"The specified column '{note_number_column}' is missing from df_notes.")

    note_numbers = df_notes[note_number_column].dropna().astype(str).tolist()

    # Create regex patterns for each note number
    patterns = []
    for note in note_numbers:
        # Pattern for [1], [note 1]
        patterns.append(rf"\[{re.escape(note)}\]")
        patterns.append(rf"\[note\s*{re.escape(note)}\]")

    # Also add patterns for composite notes like [1, 2], [notes 8 and 9]
    # These patterns match any bracketed content containing "note" or just numbers/commas
    patterns.append(r"\[notes?\s+[\d\s,and]+\]")
    patterns.append(r"\[[\d\s,]+\]")

    # Combine all patterns
    combined_pattern = "|".join(patterns)

    # Apply removal to column names
    new_columns = [re.sub(combined_pattern, "", col, flags=re.IGNORECASE) for col in df_data.columns]
    df_data.columns = new_columns
    df_data = strip_column_names(df_data)
    return df_data


def split_column_on_delimiter(
    df: pd.DataFrame,
    input_column: str,
    output_column1: str,
    output_column2: str,
    delimiters: str | list[str]
) -> pd.DataFrame:
    """Split a column into two columns based on the first occurrence of a delimiter.

    The original column is replaced with two new columns. Only the first delimiter found
    is used for splitting; any subsequent delimiters remain in output_column2. If the
    delimiter is not found, the entire value goes into output_column2 and output_column1
    is left empty.

    Args:
        df: DataFrame containing the column to split
        input_column: Name of the column to split
        output_column1: Name for the first output column (text before first delimiter)
        output_column2: Name for the second output column (text after first delimiter)
        delimiters: Single delimiter string or list of delimiter strings

    Returns:
        DataFrame with the input column replaced by two new columns

    Raises:
        ValueError: If input_column is not in DataFrame columns
    """
    if input_column not in df.columns:
        raise ValueError(f"The specified column '{input_column}' is missing from the DataFrame.")

    delimiters = [delimiters] if isinstance(delimiters, str) else delimiters
    pattern = "|".join(re.escape(d) for d in delimiters)
    has_delim = df[input_column].astype(str).str.contains(pattern, na=False, regex=True)
    split_columns = df[input_column].str.split(pattern, n=1, expand=True, regex=True)
    col_idx = df.columns.get_loc(input_column)
    new_col1 = split_columns[0].str.strip()
    new_col2 = split_columns[1].str.strip() if split_columns.shape[1] > 1 else ""
    new_col1 = new_col1.where(has_delim, "")
    new_col2 = new_col2.where(has_delim, df[input_column])
    df.insert(col_idx, output_column1, new_col1)
    df.insert(col_idx + 1, output_column2, new_col2)
    df = df.drop(columns=[input_column])
    return df


def split_demographic_variable_name_column(df: pd.DataFrame, input_column: str = "Demographic variable name") -> pd.DataFrame:
    """Split demographic variable name column to extract 'Derived from' information.

    Parses values like 'Variable name (derived from: source)' into separate columns.
    If the parenthetical content contains 'derived from', it is extracted into a
    separate 'Derived from' column.

    Args:
        df: DataFrame containing the demographic variable name column
        input_column: Name of the column to split (default: "Demographic variable name")

    Returns:
        DataFrame with the input column split into 'Demographic variable name' and 'Derived from' columns

    Raises:
        ValueError: If input_column is not in DataFrame columns
    """
    def split_row(value):
        value = str(value)
        match = re.match(r"^(.+?)\s*\((.*?)\)\s*$", value)
        if not match:
            return pd.Series({"Demographic variable name": value.strip(), "Derived from": ""})
        main = match.group(1).strip()
        inside = match.group(2).strip()
        if "derived from" in inside.lower():
            derived = re.sub(r"(?i)derived from", "", inside).strip(" :,-")
            return pd.Series({"Demographic variable name": main, "Derived from": derived})
        else:
            return pd.Series({"Demographic variable name": value.strip(), "Derived from": ""})

    if input_column not in df.columns:
        raise ValueError(f"The specified column '{input_column}' is missing from the DataFrame.")
    split_df = df[input_column].apply(split_row)
    col_idx = df.columns.get_loc(input_column)
    df = df.drop(columns=[input_column])
    df.insert(col_idx, "Demographic variable name", split_df["Demographic variable name"])
    df.insert(col_idx + 1, "Derived from", split_df["Derived from"])
    return df


def split_measure_name_column(df: pd.DataFrame, input_column: str = "Measure name") -> pd.DataFrame:
    """Split measure name column to extract definitions from parenthetical content.

    Extracts content in parentheses (except those starting with 'e.g.' or 'for example')
    into a separate 'Definition' column, removing the parenthetical content from the
    measure name. Multiple definitions are joined with semicolons.

    Args:
        df: DataFrame containing the measure name column
        input_column: Name of the column to split (default: "Measure name")

    Returns:
        DataFrame with the input column split into 'Measure name' and 'Definition' columns
    """
    def split_row(value: str) -> pd.Series:
        value = str(value)

        # Find all top-level parenthetical content using bracket depth tracking
        parentheticals = []
        depth = 0
        start_pos = None

        for i, char in enumerate(value):
            if char == "(":
                if depth == 0:
                    start_pos = i
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0 and start_pos is not None:
                    # Found complete top-level parenthetical
                    content = value[start_pos + 1:i].strip()
                    # Check if content starts with e.g. or for example
                    if not re.match(r"^(e\.?g\.?|for\s+example)($|[\s:,])", content, re.IGNORECASE):
                        parentheticals.append((start_pos, i + 1, content))
                    start_pos = None

        # Remove parentheticals from string and collect definitions
        definitions = []
        cleaned = value
        offset = 0
        for start, end, content in parentheticals:
            definitions.append(content)
            adjusted_start = start - offset
            adjusted_end = end - offset
            cleaned = cleaned[:adjusted_start] + cleaned[adjusted_end:]
            offset += (end - start)

        cleaned = cleaned.strip()
        definition_str = "; ".join(definitions)
        return pd.Series({"Measure name": cleaned, "Definition": definition_str})

    split_df = df[input_column].apply(split_row)
    col_idx = df.columns.get_loc(input_column)
    df = df.drop(columns=[input_column])
    df.insert(col_idx, "Measure name", split_df["Measure name"])
    df.insert(col_idx + 1, "Definition", split_df["Definition"])
    return df


def unpivot_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Unpivot the DataFrame to transform measure columns into rows.

    Keeps demographic and response columns as identifiers and transforms all measure
    columns into rows with 'Measure' and 'Value' columns.

    Args:
        df: DataFrame in wide format with measure columns

    Returns:
        DataFrame in long format with measures as rows
    """
    keep_columns = [col for col in [
        "Demographic variable code",
        "Demographic variable name",
        "Derived from",
        "Response",
        "Notes"
    ] if col in df.columns]
    unpivot_columns = [col for col in df.columns if col not in keep_columns]
    df_unpivoted = pd.melt(
        df,
        id_vars=keep_columns,
        value_vars=unpivot_columns,
        var_name="Measure",
        value_name="Value"
    )
    return df_unpivoted


def lowercase_response_except_first_word(df: pd.DataFrame, demographic_variable_name: str) -> pd.DataFrame:
    """Lowercase all words except the first in Response values for a specific demographic variable.

    Args:
        df: DataFrame containing Response and Demographic variable name columns
        demographic_variable_name: The demographic variable name to apply transformation to

    Returns:
        DataFrame with transformed Response values for the specified demographic variable
    """
    def transform_response(val: str) -> str:
        words = str(val).split()
        if not words:
            return val
        return words[0] + (" " + " ".join(w.lower() for w in words[1:]) if len(words) > 1 else "")

    mask = df["Demographic variable name"] == demographic_variable_name
    df.loc[mask, "Response"] = df.loc[mask, "Response"].apply(transform_response)
    return df


def reshape_data(df_data: pd.DataFrame, df_notes: pd.DataFrame, delimiters: str | list[str]) -> pd.DataFrame:
    """
    Apply a series of data transformations to reshape and clean the survey data.

    Args:
        df_data: The main survey data DataFrame to transform
        df_notes: The notes DataFrame containing note numbers to remove from column names
        delimiters: Single delimiter string or list of delimiter strings to use for splitting columns

    Returns:
        Transformed DataFrame with cleaned and reshaped data
    """
    df_data = strip_column_names(df_data)
    df_notes = strip_column_names(df_notes)
    df_data = remove_column_name_note_numbers(df_data, df_notes, note_number_column="Note number")
    df_data = split_column_on_delimiter(df_data, input_column="Demographic variable", output_column1="Demographic variable code", output_column2="Demographic variable name", delimiters=delimiters)
    df_data = split_demographic_variable_name_column(df_data, input_column="Demographic variable name")
    df_data = unpivot_dataframe(df_data)
    df_data = split_column_on_delimiter(df_data, input_column="Measure", output_column1="Measure code", output_column2="Measure name", delimiters=delimiters)
    df_data = split_measure_name_column(df_data, input_column="Measure name")
    return df_data


def clean_data(
    df: pd.DataFrame,
    grade_replacements: dict[str, str] | None = None,
    lowercase_demographic: str | None = None
) -> pd.DataFrame:
    """
    Apply data cleaning operations to the survey data.

    Args:
        df: The DataFrame to clean
        grade_replacements: Dictionary mapping grade values to their standardised replacements
        lowercase_demographic: The demographic variable name for which to lowercase all words except the first in Response values

    Returns:
        Cleaned DataFrame
    """
    # Apply grade replacements
    if grade_replacements:
        df["Response"] = df["Response"].replace(grade_replacements)

    # Remove ' (England)' suffix from Response values
    df["Response"] = df["Response"].str.replace(r"\s*\(England\)$", "", regex=True)

    # Lowercase all words except the first in Response for a specific demographic variable
    if lowercase_demographic:
        df = lowercase_response_except_first_word(df, lowercase_demographic)

    return df
