# %%
"""
    Purpose
        Extract and clean data from Civil Service People Survey demographic results files.
    Inputs
        - ods: various
            - Demographic results files for the period 2020-24
        - sql: [Reference|Civil service|People Survey|Demographics|Dataset].Results
    Outputs
        - sql: [Source|Civil service|People Survey|Demographics|Dataset].<year>
    Notes
        None
"""

import os
import uuid

import ds_utils.database_operations as dbo
import pandas as pd
from sqlalchemy import DECIMAL, INT, NVARCHAR, text
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER

from utils import (
    reshape_data,
    clean_data
)

# %%
# SET CONSTANTS
BASE_PATH = "C:/Users/" + os.getlogin() + "/Institute for Government/Data - General/Civil service/Civil Service - People Survey/Source"
FILES = {
    2020: {
        "filename": "Civil-Service-People-Survey-2020-results-by-all-demographics-v2.ods",
        "sheets": {
            "notes": {
                "sheet_name": "Notes",
                "skiprows": 2
            },
            "data": {
                "sheet_name": "Benchmarks",
                "skiprows": 4,
                "delimiters": ":"
            }
        },
    },
    2021: {
        "filename": "Civil-Service-People-Survey-2021-results-by-all-demographics.ods",
        "sheets": {
            "notes": {
                "sheet_name": "Notes",
                "skiprows": 2
            },
            "data": {
                "sheet_name": "Benchmarks",
                "skiprows": 4,
                "delimiters": "."
            }
        },
    },
    2022: {
        "filename": "Civil-Service-People-Survey-2022-results-by-all-demographics.ods",
        "sheets": {
            "notes": {
                "sheet_name": "Notes",
                "skiprows": 2
            },
            "data": {
                "sheet_name": "Benchmarks",
                "skiprows": 4,
                "delimiters": "."
            }
        },
    },
    2023: {
        "filename": "Civil-Service-People-Survey-2023-results-by-all-demographic-groups.ods",
        "sheets": {
            "notes": {
                "sheet_name": "Notes",
                "skiprows": 2
            },
            "data": {
                "sheet_name": "Benchmarks",
                "skiprows": 5,
                "delimiters": "."
            }
        },
    },
    2024: {
        "filename": "Civil-Service-People-Survey-2024-results-by-all-demographic-groups.ods",
        "sheets": {
            "notes": {
                "sheet_name": "Notes",
                "skiprows": 2
            },
            "data": {
                "sheet_name": "Table_1",
                "skiprows": 5,
                "delimiters": [".", "\n"]
            }
        },
    },
}
GRADE_REPLACEMENTS = {
    "AO/AA": "AA/AO",
    "SEO/HEO": "HEO/SEO",
    "G6/7": "G7/6",
}
COLUMN_LENGTHS = {
    "Demographic variable code": 16,
    "Demographic variable name": 256,
    "Derived from": 16,
    "Response": 128,
    "Measure code": 16,
    "Measure name": 256,
    "Definition": 256,
}
SCHEMA_REFERENCE = "Reference|Civil service|People Survey|Demographics|Dataset"
SCHEMA_SOURCE = "Source|Civil service|People Survey|Demographics|Dataset"
TABLE_COLLATED = "Collated results"
NA_VALUES = "[c]"

# %%
# CONNECT TO DATABASE
engine = dbo.connect_sql_db(
    driver="pyodbc",
    driver_version=os.environ["ODBC_DRIVER"],
    dialect="mssql",
    server=os.environ["ODBC_SERVER"],
    database=os.environ["ODBC_DATABASE"],
    authentication=os.environ["ODBC_AUTHENTICATION"],
    username=os.environ["AZURE_CLIENT_ID"],
    password=os.environ["AZURE_CLIENT_SECRET"],
)

# %%
# READ IN, PROCESS, SAVE DATA
for year, details in FILES.items():

    # Read in files
    # NB: Calamine used as odfpy drops line breaks https://github.com/eea/odfpy/issues/114
    file_path = os.path.join(BASE_PATH, str(year), details["filename"])
    sheets = details["sheets"]
    df_notes = pd.read_excel(file_path, engine="calamine", sheet_name=sheets["notes"]["sheet_name"], skiprows=sheets["notes"]["skiprows"])
    df_data = pd.read_excel(file_path, engine="calamine", sheet_name=sheets["data"]["sheet_name"], skiprows=sheets["data"]["skiprows"], na_values=NA_VALUES)
    delimiters = sheets["data"].get("delimiters")

    # Check for existing data
    try:
        df_collated = pd.read_sql_table(
            table_name=TABLE_COLLATED,
            schema=SCHEMA_REFERENCE,
            con=engine,
        )
    except ValueError as e:
        print("No existing data found:", e)

    # Reshape data
    df_data = reshape_data(df_data, df_notes, delimiters)

    # Add columns
    df_data.insert(0, "_id", [str(uuid.uuid4()) for _ in range(len(df_data))])
    df_data.insert(1, "_year", year)

    # Clean data
    df_data = clean_data(
        df_data,
        grade_replacements=GRADE_REPLACEMENTS,
        lowercase_demographic="Which of the following categories best reflects the type of work you do in your main job?"
    )

    # Check column lengths
    for col, max_len in COLUMN_LENGTHS.items():
        if col in df_data.columns:
            actual_max = df_data[col].astype(str).str.len().max()
            assert actual_max <= max_len, f"Column '{col}' has value(s) exceeding max length {max_len} in year {year} (actual max: {actual_max})"

    # Save to d/b, minus Notes column
    dtype = {
        "_id": UNIQUEIDENTIFIER,
        "_year": INT,
        "Value": DECIMAL(10, 3),
    }
    for col, length in COLUMN_LENGTHS.items():
        dtype[col] = NVARCHAR(length)

    df_data[[c for c in df_data.columns if c != "Notes"]].to_sql(
        name=str(year),
        con=engine,
        schema=SCHEMA_SOURCE,
        if_exists="replace",
        index=False,
        dtype=dtype,
    )

    # Delete existing records for this year before appending
    delete_query = text(f"""
    delete from [{SCHEMA_SOURCE}].[{TABLE_COLLATED}]
    where _year = {year}
    """)
    try:
        with engine.begin() as conn:
            conn.execute(delete_query)
    except Exception:
        pass

    df_data[[c for c in df_data.columns if c != "Notes"]].to_sql(
        name=TABLE_COLLATED,
        con=engine,
        schema=SCHEMA_SOURCE,
        if_exists="append",
        index=False,
        dtype=dtype,
    )

# %%
