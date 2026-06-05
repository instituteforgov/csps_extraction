# %%
"""
    Purpose
        Compare the output of compare_organisations_data.sql with the 'Data.Collated' worksheet of 'Organisations working file.xlsx', to validate that the SQL augmentation of extracted data matches the original source values.
    Inputs
        - xlsx: "Organisations working file.xlsx"
            - Collated CSPS organisations data
        - sql: "compare_organisations_data.sql"
            - Augmented CSPS organisations data
    Outputs
        - Printed comparison summary to console
    Notes
        - This handles several expected differences between the two datasets. Namely that:
            - Certain columns only exist in one dataset:
                - Excel: 'Organisation code', 'Departmental group (survey)', 'Release number'
                - SQL: 'id'
            - Excel data has ' - <yyyy> iteration' suffixes in the Organisation and Latest organisation columns for certain organisations
            - For organisations that have left the civil service, Excel data gives Organisation as the value for Latest organisation, whereas SQL data gives 'Non-civil service'
"""

import os

import ds_utils.database_operations as dbo
import pandas as pd

from csps_extraction.utils import compare_dataframes

# %%
# SET CONSTANTS
BASE_PATH = "C:/Users/" + os.getlogin() + "/INSTITUTE FOR GOVERNMENT/Data - General/Civil service/Civil Service - People Survey/Organisations working file.xlsx"
SHEET_NAME = "Data.Collated"
NA_VALUES = ["[c]", "[z]", "z"]
KEY_COLS = ["Headline category", "Year", "Organisation", "Section", "Measure", "Label", "Answer format"]
SQL_PATH = "C:/Users/" + os.getlogin() + "/INSTITUTE FOR GOVERNMENT/Data - General/Civil service/Civil Service - People Survey/Scripts/Extraction/csps_extraction/sql/compare_organisations_data.sql"

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
# READ IN DATA
df_excel = pd.read_excel(BASE_PATH, sheet_name=SHEET_NAME, na_values=NA_VALUES)

with open(SQL_PATH, encoding="utf-8") as f:
    sql = f.read()
df_sql = pd.read_sql(sql, engine)

# %%
# EDIT DATA
# Drop rows where 'Organisation' is blank
df_excel = df_excel.dropna(subset=["Organisation"])


# Add ' - <yyyy> iteration' suffixes to the Organisation column of df_sql
def add_iteration_suffix(row: pd.Series, column: str = "Organisation") -> str:
    """
        Add iteration suffixes to 'Organisation' values based on the following rules

        Args:
            row (pd.Series): A row of the DataFrame containing the specified column and 'Year' columns
            column (str): The name of the column to check for organisation names (default is 'Organisation')

        Returns:
            str: The modified 'Organisation' value with iteration suffix if applicable, otherwise the original 'Organisation' value

        Notes:
            We are slightly inconsistent in how we defined these names, explaining why a mix of <= and >= are used
    """
    if row[column] == "Department for Culture, Media and Sport":
        if row["Year"] <= 2017:
            return "Department for Culture, Media and Sport - 2017 iteration"
        elif row["Year"] >= 2023:
            return "Department for Culture, Media and Sport - 2023 iteration"
    elif row[column] == "Ministry of Housing, Communities & Local Government":
        if row["Year"] >= 2018 and row["Year"] <= 2021:
            return "Ministry of Housing, Communities & Local Government - 2018 iteration"
        elif row["Year"] >= 2024:
            return "Ministry of Housing, Communities & Local Government - 2024 iteration"
    return row[column]


df_sql["Organisation"] = df_sql.apply(add_iteration_suffix, column="Organisation", axis=1)

# Apply fixed latest-iteration suffixes to the Latest organisation column of df_sql
latest_org_suffixes = {
    "Department for Culture, Media and Sport": "Department for Culture, Media and Sport - 2023 iteration",
    "Ministry of Housing, Communities & Local Government": "Ministry of Housing, Communities & Local Government - 2024 iteration",
}
df_sql["Latest organisation"] = df_sql["Latest organisation"].map(lambda x: latest_org_suffixes.get(x, x))

# Set Latest organisation to Organisation where Latest organisation is 'Non-civil service'
df_sql["Latest organisation"] = df_sql.apply(
    lambda row: row["Organisation"] if row["Latest organisation"] == "Non-civil service" else row["Latest organisation"],
    axis=1,
)

# %%
# COMPARE DATA
compare_dataframes(df_excel, df_sql, KEY_COLS)

# %%
