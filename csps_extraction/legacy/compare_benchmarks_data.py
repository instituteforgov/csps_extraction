# %%
"""
    Purpose
        Compare the output of compare_benchmarks_data.sql with the 'Data.Collated' worksheet of 'Benchmarks working file.xlsx', to validate that the SQL extraction of benchmarks data matches the original source values.
    Inputs
        - xlsx: "Benchmarks working file.xlsx"
            - Collated CSPS benchmarks data
        - sql: "compare_benchmarks_data.sql"
            - CSPS benchmarks data from database
    Outputs
        - Printed comparison summary to console
    Notes
        - This handles several expected differences between the two datasets. Namely that:
            - Certain columns only exist in one dataset:
                - SQL: 'id'
"""

import os

import ds_utils.database_operations as dbo
import pandas as pd

from cs_data_utils.utils import compare_dataframes

# %%
# SET CONSTANTS
BASE_PATH = "C:/Users/" + os.getlogin() + "/INSTITUTE FOR GOVERNMENT/Data - General/Civil service/Civil Service - People Survey/Benchmarks working file.xlsx"
SHEET_NAME = "Data.Collated"
NA_VALUES = ["[z]"]
KEY_COLS = ["Headline category", "Year", "Section", "Measure", "Label", "Answer format"]
SQL_PATH = "C:/Users/" + os.getlogin() + "/INSTITUTE FOR GOVERNMENT/Data - General/Civil service/Civil Service - People Survey/Scripts/Extraction/csps_extraction/legacy/sql/compare_benchmarks_data.sql"

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
# COMPARE DATA
compare_dataframes(df_excel, df_sql, KEY_COLS)

# %%
