# %%
"""
    Purpose
        Extract existing CSPS benchmarks data and save to database.
    Inputs
        - xlsx: "Benchmarks working file.xlsx"
            - Collated CSPS benchmarks data
    Outputs
        - sql: civil_service.civil_service_people_survey_benchmarks
    Notes
        - Replaces existing data in `civil_service.civil_service_people_survey_benchmarks`
"""

import os

import ds_utils.database_operations as dbo
import pandas as pd
from sqlalchemy import DECIMAL, NVARCHAR, SMALLINT
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER

from csps_extraction.utils import prepare_csps_data

# %%
# SET CONSTANTS
BASE_PATH = "C:/Users/" + os.getlogin() + "/INSTITUTE FOR GOVERNMENT/Data - General/Civil service/Civil Service - People Survey/Benchmarks working file.xlsx"
SHEET_NAME = "Data.Collated"
NA_VALUES = ["[z]"]
CALCULATED_COLUMNS = []

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
# NB: Sets non-numeric values to NaN
df_csps = pd.read_excel(BASE_PATH, sheet_name=SHEET_NAME, na_values=NA_VALUES)

# %%
# PREPARE DATA
df_csps = prepare_csps_data(
    df_csps,
    calculated_columns=CALCULATED_COLUMNS,
)

# %%
# SAVE DATA TO DATABASE
df_csps.to_sql(
    schema="civil_service",
    name="civil_service_people_survey_benchmarks",
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=1000,
    dtype={
        "id": UNIQUEIDENTIFIER,
        "headline_category": NVARCHAR(10),
        "year": SMALLINT,
        "section": NVARCHAR(100),
        "measure": NVARCHAR(20),
        "label": NVARCHAR(300),
        "value": DECIMAL(9, 6),
        "answer_format": NVARCHAR(200),
        "based_on": NVARCHAR(200),
        "notes": NVARCHAR(200),
    }
)

# %%
