# %%
"""
    Purpose
        Extract existing CSPS organisations data and save to database.
    Inputs
        - xlsx: "Organisation working file.xlsx"
            - Collated CSPS organisations data
        - sql: civil_service.organisation
            - Canonical civil service organisation details
    Outputs
        - sql: civil_service.civil_service_people_survey_organisations
    Notes
        - Replaces existing data in `civil_service.civil_service_people_survey_organisations`
"""

import os
import uuid

from cs_organisations.resolve import resolve_org_id
import ds_utils.database_operations as dbo
import pandas as pd
from sqlalchemy import DECIMAL, NVARCHAR, SMALLINT
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER

from csps_extraction.utils import normalise_column_names

# %%
# SET CONSTANTS
BASE_PATH = "C:/Users/" + os.getlogin() + "/INSTITUTE FOR GOVERNMENT/Data - General/Civil service/Civil Service - People Survey/Organisation working file.xlsx"
SHEET_NAME = "Data.Collated"
NA_VALUES = ["[c]", "[z]", "z"]
CALCULATED_COLUMNS = [
    "Organisation aggregation?",
    "Release number",
    "Departmental group",
    "Organisation type",
    "Latest organisation",
    "Latest departmental group"
]
COLUMN_RENAMES = {"organisation": "organisation_name"}
INSERT_ORG_ID_BEFORE_COL = "Organisation code"
SURVEY_QUARTER = 4

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
# EDIT DATA
# Drop rows where 'Organisation' is blank
df_csps = df_csps.dropna(subset=["Organisation"])

# Add UUID columns
df_csps.insert(0, "id", [str(uuid.uuid4()) for _ in range(len(df_csps))])

# Drop calculated columns
df_csps = df_csps.drop(columns=CALCULATED_COLUMNS)

# Normalise column names to snake_case
df_csps.columns = [normalise_column_names(col) for col in df_csps.columns]
_insert_before = normalise_column_names(INSERT_ORG_ID_BEFORE_COL)

# Rename columns
df_csps = df_csps.rename(columns=COLUMN_RENAMES)
_insert_before = COLUMN_RENAMES.get(_insert_before, _insert_before)

# Drop ' - <yyyy> iteration' strings from organisation names that have been reused (e.g. "Department for Culture, Media and Sport - 2017 iteration" becomes "Department for Culture, Media and Sport")
df_csps["organisation_name"] = df_csps["organisation_name"].str.replace(r"\s*-\s*\d{4}\s*iteration\s*", "", regex=True)

# %%
# Resolve organisation ids
# Temporally match each row's organisation name and year to civil_service.organisation.
# Rows that don't match (e.g. aggregations like "Civil Service benchmark") remain NULL.
df_organisation = pd.read_sql(
    """select
        o.id,
        o.name,
        o.start_year,
        o.start_quarter,
        o.end_year,
        o.end_quarter
    from civil_service.organisation o""",
    engine
)

df_csps.insert(
    df_csps.columns.get_loc(_insert_before),
    "organisation_id",
    resolve_org_id(df_csps, df_organisation, quarter_col=SURVEY_QUARTER)
)

# %%
# SAVE DATA TO DATABASE
df_csps.to_sql(
    schema="civil_service",
    name="civil_service_people_survey_organisations",
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=1000,
    dtype={
        "id": UNIQUEIDENTIFIER,
        "headline_category": NVARCHAR(10),
        "year": SMALLINT,
        "organisation_id": UNIQUEIDENTIFIER,
        "organisation_code": NVARCHAR(100),
        "organisation_name": NVARCHAR(200),
        "departmental_group_survey": NVARCHAR(200),
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
