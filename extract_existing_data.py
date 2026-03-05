# %%
"""
    Purpose
        Extract existing CSPS organisations data and save to database.
    Inputs
        - xlsx: "Organisation working file.xlsx"
            - Collated CSPS organisations data
    Outputs
        - sql: testing.civil_service_people_survey_organisations
    Notes
        None
"""

import os
import uuid

import ds_utils.database_operations as dbo
import pandas as pd
from sqlalchemy import NVARCHAR, SMALLINT
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER

# %%
# SET CONSTANTS
BASE_PATH = "C:/Users/nyep/INSTITUTE FOR GOVERNMENT/Data - General/Civil service/Civil Service - People Survey/Organisation working file.xlsx"

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
df = pd.read_excel(BASE_PATH, sheet_name="Data.Collated", na_values=["[c]", "[z]", "z"])


# %%
# DEFINE FUNCTIONS
def resolve_org_id(org_name, year, df_org, quarter=SURVEY_QUARTER):
    """Return the UUID of the organisation active at the given year/quarter, or None if unresolvable."""
    mask = (
        (df_org["organisation"] == org_name) &
        (
            df_org["start_year"].isna() |
            (df_org["start_year"] < year) |
            ((df_org["start_year"] == year) & (df_org["start_quarter"] <= quarter))
        ) &
        (
            df_org["end_year"].isna() |
            (df_org["end_year"] > year) |
            ((df_org["end_year"] == year) & (df_org["end_quarter"] >= quarter))
        )
    )
    matches = df_org[mask]
    if len(matches) == 1:
        return matches.iloc[0]["id"]
    return None


# %%
# EDIT DATA
# Add UUID columns
df.insert(0, "id", [str(uuid.uuid4()) for _ in range(len(df))])

# Drop calculated columns
df = df.drop(columns=[
    "Organisation aggregation?",
    "Release number",
    "Departmental group",
    "Organisation type",
    "Latest organisation",
    "Latest departmental group"
])

# Normalise column names to snake_case
df.columns = df.columns.str.lower().str.replace(r"[^\w\s]", "", regex=True).str.replace(r"\s+", "_", regex=True).str.strip("_")

# Resolve organisation ids
# Temporally match each row's organisation name and year to testing.organisation.
# Rows that don't match (e.g. aggregations like "Civil Service benchmark") remain NULL.
df_org = pd.read_sql(
    "select id, organisation, start_year, start_quarter, end_year, end_quarter from testing.organisation",
    engine
)

df["organisation_id"] = df.apply(
    lambda row: resolve_org_id(row["organisation"], row["year"], df_org),
    axis=1
)

# %%
# SAVE DATA TO DATABASE
df.to_sql(
    schema="testing",
    name="civil_service_people_survey_organisations",
    con=engine,
    if_exists="replace",
    index=False,
    dtype={
        "id": UNIQUEIDENTIFIER,
        "headline_category": NVARCHAR(10),
        "year": SMALLINT,
        "organisation_code": NVARCHAR(100),
        "organisation_id": UNIQUEIDENTIFIER,
        "organisation": NVARCHAR(200),
        "departmental_group_survey": NVARCHAR(200),
        "section": NVARCHAR(100),
        "measure": NVARCHAR(20),
        "label": NVARCHAR(300),
        "value": NVARCHAR(25),
        "answer_format": NVARCHAR(200),
        "based_on": NVARCHAR(200),
        "notes": NVARCHAR(200),
    }
)

# %%
