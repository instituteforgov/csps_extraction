# %%
"""
    Purpose
        Extract existing CSPS organisations data and save to database.
    Inputs
        - xlsx: "Organisation working file.xlsx"
            - Collated CSPS organisations data
        - sql: testing.organisation
            - Canonical civil service organisation details
    Outputs
        - sql: testing.civil_service_people_survey_organisations
    Notes
        None
"""

import os
import uuid

import ds_utils.database_operations as dbo
import pandas as pd
from sqlalchemy import DECIMAL, NVARCHAR, SMALLINT
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER

# %%
# SET CONSTANTS
BASE_PATH = "C:/Users/" + os.getlogin() + "/INSTITUTE FOR GOVERNMENT/Data - General/Civil service/Civil Service - People Survey/Organisation working file.xlsx"

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
df_csps = pd.read_excel(BASE_PATH, sheet_name="Data.Collated", na_values=["[c]", "[z]", "z"])


# %%
# DEFINE FUNCTIONS
def resolve_org_id(
    df: pd.DataFrame,
    df_org_id: pd.DataFrame,
    org_col: str = "organisation",
    year_col: str = "year",
    quarter_col: str | int = "quarter",
) -> pd.Series:
    """Return a Series of organisation UUIDs matched by name and year/quarter.

    Args:
        df: Source DataFrame containing the rows to resolve.
        df_org_id: Reference DataFrame with columns id, organisation, start_year,
            start_quarter, end_year, end_quarter.
        org_col: Column in df containing the organisation name.
        year_col: Column in df containing the survey year.
        quarter_col: Column in df containing the survey quarter, or a scalar
            integer to apply the same quarter to all rows.

    Returns:
        Series indexed like df, with the resolved UUID where a unique active
        organisation record was found, and NaN where unresolvable.
    """
    lookup = df[[org_col, year_col]].rename(columns={org_col: "organisation", year_col: "year"})
    if isinstance(quarter_col, str):
        lookup["quarter"] = df[quarter_col]
    else:
        lookup["quarter"] = quarter_col
    merged = (
        lookup
        .rename_axis("_orig_idx")
        .reset_index()
        .merge(
            df_org_id[["id", "organisation", "start_year", "start_quarter", "end_year", "end_quarter"]],
            on="organisation",
            how="left",
        )
    )
    active = (
        (merged["start_year"].isna() |
         (merged["start_year"] < merged["year"]) |
         ((merged["start_year"] == merged["year"]) & (merged["start_quarter"] <= merged["quarter"])))
        &
        (merged["end_year"].isna() |
         (merged["end_year"] > merged["year"]) |
         ((merged["end_year"] == merged["year"]) & (merged["end_quarter"] >= merged["quarter"])))
    )
    merged = merged[active]
    counts = merged.groupby("_orig_idx")["id"].count()
    unique_idx = counts[counts == 1].index
    result = merged[merged["_orig_idx"].isin(unique_idx)].set_index("_orig_idx")["id"]
    return result.reindex(df.index)


# %%
# EDIT DATA
# Drop rows where 'Organisation' is blank
df_csps = df_csps.dropna(subset=["Organisation"])

# Add UUID columns
df_csps.insert(0, "id", [str(uuid.uuid4()) for _ in range(len(df_csps))])

# Drop calculated columns
df_csps = df_csps.drop(columns=[
    "Organisation aggregation?",
    "Release number",
    "Departmental group",
    "Organisation type",
    "Latest organisation",
    "Latest departmental group"
])

# Normalise column names to snake_case
df_csps.columns = df_csps.columns.str.lower().str.replace(r"[^\w\s]", "", regex=True).str.replace(r"\s+", "_", regex=True).str.strip("_")

# Resolve organisation ids
# Temporally match each row's organisation name and year to testing.organisation.
# Rows that don't match (e.g. aggregations like "Civil Service benchmark") remain NULL.
df_organisation = pd.read_sql(
    "select id, organisation, start_year, start_quarter, end_year, end_quarter from testing.organisation",
    engine
)

df_csps["organisation_id"] = resolve_org_id(df_csps, df_organisation, quarter_col=SURVEY_QUARTER)

# %%
# SAVE DATA TO DATABASE
df_csps.to_sql(
    schema="testing",
    name="civil_service_people_survey_organisations",
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=1000,
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
        "value": DECIMAL(4, 1),
        "answer_format": NVARCHAR(200),
        "based_on": NVARCHAR(200),
        "notes": NVARCHAR(200),
    }
)

# %%
