# %%
"""
    Purpose
        Compare the output of organisations_data_collated_query.sql with the 'Data.Collated'
        worksheet of 'Organisation working file.xlsx', to validate that the SQL
        enrichment of extracted data matches the original source values.
    Inputs
        - xlsx: "Organisation working file.xlsx"
            - Collated CSPS organisations data
        - sql: "organisations_data_collated_query.sql"
            - Enriched query over civil_service.civil_service_people_survey_organisations
    Outputs
        - Printed comparison summary to console
    Notes
        Run extract_existing_data.py first to populate
        civil_service.civil_service_people_survey_organisations.

        The SQL inner-joins to civil_service.release_number (quarter=4), so rows whose
        year has no release-number entry will appear in Excel but not in the SQL
        output; these are reported as Excel-only rows.

        The SQL selects o.departmental_group (raw) and then a case-expression also
        aliased departmental_group (the adjusted value). The raw column is renamed
        to departmental_group_raw automatically.
"""

import os

import ds_utils.database_operations as dbo
import pandas as pd

# %%
# SET CONSTANTS
BASE_PATH = "C:/Users/" + os.getlogin() + "/INSTITUTE FOR GOVERNMENT/Data - General/Civil service/Civil Service - People Survey/Organisation working file.xlsx"
SQL_PATH = "C:/Users/" + os.getlogin() + "/INSTITUTE FOR GOVERNMENT/Data - General/Civil service/Civil Service - People Survey/Scripts/Extraction/csps_extraction/sql/organisations_data_collated_query.sql"

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
df_excel = pd.read_excel(BASE_PATH, sheet_name="Data.Collated", na_values=["[c]", "[z]", "z"])

with open(SQL_PATH, encoding="utf-8") as f:
    sql = f.read()
df_sql = pd.read_sql(sql, engine)

# %%
# EDIT DATA
# Drop rows where 'Organisation' is blank
df_excel = df_excel.dropna(subset=["Organisation"])

# %%
# COMPARE DATA
# Compare columns
df_excel_cols = set(df_excel.columns)
df_sql_cols = set(df_sql.columns)
cols_in_both = df_excel_cols.intersection(df_sql_cols)
cols_excel_only = df_excel_cols - df_sql_cols
cols_sql_only = df_sql_cols - df_excel_cols
print(f"Columns in both sources: {cols_in_both}")
print(f"Columns only in Excel: {cols_excel_only}")
print(f"Columns only in SQL: {cols_sql_only}")

# %%
# Compare rows, matching on year, organisation, label, ignoring release_number column from Excel and id column from SQL
key_cols = ["Year", "Organisation", "Label"]
compare_cols = cols_in_both - {"Release number", "id"}
df_merged = df_sql.merge(df_excel, on=key_cols, how="outer", suffixes=("_sql", "_excel"), indicator=True)
rows_in_both = df_merged[df_merged["_merge"] == "both"]
rows_excel_only = df_merged[df_merged["_merge"] == "right_only"]
rows_sql_only = df_merged[df_merged["_merge"] == "left_only"]
print(f"Rows in both sources: {len(rows_in_both)}")
print(f"Rows only in Excel: {len(rows_excel_only)}")
print(f"Rows only in SQL: {len(rows_sql_only)}")

# %%
