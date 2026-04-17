# %%
"""
    Purpose
        Compare the output of select_organisations_data.sql with the 'Data.Collated'
        worksheet of 'Organisation working file.xlsx', to validate that the SQL
        augmentation of extracted data matches the original source values.
    Inputs
        - xlsx: "Organisation working file.xlsx"
            - Collated CSPS organisations data
        - sql: "select_organisations_data.sql"
            - Augmented CSPS organisations data
    Outputs
        - Printed comparison summary to console
    Notes
        None
"""

import os

import ds_utils.database_operations as dbo
import pandas as pd

# %%
# SET CONSTANTS
BASE_PATH = "C:/Users/" + os.getlogin() + "/INSTITUTE FOR GOVERNMENT/Data - General/Civil service/Civil Service - People Survey/Organisation working file.xlsx"
SQL_PATH = "C:/Users/" + os.getlogin() + "/INSTITUTE FOR GOVERNMENT/Data - General/Civil service/Civil Service - People Survey/Scripts/Extraction/csps_extraction/sql/select_organisations_data.sql"

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
# Compare keys (i.e. values which uniquely identify rows)
key_cols = ["Headline category", "Year", "Organisation", "Section", "Measure", "Label", "Answer format"]

assert len(df_sql) == len(df_excel), (
    f"Row count mismatch before merge: SQL has {len(df_sql)} rows, Excel has {len(df_excel)} rows."
)

df_merged = df_sql.merge(df_excel, on=key_cols, how="outer", suffixes=("_sql", "_excel"), indicator=True)
rows_in_both = df_merged[df_merged["_merge"] == "both"]
rows_excel_only = df_merged[df_merged["_merge"] == "right_only"]
rows_sql_only = df_merged[df_merged["_merge"] == "left_only"]
print(f"Rows in both sources: {len(rows_in_both)}")
print(f"Rows only in Excel: {len(rows_excel_only)}")
print(f"Rows only in SQL: {len(rows_sql_only)}")

assert len(df_merged) == len(df_sql), (
    f"Merged row count ({len(df_merged)}) differs from source row count ({len(df_sql)}). "
    f"{len(rows_excel_only)} Excel-only and {len(rows_sql_only)} SQL-only rows."
)

# %%
# Compare values for matched rows
value_cols = [col for col in df_excel.columns if col in cols_in_both and col not in key_cols]

mismatch_masks = {}
for col in value_cols:
    sql_col = f"{col}_sql"
    excel_col = f"{col}_excel"
    if sql_col in rows_in_both.columns and excel_col in rows_in_both.columns:
        match_mask = (
            (rows_in_both[sql_col] == rows_in_both[excel_col])
            | (rows_in_both[sql_col].isna() & rows_in_both[excel_col].isna())
        )
        if (~match_mask).any():
            mismatch_masks[col] = ~match_mask

if mismatch_masks:
    print("Columns with value mismatches in matched rows:")
    for col, mask in mismatch_masks.items():
        print(f"  {col}: {int(mask.sum())} mismatch(es)")
    print()
    for col, mask in mismatch_masks.items():
        sql_col = f"{col}_sql"
        excel_col = f"{col}_excel"
        preview = rows_in_both.loc[mask, key_cols + [sql_col, excel_col]]
        print(f"Mismatches in '{col}':")
        print(preview.to_string(index=False))
        print()
else:
    print("No value mismatches in matched rows")

# %%
