# Civil Service People Survey extraction

Scripts for extracting Civil Service People Survey (CSPS) data.

## Related repositories

- 🔓 [Civil service organisations](https://github.com/instituteforgov/cs_organisations/): Scripts for managing canonical civil service organisation data, used to augment the CSPS data with things like latest departmental groups

## Project structure

```
├── csps_extraction/
│   ├── sql/
│   │   ├── compare_benchmarks_data.sql
│   │   ├── compare_demographics_data.sql
│   │   ├── compare_organisations_data.sql
│   │   ├── select_organisations_data.sql
│   ├── compare_benchmarks_data.py
│   ├── compare_demographics_data.py
│   ├── compare_organisations_data.py
│   ├── extract_benchmarks_data.py
│   ├── extract_demographics_data.py
│   ├── extract_organisations_data.py
│   └── utils.py
├── .gitignore
├── .pre-commit-config.yaml
├── LICENSE
├── README.md
└── requirements.txt
```

## Installation

```bash
pip install -r requirements.txt
```

## Scripts

| File | Description |
| ---- | ----------- |
| `csps_extraction/extract_benchmarks_data.py` | Reads existing CSPS benchmarks data and saves to database. |
| `csps_extraction/extract_demographics_data.py` | Reads existing CSPS demographics data and saves to database. |
| `csps_extraction/extract_organisations_data.py` | Reads existing CSPS organisations data and saves to database. |
| `csps_extraction/sql/compare_benchmarks_data.sql` | Replicates the collated benchmarks data from the CSPS working file, to be used as the basis for comparison in `compare_benchmarks_data.py`. |
| `csps_extraction/compare_benchmarks_data.py` | Validates that the SQL benchmarks data matches the source Excel file. |
| `csps_extraction/sql/compare_demographics_data.sql` | Replicates the collated demographics data from the CSPS working file, to be used as the basis for comparison in `compare_demographics_data.py`. |
| `csps_extraction/compare_demographics_data.py` | Validates that the SQL demographics data matches the source Excel file. |
| `csps_extraction/sql/compare_organisations_data.sql` | Replicates the collated organisations data from the CSPS working file, to be used as the basis for comparison in `compare_organisations_data.py`. |
| `csps_extraction/compare_organisations_data.py` | Validates that the augmented SQL organisations data matches the source Excel file. |
| `csps_extraction/sql/select_organisations_data.sql` | Script to be used for (re-)insertion of augmented data into Excel. Duplicates `compare_organisations_data.sql`, with the following differences to columns: <ul><li><strong>Organisation type</strong>: Reported as is</li><li><strong>Organisation aggregation?</strong>: Removed</li><li><strong>IfG core department</strong>: Added</li><li><strong>Latest organisation</strong>: Latest actual organisation always reported, rather than latest determinate organisation</li><li><strong>Latest departmental group</strong>: Latest actual (IfG) departmental group always reported, rather than latest determinate organisation</li></ul> |

## Environment variables
The scripts require the following environment variables to be set:

### Database connection (Azure SQL Database)
| Variable | Description |
| -------- | ----------- |
| `ODBC_DRIVER` | ODBC driver version for SQL Server (e.g., `ODBC Driver 18 for SQL Server`) |
| `ODBC_SERVER` | SQL Server hostname |
| `ODBC_DATABASE` | Database name |
| `ODBC_AUTHENTICATION` | Authentication method (e.g., `ActiveDirectoryServicePrincipal`) |
| `AZURE_CLIENT_ID` | Azure service principal client ID used for database authentication |
| `AZURE_CLIENT_SECRET` | Azure service principal client secret used for database authentication |

## Contributing

This project uses `pre-commit` hooks to ensure code quality. To set up:

1. Install `pre-commit` on your system if you don't already have it:

    ```bash
    pip install pre-commit
    ```

1. Set up `pre-commit` in your copy of this project. In the project directory, run:
    ```bash
    pre-commit install
    ```

Rules that are applied can be found in [`.pre-commit-config.yaml`](.pre-commit-config.yaml).

The hooks run automatically on commit, or manually with `pre-commit run --all-files`.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
