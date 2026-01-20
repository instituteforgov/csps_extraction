# Civil Service People Survey extraction

Scripts for extracting Civil Service People Survey (CSPS) data.

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
