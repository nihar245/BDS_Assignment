# Uber dbt project

This project targets PostgreSQL database `uber_streaming` and schema `analytics`. Load the root `.env` into the host PowerShell process, change to this directory, and run:

```powershell
dbt deps
dbt run --profiles-dir .
dbt test --profiles-dir .
```

The verified project result is five successful models and 12/12 tests.
