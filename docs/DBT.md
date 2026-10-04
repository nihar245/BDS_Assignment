# dbt

The `dbt/` project uses the `postgres` adapter and reads connection values from environment variables. `stg_uber_trips` validates the staging relation, `fct_uber_trips` is the row-level analytical fact, and aggregate models support dates, hours, and bases. Run `dbt run --profiles-dir .` and `dbt test --profiles-dir .` from `dbt/` after PostgreSQL is ready.
