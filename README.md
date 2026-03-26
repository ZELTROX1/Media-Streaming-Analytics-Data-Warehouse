# Media Streaming Analytics Data Warehouse

An analytics-focused data warehouse project for movie/streaming-style content data using PostgreSQL (star schema), DuckDB (federated querying), and Python ETL scripts.

## Project Overview

This project takes cleaned movie data and loads it into a PostgreSQL warehouse designed as a **star schema**:

- `fact_movies` as the central fact table
- Dimension tables for genre, rating, country, company, people, and date

It also includes:

- A flat-table loader (`movies`) for simpler use cases
- A DuckDB script that attaches PostgreSQL and runs analytical joins
- A plan to build visual dashboards in **Metabase**

## Star Model Diagram

Add your star schema image at `assets/star-model.png`:

![Star Model](./assets/star-model.png)

## Tech Stack

- Python
- PostgreSQL
- DuckDB + DuckDB Postgres extension
- Pandas
- Psycopg2
- SQLAlchemy
- python-dotenv
- Metabase (for BI dashboards)

## Project Structure

```text
Media-Streaming-Analytics-Data-Warehouse/
├── cleaned_movies.csv
├── dataextraction.ipynb
├── postgress.py      # loads flat table: movies
├── start_model.py    # builds dimensions + fact table (star schema)
├── duck_db.py        # queries PostgreSQL warehouse through DuckDB
├── testing.py        # simple SQLAlchemy connection/query test
└── .env
```

## Prerequisites

- Python 3.9+
- PostgreSQL running locally on `localhost:5432`
- A PostgreSQL database named `moviedb`

## Environment Variables

Create `.env` in the project root:

```env
POSTGRESS_USER=your_postgres_user
POSTGRESS_PASSWORD=your_postgres_password
```

Note: the scripts currently use `POSTGRESS_*` (double `s`) variable names.

## Setup

```bash
cd Media-Streaming-Analytics-Data-Warehouse
python -m venv .venv
source .venv/bin/activate
pip install pandas psycopg2-binary python-dotenv duckdb sqlalchemy
```

## Run the Pipeline

1. Build and load the star schema:

```bash
python start_model.py
```

2. Optional: load a flat `movies` table (non-star model):

```bash
python postgress.py
```

3. Run analytical query through DuckDB attached to PostgreSQL:

```bash
python duck_db.py
```

4. Optional connection check:

```bash
python testing.py
```

## Warehouse Schema (Star)

- `fact_movies`
  - Measures: `score`, `votes`, `budget`, `gross`, `runtime`
  - Foreign keys: `genre_id`, `rating_id`, `country_id`, `company_id`, `director_id`, `writer_id`, `star_id`, `date_id`
- `dim_genre`
- `dim_rating`
- `dim_country`
- `dim_company`
- `dim_person`
- `dim_date`

## Metabase Visualization (Planned)

This project will include BI dashboards in Metabase on top of `moviedb`.

Suggested first dashboards:

- Top grossing movies by year/quarter
- Genre-wise average ratings
- Budget vs gross performance
- Production company performance
- Director/star performance trends

### Metabase Connection Steps

1. Open Metabase Admin settings.
2. Add database: PostgreSQL.
3. Host: `localhost`, Port: `5432`, DB: `moviedb`.
4. Use the same credentials from `.env`.
5. Sync schema and start building questions/dashboards from `fact_movies` + dimensions.

## Notes

- `dataextraction.ipynb` is used for dataset preparation/cleaning.
- `cleaned_movies.csv` is the input for loading scripts.
- Date parsing in `start_model.py` supports multiple release date formats.

## Future Improvements

- Add incremental loading (upsert/dedup strategy)
- Add dbt models/tests for data quality
- Add orchestration (Airflow/Prefect)
- Version and publish Metabase dashboard definitions
