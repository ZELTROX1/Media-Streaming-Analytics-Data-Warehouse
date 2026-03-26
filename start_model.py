"""
star_schema_migration.py
------------------------
Migrates a flat movies CSV into a Star Schema in PostgreSQL.

Star Schema Layout:
    fact_movies         - central fact table with FKs + measures
    dim_genre           - genre dimension
    dim_rating          - MPAA rating dimension
    dim_country         - country dimension
    dim_company         - production company dimension
    dim_person          - people dimension (director / writer / star)
    dim_date            - release date dimension (year, month, day, quarter)

Requirements:
    pip install psycopg2-binary pandas python-dotenv

Usage:
    python star_schema_migration.py
    (reads DB credentials from .env)
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────────────────────
# 1. DDL
# ──────────────────────────────────────────────────────────────

DDL = """
-- ── Dimension: Genre ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_genre (
    genre_id   SERIAL PRIMARY KEY,
    genre_name TEXT UNIQUE NOT NULL
);

-- ── Dimension: Rating ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_rating (
    rating_id   SERIAL PRIMARY KEY,
    rating_code TEXT UNIQUE NOT NULL   -- G, PG, PG-13, R, NR …
);

-- ── Dimension: Country ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name TEXT UNIQUE NOT NULL
);

-- ── Dimension: Company ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_company (
    company_id   SERIAL PRIMARY KEY,
    company_name TEXT UNIQUE NOT NULL
);

-- ── Dimension: Person ─────────────────────────────────────────
-- Shared across director / writer / star roles
CREATE TABLE IF NOT EXISTS dim_person (
    person_id   SERIAL PRIMARY KEY,
    person_name TEXT UNIQUE NOT NULL
);

-- ── Dimension: Date ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_id        SERIAL PRIMARY KEY,
    full_date      DATE,
    release_year   INT,
    release_month  INT,
    release_day    INT,
    quarter        INT,
    release_label  TEXT    -- raw "released" string as fallback
);

-- ── Fact: Movies ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_movies (
    movie_id    SERIAL PRIMARY KEY,
    movie_name  TEXT,

    -- Foreign keys to dimensions
    genre_id    INT REFERENCES dim_genre(genre_id),
    rating_id   INT REFERENCES dim_rating(rating_id),
    country_id  INT REFERENCES dim_country(country_id),
    company_id  INT REFERENCES dim_company(company_id),
    director_id INT REFERENCES dim_person(person_id),
    writer_id   INT REFERENCES dim_person(person_id),
    star_id     INT REFERENCES dim_person(person_id),
    date_id     INT REFERENCES dim_date(date_id),

    -- Measures
    score       FLOAT,
    votes       BIGINT,
    budget      BIGINT,
    gross       BIGINT,
    runtime     INT
);
"""


def clean(val):
    """Return None for NaN / empty strings, else stripped string."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s and s.lower() != "nan" else None


def to_int(val):
    try:
        return int(float(str(val).replace(",", "").strip()))
    except (ValueError, TypeError):
        return None


def to_float(val):
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


def parse_date(released_str):
    """
    Try to parse the 'released' string into a date.
    Common formats in movie datasets: 'June 5, 1995', '1995-06-05', etc.
    Returns a dict with date components or None values on failure.
    """
    import datetime
    formats = ["%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%d-%b-%Y", "%m/%d/%Y"]
    if not released_str:
        return None
    for fmt in formats:
        try:
            d = datetime.datetime.strptime(released_str.strip(), fmt).date()
            return {
                "full_date":     d,
                "release_year":  d.year,
                "release_month": d.month,
                "release_day":   d.day,
                "quarter":       (d.month - 1) // 3 + 1,
            }
        except ValueError:
            continue
    return None


# ──────────────────────────────────────────────────────────────
# 3. Dimension loaders
#    Each function inserts unique values and returns a {value: id} map
# ──────────────────────────────────────────────────────────────

def load_dim_simple(cur, table, id_col, name_col, values: set) -> dict:
    """Generic loader for single-column text dimensions."""
    rows = [(v,) for v in values if v]
    if rows:
        execute_values(
            cur,
            f"INSERT INTO {table} ({name_col}) VALUES %s ON CONFLICT ({name_col}) DO NOTHING",
            rows,
        )
    cur.execute(f"SELECT {id_col}, {name_col} FROM {table}")
    return {name: pk for pk, name in cur.fetchall()}


def load_dim_date(cur, date_rows: list) -> dict:
    """
    Insert unique date records; return {release_label: date_id}.
    date_rows = list of (release_label, parsed_dict_or_None)
    """
    seen = {}
    for label, parsed in date_rows:
        if label in seen:
            continue
        if parsed:
            cur.execute(
                """
                INSERT INTO dim_date (full_date, release_year, release_month, release_day, quarter, release_label)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING date_id
                """,
                (
                    parsed["full_date"],
                    parsed["release_year"],
                    parsed["release_month"],
                    parsed["release_day"],
                    parsed["quarter"],
                    label,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO dim_date (release_label)
                VALUES (%s)
                ON CONFLICT DO NOTHING
                RETURNING date_id
                """,
                (label,),
            )
        result = cur.fetchone()
        if result:
            seen[label] = result[0]

    # Fetch any pre-existing rows too
    cur.execute("SELECT date_id, release_label FROM dim_date")
    for pk, lbl in cur.fetchall():
        if lbl and lbl not in seen:
            seen[lbl] = pk

    return seen


# ──────────────────────────────────────────────────────────────
# 4. Main migration
# ──────────────────────────────────────────────────────────────

def migrate(csv_path: str, conn):
    print(f"[load]  Reading {csv_path} ...")
    df = pd.read_csv(csv_path, on_bad_lines="skip")
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    print(f"[load]  {len(df):,} rows, columns: {list(df.columns)}")

    # Normalise column names to match the original flat schema
    col_map = {
        "movie_name": "name",
        "rating_target_audience": "rating",
        "release_year": "year",
        "release_date": "released",
        "release_year_and_country": None,   # informational only, not loaded
    }
    for new, old in col_map.items():
        if new in df.columns and old and old not in df.columns:
            df.rename(columns={new: old}, inplace=True)

    cur = conn.cursor()

    # ── Create tables ─────────────────────────────────────────
    print("[db]    Creating star schema tables ...")
    cur.execute(DDL)
    conn.commit()

    # ── Collect unique dimension values ───────────────────────
    genres    = set(clean(v) for v in df.get("genre",    []) if clean(v))
    ratings   = set(clean(v) for v in df.get("rating",   []) if clean(v))
    countries = set(clean(v) for v in df.get("country",  []) if clean(v))
    companies = set(clean(v) for v in df.get("company",  []) if clean(v))
    persons   = set()
    for col in ("director", "writer", "star"):
        persons.update(clean(v) for v in df.get(col, []) if clean(v))

    date_rows = []
    for v in df.get("released", []):
        label = clean(v)
        if label:
            date_rows.append((label, parse_date(label)))

    # ── Load dimensions ───────────────────────────────────────
    print("[db]    Loading dimensions ...")
    genre_map   = load_dim_simple(cur, "dim_genre",   "genre_id",   "genre_name",   genres)
    rating_map  = load_dim_simple(cur, "dim_rating",  "rating_id",  "rating_code",  ratings)
    country_map = load_dim_simple(cur, "dim_country", "country_id", "country_name", countries)
    company_map = load_dim_simple(cur, "dim_company", "company_id", "company_name", companies)
    person_map  = load_dim_simple(cur, "dim_person",  "person_id",  "person_name",  persons)
    date_map    = load_dim_date(cur, date_rows)
    conn.commit()
    print(f"[db]    Dimensions loaded: "
          f"{len(genre_map)} genres, {len(rating_map)} ratings, "
          f"{len(country_map)} countries, {len(company_map)} companies, "
          f"{len(person_map)} persons, {len(date_map)} dates")

    # ── Build fact rows ───────────────────────────────────────
    print("[db]    Building fact rows ...")
    fact_rows = []
    for _, r in df.iterrows():
        fact_rows.append((
            clean(r.get("name")),
            genre_map.get(clean(r.get("genre"))),
            rating_map.get(clean(r.get("rating"))),
            country_map.get(clean(r.get("country"))),
            company_map.get(clean(r.get("company"))),
            person_map.get(clean(r.get("director"))),
            person_map.get(clean(r.get("writer"))),
            person_map.get(clean(r.get("star"))),
            date_map.get(clean(r.get("released"))),
            to_float(r.get("score")),
            to_int(r.get("votes")),
            to_int(r.get("budget")),
            to_int(r.get("gross")),
            to_int(r.get("runtime")),
        ))

    # ── Insert fact table ─────────────────────────────────────
    FACT_INSERT = """
    INSERT INTO fact_movies (
        movie_name,
        genre_id, rating_id, country_id, company_id,
        director_id, writer_id, star_id, date_id,
        score, votes, budget, gross, runtime
    ) VALUES %s
    """
    BATCH = 500
    total, inserted = len(fact_rows), 0
    with conn.cursor() as c:
        for start in range(0, total, BATCH):
            execute_values(c, FACT_INSERT, fact_rows[start : start + BATCH])
            inserted += len(fact_rows[start : start + BATCH])
            print(f"[db]    Inserted {inserted:,} / {total:,} fact rows ...", end="\r")
    conn.commit()
    print(f"\n[db]    Done - {inserted:,} fact rows committed.")

    cur.close()


# ──────────────────────────────────────────────────────────────
# 5. Entry point
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="moviedb",
        user=os.getenv("POSTGRESS_USER"),
        password=os.getenv("POSTGRESS_PASSWORD"),
    )

    try:
        migrate("cleaned_movies.csv", conn)
    except Exception as e:
        conn.rollback()
        print(f"{e}")
        raise
    finally:
        conn.close()
        print("Connection closed.")