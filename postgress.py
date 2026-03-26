import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

load_dotenv()

df = pd.read_csv("cleaned_movies.csv")

# 2. Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="moviedb",
    user=os.getenv("POSTGRESS_USER"),
    password=os.getenv("POSTGRESS_PASSWORD")
)

cur = conn.cursor()

# 3. Create table
cur.execute("""
CREATE TABLE IF NOT EXISTS movies (
    id SERIAL PRIMARY KEY,
    name TEXT,
    rating TEXT,
    genre TEXT,
    year INT,
    released TEXT,
    score FLOAT,
    votes BIGINT,
    director TEXT,
    writer TEXT,
    star TEXT,
    country TEXT,
    budget BIGINT,
    gross BIGINT,
    company TEXT,
    runtime INT
);
""")

rows = [tuple(x) for x in df.to_numpy()]

execute_values(cur, """
INSERT INTO movies (
    name, rating, genre, year, released,
    score, votes, director, writer, star,
    country, budget, gross, company, runtime
) VALUES %s
""", rows)

conn.commit()
cur.close()
conn.close()

print("Data inserted successfully!")