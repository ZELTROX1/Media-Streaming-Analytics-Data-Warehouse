import duckdb
import os
from dotenv import load_dotenv
 
load_dotenv()
 
PG_USER     = os.getenv("POSTGRESS_USER")
PG_PASSWORD = os.getenv("POSTGRESS_PASSWORD")
PG_HOST     = "localhost"
PG_PORT     = 5432
PG_DB       = "moviedb"
 


con = duckdb.connect()
 
con.execute("INSTALL postgres; LOAD postgres;")
 
con.execute(f"""
    ATTACH 'host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASSWORD}'
    AS moviedb (TYPE POSTGRES);
""")

result = con.execute("""
    SELECT
        f.movie_name,
        g.genre_name,
        r.rating_code,
        p_dir.person_name   AS director,
        p_star.person_name  AS star,
        d.release_year,
        d.quarter,
        f.score,
        f.votes,
        f.budget,
        f.gross,
        f.runtime
    FROM  moviedb.fact_movies   f
    JOIN  moviedb.dim_genre     g     ON f.genre_id    = g.genre_id
    JOIN  moviedb.dim_rating    r     ON f.rating_id   = r.rating_id
    JOIN  moviedb.dim_person    p_dir ON f.director_id = p_dir.person_id
    JOIN  moviedb.dim_person    p_star ON f.star_id    = p_star.person_id
    JOIN  moviedb.dim_date      d     ON f.date_id     = d.date_id
    ORDER BY f.gross DESC NULLS LAST
    LIMIT 10
""").fetchdf()


print(result.to_string(index=False))
 
con.close()
 