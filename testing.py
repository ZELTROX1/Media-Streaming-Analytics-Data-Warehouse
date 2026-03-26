from sqlalchemy import create_engine, text

engine = create_engine("postgresql://praneeth:praneeth123@localhost:5432/moviedb")

with engine.connect() as conn:
    result = conn.execute(text("SELECT movie_name FROM movies LIMIT 10;"))

    for row in result:
        print(row)