import os
import requests
import csv
import time
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Text, Date,
    ForeignKey, Table
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# =====================================
# ENV
# =====================================
load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")

DATABASE_URL = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

# =====================================
# DB
# =====================================
Base = declarative_base()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# =====================================
# ASSOCIATION
# =====================================
movies_genres_table = Table(
    "movies_genres",
    Base.metadata,
    Column("movie_id", Integer, ForeignKey("movies.id"), primary_key=True),
    Column("genre_id", Integer, ForeignKey("genres.genre_id"), primary_key=True)
)

# =====================================
# MODELS
# =====================================
class Movie(Base):
    __tablename__ = "movies"

    id = Column(Integer, primary_key=True)
    title = Column(String(225))
    overview = Column(Text)
    release_date = Column(Date)
    vote_average = Column(Float)
    vote_count = Column(Integer)
    popularity = Column(Float)

    genres = relationship(
        "Genre",
        secondary=movies_genres_table,
        back_populates="movies"
    )


class Genre(Base):
    __tablename__ = "genres"

    genre_id = Column(Integer, primary_key=True)
    genre_name = Column(Text)

    movies = relationship(
        "Movie",
        secondary=movies_genres_table,
        back_populates="genres"
    )

# =====================================
# HELPERS
# =====================================
def parse_date_safe(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except:
        return None

# =====================================
# HTTP SESSION (IMPORTANT)
# =====================================
http_session = requests.Session()
http_session.headers.update({
    "Accept": "application/json"
})

# =====================================
# FETCH WITH RETRY + BACKOFF
# =====================================
def fetch_movies_batch(start_page, pages_per_batch):
    movies = []

    for page in range(start_page, start_page + pages_per_batch):
        retries = 3

        while retries > 0:
            try:
                print(f"Fetching Page {page}...")

                url = (
                    f"https://api.themoviedb.org/3/discover/movie"
                    f"?api_key={TMDB_API_KEY}&language=en-US&page={page}"
                )

                response = http_session.get(url, timeout=10)

                if response.status_code != 200:
                    raise Exception(f"HTTP {response.status_code}")

                data = response.json()
                results = data.get("results", [])

                if not results:
                    return movies

                movies.extend(results)
                time.sleep(0.4)  # SAFE rate
                break

            except Exception as e:
                retries -= 1
                print(f"Retrying page {page} ({retries} left): {e}")
                time.sleep(2)

        else:
            print(f"Skipping page {page} after retries")

    return movies

# =====================================
# CSV (APPEND)
# =====================================
def save_to_csv(movies, filename="popular_movies.csv"):
    file_exists = os.path.exists(filename)

    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow([
                "id", "title", "overview", "release_date",
                "vote_average", "vote_count", "popularity", "genre_ids"
            ])

        for m in movies:
            writer.writerow([
                m.get("id"),
                m.get("title"),
                m.get("overview"),
                m.get("release_date"),
                m.get("vote_average"),
                m.get("vote_count"),
                m.get("popularity"),
                ",".join(map(str, m.get("genre_ids", [])))
            ])

# =====================================
# DB BATCH INSERT
# =====================================
def save_batch_to_db(movies):
    session = Session()

    genre_cache = {
        g.genre_id: g
        for g in session.query(Genre).all()
    }

    for m in movies:
        if session.get(Movie, m["id"]):
            continue

        movie = Movie(
            id=m["id"],
            title=m.get("title"),
            overview=m.get("overview"),
            release_date=parse_date_safe(m.get("release_date")),
            vote_average=m.get("vote_average"),
            vote_count=m.get("vote_count"),
            popularity=m.get("popularity")
        )

        for gid in m.get("genre_ids", []):
            if gid not in genre_cache:
                genre_cache[gid] = Genre(
                    genre_id=gid,
                    genre_name=f"Genre {gid}"
                )
                session.add(genre_cache[gid])

            movie.genres.append(genre_cache[gid])

        session.add(movie)

    session.commit()
    session.close()
    print(f"Inserted {len(movies)} movies")

# =====================================
# MAIN
# =====================================
def main():
    MAX_PAGES = 100
    PAGES_PER_BATCH = 5

    print("Starting TMDB Batch Pipeline...")
    print(f"DB Connected: {DATABASE_URL}")

    page = 1

    while page <= MAX_PAGES:
        print(f"\nProcessing batch from page {page}")

        batch = fetch_movies_batch(page, PAGES_PER_BATCH)

        if not batch:
            break

        save_to_csv(batch)
        save_batch_to_db(batch)

        page += PAGES_PER_BATCH

    print("\nPipeline completed successfully")

# =====================================
# Main ENTRY
# =====================================
if __name__ == "__main__":
    main()







# import os
# import requests
# import csv
# from datetime import datetime
# from dotenv import load_dotenv
# from sqlalchemy import (
#     create_engine, Column, Integer, String, Float, Text, Date,
#     ForeignKey, Table
# )
# from sqlalchemy.orm import declarative_base, relationship, sessionmaker
# import time

# # =====================================
# # STEP 1: Load environment variables
# # =====================================
# load_dotenv()

# TMDB_API_KEY = os.getenv("TMDB_API_KEY")
# PG_USER = os.getenv("PG_USER")
# PG_PASSWORD = os.getenv("PG_PASSWORD")
# PG_HOST = os.getenv("PG_HOST")
# PG_PORT = os.getenv("PG_PORT")
# PG_DB = os.getenv("PG_DB")

# DATABASE_URL = (
#     f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
# )

# # =====================================
# # STEP 2: Database Setup
# # =====================================
# Base = declarative_base()
# engine = create_engine(DATABASE_URL)
# Session = sessionmaker(bind=engine)
# session = Session()

# # =====================================
# # STEP 3: Association Table
# # =====================================
# movies_genres_table = Table(
#     "movies_genres",
#     Base.metadata,
#     Column("movie_id", Integer, ForeignKey("movies.id"), primary_key=True),
#     Column("genre_id", Integer, ForeignKey("genres.genre_id"), primary_key=True)
# )

# # =====================================
# # STEP 4: Models
# # =====================================
# class Movie(Base):
#     __tablename__ = "movies"

#     id = Column(Integer, primary_key=True)
#     title = Column(String(225))
#     overview = Column(Text)
#     release_date = Column(Date)
#     vote_average = Column(Float)
#     vote_count = Column(Integer)
#     popularity = Column(Float)

#     genres = relationship(
#         "Genre",
#         secondary=movies_genres_table,
#         back_populates="movies"
#     )


# class Genre(Base):
#     __tablename__ = "genres"

#     genre_id = Column(Integer, primary_key=True)
#     genre_name = Column(Text)

#     movies = relationship(
#         "Movie",
#         secondary=movies_genres_table,
#         back_populates="genres"
#     )


# # =====================================
# # STEP 5: Fetch Movies From TMDB
# # =====================================
# def fetch_popular_movies(max_pages=100):
#     all_movies = []

#     for page in range(1, max_pages + 1):
#         print(f"Fetching Page {page}...")

#         url = (
#             f"https://api.themoviedb.org/3/discover/movie?"
#             f"api_key={TMDB_API_KEY}&language=en-US&page={page}"
#         )

#         response = requests.get(url)
#         data = response.json()

#         results = data.get("results", [])
#         if not results:
#             break

#         all_movies.extend(results)
#         time.sleep(0.2)  # Avoid rate limit

#     print(f"Total movies fetched: {len(all_movies)}")
#     return all_movies


# # =====================================
# # STEP 6: Save Movies to CSV FIRST
# # =====================================
# def save_to_csv(movies, filename="popular_movies.csv"):
#     print(f"Saving fetched movies to CSV: {filename}")

#     csv_header = [
#         "id", "title", "overview", "release_date",
#         "vote_average", "vote_count", "popularity", "genre_ids"
#     ]

#     with open(filename, "w", newline="", encoding="utf-8") as f:
#         writer = csv.DictWriter(f, fieldnames=csv_header)
#         writer.writeheader()

#         for m in movies:
#             writer.writerow({
#                 "id": m.get("id"),
#                 "title": m.get("title"),
#                 "overview": m.get("overview"),
#                 "release_date": m.get("release_date"),
#                 "vote_average": m.get("vote_average"),
#                 "vote_count": m.get("vote_count"),
#                 "popularity": m.get("popularity"),
#                 "genre_ids": ",".join(str(g) for g in m.get("genre_ids", []))
#             })

#     print("CSV saved successfully!")


# # =====================================
# # STEP 7: Helper – Fix Release Date
# # =====================================
# def parse_date_safe(date_str):
#     if not date_str or date_str.strip() == "":
#         return None

#     try:
#         return datetime.strptime(date_str, "%Y-%m-%d").date()
#     except:
#         return None


# # =====================================
# # STEP 8: Save Movies to PostgreSQL
# # =====================================
# def save_to_db(movies):
#     print("Saving movies + genres + mapping to DB...")

#     for movie in movies:

#         release_date_fixed = parse_date_safe(movie.get("release_date"))

#         existing_movie = session.get(Movie, movie["id"])

#         if not existing_movie:
#             new_movie = Movie(
#                 id=movie["id"],
#                 title=movie.get("title"),
#                 overview=movie.get("overview"),
#                 release_date=release_date_fixed,
#                 vote_average=movie.get("vote_average"),
#                 vote_count=movie.get("vote_count"),
#                 popularity=movie.get("popularity")
#             )
#             session.add(new_movie)
#             session.flush()
#             existing_movie = new_movie

#         genre_ids = movie.get("genre_ids", [])

#         for gid in genre_ids:
#             genre = session.get(Genre, gid)

#             if not genre:
#                 genre = Genre(
#                     genre_id=gid,
#                     genre_name=f"Genre {gid}"
#                 )
#                 session.add(genre)
#                 session.flush()

#             if genre not in existing_movie.genres:
#                 existing_movie.genres.append(genre)

#     session.commit()
#     print("Data Saved Successfully to DB!")


# # =====================================
# # STEP 9: MAIN
# # =====================================
# def main():
#     print("Starting TMDB → CSV → PostgreSQL Sync...")
#     print(f"Connected to DB: {DATABASE_URL}")

#     movies = fetch_popular_movies(max_pages=100)  # Fetch 2000 movies

#     if movies:
#         save_to_csv(movies)
#         save_to_db(movies)
#     else:
#         print("No movies fetched.")


# if __name__ == "__main__":
#     main()


