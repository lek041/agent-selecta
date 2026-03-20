import sqlite3
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

SPOTIFY_DIR = "/Volumes/Data - 01 GiB/spotify-metadata-by-annas-archive/spotify_clean_parquet"
SQLITE_PATH = "/Volumes/Data - 01 GiB/spotify-metadata-by-annas-archive/spotify_agent.db"

def converter():
    print("=" * 45)
    print("   Convertendo Spotify Parquet -> SQLite")
    print("=" * 45)
    print()

    conn = sqlite3.connect(SQLITE_PATH)
    cur  = conn.cursor()

    # ── artists ──────────────────────────────
    print("[1/4] Convertendo artists...")
    cur.execute("DROP TABLE IF EXISTS artists")
    cur.execute("""
        CREATE TABLE artists (
            rowid INTEGER PRIMARY KEY,
            name  TEXT
        )
    """)
    artists_df = pd.read_parquet(f"{SPOTIFY_DIR}/artists.parquet", columns=["rowid", "name"])
    artists_df.to_sql("artists", conn, if_exists="append", index=False, chunksize=100000)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_artists_name ON artists(name)")
    conn.commit()
    print(f"   {len(artists_df)} artistas inseridos!")

    # ── artist_genres ─────────────────────────
    print("[2/4] Convertendo artist_genres...")
    cur.execute("DROP TABLE IF EXISTS artist_genres")
    cur.execute("""
        CREATE TABLE artist_genres (
            artist_rowid INTEGER,
            genre        TEXT
        )
    """)
    genres_df = pd.read_parquet(f"{SPOTIFY_DIR}/artist_genres.parquet")
    genres_df.to_sql("artist_genres", conn, if_exists="append", index=False, chunksize=100000)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_genres_artist ON artist_genres(artist_rowid)")
    conn.commit()
    print(f"   {len(genres_df)} generos inseridos!")

    # ── albums ────────────────────────────────
    print("[3/4] Convertendo albums (pode demorar)...")
    cur.execute("DROP TABLE IF EXISTS albums")
    cur.execute("""
        CREATE TABLE albums (
            rowid        INTEGER PRIMARY KEY,
            name         TEXT,
            album_type   TEXT,
            release_date TEXT,
            popularity   INTEGER
        )
    """)
    # Lê em chunks para não travar
    parquet_file = pq.ParquetFile(f"{SPOTIFY_DIR}/albums.parquet")
    total = 0
    for batch in parquet_file.iter_batches(
        batch_size=500000,
        columns=["rowid", "name", "album_type", "release_date", "popularity"]
    ):
        df = batch.to_pandas()
        df.to_sql("albums", conn, if_exists="append", index=False, chunksize=100000)
        total += len(df)
        print(f"   {total:,} albums inseridos...", end="\r")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_albums_rowid ON albums(rowid)")
    conn.commit()
    print(f"\n   {total:,} albums inseridos!")

    # ── artist_albums ─────────────────────────
    print("[4/4] Convertendo artist_albums (pode demorar)...")
    cur.execute("DROP TABLE IF EXISTS artist_albums")
    cur.execute("""
        CREATE TABLE artist_albums (
            artist_rowid INTEGER,
            album_rowid  INTEGER,
            is_appears_on INTEGER
        )
    """)
    parquet_file = pq.ParquetFile(f"{SPOTIFY_DIR}/artist_albums.parquet")
    total = 0
    for batch in parquet_file.iter_batches(
        batch_size=500000,
        columns=["artist_rowid", "album_rowid", "is_appears_on"]
    ):
        df = batch.to_pandas()
        # Filtra apenas albums principais (nao appears_on)
        df = df[df["is_appears_on"] == 0]
        df.to_sql("artist_albums", conn, if_exists="append", index=False, chunksize=100000)
        total += len(df)
        print(f"   {total:,} relacoes inseridas...", end="\r")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_artist_albums ON artist_albums(artist_rowid)")
    conn.commit()
    print(f"\n   {total:,} relacoes inseridas!")

    conn.close()
    print(f"\n{'=' * 45}")
    print(f"Concluido! SQLite salvo em:")
    print(f"{SQLITE_PATH}")
    print(f"{'=' * 45}\n")

if __name__ == "__main__":
    converter()
