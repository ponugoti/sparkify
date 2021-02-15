# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INT,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INT,
        duration NUMERIC
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        latitude NUMERIC,
        longitude NUMERIC
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY,
        hour SMALLINT,
        day SMALLINT,
        week SMALLINT,
        month SMALLINT,
        year SMALLINT,
        weekday SMALLINT
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    VALUES %s
    ON CONFLICT
    DO NOTHING
""")

user_table_insert = ("""
    INSERT INTO users
    VALUES %s
    ON CONFLICT
    DO NOTHING
""")

song_table_insert = ("""
    INSERT INTO songs
    VALUES %s
    ON CONFLICT (song_id)
    DO UPDATE
    SET (title, artist_id, year, duration) =
        (EXCLUDED.title, EXCLUDED.artist_id, EXCLUDED.year, EXCLUDED.duration)
""")

artist_table_insert = ("""
    INSERT INTO artists
    VALUES %s
    ON CONFLICT (artist_id)
    DO UPDATE
    SET (name, location, latitude, longitude) =
        (EXCLUDED.name, EXCLUDED.location, EXCLUDED.latitude, EXCLUDED.longitude)
""")


time_table_insert = ("""
    INSERT INTO time
    VALUES %s
    ON CONFLICT
    DO NOTHING
""")

# FIND SONGS

song_select = ("""
    SELECT
        songs.song_id,
        artists.artist_id
    FROM songs
    JOIN artists
    ON songs.artist_id = artists.artist_id
    AND songs.title = %s
    AND artists.name = %s
    AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]