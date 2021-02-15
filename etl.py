from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm

from sql_queries import *


def generate_tuples(df, columns=None):
    """ Generate data tuples corresponding to an database row insert. """
    df = df[columns] if columns else df
    df = df.replace({np.nan: None, 0: None})
    tuples = list(tuple(x) for x in df.to_numpy())

    return tuples


def timestamp_from_ms(series):
    """ Convert time in millisecond format to `datatime.timestamp`. """
    return pd.to_datetime(series, unit='ms')


def process_song_file(cur, filepath):
    """ Process each song file to generate rows for the `songs` and `artists` tables """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_tuples = generate_tuples(df, ['song_id', 'title', 'artist_id', 'year', 'duration'])
    execute_values(cur, song_table_insert, song_tuples)

    # insert artist record
    artist_tuples = generate_tuples(df,
        ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    execute_values(cur, artist_table_insert, artist_tuples)


def process_log_file(cur, filepath):
    """ Process each log file to generate the `users` and `songplay` tables """
    df = pd.read_json(filepath, lines=True)         # read in log file
    df = df[df.page == 'NextSong']                  # filter by NextSong action
    t = timestamp_from_ms(df['ts']).to_list()       # convert timestamp column to datetime

    # insert time data records
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_data = [(str(x), x.hour, x.day, x.week, x.month, x.year, x.dayofweek) for x in t]
    time_df = pd.DataFrame(time_data, columns=column_labels)
    time_tuples = generate_tuples(time_df)
    execute_values(cur, time_table_insert, time_tuples)

    # load and write to user table
    user_tuples = generate_tuples(df, ['userId', 'firstName', 'lastName', 'gender', 'level'])
    execute_values(cur, user_table_insert, user_tuples)

    # insert songplay records
    for _, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        song_id, artist_id = results if (results := cur.fetchone()) else (None, None)

        # insert songplay record
        songplay_data = (
            str(timestamp_from_ms(row.ts)),
            row.userId,
            row.level,
            song_id,
            artist_id,
            row.sessionId,
            row.location,
            row.userAgent
        )
        execute_values(cur, songplay_table_insert, [songplay_data])


def process_data(cur, conn, root, func):
    # get all files matching extension from directory
    root = Path(root).resolve()
    all_paths = list(root.rglob('*.json'))

    # get total number of files found
    num_files = len(all_paths)
    print(f'{num_files} files found in {root.name}')

    # perform ETL on all found files
    for i, datapath in enumerate(tqdm(all_paths), 1):
        func(cur, datapath)
        conn.commit()


def main():
    # conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn = psycopg2.connect(host='localhost', dbname='sparkifydb', user='jessep', password='admin')
    cur = conn.cursor()

    process_data(cur, conn, root='data/song_data', func=process_song_file)
    process_data(cur, conn, root='data/log_data', func=process_log_file)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
