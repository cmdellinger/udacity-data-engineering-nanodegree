""" ETL pipline

Reads input JSON files in `data/log_data` and `data/song_data` trees,
then transforms and inserts the information into the Postgres database
schema defined in `sql_queries.py` and `README.md`.
"""

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ Extracts song JSON file based on schema and inserts it into SQL table.

    The function extracts applicable data in the song data JSON file located at
    `filepath`. The applicable data columns are transformed and inserted into
    the `songs` and `artists` tables according to the schema in `sql_queries`
    and `README`.

    Args:
        cur (psycopg2 connection cursor): cursor for the database connection
        filepath (str): filepath of song data JSON file
    Returns:
        `None`: actions performed, but no return value
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', \
                    'year', 'duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', \
                      'artist_latitude', 'artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ Extracts log JSON file based on schema and inserts it into SQL table.

    The function extracts applicable information from the log event JSON file
    located at `filepath`.  The applicable data columns are transformed and
    inserted into the `time`, `users`, and `songplays` tables according to the
    schema in `sql_queries` and `README`.

    Args:
        cur (psycopg2 connection cursor): cursor for the database connection
        filepath (str): filepath of song data JSON file
    Returns:
        `None`: actions performed, but no return value
    """
    # open log file
    df = pd.read_json(filepath, lines=True, \
                      convert_dates=['ts'], date_unit='ms')

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = df.ts # already converted on load

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, \
                 t.dt.month, t.dt.year, t.dt.weekday_name)
    column_labels = ('timestamp', 'hour', 'day', 'week_of_year', \
                     'month', 'year', 'weekday')
    time_df = pd.DataFrame({label:column for column, label in \
                            zip(time_data, column_labels)})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, \
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ Wrapper that scans for JSON files and passes them to a function.

    Function recursively scans directory trees with root directory of
    `filepath` for any JSON files. The passed function `func` is then called on
    each JSON file.

    Args:
        cur (psycopg2 connection cursor): cursor for the database connection
        conn (psycopg2 connection): connection to database
        filepath (str): filepath of directory to scan recursively
        func (function): function object called on JSON files in `filepath`
    Returns:
        `None`: actions performed, but no return value
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb \
                             user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
