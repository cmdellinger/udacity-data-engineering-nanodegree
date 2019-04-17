# DROP TABLES

songplay_table_drop = "DROP table songplays"
user_table_drop = "DROP table users"
song_table_drop = "DROP table songs"
artist_table_drop = "DROP table artists"
time_table_drop = "DROP table time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(songplay_id int PRIMARY KEY,
 start_time REFERENCES time(start_time),
 user_id REFERENCES users(user_id),
 level REFERENCES users(level),
 song_id REFERENCES songs(song_id),
 artist_id REFERENCES artists(artist_id),
 session_id INTEGER,
 location TEXT,
 user_agent TEXT
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(user_id INTEGER PRIMARY KEY,
 first_name VARCHAR,
 last_name VARCHAR,
 gender VARCHAR,
 level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(song_id VARCHAR PRIMARY KEY,
 title TEXT,
 artist_id REFERENCES artists(artist_id),
 year INTEGER,
 duration NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(artist_id INTEGER PRIMARY KEY,
 name VARCHAR,
 location TEXT,
 lattitude NUMBERIC,
 longitude NUMBERIC
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(start_time NUMBERIC PRIMARY KEY,
 hour INTEGER,
 day INTEGER,
 week INTEGER,
 month INTEGER,
 year INTEGER,
 weekday INTEGER
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]