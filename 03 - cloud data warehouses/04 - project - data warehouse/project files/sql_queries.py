import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')
REGION = config.get('AWS_SITE','REGION')
ARN = config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(artist TEXT,
 auth TEXT,
 firstName TEXT,
 gender CHAR(1),
 itemInSession INT,
 lastName TEXT,
 length DECIMAL,
 level TEXT,
 location TEXT,
 method TEXT,
 page TEXT,
 registration TEXT,
 sessionId INT,
 song TEXT,
 status INT,
 ts TIMESTAMP,
 userAgent TEXT,
 userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(artist_id TEXT,
 artist_latitude DECIMAL,
 artist_location TEXT,
 artist_longitude DECIMAL,
 artist_name TEXT,
 duration DECIMAL,
 num_songs INT,
 song_id TEXT,
 title TEXT,
 year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(songplay_id INT IDENTITY PRIMARY KEY,
 start_time TIMESTAMP NOT NULL REFERENCES time(start_time) sortkey,
 user_id INTEGER NOT NULL REFERENCES users(user_id),
 level VARCHAR,
 song_id VARCHAR NOT NULL REFERENCES songs(song_id),
 artist_id VARCHAR NOT NULL REFERENCES artists(artist_id) distkey,
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
 gender CHAR(1),
 level CHAR(4)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(song_id VARCHAR PRIMARY KEY,
 title VARCHAR NOT NULL,
 artist_id VARCHAR NOT NULL,
 year INTEGER,
 duration DECIMAL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(artist_id VARCHAR PRIMARY KEY distkey,
 name VARCHAR,
 location TEXT,
 latitude NUMERIC,
 longitude NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(start_time TIMESTAMP PRIMARY KEY sortkey,
 hour INTEGER,
 day INTEGER,
 week INTEGER,
 month INTEGER,
 year INTEGER,
 weekday TEXT
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON {}
    TIMEFORMAT 'epochmillisecs'
    REGION {};
    """).format(LOG_DATA, ARN, LOG_JSONPATH, REGION)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto'
    REGION {};
    """).format(SONG_DATA, ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(start_time,
 user_id,
 level,
 song_id,
 artist_id,
 session_id,
 location,
 user_agent
)
SELECT DISTINCT se.ts,
                se.userId,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId,
                ss.artist_location,
                se.userAgent
FROM staging_events se
JOIN staging_songs ss ON se.song = ss.title
WHERE se.page = 'NextSong'
""")


user_table_insert = ("""
INSERT INTO users
(user_id,
 first_name,
 last_name,
 gender,
 level
)
SELECT DISTINCT se.userId,
                se.firstName,
                se.lastName,
                se.gender,
                se.level
FROM staging_events se
WHERE se.userId IS NOT NULL
  AND se.ts = (SELECT MAX(ts)
               FROM staging_events
               WHERE se.userId = staging_events.userId)
""")

song_table_insert = ("""
INSERT INTO songs
(song_id,
 title,
 artist_id,
 year,
 duration
)
SELECT DISTINCT ss.song_id,
                ss.title,
                ss.artist_id,
                ss.year,
                ss.duration
FROM staging_songs ss
WHERE ss.song_id iS NOT NULL
  AND ss.artist_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id,
 name,
 location,
 latitude,
 longitude
)
SELECT DISTINCT ss.artist_id,
                ss.artist_name,
                ss.artist_location,
                ss.artist_latitude,
                ss.artist_longitude
FROM staging_songs ss
WHERE ss.artist_id IS NOT NULL
  AND ss.artist_name IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time
(start_time,
 hour,
 day,
 week,
 month,
 year,
 weekday
)
SELECT DISTINCT ts,
                EXTRACT(hour from ts),
                EXTRACT(day from ts),
                EXTRACT(week from ts),
                EXTRACT(month from ts),
                EXTRACT(year from ts),
                EXTRACT(dayofweek from ts)
FROM staging_events se
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
