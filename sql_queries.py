import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events(
artist text,
auth text,	
firstName text,	
gender text,	
itemInSession int,	
lastName text,	
length numeric,	
level text,	
location text,	
method text,	
page text,	
registration numeric,	
sessionId int,	
song text,	
status int,	
ts bigint,	
userAgent text,	
userId int
)
"""


staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs(
num_songs int, 
artist_id text, 
artist_latitude numeric,
artist_longitude numeric,	
artist_location text,	
artist_name text,	
song_id text,	
title text,	
duration numeric,	
year int
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays(
songplay_id int IDENTITY(0,1) PRIMARY KEY, 
start_time timestamp NOT NULL, 
user_id int NOT NULL, 
level text, 
song_id text,
artist_id text, 
session_id int, 
location text,
user_agent text
)
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users(
user_id int PRIMARY KEY, 
first_name text, 
last_name text, 
gender text, 
level text
)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs(
song_id text PRIMARY KEY, 
title text, 
artist_id text, 
year int, 
duration numeric
)
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists(
artist_id text PRIMARY KEY, 
name text, 
location text, 
latitude numeric, 
longitude numeric
)
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time(
start_time timestamp PRIMARY KEY, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday int
)
"""

# STAGING TABLES

staging_events_copy = """
copy staging_events 
from {} iam_role {} 
compupdate off region 'us-west-2' json {} """.format(
    config.get("S3", "LOG_DATA"),
    config.get("IAM_ROLE", "ARN"),
    config.get("S3", "LOG_JSONPATH"),
)


staging_songs_copy = """copy staging_songs
from {} iam_role {} 
compupdate off region 'us-west-2' json 'auto'
""".format(
    config.get("S3", "SONG_DATA"),
    config.get("IAM_ROLE", "ARN"),
)
# FINAL TABLES

songplay_table_insert = """
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
    SELECT DISTINCT
        (TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second') AS start_time, 
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_events se
        LEFT JOIN staging_songs ss
            ON se.song = ss.title
    WHERE se.page = 'NextSong';
"""

user_table_insert = """
    INSERT INTO users(user_id, first_name, last_name, gender, level) 
    SELECT DISTINCT
        se.userId,
        se.firstName,
        se.lastName,
        se.gender,
        se.level
    FROM 
        staging_events se
    WHERE se.page = 'NextSong';
"""

song_table_insert = """
    INSERT INTO songs(song_id, title, artist_id, year, duration) 
    SELECT DISTINCT
        ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        ss.duration
    FROM 
        staging_songs ss;
"""

artist_table_insert = """
    INSERT INTO artists(artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT
        ss.artist_id,
        ss.title,
        ss.artist_location,
        ss.artist_latitude,
        ss.artist_longitude
    FROM 
        staging_songs ss;
"""


time_table_insert = """
    INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
    SELECT DISTINCT
        a.start,
        EXTRACT(HOUR FROM a.start),
        EXTRACT(DAY FROM a.start),
        EXTRACT(WEEK FROM a.start),
        EXTRACT(MONTH FROM a.start),
        EXTRACT(YEAR FROM a.start),
        EXTRACT(WEEKDAY FROM a.start)
    FROM
    (SELECT (TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second') AS start FROM staging_events se
    WHERE se.page = 'NextSong') a;
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
