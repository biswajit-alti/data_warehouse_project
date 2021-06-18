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
artist varchar,
auth varchar,	
firstName varchar,	
gender varchar,	
itemInSession int,	
lastName varchar,	
length float,	
level varchar,	
location varchar,	
method varchar,	
page varchar,	
registration float,	
sessionId int,	
song varchar,	
status int,	
ts bigint,	
userAgent varchar,	
userId int
)
"""


staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs(
num_songs int, 
artist_id varchar, 
artist_latitude float,
artist_longitude float,	
artist_location varchar,	
artist_name varchar,	
song_id varchar,	
title varchar,	
duration float,	
year int
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays(
songplay_id int IDENTITY(0,1) PRIMARY KEY, 
start_time timestamp NOT NULL, 
user_id int NOT NULL, 
level varchar, 
song_id varchar NOT NULL,
artist_id varchar NOT NULL, 
session_id int, 
location varchar,
user_agent varchar
)
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users(
user_id int PRIMARY KEY, 
first_name varchar, 
last_name varchar, 
gender varchar, 
level varchar
)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs(
song_id varchar PRIMARY KEY, 
title varchar, 
artist_id varchar, 
year int, 
duration float
)
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists(
artist_id varchar PRIMARY KEY, 
name varchar, 
location varchar, 
latitude float, 
longitude float
)
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time(
art_time timestamp PRIMARY KEY, 
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
INSERT INTO songplays(start_time, user_id, level, song_id,artist_id, session_id, location, user_agent) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT(songplay_id) DO NOTHING"""

user_table_insert = """
INSERT INTO users(user_id, first_name, last_name, gender, level) 
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT(user_id) DO UPDATE 
SET level = EXCLUDED.level"""

song_table_insert = """
INSERT INTO songs(song_id, title, artist_id, year, duration) 
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT(song_id) DO NOTHING"""

artist_table_insert = """
INSERT INTO artists(artist_id, name, location, latitude, longitude) 
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT(artist_id) DO NOTHING"""


time_table_insert = """
INSERT INTO time(art_time, hour, day, week, month, year, weekday) 
VALUES(%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT(art_time) DO NOTHING"""

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
