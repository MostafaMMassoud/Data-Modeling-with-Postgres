# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL,
start_time TIMESTAMP NOT NULL,
user_id INT NOT NULL,
level TEXT,
song_id TEXT ,
artist_id TEXT,
session_id INT,
location TEXT,
user_agent TEXT,
PRIMARY KEY (songplay_id))
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
user_id INT,
first_name TEXT,
last_name TEXT,
gender CHAR(1), 
level TEXT,
PRIMARY KEY (user_id))
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
song_id TEXT, 
title TEXT NOT NULL, 
artist_id TEXT, 
year INT, 
duration FLOAT NOT NULL,
PRIMARY KEY (song_id))
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (
artist_id TEXT, 
name TEXT NOT NULL, 
location TEXT, 
latitude FLOAT, 
longitude FLOAT,
PRIMARY KEY (artist_id))
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
start_time TIMESTAMP, 
hour  INT, 
day  INT, 
week INT, 
month INT , 
year INT , 
weekday INT,
PRIMARY KEY (start_time))
""")

# INSERT RECORDS

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
""")

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s,%s,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s,%s,%s,%s,%s) ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = (""" INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s,%s,%s,%s,%s) ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (start_time) DO NOTHING
""")

# FIND songs


song_select = (""" SELECT songs.song_id, songs.artist_id
FROM songs JOIN artists ON artists.artist_id = songs.artist_id
WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]