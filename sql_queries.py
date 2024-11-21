import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_SOURCE = config.get("S3", "LOG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSONPATH")
SONG_SOURCE = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Staging table for raw log events
staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR(MAX),
    auth VARCHAR(MAX),
    firstName VARCHAR(MAX),
    gender VARCHAR(MAX),
    itemInSession INT,
    lastName VARCHAR(MAX),
    length FLOAT,
    level VARCHAR(MAX),
    location VARCHAR(MAX),
    method VARCHAR(MAX),
    page VARCHAR(MAX),
    registration VARCHAR(MAX),
    sessionId INT,
    song VARCHAR(MAX),
    status INT,
    ts BIGINT,
    userAgent VARCHAR(MAX),
    userId INT
)
"""

# Staging table for raw song data
staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs(
    song_id VARCHAR(MAX) PRIMARY KEY,
    artist_id VARCHAR(MAX),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(MAX),
    artist_name VARCHAR(MAX),
    duration FLOAT,
    num_songs INT,
    title VARCHAR(MAX),
    year INT
)
"""

# Fact table for songplay records
songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL sortkey distkey,
    user_id INT NOT NULL,
    level VARCHAR(MAX),
    song_id VARCHAR(MAX) NOT NULL,
    artist_id VARCHAR(MAX) NOT NULL,
    session_id INT,
    location VARCHAR(MAX),
    user_agent VARCHAR(MAX)
)
"""

# Dimension table for user data
user_table_create = """
CREATE TABLE IF NOT EXISTS users(
    user_id VARCHAR(MAX) PRIMARY KEY NOT NULL,
    first_name VARCHAR(MAX),
    last_name VARCHAR(MAX),
    gender VARCHAR(MAX),
    level VARCHAR(MAX)
)
"""

# Dimension table for song data
song_table_create = """
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(MAX) PRIMARY KEY NOT NULL,
    title VARCHAR(MAX) NOT NULL,
    artist_id VARCHAR(MAX) NOT NULL,
    year INT,
    duration FLOAT
)
"""

# Dimension table for artist data
artist_table_create = """
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(MAX) PRIMARY KEY NOT NULL,
    name VARCHAR(MAX),
    location VARCHAR(MAX),
    latitude FLOAT,
    longitude FLOAT
)
"""

# Dimension table for time data
time_table_create = """
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP NOT NULL distkey sortkey PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday VARCHAR(MAX) NOT NULL
)
"""

# STAGING TABLES

# Copy data into staging_events from S3 bucket
staging_events_copy = ("""
COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as JSON {}
    timeformat as 'epochmillisecs'
""").format(LOG_SOURCE, IAM_ROLE, LOG_JSON_PATH)

# Copy data into staging_songs from S3 bucket
staging_songs_copy = ("""
COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as JSON 'auto'
""").format(SONG_SOURCE, IAM_ROLE)

# FINAL TABLES

# Insert data into songplays fact table
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time,
        e.userId as user_id,
        e.level as level,
        s.song_id as song_id,
        s.artist_id as artist_id,
        e.sessionId as session_id,
        e.location as location,
        e.userAgent as user_agent
    FROM staging_events e
    JOIN staging_songs s
    ON e.song = s.title 
    AND e.artist = s.artist_name 
    AND e.page = 'NextSong' 
    AND e.length = s.duration
""")

# Insert data into users dimension table
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT e.userId as user_id,
        e.firstName as first_name,
        e.lastName as last_name,
        e.gender,
        e.level
    FROM staging_events e
    WHERE e.userId IS NOT NULL
    AND e.page = 'NextSong'
""")

# Insert data into songs dimension table
song_table_insert = ("""
INSERT INTO songs 
    SELECT DISTINCT s.song_id,
        s.title,
        s.artist_id,
        s.year,
        s.duration
    FROM staging_songs s
""")

# Insert data into artists dimension table
artist_table_insert = ("""
INSERT INTO artists 
    SELECT DISTINCT s.artist_id,
        s.artist_name,
        s.artist_location,
        s.artist_latitude,
        s.artist_longitude
    FROM staging_songs s
""")

# Insert data into time dimension table
time_table_insert = ("""
INSERT INTO time
    WITH temp_time AS (
        SELECT TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as ts 
        FROM staging_events e
    )
    SELECT DISTINCT ts,
        EXTRACT(hour FROM ts) AS hour,
        EXTRACT(day FROM ts) AS day,
        EXTRACT(week FROM ts) AS week,
        EXTRACT(month FROM ts) AS month,
        EXTRACT(year FROM ts) AS year,
        EXTRACT(weekday FROM ts) AS weekday
    FROM temp_time
""")

# QUERY LISTS
create_table_queries = [
    staging_events_table_create, 
    staging_songs_table_create, 
    songplay_table_create, 
    user_table_create, 
    song_table_create, 
    artist_table_create, 
    time_table_create
]
drop_table_queries = [
    staging_events_table_drop, 
    staging_songs_table_drop, 
    songplay_table_drop, 
    user_table_drop, 
    song_table_drop, 
    artist_table_drop, 
    time_table_drop
]
copy_table_queries = [
    staging_events_copy, 
    staging_songs_copy
]
insert_table_queries = [
    songplay_table_insert, 
    user_table_insert, 
    song_table_insert, 
    artist_table_insert, 
    time_table_insert
]
