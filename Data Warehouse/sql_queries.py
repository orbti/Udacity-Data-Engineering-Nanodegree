import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DB_NAME = config.get('CLUSTER', 'DB_NAME')
DB_USER = config.get('CLUSTER', 'DB_USER')
DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
DB_PORT = config.get('CLUSTER', 'DB_PORT')

ENDPOINT = config.get('CLUSTER', 'HOST')
ROLE_ARN = config.get('IAM_ROLE', 'ARN')

S3_LOG_DATA = config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS song_plays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        event_id        INT IDENTITY(0,1) NOT NULL,
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          VARCHAR,
        itemInSession   INT,
        lastName        VARCHAR,
        length          DECIMAL(10),
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    BIGINT,
        sessionId       INT SORTKEY DISTKEY,
        song            VARCHAR,
        status          INT,
        ts              TIMESTAMP,
        userAgent       VARCHAR,
        userId          INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs           INT,
        artist_id           VARCHAR SORTKEY DISTKEY,
        artist_latitude     VARCHAR,
        artist_longitude    VARCHAR,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            DECIMAL(10),
        year                INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE song_plays (
        song_play_id        INT IDENTITY(0,1) NOT NULL SORTKEY,
        start_time          TIMESTAMP NOT NULL,
        user_id             INT NOT NULL DISTKEY,
        level               VARCHAR NOT NULL,
        song_id             VARCHAR NOT NULL,
        artist_id           VARCHAR NOT NULL,
        session_id          INT NOT NULL,
        location            VARCHAR, 
        user_agent          VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id         INT NOT NULL SORTKEY,
        first_name      VARCHAR(25) NOT NULL,
        last_name       VARCHAR(25) NOT NULL,
        gender          VARCHAR(10) NOT NULL,
        level           VARCHAR(10) NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id         VARCHAR NOT NULL SORTKEY,
        title           VARCHAR NOT NULL,
        artist_id       VARCHAR NOT NULL,
        year            INT NOT NULL,
        duration        FLOAT NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id       VARCHAR NOT NULL SORTKEY,
        artist_name     VARCHAR NOT NULL,
        location        VARCHAR,
        lattitude       VARCHAR,
        longitude       VARCHAR
    );
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time      TIMESTAMP NOT NULL SORTKEY,
        hour            INT NOT NULL,
        day             INT NOT NULL,
        week            INT NOT NULL,
        month           INT NOT NULL,
        year            INT NOT NULL,
        weekday         INT NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY staging_events FROM {S3_LOG_DATA}
    IAM_ROLE {ROLE_ARN}
    FORMAT JSON {S3_LOG_JSONPATH}
    REGION 'us-west-2'
    TIMEFORMAT 'epochmillisecs';
""")

staging_songs_copy = (f"""
    COPY staging_songs FROM {S3_SONG_DATA}
    IAM_ROLE {ROLE_ARN}
    FORMAT JSON 'auto ignorecase'
    REGION 'us-west-2';
""")

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO song_plays(
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location, 
        user_agent
    )
        SELECT 
            DISTINCT se.ts      AS start_time,
            se.userId           AS user_id,
            se.level            AS level,
            ss.song_id          AS song_id,
            ss.artist_id        AS artist_id,
            se.sessionId        AS session_id,
            se.location         AS location, 
            se.userAgent        AS user_agent
        FROM staging_events AS se
        JOIN staging_songs AS ss ON (se.song=ss.title AND se.artist=ss.artist_name)
        WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users
        SELECT
            DISTINCT se.userId  AS user_id,
            se.firstName        AS first_name,
            se.lastName         AS last_name,
            se.gender           AS gender,
            se.level            AS level
        FROM staging_events AS se
        WHERE se.page = 'NextSong'
        AND user_id IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs
        SELECT
            DISTINCT ss.song_id,
            ss.title,
            ss.artist_id,
            ss.year,
            ss.duration
        FROM staging_songs AS ss
        WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists
        SELECT
            DISTINCT ss.artist_id   AS artist_id,
            ss.artistName           AS artist_name,
            ss.location             AS location,
            ss.lattitude            AS lattitude,
            ss.longitude            AS longitude
        FROM staging_events AS se
        JOIN staging_songs AS ss ON se.artist=ss.artist_name
        WHERE se.page = 'NextSong'
        AND artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time
        SELECT
            DISTINCT start_time                 AS start_time,
            EXTRACT(hour FROM start_time)       AS hour,
            EXTRACT(day FROM start_time)        AS day ,
            EXTRACT(week FROM start_time)       AS week,
            EXTRACT(month FROM start_time)      AS month,
            EXTRACT(year FROM start_time)       AS year,
            EXTRACT(dayofweek FROM start_time)  AS weekday
        FROM song_plays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
