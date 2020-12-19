'''
This file contains set of SQL to create/drop tables, SQL to load raw data in staging tables and SQL to load data in dimension and fact tables.
'''
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = ""
staging_songs_table_drop = ""
songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (   artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR(1),
        itemInSession INTEGER,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR(10),
        location VARCHAR,
        method VARCHAR(10),
        page VARCHAR(20),
        registration FLOAT,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR,
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs INTEGER,
        artist_id VARCHAR(18),
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR(18),
        title VARCHAR,
        duration FLOAT,
        year INTEGER
    );
""")

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplay
    (
    songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
    start_time varchar sortkey,
    user_id varchar,
    level varchar(10),
    song_id varchar(18) distkey,
    artist_id varchar(18),
    session_id integer,
    location varchar,
    user_agent varchar
    );
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS users
        (
            userId INTEGER sortkey,
            firstName VARCHAR,
            lastName VARCHAR,
            gender VARCHAR(1),
            level VARCHAR(10),
            primary key(userId)
        )
     diststyle all;
"""


song_table_create = """
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id VARCHAR(18) sortkey,
        title VARCHAR,
        artist_id VARCHAR(18),
        year Integer,
        duration FLOAT,
        primary key(song_id)
    )
    diststyle all;
"""

artist_table_create = """
    create table artists
    (
        artist_id varchar(18) sortkey,
        name varchar,
        location varchar,
        latitude FLOAT,
        longitude FLOAT,
        PRIMARY KEY(artist_id)
    )
    diststyle all;
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS time
    (
        start_time Bigint sortkey,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday integer,
        primary key(start_time)
    )
    diststyle all;
"""

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM 's3://udacity-dend/log_data'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 's3://udacity-dend/log_json_path.json';
""").format(config.get('IAM_ROLE', 'ARN'))


staging_songs_copy = ("""
    COPY staging_songs FROM 's3://udacity-dend/song_data'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    JSON 'auto';
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
    (
        SELECT ev.ts AS start_time,
        ev.userId AS user_id,
        ev.level AS level,
        so.song_id AS song_id,
        so.artist_id AS artist_id,
        ev.sessionId AS session_id,
        ev.location AS location,
        ev.userAgent AS user_agent
        FROM staging_events ev
        JOIN staging_songs so ON
            (ev.song = so.title AND ev.artist = so.artist_name)
    )
""")

user_table_insert = ("""
insert into users
    (
      SELECT userid, firstName, lastName, gender, level
      FROM staging_events
      where ts in (SELECT max(ts) FROM staging_events group by userid)
      and userId is NOT null
    );
""")

song_table_insert = ("""
    INSERT INTO songs
    (
    SELECT DISTINCT song_id, title,
        artist_id, year, duration
    FROM staging_songs
    )
""")

artist_table_insert = ("""
    insert into artists
    (
        (
            with DISTINCTArtists AS
                (
                    SELECT artist_id, artist_name,
                        artist_location, artist_latitude,
                        artist_longitude,
                        ROW_NUMBER() OVER(PARTITION BY artist_id ORDER BY year)
                            AS rowNum
                    FROM staging_songs
                )
            SELECT artist_id, artist_name, artist_location,
                artist_latitude, artist_longitude
            FROM DISTINCTArtists
            WHERE RowNum=1
        )
    )
""")

time_table_insert = ("""
    INSERT into time
    (
        SELECT ts AS start_time,
        EXTRACT(hour FROM p_ts ) AS hour,
        EXTRACT(day FROM p_ts ) AS day,
        EXTRACT(week FROM p_ts ) AS week,
        EXTRACT(month FROM p_ts ) AS month,
        EXTRACT(year FROM p_ts ) AS year,
        EXTRACT(weekday FROM p_ts ) AS weekday
        FROM
        (
            SELECT DISTINCT ts,
                (timestamp 'epoch' + ts/1000* interval '1 second')
                AS p_ts
            FROM staging_events
        )
    );
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
