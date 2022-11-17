import configparser

# REDSHIFT AND S3 CONFIGURATIONS LOADED FROM THE CONFIG FILE
config = configparser.ConfigParser()
config.read('dwh.cfg')

# SQL QUERIES TO DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# SQL QUERIES TO CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
      artist varchar(256), 
      auth varchar(256), 
      firstName varchar(256), 
      gender char, 
      itemInSession int, 
      lastName varchar(256), 
      length float, 
      level varchar(256), 
      location varchar(256), 
      method varchar(256), 
      page varchar(256), 
      registration float, 
      sessionId int, 
      song varchar(256), 
      status int, 
      ts varchar, 
      userAgent varchar(256), 
      userId varchar(256)
    );

""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
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

""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
      songplay_id bigint IDENTITY(0, 1) PRIMARY KEY, 
      start_time timestamp NOT NULL, 
      user_id varchar NOT NULL, 
      level varchar, 
      song_id varchar, 
      artist_id varchar, 
      session_id int, 
      location varchar, 
      user_agent varchar
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
      user_id varchar NOT NULL PRIMARY KEY, 
      first_name varchar, 
      last_name varchar, 
      gender varchar, 
      level varchar
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
      song_id varchar PRIMARY KEY, 
      title varchar, 
      artist_id varchar, 
      year int, 
      duration float
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
      artist_id varchar PRIMARY KEY,
      name varchar, 
      location varchar, 
      latitude float, 
      longitude float
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time  timestamp PRIMARY KEY SORTKEY,
        hour        int,
        day         int,
        week        int,
        month       int,
        year        int DISTKEY,
        weekday     int
    ) diststyle key;
""")

# SQL QUERIES TO COPY DATA FROM S3 INTO STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM {}
    IAM_ROLE {}
    JSON {} region 'us-west-2';
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH']
)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    IAM_ROLE {}
    JSON 'auto' region 'us-west-2';
""").format(
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN']
)



# SQL QUERIES FOR INSERTING DATA FROM STAGING AREA INTO REDSHIFT
songplay_table_insert = ("""
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
    SELECT e.ts as start_time,
        e.userId as user_id,
        e.level as level,
        s.song_id as song_id,
        s.artist_id as artist_id,
        e.sessionId as session_id,
        e.location as location,
        e.userAgent as user_agent
    FROM staging_events e
    JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name) 
    WHERE 
      e.page = 'NextSong'
    """)

user_table_insert = ("""
    INSERT INTO users (
      user_id, first_name, last_name, gender, 
      level
    ) 
    SELECT 
      DISTINCT user_id, 
      firstname, 
      lastname, 
      gender, 
      level 
    FROM 
      staging_events 
    WHERE 
      page = 'NextSong'
    """)

song_table_insert = ("""
    INSERT INTO song (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT s.song_id as song_id,
        s.title as title,
        s.artist_id as artist_id,
        s.year as year,
        s.duration as duration
    FROM staging_songs s
    """)

artist_table_insert = ("""
    INSERT INTO artist (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT s.artist_id as artist_id,
        s.artist_name as name,
        s.artist_location as location,
        s.artist_latitude as latitude,
        s.artist_longitude as longitude
    FROM staging_songs s
    """)

time_table_insert = ("""
    INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
    SELECT t.start_time
        , EXTRACT(hour FROM t.start_time)
        , EXTRACT(day FROM t.start_time)
        , EXTRACT(week FROM t.start_time)
        , EXTRACT(month FROM t.start_time)
        , EXTRACT(year FROM t.start_time)
        , EXTRACT(weekday FROM t.start_time)        
    FROM (
        SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000)::BIGINT 
            * INTERVAL '1 second' AS start_time
        FROM staging_events
    ) AS t;
    """)


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
