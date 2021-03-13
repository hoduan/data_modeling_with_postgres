fct_songplays = 'fct_songplays'
dim_users = 'dim_users'
dim_songs = 'dim_songs'
dim_artists = 'dim_artists'
dim_times = 'dim_times'

# DROP TABLES
songplay_table_drop = f"DROP TABLE IF EXISTS {fct_songplays}"
user_table_drop = f"DROP TABLE IF EXISTS {dim_users}"
song_table_drop = f"DROP TABLE IF EXISTS {dim_songs}"
artist_table_drop = f"DROP TABLE IF EXISTS  {dim_artists}"
time_table_drop = f"DROP TABLE  IF EXISTS {dim_times}"

# CREATE TABLES
songplay_table_create = (
    f""" CREATE TABLE IF NOT EXISTS {fct_songplays}(
        songplay_id SERIAL PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INT,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR);
    """)

user_table_create = (
    f""" CREATE TABLE IF NOT EXISTS {dim_users} (
        user_id INT PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR);
    """)

song_table_create = (
    f"""CREATE TABLE IF NOT EXISTS {dim_songs} (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INT,
        duration FLOAT);
    """)

artist_table_create = (
    f"""CREATE TABLE IF NOT EXISTS {dim_artists} (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        latitude VARCHAR,
        longitude VARCHAR);
    """)

time_table_create = (
    f"""CREATE TABLE IF NOT EXISTS  {dim_times} (
        start_time TIMESTAMP PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month int,
        year INT,
        weekday INT);
    """)

# QUERY LISTS
select_from_songplay_table = (f"""select * from {fct_songplays}""")

create_table_queries = [songplay_table_create, user_table_create, song_table_create,
                        artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop,
                      time_table_drop]
