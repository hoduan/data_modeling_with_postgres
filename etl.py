import os
import glob
import psycopg2
import pandas as pd
from sql_queries import dim_artists, dim_users, dim_songs, dim_times, fct_songplays
import json
import csv

songs_file = 'songs.csv'
artists_file = 'artists.csv'
users_file = 'users.csv'
times_file = 'times.csv'
delimeter = '%'


def make_a_tmp_table(cur, main_table, temp_table):
    """
    :param cur: psycopg2 connection cursor
    :param main_table: the target table that record should get inserted into
    :param temp_table: a temporary table that will get created
    :return: None

    - creat a temp table with main_table's structure copied and drop this temp table at the end
    of the session
    """
    try:
        cur.execute(
            f"""
            CREATE TEMP TABLE {temp_table} ON COMMIT DROP
            AS SELECT * FROM {main_table} WITH NO DATA;
            """
        )
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)


def temp_to_main_table(cur, main_table, temp_table, conflict_key, query_on_conflict=None):
    """
    :param cur: psycopg2 connection cursor
    :param main_table: the target table that records should get inserted into
    :param temp_table: a temporary table that holds the records which should get inserted into
    main_table
    :param conflict_key: the column on which the conflict would happen
    :query_on_conflict: the query to run on conflict
    :return: None

    - load all the records from the temp table into the target table, and do nothing if there
    is conflict on key conflict_key
    """
    query = """DO NOTHING"""
    if query_on_conflict:
        query = query_on_conflict

    try:
        cur.execute(
            f"""
                INSERT INTO {main_table}
                SELECT * FROM {temp_table}
                ON CONFLICT ({conflict_key})
                {query}
            """
        )

    except (Exception, psycopg2.DatabaseError) as e:
        print(e)


def run_copy_from(cur, file, table, sep, columns):
    """
    :param cur: psycopg2 connection cursor
    :param file: the file contains that data being loaded into the table
    :param table: the table that data form file are going to be inserted into
    :param sep: the field delimeter used in the file
    :param columns: the column names of the table
    :return: None

    - copy data entries from file into table using sep as the delimeter by providing the column
    names

    """
    try:
        cur.copy_from(file, table, sep=sep, columns=columns)

    except (Exception, psycopg2.DatabaseError) as e:
        print(e)


def process_song_file(cur, filepath):
    """
    :param cur: psycopg2 connection cursor
    :param filepath: the json file that contains the song data
    :return: None

    - parse the song json file and load data entries into postgres dim tables
    """

    # open song file
    with open(filepath, 'r') as df, open(songs_file, 'w+') as sf, open(artists_file, 'w+') as af:
        song_writer = csv.writer(sf, delimiter=delimeter)
        artist_writer = csv.writer(af, delimiter=delimeter)
        for line in df:
            song_data = json.loads(line)

            # for dim_songs table
            song_id = song_data['song_id']
            title = song_data['title'].replace("'", " ")
            year = song_data['year']
            duration = song_data['duration']

            # for dim_artists table
            artist_name = song_data['artist_name'].replace("'", " ")
            artist_location = song_data['artist_location']
            artist_latitude = song_data['artist_latitude']
            artist_longitude = song_data['artist_longitude']

            # for both dim_songs and dim_artists
            artist_id = song_data['artist_id']

            # write to a csv file so it can be imported into the table later
            song_writer.writerow([song_id, title, artist_id, year, duration])
            artist_writer.writerow(
                [artist_id, artist_name, artist_location, artist_latitude, artist_longitude])

    with open(songs_file, 'r') as sf, open(artists_file, 'r') as af:
        # import the data into a tmp table first then to the dim table to drop conflict
        print(f'inserting song records into {dim_songs}')
        tmp_songs = 'tmp_songs'
        make_a_tmp_table(cur, dim_songs, tmp_songs)
        run_copy_from(cur, sf, tmp_songs, sep=delimeter,
                      columns=['song_id', 'title', 'artist_id', 'year', 'duration'])
        temp_to_main_table(cur, dim_songs, tmp_songs, 'song_id')
        print(f'finished inserting song records into {dim_songs}')

        print(f'inserting artist records into {dim_artists}')
        tmp_artists = 'tmp_artists'
        make_a_tmp_table(cur, dim_artists, tmp_artists)
        run_copy_from(cur, af, tmp_artists, sep=delimeter,
                      columns=['artist_id', 'name', 'location', 'latitude', 'longitude'])
        temp_to_main_table(cur, dim_artists, tmp_artists, 'artist_id')
        print(f'finished inserting artist records into {dim_artists}')


def process_timestamp(data_entries: list):
    """
    :param data_entries: song play data entries from the log_data file
    :return: a pandas DataFrame

    - extract the timestamp field from the song play data entries and turns it into a pandas'
    datetime object, and then get the year, month, weekday, day, week and hour info of this
    timestamp and put all of them into a pandas DataFrame obj and returns it
    """

    ts = []
    for record in data_entries:
        ds = pd.to_datetime(record['ts'], unit='ms')
        year = ds.year
        month = ds.month
        weekday = ds.dayofweek
        day = ds.dayofyear
        week = ds.week
        hour = ds.hour
        ts += [(ds, year, month, day, week, weekday, hour)]

    df = pd.DataFrame(data=ts,
                      columns=['start_time', 'year', 'month', 'day', 'week', 'weekday', 'hour'])
    return df


def handle_users_table(cur, song_play_list):
    """
    :param cur: psycopg2 connection cursor
    :param song_play_list: song play data entries
    :return: None

    - process the song play data entries to extract users info and then write user entries into a
    csv file, after that load the user entries from the csv file into the dim_users table
    """
    with open(users_file, 'w+') as df:
        users_writer = csv.writer(df, delimiter=delimeter)
        for line in song_play_list:
            user_id = line['userId']
            first_name = line['firstName']
            last_name = line['lastName']
            gender = line['gender']
            level = line['level']
            users_writer.writerow([user_id, first_name, last_name, gender, level])

    with open(users_file, 'r') as df:
        tmp_users = 'tmp_users'
        make_a_tmp_table(cur, dim_users, tmp_users)
        run_copy_from(cur, df, tmp_users, sep=delimeter,
                      columns=['user_id', 'first_name', 'last_name', 'gender', 'level'])
        query_on_conflict = """DO UPDATE SET level=EXCLUDED.level"""
        temp_to_main_table(cur, dim_users, tmp_users, conflict_key='user_id',
                           query_on_conflict=query_on_conflict)


def handle_time_table(cur, song_play_list):
    """
    :param cur: psycopg2 connection cursor
    :param song_play_list: song play data entries
    :return: None

    - process the timestamp field from the song play data entries to get a pandas' DataFrame obj,
    then output the DataFrame obj into a csv file, after that, load the time entries from the
    csv files into dim_times table
    """

    df = process_timestamp(song_play_list)
    df.to_csv(times_file, mode='w+', index=False, header=False)
    tmp_time = 'tmp_time_table'
    with open(times_file, 'r') as tf:
        make_a_tmp_table(cur, dim_times, tmp_time)
        run_copy_from(cur, tf, tmp_time, sep=',',
                      columns=['start_time', 'year', 'month', 'day', 'week', 'weekday', 'hour'])
        temp_to_main_table(cur, dim_times, tmp_time, 'start_time')


def handle_fct_table(cur, song_play_list):
    """
    :param cur: psycopg2 connection cursor
    :param song_play_list: song play data entries
    :return: None

    - extract song play fields from song play data entries and query dim_artists and dim_songs to
     get the artist id and song id respectively, if both artist_id and song_id were found for
     the record then insert the record into fct_songplay table
    """
    for line in song_play_list:
        session_id = line['sessionId']
        start_time = pd.to_datetime(line['ts'], unit='ms')
        user_id = line['userId']
        level = line['level']

        # handle special character '
        song = line['song'].replace("'", " ")
        artist = line['artist'].replace("'", " ")
        user_agent = line['userAgent']
        location = line['location']

        # query dim_artists by artist name to get artist_id
        try:
            cur.execute(
                f"""select artist_id from {dim_artists} where LOWER(name)=LOWER('{artist}')""")
        except (Exception, psycopg2.DatabaseError) as e:
            print(e)
        try:
            res = cur.fetchone()
        except Exception as e:
            print(e)
        # if no record found, ignore this entry and continue
        if not res:
            continue
        # found artist_id
        artist_id = res[0]

        # query dim_songs table by song title to get song_id
        try:
            cur.execute(f"""select song_id from {dim_songs} where LOWER(title)=LOWER('{song}')""")
        except (Exception, psycopg2.DatabaseError) as e:
            print(e)
        try:
            res = cur.fetchone()
        except Exception as e:
            print(e)
        # if no record found, ignore this entry and continue
        if not res:
            continue
        # found song_id
        song_id = res[0]

        # insert the record into fct_songplay
        try:
            cur.execute(f"""
                insert into {fct_songplays} (start_time, user_id, level,song_id,artist_id,
                    session_id,location, user_agent) values('{start_time}', '{user_id}', '{level}',
                    '{song_id}', '{artist_id}', '{session_id}', '{location}', '{user_agent}')
            """)
        except (Exception, psycopg2.DatabaseError) as e:
            print(e)


def process_log_file(cur, filepath):
    """
    :param cur: psycopg2 connection curson
    :param filepath: the relative path to data files
    :return: None

    - open the song log file and filter entries by
    """

    # open log file and filter by NextSong action
    song_play_lists = []
    with open(filepath, 'r') as df:
        for line in df:
            log_data = json.loads(line)
            if log_data['page'] == 'NextSong':
                song_play_lists += [log_data]

    # convert timestamp to a DataFrame contains timestamp, year, month, day, week, weekday, hour
    # and load time records into dim_times
    print(f'inserting time records into {dim_times} table')
    handle_time_table(cur, song_play_lists)
    print(f'finished inserting time records into {dim_times} table')

    # parse the song play list and load users into dim_users
    print(f'inserting user records into {dim_times} table')
    handle_users_table(cur, song_play_lists)
    print(f'finished inserting user records into {dim_users} table')

    # parse the song play list and load song play record into fct_songplay
    print(f'inserting song play records into {fct_songplays} table')
    handle_fct_table(cur, song_play_lists)
    print(f'finished inserting song play records into {fct_songplays} table')


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f'{num_files} files found in {filepath}')

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f'{i}/{num_files} files processed.')


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
