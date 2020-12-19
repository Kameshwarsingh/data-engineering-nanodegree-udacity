'''
ETL (Extrcat transform and load) pipeline, set of functions  to read Song and Log data (JSON format) and load in PostgresDB.

Functions:
    process_song_file(cur, filepath)
    This function processes Songs data file - reads JSON file, and inserts data in songs and artists table.

    process_log_file(cur, filepath)
    This function processes Logs data file - reads JSON file, and inserts data in users and songplays table.

    process_data(cur, conn, filepath, func)
    This function processes set of data files  - reads list of all *.json files, iterates over each datafile and processes data.

'''
import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)




def process_song_file(cur, filepath):
    '''
    This function processes Songs data file - reads JSON file, and inserts data in songs and artists table.

            Parameters:
                    cur (Cursor): Cursor object to Postgres DB
                    filepath (str): File path of Songs data file
            Returns:
                    No return
    
    '''
    # open song file
    df =  pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df.loc[0, ["song_id", "title", "artist_id", "year", "duration"]].values)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df.loc[0,["artist_id","artist_name","artist_location","artist_latitude","artist_longitude",],].values)
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    This function processes Logs data file - reads JSON file, and inserts data in users and songplays table.

            Parameters:
                    cur (Cursor): Cursor object to Postgres DB
                    filepath (str): File path of Logs data file
            Returns:
                    No return
    
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df =  df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = df["ts"]
    
    # insert time data records
    t_as_datetime = pd.to_datetime(df["ts"])
    time_data = (t, t_as_datetime.dt.hour, t_as_datetime.dt.day, t_as_datetime.dt.week, t_as_datetime.dt.month, t_as_datetime.dt.year, t_as_datetime.dt.weekday)

    column_labels = ("start_time", "hour", "day","week", "month", "year", "weekday")
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent,) 
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    This function processes set of data files  - reads list of all *.json files, iterates over each datafile and processes data.

            Parameters:
                    cur (Cursor): Cursor object to Postgres DB
                    conn (Connection): Connection object to Postgres DB 
                    filepath (str): File path of data file
                    func (function): Reference to function to be executed
            Returns:
                    No return
    
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))



def main():
    '''
    Main function is entry point for ETL. It processes Song and Log  data files.

            Parameters:
                    No parameters
            Returns:
                    No return
    
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()