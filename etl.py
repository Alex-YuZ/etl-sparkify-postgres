import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Open song file.
    
    Parameters:
        cur: the cursor object
        filepath: where the song json file lives
        
    Returns:
        None
    """
    
    # read the json file into a DataFrame
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = list(song_data.values[0]) 
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = list(artist_data.values[0]) 
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Open log file.
    
    Parameters:
        cur: the cursor object
        filepath: where the event log json file lives

    Returns:
        None
    """
    
    # read the json file into a DataFrame
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    time_data = (df['ts'], t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.dayofweek)
    column_labels = ('timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(list(zip(column_labels, time_data))))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

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
        songplay_data = (row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    Get all files matching extension from directory
    
    Parameters:
        cur: the cursor object
        conn: the object of connection to db 
        filepath: where the event log json file lives
        func: the type of function (process_song_file, process_log_file) used to process data. 

    Returns:
        None
    
    """
    
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
    
    """
    Packages all the functions defined above. 
    Connect to the database and acquire the cursor.
    Close the connection to the database.
    
    Parameters: None
    Returns: None
    
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

# Activate the main() function when the etl.py script is run as the main program (Not as a module into other scripts)
if __name__ == "__main__":
    main()