import os
import glob
import psycopg2
import numpy as np
import pandas as pd
import pandas.io.sql as sqlio
from fast_sql_queries import *
import json
import csv
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData
from monitor_script import profile



def iter_song_data(filepath):
    """
    Iterator function for reading song JSON files in filepath with one row inside and yield data as
    dictionary
    
    Args:
        filepath (str) -> root file path contain all json file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            if 'checkpoint' not in f:
                all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    #print('{} files found in {}'.format(num_files, filepath))
    
    for file in all_files:
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        yield data
        
def song_data_extract(iter_file):
    """
    Function that proccess song file iterator and accumulate correct data from all the files into a list of tuples.
    
    Args:
        iter_file (iterator) -> Iterator for song files
    
    Returns:
        song_lst (list) --> List of tuples contain valid data songs
        artist_lst (list) --> List of tupels contai valid data for artist
    """
    song_columns = ["song_id", "title", "artist_id", "year", "duration"]
    artist_columns = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    song_lst = []
    artist_lst = []
    
    for file in iter_file:
        song_lst.append(tuple(file[k] for k in song_columns))
        artist_lst.append(tuple(file[k] for k in artist_columns))
        
    return song_lst,artist_lst


def log_file_extract(filepath):
    """
    Function to process log files and accumulate all file data into one pandas dataframe
    
    Args:
        filepath (str) -> root file path contain all json file
        
    Return:
        df (pandas.dataframe) --> Dataframe with all data from files 
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            if 'checkpoint' not in f:
                all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    #print('{} files found in {}'.format(num_files, filepath))
    # collecting all files into dataframe
    df = pd.read_json(all_files[0], lines = True)
    for file in all_files[1:]:
        df = df.append(pd.read_json(file, lines = True))
    return df[df['page'] =='NextSong'] # filter data having page = NextSong
        
        
def log_data_extract(cur,df):
    """
    function to process log files data extracted by log_file_extract
    
    Args:
        cur (psycopg2.cursor) --> Database cursor
        df (pandas.dataframe) --> log_files df from log_file_extract function
    Returns :
        time_df (pandas.dataframe) --> time table data
        user_df (pandas.dataframe) --> user table data
        songplay_data (pandas.dataframe) --> songplays table data
    """
    def extract_song_artists_id(cur,row):
        """
        function to look for songid and artistid from db  to be inserted in songplays table
        
        Args:
            cur (psycopg2.cursor) --> Database cursor
            row (pandas.series) --> row without songid & artistid
        Returns:
            row (pandas.series) --> row after adding correct songid & artistid
        """
        # extract correct songid & artist id from db
        cur.execute(song_select, (row['song'], row['artist'], row['length']))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        
        row['song_id'] = songid
        row['artist_id'] = artistid
        return row
    
    time_columns = ["ts", "hour", "day", "week", "month", "year", "weekday"]
    user_columns = ["userId", "firstName", "lastName", "gender", "level"]
    songplay_columns = ['ts', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']
    
    # time table data
    df['ts'] = pd.to_datetime(df['ts'],unit='ms')       
    t = df['ts']
    time_data = [t.values,t.dt.hour,t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.dayofweek]
    time_df = pd.DataFrame({time_columns[i] : time_data[i] for i in range(len(time_data))})
    
    # users table data
    user_df = df[user_columns]
    
    #songplay data
    df['song_id'] = 0
    df['artist_id'] = 0
    df.apply(lambda x:extract_song_artists_id(cur,x),axis=1)
    songplay_data = df[songplay_columns]
    
    return time_df,user_df,songplay_data

def insert_mogrify(cur,tuples, insert, values_statement, clause):
    """
    Using cursor.mogrify() to build the bulk insert query
    then cursor.execute() to execute the query
    """
    values = [cur.mogrify(values_statement, tup).decode('utf8') for tup in tuples]
    query  = insert + ",".join(values) + clause
    try:
        cur.execute(query)
        conn.commit()
        print("execute_mogrify() done")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cur.close()
        print(1)
        
def sql_insert_execution(cur, data, table_name, clause='ON CONFLICT DO NOTHING'):
    """
    This function utilize psycopg2 copy_expert which copy from csv file to data base. 
    First it convert data to csv and then perform the sql query to specified table.
    
    Args:
        cur (psycopg2.cursor) --> Database cursor
        data (list | pandas.dataframe) --> list of tupels or pandas dataframe contain valid data
        table_name (str) --> name of the table in database
        clause (str) --> special insert into clause for the insert query
    """
    
    sql = f"""
    CREATE TABLE {table_name}_h AS
    TABLE {table_name};
    
    COPY {table_name}_h FROM STDIN With CSV;

    INSERT INTO {table_name}
    SELECT *
    FROM {table_name}_h {clause};

    DROP TABLE {table_name}_h;
    """
    file = './{}.csv'.format(table_name)
    ix = False
    if table_name == 'songplays':
        ix = True
    if type(data) != type(pd.DataFrame()):
        with open(file,'w') as out:
            csv_out=csv.writer(out)
            csv_out.writerows(data)
    else:
        data.to_csv(file, index=ix, header=False)
    with open(file, 'r') as f:
        cur.copy_expert(sql,f)
    os.remove(file)

@profile
def main():
    
    # connecting to db
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    # getting song files data
    iter_file = iter_song_data('data/song_data/')
    song_data, artist_data = song_data_extract(iter_file)
    
    # Insert songs and artists data to database
    sql_insert_execution(cur, song_data, 'songs', clause=song_clause)
    sql_insert_execution(cur, artist_data, 'artists', clause=artist_clause)
    #insert_mogrify(cur, conn,song_data, song_table_insert, song_values, song_clause)
    #insert_mogrify(cur, conn,artist_data, artist_table_insert, artist_values, artist_clause)
    
    # getting log files data
    log_file = log_file_extract('data/log_data')
    time_df,user_df,songplay_data = log_data_extract(cur,log_file)
                                                     
    # inserting log data into db
    sql_insert_execution(cur, time_df,'time', time_clause)
    sql_insert_execution(cur, user_df,'users', "ON CONFLICT DO NOTHING") # I couldn't use appropriate on conflict so I just skip for the sake of timing
    sql_insert_execution(cur, songplay_data,'songplays', songplay_clause)
                                                     
    # producing erd diagram of db
    graph = create_schema_graph(metadata=MetaData('postgresql://student:student@127.0.0.1/sparkifydb'))
    graph.write_png('sparkifydb_erd.png')

    conn.close()


if __name__ == "__main__":
    main()