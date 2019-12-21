import boto3
import botocore

import pymysql
import psycopg2

import pandas as pd
import numpy as np

import re
from tqdm import tqdm

import os, sys
import json
import datetime


#========================
### Cache data from SQL

def _cache(sql, connection, cache_file_name=None, verbose=False):
    rt = 'C:' + os.sep if sys.platform == 'win32' else '/'
    # Check if cache folder exists
    if not os.path.exists(os.path.join(rt, 'tmp', 'pycof', 'cache')):
        # If the PYCOF cache folder does not exist, we create all folders
        folds, fs = [rt + 'tmp', 'pycof', 'cache', 'queries'], []
        for fold in folds:
            fs = fs + [fold]
            os.mkdir(os.path.join(*fs)) if os.path.exists(os.path.join(*fs)) == False else ''
        # Create data folder
        data_fold = os.path.join('/tmp', 'pycof', 'cache', 'data')
        os.mkdir(data_fold) if os.path.exists(data_fold) == False else ''
    
    # Hash the file's name to save the query and the data
    file_name = hash(sql) if cache_file_name is None else cache_file_name

    # Set the Query and data paths
    query_path = os.path.join(rt, 'tmp', 'pycof', 'cache', 'queries')
    data_path = os.path.join(rt, 'tmp', 'pycof', 'cache', 'data')
    

    if (query_type.upper() == "SELECT") & (file_name in os.listdir(data_path)):
        # If file exists, checks its age
        file_age = (datetime.datetime.now() - datetime.datetime.utcfromtimestamp(os.stat(data_path + file_name).st_mtime)).total_seconds()
        
        if (query_type.upper() == "SELECT") & (file_age < cache_time):
            read = pd.read_csv(data_path + file_name)
        else:
            read = pd.read_sql(sql_query, connection)
            write(sql, query_path + file_name, perm='w', verbose=verbose)
    else:
        read = pd.read_sql(sql_query, connection)
        write(sql, query_path + file_name, perm='w', verbose=verbose)
    
    return read



def _get_config(credentials):
    #==========
    # Parse credentials argument
    if type(credentials) == str:
        if '/' in credentials:
            path = credentials
        elif sys.platform == 'win32':
            path = 'C:/Windows/' + credentials
        else:
            path = '/etc/' + credentials
    elif (type(credentials) == dict) & (credentials == {}):
        if sys.platform == 'win32':
            path = 'C:/Windows/config.json'
        else:
            path = '/etc/config.json'
    else:
        path = ''

    #==========
    # Load credentials
    if path == '':
        config = credentials
    else:
        with open(path) as config_file:
            config = json.load(config_file)
    
    return config

#========================
### Get DB credentials

def _get_credentials(config, useIAM=False):
    
    ## Access DB credentials
    hostname = config.get('DB_HOST') # Read the host name value from the config dictionnary
    port = int(config.get('DB_PORT')) # Get the port from the config file and convert it to int
    user = config.get('DB_USER')    # Get the user name for connecting to the DB
    password = config.get('DB_PASSWORD') # Get the DB
    database = config.get('DB_DATABASE') # For Redshift, use the database, for MySQL set it by default to ""
    try:
        access_key   = config.get("AWS_ACCESS_KEY_ID")
        secret_key   = config.get("AWS_SECRET_ACCESS_KEY")
        region       = config.get("REGION")
        cluster_name = config.get("CLUSTER_NAME")
    except:
        access_key   = ""
        secret_key   = ""
        region       = "eu-west-1"
        cluster_name = ""

    # Get AWS credentials with access and secret key
    if (useIAM):
        session = boto3.Session(profile_name='default', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
        rd_client = session.client('redshift')
        cluster_creds = rd_client.get_cluster_credentials(
            DbUser=user,
            DbName=database, 
            ClusterIdentifier=cluster_name, 
            AutoCreate=False)
        # Update user and password
        user     = cluster_creds['DbUser']
        password = cluster_creds['DbPassword']
    
    return hostname, port, user, password, database



#========================
### Define connector from credentials

def _define_connector(hostname, port, user, password, database=""):
    # Initiate sql connection to the Database
    if 'redshift' in hostname.lower().split('.'):
        try:
            connector = psycopg2.connect(host=hostname, port=port, user=user, password=password, database=database)
            cursor = connector.cursor()
        except:
            raise ValueError('Failed to connect to the Redshfit cluster')
    else:
        try:
            # Add new encoder of numpy.float64
            pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
            pymysql.converters.conversions = pymysql.converters.encoders.copy()
            pymysql.converters.conversions.update(pymysql.converters.decoders)
            # Create connection
            connector = pymysql.connect(host=hostname, port=port, user=user, password=password)
            cursor = connector.cursor()
        except:
            raise ValueError('Failed to connect to the MySQL database')
    
    return connector, cursor


#========================
### Insert data to DB

def _insert_data(data, table, connector, cursor, autofill_nan=False, verbose=False):
    # Check if user defined the table to publish
        if table == "":
            raise SyntaxError('Destination table not defined by user')
        # Create the column string and the number of columns used for push query 
        columns_string = (', ').join(list(data.columns))
        col_num = len(list(data.columns))-1
        
        #calculate the size of the dataframe to be pushed
        num = len(data)
        batches = int(num/10000)+1

        #====================================
        # Fill Nan values if requested by user
        if autofill_nan:
            """
                For each row of the dataset, we fill the NaN values
                with a specific string that will be replaced by None
                value (converted by NULL in MySQL). This aims at avoiding
                the PyMySQL 1054 error.
            """
            data_load = []
            for ls in [v for v in data.fillna('@@@@EMPTYDATA@@@@').values.tolist()]:
                data_load += [[None if vv == '@@@@EMPTYDATA@@@@' else vv for vv in ls]]
        else:
            data_load = data.values.tolist()

        #====================================
        # Push 10k batches iterativeley and then push the remainder
        if num == 0:
            raise ValueError('len(data) == 0 -> No data to insert')
        elif num > 10000:
            rg = tqdm(range(0, batches-1)) if verbose else range(0, batches-1)
            for i in rg:
                cursor.executemany(f'INSERT INTO {table} ({columns_string}) VALUES ({"%s, "*col_num} %s )', data_load[i*10000: (i+1)*10000])
                connector.commit()
            # Push the remainder
            cursor.executemany(f'INSERT INTO {table} ({columns_string}) VALUES ({"%s, "*col_num} %s )', data_load[(batches-1)*10000:])
            connector.commit()
        else:
            # Push everything if less then 10k (SQL Server limit)
            cursor.executemany(f'INSERT INTO {table} ({columns_string}) VALUES ({"%s, "*col_num} %s )', data_load)
            connector.commit()

