import boto3
import botocore

import pymysql
import psycopg2

import pandas as pd
import numpy as np

import re
from tqdm import tqdm

import os
import sys
import getpass
import json
import datetime
import hashlib

from .misc import verbose_display, file_age, write, _get_config, _create_pycof_folder


########################################################################################################################
# Cache data from SQL

def _cache(sql, connection, query_type="SELECT", cache_time='24h', cache_file_name=None, verbose=False):
    # Parse cache_time value
    if type(cache_time) in [float, int]:
        c_time = cache_time
    else:
        # Force the input to be a string
        str_c_time = str(cache_time).lower().replace(' ', '')
        # Get the numerical part of the input
        c_time = float(''.join(re.findall('[^a-z]', str_c_time)))
        # Get the str part of the input - for the format
        age_fmt = ''.join(re.findall('[a-z]', str_c_time))

    root_path = _create_pycof_folder()

    # Hash the file's name to save the query and the data
    file_name = hashlib.sha224(bytes(sql, 'utf-8')).hexdigest().replace('-', 'm') if cache_file_name is None else cache_file_name

    # Set the query and data paths
    query_path = os.path.join(root_path, 'tmp', 'pycof', 'cache', 'queries') + '/'
    data_path = os.path.join(root_path, 'tmp', 'pycof', 'cache', 'data') + '/'

    # Chec if the cached data already exists
    if (query_type.upper() == "SELECT") & (file_name in os.listdir(data_path)):
        # If file exists, checks its age
        age = file_age(data_path + file_name, format=age_fmt)
        if (query_type.upper() == "SELECT") & (age < c_time):
            # If file is younger than c_time, we read the cached data
            verbose_display('Reading cached data', verbose)
            read = pd.read_csv(data_path + file_name)
        else:
            # Else we execute the SQL query and save the ouput + the query
            verbose_display('Execute SQL query and cache the data - updating cache', verbose)
            read = pd.read_sql(sql, connection)
            write(sql, query_path + file_name, perm='w', verbose=verbose)
            read.to_csv(data_path + file_name, index=False)
    else:
        # If the file does not even exist, we execute SQL, save the query and its output
        verbose_display('Execute SQL query and cache the data', verbose)
        read = pd.read_sql(sql, connection)
        write(sql, query_path + file_name, perm='w', verbose=verbose)
        read.to_csv(data_path + file_name, index=False)

    return read


########################################################################################################################
# Get DB credentials

def _get_credentials(config, useIAM=False):

    # Access DB credentials
    hostname = config.get('DB_HOST')      # Read the host name value from the config dictionnary
    port = int(config.get('DB_PORT'))     # Get the port from the config file and convert it to int
    user = config.get('DB_USER')          # Get the user name for connecting to the DB
    password = config.get('DB_PASSWORD')  # Get the DB
    database = config.get('DB_DATABASE')  # For Redshift, use the database, for MySQL set it by default to ""
    #
    access_key   = config.get("AWS_ACCESS_KEY_ID")
    secret_key   = config.get("AWS_SECRET_ACCESS_KEY")
    region       = config.get("REGION")
    cluster_name = config.get("CLUSTER_NAME")
    #
    boto_error = """Cannot initialize the boto3 session. Please check your config file and ensure awscli is installed.\n
    To install awcli, please run: \n
    pip install awscli -y && aws configure\n
    Values from `aws configure` command can remain empty.
    """
    # Get AWS credentials with access and secret key
    if (useIAM) & (secret_key in [None, 'None', '']):
        try:
            session = boto3.Session(profile_name='default')
        except Exception:
            raise SystemError(boto_error)
    elif (useIAM):
        try:
            session = boto3.Session(profile_name='default', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
        except Exception:
            raise SystemError(boto_error)
    #
    if useIAM:
        try:
            rd_client = session.client('redshift')
            cluster_creds = rd_client.get_cluster_credentials(
                DbUser=user,
                DbName=database,
                ClusterIdentifier=cluster_name,
                AutoCreate=False)
            # Update user and password
            user     = cluster_creds['DbUser']
            password = cluster_creds['DbPassword']
        except Exception:
            raise ValueError('Could not retreive cluster information. Please check your config file, region or user permissions.')
    #
    return hostname, port, user, password, database


########################################################################################################################
# Define connector from credentials

def _define_connector(hostname, port, user, password, database="", engine='default'):
    # Initiate sql connection to the Database
    if ('redshift' in hostname.lower().split('.')) or (engine.lower() == 'redshift'):
        try:
            connector = psycopg2.connect(host=hostname, port=port, user=user, password=password, database=database)
            cursor = connector.cursor()
        except Exception:
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
        except Exception:
            raise ValueError('Failed to connect to the MySQL database')

    return connector, cursor


########################################################################################################################
# Insert data to DB

def _insert_data(data, table, connector, cursor, autofill_nan=False, verbose=False):
    # Check if user defined the table to publish
    if table == "":
        raise SyntaxError('Destination table not defined by user')
    # Create the column string and the number of columns used for push query
    columns_string = (', ').join(list(data.columns))
    col_num = len(list(data.columns)) - 1

    # calculate the size of the dataframe to be pushed
    num = len(data)
    batches = int(num / 10000) + 1

    ########################################################################################################################============
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

    ########################################################################################################################============
    # Push 10k batches iterativeley and then push the remainder
    if num == 0:
        raise ValueError('len(data) == 0 -> No data to insert')
    elif num > 10000:
        rg = tqdm(range(0, batches - 1)) if verbose else range(0, batches - 1)
        for i in rg:
            cursor.executemany(f'INSERT INTO {table} ({columns_string}) VALUES ({"%s, "*col_num} %s )', data_load[i * 10000:(i + 1) * 10000])
            connector.commit()
        # Push the remainder
        cursor.executemany(f'INSERT INTO {table} ({columns_string}) VALUES ({"%s, "*col_num} %s )', data_load[(batches - 1) * 10000:])
        connector.commit()
    else:
        # Push everything if less then 10k (SQL Server limit)
        cursor.executemany(f'INSERT INTO {table} ({columns_string}) VALUES ({"%s, "*col_num} %s )', data_load)
        connector.commit()
