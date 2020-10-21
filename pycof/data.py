import os
import sys
import getpass
import boto3

import pandas as pd
import numpy as np
import math
import json
import xlrd
import hashlib
import pyarrow.parquet as pq
from io import StringIO, BytesIO

import re

from tqdm import tqdm
import datetime

from .misc import write, _get_config, file_age, verbose_display, _pycof_folders


##############################################################################################################################

# Easy file read
def f_read(path, extension=None, parse=True, remove_comments=True, sep=',', sheet_name=0, engine='pyarrow', credentials={}, cache='30mins', verbose=False, **kwargs):
    """Read and parse a data file.
    It can read multiple format. For data frame-like format, the function will return a pandas data frame, otherzise a string.
    The function will by default detect the extension from the file path. You can force an extension with the argument.
    It can remove comments, trailing spaces, breaklines and tabs. It can also replace f-strings with provided values.


    :Parameters:
        * **path** (:obj:`str`): path to the SQL file.
        * **extension** (:obj:`str`): extension to use. Can be 'csv', 'txt', 'xslsx', 'sql', 'html', 'py', 'json', 'js', 'parquet', 'read-only' (defaults None).
        * **parse** (:obj:`bool`): Format the query to remove trailing space and comments, ready to use format (defaults True).
        * **remove_comments** (:obj:`bool`): Remove comments from the loaded file (defaults True).
        * **sep** (:obj:`str`): Columns delimiter for pd.read_csv (defaults ',').
        * **sheet_name** (:obj:`str`): Tab column to load when reading Excel files (defaults 0).
        * **engine** (:obj:`str`): Engine to use to load the file. Can be 'pyarrow' or the function from your preferred library (defaults 'pyarrow').
        * **credentials** (:obj:`dict`): Credentials to use to connect to AWS S3. You can also provide the credentials path or the json file name from '/etc/' (defaults {}).
        * **\\*\\*kwargs** (:obj:`str`): Arguments to be passed to the engine or values to be formated in the file to load.

    :Example:

        >>> sql = pycof.f_read('/path/to/file.sql', country='FR')
        >>> df1 = pycof.f_read('/path/to/df_file.json')
        >>> df2 = pycof.f_read('/path/to/df.csv')
        >>> df3 = pycof.f_read('s3://bucket/path/to/file.parquet')

    :Returns:
        * :obj:`pandas.DataFrame`: Data frame a string from file read.
    """
    ext = path.split('.')[-1] if extension is None else extension
    data = []

    useIAM = path.startswith('s3://')

    if useIAM:
        config = _get_config(credentials)
        if config.get("AWS_SECRET_ACCESS_KEY") in [None, 'None', '']:
            try:
                s3 = boto3.client('s3')
                s3_resource = boto3.resource('s3')
            except Exception:
                raise ConnectionError("Please run 'aws config' on your terminal and initialize the parameters.")
        else:
            s3 = boto3.client('s3', aws_access_key_id=config.get("AWS_ACCESS_KEY_ID"),
                              aws_secret_access_key=config.get("AWS_SECRET_ACCESS_KEY"),
                              region_name=config.get("REGION"))
            s3_resource = boto3.resource('s3', aws_access_key_id=config.get("AWS_ACCESS_KEY_ID"),
                                         aws_secret_access_key=config.get("AWS_SECRET_ACCESS_KEY"),
                                         region_name=config.get("REGION"))

        bucket = path.replace('s3://', '').split('/')[0]
        folder_path = '/'.join(path.replace('s3://', '').split('/')[1:])

        if ext.lower() in ['csv', 'txt', 'parq', 'parquet']:
            # If file can be loaded by pandas, we do not download locally
            verbose_display('Loading the data from S3 directly', verbose)
            obj = s3.get_object(Bucket=bucket, Key=folder_path)
            path = BytesIO(obj['Body'].read())
        else:
            # This step will only check the cache and download the file to tmp if not available.
            # The normal below steps will still run, only the path will change if the file comes from S3
            # and cannot be loaded by pandas.

            cache_time = 0. if cache is False else cache
            _disp = tqdm if verbose else list
            # Force the input to be a string
            str_c_time = str(cache_time).lower().replace(' ', '')
            # Get the numerical part of the input
            c_time = float(''.join(re.findall('[^a-z]', str_c_time)))
            # Get the str part of the input - for the format
            age_fmt = ''.join(re.findall('[a-z]', str_c_time))

            # Hash the path to create filename
            file_name = hashlib.sha224(bytes(path, 'utf-8')).hexdigest().replace('-', 'm')
            data_path = _pycof_folders('data')

            # Changing path to local once file is downloaded to tmp folder
            path = data_path + file_name

            # Set the S3 bucket
            s3bucket = s3_resource.Bucket(bucket)

            # First, check if the same path has already been downloaded locally
            if file_name in os.listdir(data_path):
                # If yes, check when and compare to cache time
                if file_age(data_path + file_name, format=age_fmt) < c_time:
                    # If cache is recent, no need to download
                    ext = os.listdir(path)[0].split('.')[-1]
                    verbose_display('Data file available in cache', verbose)
                else:
                    # Otherwise, we update the cache
                    verbose_display('Updating data in cache', verbose)
                    for obj in _disp(s3bucket.objects.filter(Prefix=folder_path)):
                        if obj.key == folder_path:
                            continue
                        s3bucket.download_file(obj.key, path + obj.key.split('/')[-1])
                        ext = obj.key.split('.')[-1]
            else:
                # If the file is not in the cache, we download it
                verbose_display('Downloading and caching data', verbose)
                # Creating the directory
                os.makedirs(path, exist_ok=True)
                for obj in _disp(s3bucket.objects.filter(Prefix=folder_path)):
                    if obj.key == folder_path:
                        continue
                    s3bucket.download_file(obj.key, path + obj.key.split('/')[-1])
                    ext = obj.key.split('.')[-1]

    # CSV / txt
    if ext.lower() in ['csv', 'txt']:
        data = pd.read_csv(path, sep=sep, **kwargs)
    # XLSX
    elif ext.lower() in ['xls', 'xlsx']:
        data = pd.read_excel(path, sheet_name=sheet_name, **kwargs)
    # SQL
    elif ext.lower() in ['sql']:
        with open(path) as f:
            file = f.read()
        for line in file.split('\n'):  # Parse the data
            l_striped = line.strip()  # Removing trailing spaces
            if parse:
                l_striped = l_striped.format(**kwargs)  # Formating
            if remove_comments:
                l_striped = l_striped.split('--')[0]  # Remove comments
                re.sub(r"<!--(.|\s|\n)*?-->", "", l_striped.replace('/*', '<!--').replace('*/', '-->'))
            if l_striped != '':
                data += [l_striped]
        data = ' '.join(data)
    # HTML
    elif ext.lower() in ['html']:
        with open(path) as f:
            file = f.read()
        # Parse the data
        for line in file.split('\n'):
            l_striped = line.strip()  # Removing trailing spaces
            if parse:
                l_striped = l_striped.format(**kwargs)  # Formating
            if remove_comments:
                l_striped = re.sub(r"<!--(.|\s|\n)*?-->", "", l_striped)  # Remove comments
            if l_striped != '':
                data += [l_striped]
        data = ' '.join(data)
    # Python
    elif ext.lower() in ['py', 'sh']:
        with open(path) as f:
            file = f.read()
        # Parse the data
        for line in file.split('\n'):
            l_striped = line.strip()  # Removing trailing spaces
            if parse:
                l_striped = l_striped.format(**kwargs)  # Formating
            if remove_comments:
                l_striped = l_striped.split('#')[0]  # Remove comments
            if l_striped != '':
                data += [l_striped]
        data = ' '.join(data)
    # JavaScript
    elif ext.lower() in ['js']:
        with open(path) as f:
            file = f.read()
        for line in file.split('\n'):  # Parse the data
            l_striped = line.strip()  # Removing trailing spaces
            if parse:
                l_striped = l_striped.format(**kwargs)  # Formating
            if remove_comments:
                l_striped = l_striped.split('//')[0]  # Remove comments
                re.sub(r"<!--(.|\s|\n)*?-->", "", l_striped.replace('/*', '<!--').replace('*/', '-->'))
            if l_striped != '':
                data += [l_striped]
        data = ' '.join(data)
    # Json
    elif ext.lower() in ['json']:
        if engine.lower() in ['json']:
            with open(path) as json_file:
                data = json.load(json_file)
        else:
            data = pd.read_json(path, **kwargs)
    # Parquet
    elif ext.lower() in ['parq', 'parquet']:
        if useIAM:
            data = pd.read_parquet(path)
        elif type(engine) == str:
            if engine.lower() in ['py', 'pa', 'pyarrow']:
                dataset = pq.ParquetDataset(path, **kwargs)
                table = dataset.read()
                data = table.to_pandas()
            else:
                raise ValueError('Engine value not allowed')
        else:
            data = engine(path, **kwargs)
    # Else, read-only
    elif ext.lower() in ['readonly', 'read-only', 'ro']:
        with open(path) as f:
            for line in f:
                print(line.rstrip())
    else:
        with open(path) as f:
            file = f.read()
        data = file
    # If not read-only
    return data
