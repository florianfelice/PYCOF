import os
import sys
import getpass
import json
import boto3

import pandas as pd
import numpy as np

from io import StringIO, BytesIO

from tqdm import tqdm
import datetime


########################################################################################################################
# Get config file

def _create_pycof_folder(verbose=True):
    # Define the root folder depending on the OS
    root_path = f'C:/Users/{getpass.getuser()}/' + os.sep if sys.platform == 'win32' else '/'

    # Check if cache folder exists
    if not os.path.exists(os.path.join(root_path, 'tmp', 'pycof', 'cache')):
        if verbose:
            print('Creating the PYCOF cache folders')
        # If the PYCOF cache folder does not exist, we create all folders
        folds, fs = [root_path + 'tmp', 'pycof', 'cache', 'queries'], []

        for fold in folds:
            # For each sub folder, we check if it already esists and created if not
            fs = fs + [fold]
            os.mkdir(os.path.join(*fs)) if os.path.exists(os.path.join(*fs)) is False else ''

        # Create data folder if cache folder does not exist
        data_fold = os.path.join(root_path, 'tmp', 'pycof', 'cache', 'data')
        os.mkdir(data_fold) if os.path.exists(data_fold) is False else ''
    elif verbose:
        print('PYCOF cache folders already exist')

    return root_path




########################################################################################################################
# Get config file

def _get_config(credentials):
    # ==========
    # Parse credentials argument
    if type(credentials) == str:
        if '/' in credentials:
            path = credentials
        elif sys.platform == 'win32':
            path = f'C:/Users/{getpass.getuser()}/' + credentials
        else:
            path = '/etc/' + credentials
    elif (type(credentials) == dict) & (credentials == {}):
        if sys.platform == 'win32':
            path = f'C:/Users/{getpass.getuser()}/config.json'
        else:
            path = '/etc/config.json'
    else:
        path = ''

    # ==========
    # Load credentials
    if path == '':
        config = credentials
    else:
        with open(path) as config_file:
            config = json.load(config_file)

    return config


########################################################################################################################
# Write to a txt file

def write(file, path, perm='a', verbose=False, end_row='\n', credentials={}, **kwargs):
    """Write a line of text into a file (usually .txt) or saves data objects.
    As opposed to Pandas' built-in functions (:obj:`to_csv` or :obj:`to_parquet`), this function allows to pass AWS IAM credentials similar to :py:meth:`pycof.sql.remote_execute_sql`.

    :Parameters:
        * **file** (:obj:`str` or :obj:`pandas.DataFrame`): Line of text or object to be inserted in the file.
        * **path** (:obj:`str`): File on which to write (`/path/to/file.txt`). Can be any format, not necessarily txt.
        * **perm** (:obj:`str`): Permission to use when opening file (usually 'a' for appending text, or 'w' to (re)write file).
        * **verbose** (:obj:`bool`): Return the length of the inserted text if set to True (defaults False).
        * **end_row** (:obj:`str`): Character to end the row (defaults '\\n').
        * **credentials** (:obj:`dict`): Credentials to use to connect to AWS S3. You can also provide the credentials path or the json file name from '/etc/' (defaults {}).
        * **\\*\\*kwargs** (:obj:`str`): Arguments to be passed to pandas function (either :obj:`to_csv` or :obj:`to_parquet`).

    :Example:
        >>> pycof.write('This is a test', path='~/pycof_test_write.txt', perm='w')
        >>> pycof.write(df, path='s3://bucket/path/to/file.parquet', credentials='config.json')

    :Returns:
        * :obj:`int`: Number of characters inserted if verbose is True.
    """
    # Check if path provided is S3
    useIAM = path.startswith('s3://')

    if useIAM:
        # If S3, get credentials
        config = _get_config(credentials)
        if config.get("AWS_SECRET_ACCESS_KEY") in [None, 'None', '']:
            s3 = boto3.client("s3", profile_name='default')
            s3_resource = boto3.resource("s3", profile_name='default')
        else:
            s3 = boto3.client("s3", aws_access_key_id=config.get("AWS_ACCESS_KEY_ID"),
                                         aws_secret_access_key=config.get("AWS_SECRET_ACCESS_KEY"),
                                         region_name=config.get("REGION"))
            s3_resource = boto3.resource("s3", aws_access_key_id=config.get("AWS_ACCESS_KEY_ID"),
                                         aws_secret_access_key=config.get("AWS_SECRET_ACCESS_KEY"),
                                         region_name=config.get("REGION"))

    # Pandas DataFrame
    if type(file) == pd.DataFrame:
        # CSV or TXT
        if path.endswith('.csv') or path.endswith('.txt'):
            out_buffer = StringIO() if useIAM else path
            file.to_csv(out_buffer, **kwargs)
        # Parquet
        elif path.endswith('.parquet'):
            out_buffer = BytesIO() if useIAM else path
            file.to_parquet(out_buffer, **kwargs)
        # Json
        elif path.endswith('.json'):
            out_buffer = StringIO() if useIAM else path
            file.to_json(out_buffer, **kwargs)

        # If S3, push to bucket
        if useIAM:
            bucket = path.replace('s3://', '').split('/')[0]
            folder_path = '/'.join(path.replace('s3://', '').split('/')[1:])
            s3_resource.Object(bucket, folder_path).put(Body=out_buffer.getvalue())
    # Other input file format
    else:
        if useIAM:
            root_path = _create_pycof_folder()
            splitted_path = path.replace('s3://', '').split('/')
            bucket = splitted_path[0]
            folder_path = '/'.join(splitted_path[1:])
            file_name = splitted_path[-1]
            data_path = os.path.join(root_path, 'tmp', 'pycof', 'cache', 'data') + '/'
            path = data_path + file_name

        with open(path, perm) as f:
            f.write(file + end_row)

        if useIAM:
            s3.upload_file(path, bucket, folder_path)
    
    if verbose:
        return(len(file))


########################################################################################################################
# Compute the age of a given file

def file_age(file_path, format='seconds'):
    """Computes the age of a file.

    :Parameters:
        * **file_path** (:obj:`str`): Path of the file to compute the age.
        * **format** (:obj:`str`): Unit in which to compute the age (defaults 'seconds'). Can either be 'seconds', 'minutes', 'hours' or 'days'.

    :Example:
        >>> pycof.file_age('/home/ubuntu/.bashrc')
        ... 9937522.32319
        >>> pycof.file_age('/home/ubuntu/.bashrc', format='days')
        ... 11.01812981440972

    :Returns:
        * :obj:`int`: Age of the file.
    """
    ttl_sec = (datetime.datetime.now() - datetime.datetime.fromtimestamp(os.stat(file_path).st_mtime)).total_seconds()
    if format.lower() in ['s', 'sec', 'second', 'seconds']:
        return ttl_sec
    elif format.lower() in ['m', 'min', 'mins', 'minute', 'minutes']:
        return ttl_sec/60
    elif format.lower() in ['h', 'hr', 'hrs', 'hour', 'hours']:
        return ttl_sec/3600
    elif format.lower() in ['d', 'day', 'days']:
        return ttl_sec/(24*60*60)
    elif format.lower() in ['w', 'wk', 'wks', 'week', 'weeks']:
        return ttl_sec/(7*24*60*60)
    else:
        raise ValueError(f"Format value is not correct. Can be 'seconds', 'minutes', 'hours', 'days' or 'weeks'. Got '{format}'.")


########################################################################################################################
# Display tqdm only if argument for verbosity is 1 (works for lists, range and str)

def verbose_display(element, verbose=True, sep=' ', end='\n', return_list=False):
    """Extended print function with tqdm display for loops.
    Also has argument verbose for automated scripts with overall verbisity argument.

    :Parameters:
        * **element** (:obj:`str`): The element to be displayed. Can either be str, range, list.
        * **verbose** (:obj:`bool`): Display the element or not (defaults True).
        * **sep** (:obj:`str`): The deperator to use of displaying different lists/strings (defaults ' ').
        * **end** (:obj:`str`): How to end the display (defaults '\\n').
        * **return_list** (:obj:`bool`): If it is a list, can return in for paragraph format (defaults False).

    :Example:
        >>> for i in pycof.verbose_display(range(15)):
        >>>     i += 1
        ... 100%|#######################################| 15/15 [00:00<00:00, 211122.68it/s]
    
    :Returns:
        :obj:`str`: The element to be displayed.
    """
    if (verbose in [1, True]) & (type(element) in [list, range]) & (return_list == False):
        return(tqdm(element))
    elif (verbose in [1, True]) & (type(element) in [list]) & (return_list == True):
        return(print(*element, sep = sep, end = end))
    elif (verbose in [1, True]) & (type(element) in [str]) & (return_list == False):
        return(print(element, sep = sep, end = end))
    elif (verbose in [0, False]) & (type(element) in [str, type(None)]):
        disp = 0 # we don't display anything
    else:
        return(element)
