import os
import sys
import getpass
import json
import boto3
from botocore.exceptions import ProfileNotFound

import pandas as pd
import numpy as np

from io import StringIO, BytesIO

from tqdm import tqdm
import datetime


########################################################################################################################
# Get config file

def _pycof_folders(output=None, verbose=False):
    # Define the root folder depending on the OS
    if sys.platform in ['win32', 'win64', 'cygwin', 'msys']:
        temp_path = os.environ['TEMP'] + os.sep
        home = os.path.expanduser('~')
        creds_fold = os.path.join(home, '.pycof')  + os.sep
    else:
        temp_path = os.path.join(os.sep, 'tmp') + os.sep
        creds_fold = os.path.join(os.sep, 'etc', '.pycof') + os.sep
        home = os.path.expanduser('~')

    # Initialize the creation count variable
    _created = 0

    # Credentials folder
    if not os.path.exists(creds_fold):
        try:
            os.makedirs(creds_fold)
            _created += 1
        except PermissionError as err:
            raise PermissionError(f"""Could not create the PYCOF config folder, permission denied: {creds_fold}.
                    Please create folder with: sudo mkdir {creds_fold} or run your script with super-user.""")

    # Queries temp folder
    folds_q = os.path.join(temp_path, 'pycof', 'cache', 'queries') + os.sep
    if not os.path.exists(folds_q):
        try:
            os.makedirs(folds_q)
            _created += 1
        except PermissionError as err:
            raise PermissionError(f"""Could not create the PYCOF temp folder, permission denied: {folds_q}.
                    Please create folder with: sudo mkdir {folds_q} or run your script with super-user.""")

    # Data temp folder
    folds_d = os.path.join(temp_path, 'pycof', 'cache', 'data') + os.sep
    if not os.path.exists(folds_d):
        try:
            os.makedirs(folds_d)
            _created += 1
        except PermissionError as err:
            raise PermissionError(f"""Could not create the PYCOF temp data folder, permission denied: {folds_d}.
                    Please create folder with: sudo mkdir {folds_d} or run your script with super-user.""")

    # Return path if asked by user
    if output in ['tmp', 'temp']:
        return temp_path
    elif output == 'creds':
        return creds_fold
    elif output == 'queries':
        return folds_q
    elif output == 'data':
        return folds_d
    elif output == 'home':
        return home
    elif verbose:
        print(f'PYCOF folder created: {_created}')


# #######################################################################################################################
# Get config file

def _get_config(credentials={}):
    # ==========
    # Parse credentials argument
    if type(credentials) == str:
        if '/' in credentials:
            path = credentials
        else:
            creds = credentials if credentials.endswith('.json') else credentials + '.json'
            path = os.path.join(_pycof_folders(output='creds'), creds)
    elif (type(credentials) == dict) & (credentials == {}):
        path = os.path.join(_pycof_folders(output='creds'), 'config.json')
    else:
        path = ''

    # ==========
    # Load credentials
    if path == '':
        config = credentials
    else:
        try:
            with open(path) as config_file:
                config = json.load(config_file)
        except Exception:
            raise ValueError("""Could not load config file. 
                    Note that from version 1.2.0, config file location has changed. Make sure your file is in {}""".format(_pycof_folders('creds')))

    return config


########################################################################################################################
# Write to a txt file

def write(file, path, perm='a', verbose=False, end_row='\n', credentials={}, profile_name=None, **kwargs):
    """Write a line of text into a file (usually .txt) or saves data objects.
    As opposed to Pandas' built-in functions (:obj:`to_csv` or :obj:`to_parquet`), this function allows to pass AWS IAM credentials similar to
    :py:meth:`pycof.sql.remote_execute_sql`.

    :Parameters:
        * **file** (:obj:`str` or :obj:`pandas.DataFrame`): Line of text or object to be inserted in the file.
        * **path** (:obj:`str`): File on which to write (`/path/to/file.txt`). Can be any format, not necessarily txt.
        * **perm** (:obj:`str`): Permission to use when opening file (usually 'a' for appending text, or 'w' to (re)write file).
        * **verbose** (:obj:`bool`): Return the length of the inserted text if set to True (defaults False).
        * **end_row** (:obj:`str`): Character to end the row (defaults '\\n').
        * **credentials** (:obj:`dict`): Credentials to use to connect to AWS S3. You can also provide the credentials path or the json file name from '/etc/' (defaults {}).
        * **profile_name** (:obj:`str`): Profile name of the AWS profile configured with the command `aws configure` (defaults None).
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
        # If S3, try AWS cli profile or credentials
        try:
            sess = boto3.session.Session(profile_name=profile_name)
            s3 = sess.client('s3')
            s3_resource = sess.resource('s3')
        except ProfileNotFound:
            config = _get_config(credentials)
            s3 = boto3.client('s3', aws_access_key_id=config.get("AWS_ACCESS_KEY_ID"), aws_secret_access_key=config.get("AWS_SECRET_ACCESS_KEY"),
                              region_name=config.get("REGION"))
            s3_resource = boto3.resource('s3', aws_access_key_id=config.get("AWS_ACCESS_KEY_ID"),
                                         aws_secret_access_key=config.get("AWS_SECRET_ACCESS_KEY"),
                                         region_name=config.get("REGION"))
        except FileNotFoundError:
            raise ConnectionError("Please run 'aws config' on your terminal and initialize the parameters or profide a correct value for crendetials.")

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
            splitted_path = path.replace('s3://', '').split('/')
            bucket = splitted_path[0]
            folder_path = '/'.join(splitted_path[1:])
            file_name = splitted_path[-1]
            data_path = os.path.join(_pycof_folders('temp'), 'pycof', 'cache', 'data') + os.sep
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
        return ttl_sec / 60
    elif format.lower() in ['h', 'hr', 'hrs', 'hour', 'hours']:
        return ttl_sec / 3600
    elif format.lower() in ['d', 'day', 'days']:
        return ttl_sec / (24 * 60 * 60)
    elif format.lower() in ['w', 'wk', 'wks', 'week', 'weeks']:
        return ttl_sec / (7 * 24 * 60 * 60)
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
    if (verbose in [1, True]) & (type(element) in [list, range]) & (return_list is False):
        return(tqdm(element))
    elif (verbose in [1, True]) & (type(element) in [list]) & (return_list is True):
        return(print(*element, sep=sep, end=end))
    elif (verbose in [1, True]) & (type(element) in [str]) & (return_list is False):
        return(print(element, sep=sep, end=end))
    elif (verbose in [0, False]) & (type(element) in [str, type(None)]):
        disp = 0  # we don't display anything
    else:
        return(element)
