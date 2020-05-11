import os
import sys
import getpass

import re

import pandas as pd
import numpy as np
import math
import json
import xlrd
import pyarrow.parquet as pq

from tqdm import tqdm
import datetime

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# from statinf.ml.losses import mape
# from statinf.ml.losses import mean_squared_error as mse
# from statinf.ml.performance import BinaryPerformance

from .sqlhelper import _get_config, _get_credentials, _define_connector
from .sqlhelper import _insert_data, _cache
from .misc import write, file_age, verbose_display


##############################################################################################################################

## TODO: Test send_email by list of recipients
## TODO: remote_execute_sql with dump to S3 or to S3 and Redshift.

##############################################################################################################################

## Publish or read from DB
def remote_execute_sql(sql_query="", query_type="", table="", data={}, credentials={}, verbose=True, autofill_nan=True, useIAM=False, cache=False, cache_time=24*60*60, cache_name=None):
    """Simplified function for executing SQL queries. Will look at the credentials at :obj:`/etc/config.json`. User can also pass a dictionnary for credentials.

    .. code-block:: python

        pycof.remote_execute_sql(sql_query="", query_type="", table="", data={}, credentials={}, verbose=True, autofill_nan=True, useIAM=False, cache=False, cache_time=24*60*60, cache_name=None)

    :Parameters:
        * **sql_query** (:obj:`str`): SQL query to be executed (defaults "").
        * **query_type** (:obj:`str`): Type of SQL query to execute. Can either be SELECT, INSERT, COPY, DELETE or UNLOAD (defaults "SELECT").
        * **table** (:obj:`str`): Table in which we want to operate, only used for INSERT and DELETE (defaults "").
        * **data** (:obj:`pandas.DataFrame`): Data to load on the database (defaults {}).
        * **credentials** (:obj:`dict`): Credentials to use to connect to the database. You can also provide the credentials path or the json file name from '/etc/' (defaults {}).
        * **verbose** (:obj:`bool`): Display progression bar (defaults True).
        * **autofill_nan** (:obj:`bool`): Replace NaN values by 'NULL' (defaults True).
        * **useIAM** (:obj:`bool`): Get AWS IAM credentials using access and secret key (defaults False).
        * **cache** (:obj:`bool`): Caches the data to avoid running again the same SQL query (defaults False).
        * **cache_time** (:obj:`int`): How long to keep the caching data without reloading (defaults 1 day).
        * **cache_name** (:obj:`str`): File name for storing cache data, if None will use WHERE clause from SQL (defaults None).

    :Example:
        >>> pycof.remote_execute_sql("SELECT * FROM SCHEMA.TABLE LIMIT 10")

    :Returns:
        * :obj:`pandas.DataFrame`: Result of an SQL query in case of :obj:`query_type` as "SELECT".
    """

    # ============================================================================================
    # Define the SQL type
    all_query_types = ['SELECT', 'INSERT', 'DELETE', 'COPY', 'UNLOAD']

    if (query_type != ""):
        # Use user input if query_type is not as its default value
        sql_type = query_type
    elif (sql_query != ""):
        # If a query is inserted, use select.
        # For DELETE or COPY, user needs to provide the query_type
        sql_type = "SELECT"
    elif (data != {}) or (type(sql_query) == pd.DataFrame):
        # If data is provided, use INSERT sql_type
        sql_type = 'INSERT'
    elif ("UNLOAD" in sql_query.upper()):
        sql_type = 'UNLOAD'
    elif ("COPY" in sql_query.upper()):
        sql_type = 'COPY'
    else:
        allowed_queries = f"Your query_type value is not correct, allowed values are {', '.join(all_query_types)}"
        # Check if the query_type value is correct
        raise ValueError(allowed_queries + f'. Got {query_type}')
        # assert query_type.upper() in all_query_types, allowed_queries

    # ============================================================================================
    # Credentials load
    hostname, port, user, password, database = _get_credentials(_get_config(credentials), useIAM)

    # ============================================================================================
    # Set default value for table
    if (sql_type == 'SELECT'):  # SELECT
        if (table == ""):  # If the table is not specified, we get it from the SQL query
            table = sql_query.upper().replace('\n', ' ').split('FROM ')[1].split(' ')[0]
        elif (sql_type == 'SELECT') & (table.upper() in sql_query.upper()):
            table = table
        else:
            raise SyntaxError('Argument table does not match with SQL statement')

    # ============================================================================================
    # Database connector
    conn, cur = _define_connector(hostname, port, user, password, database)

    # ========================================================================================
    # SELECT - Read query
    if sql_type.upper() == "SELECT":
        if cache:
            read = _cache(sql_query, conn, sql_type, cache_time=cache_time, verbose=verbose)
        else:
            read = pd.read_sql(sql_query, conn)
        return(read)
    # ============================================================================================
    # INSERT - Load data to the db
    elif sql_type.upper() == "INSERT":
        _insert_data(data, table, conn, cur, autofill_nan, verbose)

    # ============================================================================================
    # DELETE / COPY / UNLOAD - Execute SQL command which does not return output
    elif sql_type.upper() in ["DELETE", "COPY", "UNLOAD"]:
        if table.upper() in sql_query.upper():
            cur.execute(sql_query)
            conn.commit()
        else:
            raise ValueError('Table does not match with SQL query')
    else:
        raise ValueError(f'Unknown query_type, should be as: {all_query_types}')

    # Close SQL connection
    conn.close()


##############################################################################################################################

## Easy file read
def f_read(path, extension=None, parse=True, remove_comments=True, sep=',', sheet_name=0, engine='pyarrow', **kwargs):
    """Read and parse a data file.
    It can read multiple format. For data frame-like format, the function will return a pandas data frame, otherzise a string.
    The function will by default detect the extension from the file path. You can force an extension with the argument.
    It can remove comments, trailing spaces, breaklines and tabs. It can also replace f-strings with provided values.

    .. code-block:: python

        pycof.f_read(path, extension=None, parse=True, remove_comments=True, sep=',', sheet_name=0, engine='pyarrow', **kwargs)

    :Parameters:
        * **path** (:obj:`str`): path to the SQL file.
        * **extension** (:obj:`str`): extension to use. Can be 'csv', 'txt', 'xslsx', 'sql', 'html', 'py', 'json', 'js', 'parquet', 'read-only' (defaults None).
        * **parse** (:obj:`bool`): Format the query to remove trailing space and comments, ready to use format (defaults True).
        * **remove_comments** (:obj:`bool`): Remove comments from the loaded file (defaults True).
        * **sheet_name** (:obj:`str`): Tab column to load when reading Excel files (defaults 0).
        * **engine** (:obj:`str`): Engine to use to load the file. Can be 'pyarrow' or the function from your preferred library (defaults 'pyarrow').
        * **sep** (:obj:`str`): Columns delimiter for pd.read_csv (defaults ',').

    :Example:

        >>> pycof.f_read('/path/to/file.sql', country='FR')

    :Returns:
        * :obj:`pandas.DataFrame`: Data frame a string from file read.
    """
    ext = path.split('.')[-1] if extension is None else extension
    data = []
    
    ## CSV / txt
    if ext.lower() in ['csv', 'txt']:
        data = pd.read_csv(path, sep=sep, **kwargs)
    ## XLSX
    elif ext.lower() in ['xls', 'xlsx']:
        data = pd.read_excel(path, sheet_name=sheet_name, **kwargs)
    ## SQL
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
    ## HTML
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
    ## Python
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
    ## JavaScript
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
    ## Json
    elif ext.lower() in ['json']:
        if engine.lower() in ['json']:
            with open(path) as json_file:
                data = json.load(json_file)
        else:
            data = pd.read_json(path, **kwargs)
    ## Parquet
    elif ext.lower() in ['parq', 'parquet']:
        if type(engine) == str:
            if engine.lower() in ['py', 'pa', 'pyarrow']:
                data = pq.read_table(path, **kwargs).to_pandas()
            else:
                raise ValueError('Engine value not allowed')
        else:
            data = engine(path, **kwargs)
    ## Else, read-only
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


##############################################################################################################################

## Send an Email
def send_email(to, subject, body, cc='', credentials={}):
    """Simplified function to send emails.
    Will look at the credentials at :obj:`/etc/config.json`. User can also pass a dictionnary for credentials.

    .. code-block:: python

        pycof.send_email(to, subject, body, cc='', credentials={})
    
    :Parameters:
        * **to** (:obj:`str`): Recipient of the email.
        * **subject** (:obj:`str`): Subject of the email.
        * **body** (:obj:`str`): Content of the email to be send.
        * **cc** (:obj:`str`): Email address to be copied (defaults None).
        * **credentials** (:obj:`dict`): Credentials to use to connect to the database. You can also provide the credentials path or the json file name from :obj:`/etc/` (defaults {}).
        * **verbose** (:obj:`bool`): Displays if the email was sent successfully (defaults False).
    
    :Example:
        >>> content = "This is a test"
        >>> pycof.send_email(to="test@domain.com", body=content, subject="Hello world!")
    """
    config = _get_config(credentials)
    msg = MIMEMultipart()
    msg['From'] = config.get('EMAIL_USER')
    msg['To'] = to
    msg['Cc'] = '' if cc == '' else cc
    msg['Subject'] = subject

    mail_type = 'html' if '</' in body else 'plain'
    msg.attach(MIMEText(body, mail_type))

    text = msg.as_string()

    # Server login
    try:
        port = int(config.get('EMAIL_PORT'))
    except:
        port = '587' # Default Google port number
    connection = config.get('EMAIL_SMTP') + ':' + port
    server = smtplib.SMTP(connection)
    server.starttls()
    server.login(user=config.get('EMAIL_USER'), password=config.get('EMAIL_PASSWORD'))

    # Send email
    server.sendmail(config.get('EMAIL_USER'), [to, '', cc], text)
    server.quit()


#############################################################################################################################

## Add zero to int less than 10 and return a string
def add_zero(nb):
    """Converts a number to a string and adds a '0' if less than 10.

    .. code-block:: python

        pycof.add_zero(nb)

    :Parameters:
        * **nb** (:obj:`float`): Number to be converted to a string.
    
    :Example:
        >>> pycof.add_zero(2)
        ... '02'

    :Returns:
        * :obj:`str`: Converted number qs a string.
    """
    if nb < 10:
        return('0' + str(nb))
    else:
        return(str(nb))


##############################################################################################################################

## Adding One Hot Encoding
def OneHotEncoding(dataset, column, drop=True, verbose=False):
    """Performs One Hot Encoding (OHE) usally used in Machine Learning.

    .. code-block:: python

        pycof.OneHotEncoding(dataset, column, drop=True, verbose=False)

    :Parameters:
        * **dataset** (:obj:`pandas.DataFrame`): Data Frame on which we apply One Hot Encoding.
        * **column** (:obj:`list`): Column to be converted to dummy variables.
        * **drop** (:obj:`bool`): Drop the column that needs to be converted to dummies (defaults True).
        * **verbose** (:obj:`bool`): Display progression (defaults False).
    
    :Example:
        >>> print(df)
        ... +----+--------+----------+-----+
        ... | Id | Gender | Category | Age |
        ... +----+--------+----------+-----+
        ... |  1 | Male   |        A |  23 |
        ... |  2 | Female |        B |  21 |
        ... |  3 | Female |        A |  31 |
        ... |  4 | Male   |        C |  22 |
        ... |  5 | Female |        A |  26 |
        ... +----+--------+----------+-----+
        >>> # Encoding columns "Gender" and "Category"
        >>> new_df = pycof.OneHotEncoding(df, column="Gender")
        >>> new_df = pycof.OneHotEncoding(new_df, column="Category")
        >>> print(new_df)
        ... +----+---------------+------------+------------+-----+
        ... | Id | Gender_Female | Category_A | Category_B | Age |
        ... +----+---------------+------------+------------+-----+
        ... |  1 |             0 |          1 |          0 |  23 |
        ... |  2 |             1 |          0 |          1 |  21 |
        ... |  3 |             1 |          1 |          0 |  31 |
        ... |  4 |             0 |          0 |          0 |  22 |
        ... |  5 |             1 |          1 |          0 |  26 |
        ... +----+---------------+------------+------------+-----+

    :Returns:
        :obj:`pandas.DataFrame`: Transformed dataset with One Hot Encoding.
    """
    all_values = dataset[column].unique()
    
    for val in all_values:
        if verbose:
            print('Encoding for value: ' + str(val))
        dataset[column + '_' + str(val)] = 0
        dataset[column + '_' + str(val)][dataset[column] == val] = 1
    
    if drop:
        dataset = dataset.drop(columns = [column])
    return(dataset)


##############################################################################################################################

## convert an array of values into a dataset matrix: used for LSTM data pre-processing
def create_dataset(dataset, look_back=1):
    """Function to convert a DataFrame to array format readable for keras LSTM.

    .. code-block:: python

        pycof.create_dataset(dataset, look_back=1)
    
    :Parameters:
        * **dataset** (:obj:`pandas.DataFrame`): DataFrame on which to aply the transformation.
        * **look_back** (:obj:`int`): Number of periods in the past to consider (defaults 1).
    
    :Example:
        >>> pc.create_dataset(df)

    :Returns:
        * :obj:`numpy.array`: Features X converted for keras LSTM.
        * :obj:`numpy.array`: Dependent variable Y converted for keras LSTM.
    """
    dataX, dataY = [], []
    for i in range(len(dataset)-look_back-1):
        a = dataset[i:(i+look_back), 0]
        dataX.append(a)
        dataY.append(dataset[i + look_back, 0])
    return np.array(dataX), np.array(dataY)


##############################################################################################################################

### Put thousand separator
def group(nb, digits=0):
    """Transforms a number into a string with a thousand separator.

    .. code-block:: python

        pycof.group(nb, digits=0)

    :Parameters:
        * **nb** (:obj:`float`): Number to be transformed.
        * **digits** (:obj:`int`): Number of digits to round.
    
    :Example:
        >>> pycof.group(12345)
        ... '12,345'
        >>> pycof.group(12345.54321, digits=3)
        ... '12,345.543'

    :Returns:
        * :obj:`str`: Transformed number.
    """
    s = '%d' % nb
    groups = []
    if digits > 0:
        dig = '.' + str(nb).split('.')[1][:digits]
    else:
        dig = ''
    while s and s[-1].isdigit():
        groups.append(s[-3:])
        s = s[:-3]
    return s + ','.join(reversed(groups)) + dig


##############################################################################################################################

### Transform 0 to '-'
def replace_zero(nb, digits=0):
    """For a given number, will transform 0 by '-' for display puspose.

    .. code-block:: python

        pycof.replace_zero(nb)

    :Parameters:
        * **nb** (:obj:`float`): Number to be transformed.

    :Example:
        >>> pycof.replace_zero(0)
        ... '-'
        >>> pycof.replace_zero(12345)
        ... '12'
        >>> pycof.replace_zero(12345, digits=1)
        ... '12,3'

    :Returns:
        * :obj:`str`: Transformed number as a string.
    """
    if (str(nb) == '0'):
        return '-'
    else:
        return(group(nb/1000, digits))


##############################################################################################################################

### Get the week (sunday) date
def week_sunday(date, return_week_nb=False):
    """For a given date, will return the date from previous sunday or week number.

    .. code-block:: python

        pycof.week_sunday(date, return_week_nb=False)

    :Parameters:
        * **date** (:obj:`datetime.date`): Date tfrom which we extract the week number/sunday date.
        * **return_week_nb** (:obj:`bool`): If True will return week number with sunday basis (defaults False).

    :Example:
        >>> pycof.week_sunday(datetime.date(2020, 4, 15))
        ... datetime.date(2020, 4, 12)
        >>> pycof.week_sunday(datetime.date(2020, 4, 15), return_week_nb = True)
        ... 16

    :Returns:
        * :obj:`int`: Week number (from 1 to 52) if :obj:`return_week_nb` else date format.
    """
    # Get when was the last sunday
    idx = (date.weekday() + 1) % 7 # MON = 0, SUN = 6 -> SUN = 0 .. SAT = 6
    # Get the date
    last_sunday = date - datetime.timedelta(idx)
    if return_week_nb:
        # Return iso week number
        return(last_sunday.isocalendar()[1] + 1)
    else:
        # Return date
        return(last_sunday)


##############################################################################################################################

### Get use name (not only login)
def display_name(display='first'):
    """Displays current user name (either first/last or full name)

    .. code-block:: python

        display_name(display='first')
    
    :Parameters:
        * **display** (:obj:`str`): What name to display 'first', 'last' or 'full' (defaults 'first').
    
    :Example:
        >>> pycof.display_name()
        ... 'Florian'

    :Returns:
        * :obj:`str`: Name to be displayed.
    """
    try:
        if sys.platform in ['win32']:
            import ctypes
            GetUserNameEx = ctypes.windll.secur32.GetUserNameExW
            NameDisplay = 3
            #
            size = ctypes.pointer(ctypes.c_ulong(0))
            GetUserNameEx(NameDisplay, None, size)
            #
            nameBuffer = ctypes.create_unicode_buffer(size.contents.value)
            GetUserNameEx(NameDisplay, nameBuffer, size)
            user = nameBuffer.value
            if display == 'first':
                return(user.split(', ')[1])
            elif display == 'last':
                return(user.split(', ')[0])
            else:
                return(user)
        else:
            import pwd
            user = pwd.getpwuid(os.getuid())[4]
            if display == 'first':
                return (user.split(', ')[1])
            elif display == 'last':
                return (user.split(', ')[0])
            else:
                return (user)
    except:
        return(getpass.getuser())


##############################################################################################################################

# Convert a string to boolean
def str2bool(value):
    """Convert a string into boolean.

    .. code-block:: python

        pycof.str2bool(value)

    :Parameters:
        * **value** (:obj:`str`): Value to be converted to boolean.
    
    :Example:
        >>> pycof.str2bool('true')
        ... True
        >>> pycof.str2bool(1)
        ... True
        >>> pycof.str2bool(0)
        ... False

    :Returns:
        * :obj:`bool`: Returns either True or False.
    """
    return str(value).lower() in ("yes", "y", "true", "t", "1")


##############################################################################################################################

