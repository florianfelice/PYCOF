import os
import sys
import getpass
import json

import pandas as pd
import numpy as np
import re
import xlrd

from tqdm import tqdm
import datetime

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from statinf.ml.losses import mape
from statinf.ml.losses import mean_squared_error as mse

from .sqlhelper import _get_config, _get_credentials, _define_connector
from .sqlhelper import _insert_data, _cache
from .misc import write, file_age, verbose_display


##############################################################################################################################

## TODO: Test send_email by list of recipients
## TODO: remote_execute_sql with dump to S3 or to S3 and Redshift.

##############################################################################################################################

## Publish or read from DB
def remote_execute_sql(sql_query="", query_type="", table="", data={}, credentials={}, verbose=True, autofill_nan=True, useIAM=False, cache=False, cache_time=24*60*60, cache_name=None):
    """Simplified function for executing SQL queries.
    Will look qt the credentials at /etc/config.json. User can also pass a dictionnary for credentials.

    Example:
        > remote_execute_sql("SELECT * FROM SCHEMA.TABLE LIMIT 10")

    Args:
        sql_query (str): SQL query to be executed (defaults "").
        query_type (str): Type of SQL query to execute. Can either be SELECT, INSERT or DELETE (defaults "SELECT").
        table (str): Table in which we want to operate, only used for INSERT and DELETE (defaults "").
        data (pandas.DataFrame): Data to load on the database (defaults {}).
        credentials (dict): Credentials to use to connect to the database. You can also provide the credentials path or the json file name from '/etc/' (defaults {}).
        verbose (bool): Display progression bar (defaults True).
        autofill_nan (bool): Replace NaN values by 'NULL' (defaults True).
        useIAM (bool): Get AWS IAM credentials using access and secret key (defaults False).
        cache (bool): Caches the data to avoid running again the same SQL query (defaults False).
        cache_time (int): How long to keep the caching data without reloading (defaults 1 day).
        cache_name (str): File name for storing cache data, if None will use WHERE clause from SQL (defaults None).

    Returns:
        pandas.DataFrame: Result of an SQL query in case of query_type as SELECT.
    """

    #====================================
    # Define the SQL type
    all_query_types = ['SELECT', 'INSERT', 'DELETE', 'COPY']

    if (query_type != ""):
        #Use user input if query_type is not as its default value
        sql_type = query_type
    elif (sql_query != ""):
        # If a query is inserted, use select.
        # For DELETE or COPY, user needs to provide the query_type
        sql_type = "SELECT"
    elif (data != {}):
        # If data is provided, use INSERT sql_type
        sql_type = 'INSERT'
    else:
        # Check if the query_type value is correct
        assert sql_type.upper() in all_query_types,  f"Your query_type value is not correct, allowed values are {', '.join(all_query_types)}"

    #==============================================================================================================================
    # Credentials load
    hostname, port, user, password, database = _get_credentials(_get_config(credentials), useIAM)
    
    #==============================================================================================================================
    # Set default value for table
    if (sql_type == 'SELECT'): # SELECT
        if (table == ""): # If the table is not specified, we get it from the SQL query
            table = sql_query.upper().replace('\n', ' ').split('FROM ')[1].split(' ')[0]
        elif (sql_type == 'SELECT') & (table.upper() in sql_query.upper()):
            table = table
        else:
            raise SyntaxError('Argument table does not match with SQL statement')
    
    #==============================================================================================================================
    # Database connector
    conn, cur = _define_connector(hostname, port, user, password, database)
    
    #==============================================================================================================================
    # Read query
    if sql_type.upper() == "SELECT": # SELECT
        if cache:
            read = _cache(sql_query, conn, sql_type, cache_time=cache_time, verbose=verbose)
        else:
            read = pd.read_sql(sql_query, conn)
        return(read)
    #==============================================================================================================================
    # Insert query
    elif sql_type.upper() == "INSERT": # INSERT
        _insert_data(data, table, conn, cur, autofill_nan, verbose)

    #==============================================================================================================================
    # Delete query
    elif sql_type.upper() in ["DELETE", "COPY"]:
        if table.upper() in sql_query.upper():
            cur.execute(sql_query)
            conn.commit()
        else:
            raise ValueError('Table does not match with SQL query')
    else:
        raise ValueError(f'Unknown query_type, should be as: {all_query_types}')
    
    #close sql connection
    conn.close()



##############################################################################################################################

def f_read(path, extension=None, parse=True, remove_comments=True, sep=',', sheet_name=0, **kwargs):
    """Read and parse a data file.
    It can read multiple format. For data frame-like format, the function will return a pandas data frame, otherzise a string.
    The function will by default detect the extension from the file path. You can force an extension with the argument.
    It can remove comments, trailing spaces, breaklines and tabs. It can also replace f-strings with provided values.

    Example:
        > f_read('/path/to/file.sql', country='FR')

    Args:
        path (str): path to the SQL file.
        extension (str): extension to use. Can be 'csv', 'xslsx', 'sql', 'html', 'py', 'json', 'read-only' (defaults None).
        parse (bool): Format the query to remove trailing space and comments, ready to use format (defaults True).
        remove_comments (bool): Remove comments from the loaded file (defaults True).
        sep (str): Columns delimiter for pd.read_csv (defaults ',').


    """
    ext = path.split('.')[-1] if extension is None else extension
    data = []
    ret = True # Need to return a value?
    
    ## CSV
    if ext.lower() in ['csv']:
        data = pd.read_csv(path, sep=sep, **kwargs)
    ## XLSX
    elif ext.lower() in ['xls', 'xlsx']:
        data = pd.read_excel(path, sheet_name=sheet_name, **kwargs)
    ## SQL
    elif ext.lower() in ['sql']:
        with open(path) as f:
            file = f.read()
        for line in file.split('\n'): # Parse the data
            l = line.strip() # Removing trailing spaces
            l = l.format(**kwargs) # Formating
            if remove_comments:
                l = l.split('--')[0] # Remove comments
                re.sub(r"<!--(.|\s|\n)*?-->", "", l.replace('/*', '<!--').replace('*/', '-->'))
            if l != '':
                data += [l]
        data = ' '.join(data)
    ## HTML
    elif ext.lower() in ['html']:
        with open(path) as f:
            file = f.read()
        # Parse the data
        for line in file.split('\n'):
            l = line.strip() # Removing trailing spaces
            l = l.format(**kwargs) # Formating
            if remove_comments:
                l = re.sub(r"<!--(.|\s|\n)*?-->", "", l) # Remove comments
            if l != '':
                data += [l]
        data = ' '.join(data)
    ## Python
    elif ext.lower() in ['py']:
        with open(path) as f:
            file = f.read()
        # Parse the data
        for line in file.split('\n'):
            l = line.strip() # Removing trailing spaces
            l = l.format(**kwargs) # Formating
            if remove_comments:
                l = l.split('#')[0] # Remove comments
            if l != '':
                data += [l]
        data = ' '.join(data)
    ## Json
    elif ext.lower() in ['json']:
        with open(path) as json_file:
            data = json.load(json_file)
    ## Else, read-only
    elif ext.lower() in ['readonly', 'read-only', 'ro']:
        ret = False
        with open(path) as f:
            for line in f:
                print(line.rstrip())
    else:
        ret = False

    # If not read-only
    if ret:
        return data


##############################################################################################################################

## Send an Email
def send_email(to, subject, body, cc='', credentials={}):
    """Simplified function to send emails.
    Will look at the credentials at /etc/config.json. User can also pass a dictionnary for credentials.

    Example:
        > content = 'This is a test'
        > send_email(to='test@domain.com', body=content, subject='Hello world!')

    Args:
        to (str): Recipient of the email.
        subject (str): Subject of the email.
        body (str): Content of the email to be send.
        cc (str): Email address to be copied (defaults None).
        credentials (dict): Credentials to use to connect to the database. You can also provide the credentials path or the json file name from '/etc/' (defaults {}).
        verbose (bool): Displays if the email was sent successfully (defaults False).
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

    Args:
        nb (float): Number to be converted to a str.

    Returns:
        str: Converted number qs a string.
    """
    if nb < 10:
        return('0' + str(nb))
    else:
        return(str(nb))

##############################################################################################################################


## Adding One Hot Encoding
def OneHotEncoding(dataset, column, drop=True, verbose=False):
    """Performs One Hot Encoding (OHE) usally used in Machine Learning.

    Args:
        dataset (pandas.DataFrame): Data Frame on which we apply One Hot Encoding.
        column (list): Column to be converted to dummy variables.
        drop (bool): Drop the column that needs to be converted to dummies (defaults True).
        verbose (bool): Display progression (defaults False).

    Returns:
        pandas.DataFrame: Transformed dataset with One Hot Encoding.
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

    Args:
        dataset (pandas.DataFrame): DataFrame on which to aply the transformation.
        look_back (int): Number of periods in the past to consider (defaults 1).

    Returns:
        np.array: Features X converted for keras LSTM.
        np.array: Dependent variable Y converted for keras LSTM.
    """
    dataX, dataY = [], []
    for i in range(len(dataset)-look_back-1):
        a = dataset[i:(i+look_back), 0]
        dataX.append(a)
        dataY.append(dataset[i + look_back, 0])
    return np.array(dataX), np.array(dataY)


##############################################################################################################################


### Put thousand separator
def group(number, digits=0):
    """Transforms a number into a string with a thousand separator.

    Args:
        number (float): Number to be transformed.
        digits (int): Number of digits to round.

    Returns:
        str: Transformed number.
    """
    s = '%d' % number
    groups = []
    if digits > 0:
        str(number).split('.')[1][:digits]
    while s and s[-1].isdigit():
        groups.append(s[-3:])
        s = s[:-3]
    return s + ','.join(reversed(groups))


##############################################################################################################################


### Transform 0 to '-'
def replace_zero(nb):
    """For a given number, will transform 0 by '-' for display puspose.

    Args:
        nb (float): Number to be transformed.

    Returns:
        str: Transformed number as a string.
    """
    if (str(nb) == '0'):
        return '-'
    else:
        return(group(nb/1000))



##############################################################################################################################


### Get the week (sunday) date
def week_sunday(date, return_week_nb = False):
    """For a given date, will return the date from previous sunday or week number.

    Args:
        date (datetime.date): Date tfrom which we extract the week number/sunday date.
        return_week_nb (bool): If True will return week number with sunday basis (defaults False).

    Returns:
        int: Week number (from 1 to 52) if return_week_nb else date format.
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
    
    Args:
        display (str): What name to display 'first', 'last' or 'full' (defaults 'first').

    Returns:
        str: Name to be displayed.
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
def str2bool(v):
    """Convert a string into boolean.

    Args:
        v (str): Value to be converted to boolean.

    Returns:
        bool: Returns either True or False.
    """
    return v.lower() in ("yes", "y", "true", "t", "1")


##############################################################################################################################

# MAPE formula
def mape(y_true, y_pred):
    """Computes the Mean Absolute Percentage Error.

    Args:
        y (list): Real values on which to compare.
        yhat (list): Predicted values.

    Returns:
        float: MAPE.
    """
    y = np.array(y_true)
    yhat = np.array(y_pred)
    m = len(y)
    mape = (100/m) * sum(np.abs(y-yhat))/sum(y)
    return mape



##############################################################################################################################

# MSE formula
def mse(y_estimated, y_actual, root=False):
    """Computes the Mean Squared Error

    Args:
        y_estimated (list): Real values on which to compare.
        y_actuams (list): Predicted values.
        root (bool): Return Root Mean Squared Error (RMSE) or simple MSE.

    Returns:
        float: MSE or RMSE.
    """
    y_estimates = np.array(y_estimates)
    y_actuals = np.array(y_actuals)
    if root:
        output = ((sum((y_estimates - y_actuals))**2)/len(y_estimates))**(1/2)
    else:
        output = (sum((y_estimates - y_actuals))**2)/len(y_estimates)
    return output
