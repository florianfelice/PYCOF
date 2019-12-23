import os
import sys
import getpass
import json

import pandas as pd
import numpy as np

from tqdm import tqdm
import datetime

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from .sqlhelper import _get_config, _get_credentials, _define_connector
from .sqlhelper import _insert_data, _cache


##############################################################################################################################

## TODO: Check _get_credentials for IAM roles
## TODO: Test send_email by list of recipients
## TODO: remote_execute_sql with dump to S3 or to S3 and Redshift.

##############################################################################################################################

## Display tqdm only if argument for verbosity is 1 (works for lists, range and str)

def verbose_display(element, verbose = True, sep = ' ', end = '\n', return_list = False):
    """Extended print function with tqdm display for loops.
    Also has argument verbose for automated scripts with overall verbisity argument

    Example:
        > for i in pycof.verbose_display(range(15)):
        ...     i += 1
    
    Args:
        element (str): The element to be displayed. Can either be str, range, list.
        verbose (bool): Display the element or not (defaults True).
        sep (str): The deperator to use of displaying different lists/strings (defaults ' ').
        end (str): How to end the display (defaults '\n').
        return_list (bool): If it is a list, can return in for paragraph format (defaults False).

    Returns:
        str: The element to be displayed.
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


##############################################################################################################################

## Publish or read from DB
def remote_execute_sql(sql_query="", query_type="SELECT", table="", data={}, credentials={}, verbose=True, autofill_nan=True, useIAM=False, cache=False, cache_time=24*60*60, cache_name=None):
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

    #==============================================================================================================================
    # Credentials load
    hostname, port, user, password, database = _get_credentials(_get_config(credentials), useIAM)
    
    #====================================
    # Check if the query_type value is correct
    all_query_types = ['SELECT', 'INSERT', 'DELETE', 'COPY']
    assert query_type.upper() in all_query_types,  f"Your query_type value is not correct, allowed values are {', '.join(all_query_types)}"
    
    #==============================================================================================================================
    # Set default value for table
    if (query_type == 'SELECT'): # SELECT
        if (table == ""): # If the table is not specified, we get it from the SQL query
            table = sql_query.replace('\n', ' ').split('FROM ')[1].split(' ')[0]
        elif (query_type == 'SELECT') & (table.upper() in sql_query.upper()):
            table = table
        else:
            raise SyntaxError('Argument table does not match with SQL statement')
    
    #==============================================================================================================================
    # Database connector
    conn, cur = _define_connector(hostname, port, user, password, database)
    
    #==============================================================================================================================
    # Read query
    if query_type.upper() == "SELECT": # SELECT
        read = pd.read_sql(sql_query, conn)
        if cache:
            read = cache(sql_query, conn, verbose=verbose)
        return(read)
    #==============================================================================================================================
    # Insert query
    elif query_type.upper() == "INSERT": # INSERT
        _insert_data(data, table, conn, cur, autofill_nan, verbose)

    #==============================================================================================================================
    # Delete query
    elif query_type.upper() in ["DELETE", "COPY"]:
        if table.upper() in sql_query.upper():
            cur.execute(sql_query)
            conn.commit()
        else:
            raise ValueError('Table does not match with SQL query')
    else:
        raise SyntaxError(f'Unknown query_type, should be as: {all_query_types}')
    
    #close sql connection
    conn.close()



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

    msg.attach(MIMEText(body, 'plain'))

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
def OneHotEncoding(df, colName, drop = True, verbose = False):
    """Performs One Hot Encoding (OHE) usally used in Machine Learning.

    Args:
        df (pandas.DataFrame): Data Frame on which we apply One Hot Encoding.
        colName (list): Columns to be converted to dummy variables.
        drop (bool): Keep the columns that need to be converted to dummies (defaults True).
        verbose (bool): Display progression (defaults False).

    Returns:
        pandas.DataFrame: Transformed dataset with One Hot Encoding.
    """
    all_values = df[colName].unique()
    
    for val in all_values:
        if verbose:
            print('Encoding for value: ' + str(val))
        df[colName + '_' + str(val)] = 0
        df[colName + '_' + str(val)][df[colName] == val] = 1
    
    if drop:
        df = df.drop(columns = [colName])
    return(df)


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
def group(number):
    """Transforms a number into a string with a thousand separator.

    Args:
        number (float): Number to be transformed.

    Returns:
        str: Transformed number.
    """
    s = '%d' % number
    groups = []
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

# Write to a txt file
def write(text, file, perm = 'a', verbose = False, end_row = '\n'):
    """Write a line of text into a file (usually .txt).

    Args:
        text (str): Line of text to be inserted in the file.
        file (str): File on which to write (/path/to/file.txt). Can be any format, not necessarily txt.
        perm (str): Permission to use when opening file (usually 'a' for appending text, or 'w' to (re)write file).
        verbose (bool): Return the length of the inserted text if set to True (defaults False).
        end_row (str): Character to end the row (defaults '\n').

    Returns:
        int: Number of characters inserted if verbose is True.
    """
    with open(file, perm) as f:
        f.write(text + end_row)
    if verbose:
        return(len(text))


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

# WMAPE formula
def wmape(y, yhat):
    """Computes the Weighted Mean Absolute Percentage Error.

    Args:
        y (list): Real values on which to compare.
        yhat (list): Predicted values.

    Returns:
        float: Weighted MAPE.
    """
    y = np.array(y)
    yhat = np.array(yhat)
    mape = sum(np.abs(y-yhat))/sum(y)*100
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
