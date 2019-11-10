import os
import sys
import getpass
import json

import pymysql
import psycopg2
import pandas as pd
from tqdm import tqdm


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
def remote_execute_sql(sql_query="", query_type="SELECT", table="", data={}, credentials={}, verbose=True):
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

    Returns:
        pandas.DataFrame: Result of an SQL query in case of query_type as SELECT.
    """
    # Check if credentials of credentials path is provided
    if type(credentials) == str:
        if '/' in credentials:
            path = credentials
        elif sys.platform == 'win32'
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
    
    # Load credentials
    if path == '':
        config = credentials
    else:
        with open(path) as config_file:
            config = json.load(config_file)
    
    # Check if the query_type value is correct
    all_query_types = ['SELECT', 'INSERT', 'DELETE']
    assert query_type.upper() in all_query_types,  f"Your query_type value is not correct, allowed values are {', '.join(all_query_types)}"
    
    ## Access DB credentials
    hostname = config.get('DB_HOST') # Read the host name value from the config dictionnary
    port = int(config.get('DB_PORT')) # Get the port from the config file and convert it to int
    user = config.get('DB_USER')    # Get the user name for connecting to the DB
    password = config.get('DB_PASSWORD') # Get the DB
    database = config.get('DB_DATABASE') # For Redshift, use the database, for MySQL set it by default to ""
    
    # Set default value for table
    if (query_type == 'SELECT'): # SELECT
        if (table == ""): # If the table is not specified, we get it from the SQL query
            table = sql_query.replace('\n', ' ').split('FROM ')[1].split(' ')[0]
        elif (query_type == 'SELECT') & (table.upper() in sql_query.upper()):
            table = table
        else:
            raise SyntaxError('Argument table does not match with SQL statement')
    
    # Initiate sql connection to the Database
    if 'redshift' in hostname.split('.'):
        try:
            conn = psycopg2.connect(host=hostname, port=port, user=user, password=password, database=database)
            cur = conn.cursor()
        except:
            raise ValueError('Failed to connect to the Redshfit cluster')
    else:
        try:
            conn = pymysql.connect(host=hostname, port=port, user=user, password=password)
            cur = conn.cursor()
        except:
            raise ValueError('Failed to connect to the MySQL database')
    
    # Read query
    if query_type.upper() == "SELECT": # SELECT
        read = pd.read_sql(sql_query, conn)
        return(read)
    
    # Insert query
    elif query_type.upper() == "INSERT": # INSERT
        # Check if user defined the table to publish
        if table == "":
            raise SyntaxError('Destination table not defined by user')
        # Create the column string and the number of columns used for push query 
        columns_string = (', ').join(list(data.columns))
        col_num = len(list(data.columns))-1
        
        #calculate the size of the dataframe to be pushed
        num = len(data)
        batches = int(num/10000)+1
        
        # Push 10k batches iterativeley and then push the remainder
        if num == 0:
            raise ValueError('len(data) == 0 -> No data to insert')
        elif num > 10000:
            for i in verbose_display(range(0, batches-1), verbose = verbose):
                cur.executemany(f'INSERT INTO {table} ({columns_string}) VALUES ({"%s, "*col_num} %s )', data[i*10000: (i+1)*10000].values.tolist())
                conn.commit()
            # push the remainder
            cur.executemany(f'INSERT INTO {table} ({columns_string}) VALUES ({"%s, "*col_num} %s )', data[(batches-1)*10000 :].values.tolist())
            conn.commit()
        else:
            # Push everything if less then 10k (SQL Server limit)
            cur.executemany(f'INSERT INTO {table} ({columns_string}) VALUES ({"%s, "*col_num} %s )', data.values.tolist())
            conn.commit()
    
    elif query_type.upper() == "DELETE": # DELETE
        if table.upper() in sql_query.upper():
            cur.execute(sql_query)
            conn.commit()
        else:
            raise ValueError('Table does not match with SQL query')
    else:
        raise SyntaxError('Unknown query_type, should be as: {all_query_types}')
    
    #close sql connection
    conn.close()


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
        df (pandas.DataFrame): Data Frame on whioch we apply One Hot Encoding.
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
def replace_zero(bn):
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
    return v.lower() in ("yes", "true", "t", "1")


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
