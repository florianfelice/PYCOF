import os
import sys

import pymysql
import psycopg2

import json
import pandas as pd


##############################################################################################################################


## Display tqdm only if argument for verbosity is 1 (works for lists, range and str)

def verbose_display(element, verbose = True, sep = ' ', end = '\n', return_list = False):
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
    #
    if credentials == {}:
	    with open('/etc/config.json') as config_file:
	    	config = json.load(config_file)
    else:
    	config = credentials
    #
    all_query_types = ['SELECT', 'INSERT', 'DELETE']
    
    ## Access DB credentials
    hostname = config.get('DB_HOST') # Read the host name value from the config dictionnary
    port = int(config.get('DB_PORT'))
    user = config.get('DB_USER')
    password = config.get('DB_PASSWORD')
    database = config.get('DB_DATABASE')
    #
    # Set default value for table
    if (query_type == all_query_types[0]): # SELECT
        if (table == ""):
        	## If the table is not specified, we get it from the SQL query
            table = sql_query.replace('\n', ' ').split('FROM ')[1].split(' ')[0]
        elif (query_type == all_query_types[0]) & (table.upper() in sql_query.upper()):
            table = table
        else:
            raise SyntaxError('Argument table does not match with SQL statement')
    #
    # Initiate sql connection to the 
    if 'redshift' in hostname.split('.'):
        conn = psycopg2.connect(host=hostname, port=port, user=user, password=password, database=database)
        cur = conn.cursor()
    else:
        conn = pymysql.connect(host=hostname, port=port, user=user, password=password)
        cur = conn.cursor()
    #
    # Read query
    if query_type.upper() == all_query_types[0]: # SELECT
        read = pd.read_sql(sql_query, conn)
        return(read)
    	
    # Insert query
    elif query_type.upper() == all_query_types[1]: # INSERT
        #
        # Check if user defined the table to publish
        if table == "":
            raise SyntaxError('Destination table not defined by user')
        # Create the column string and the number of columns used for push query 
        columns_string = (', ').join(list(data.columns))
        col_num = len(list(data.columns))-1
        #
        #calculate the size of the dataframe to be pushed
        num = len(data)
        batches = int(num/10000)+1
        #
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
        #elif num == 1:
        #    str_values = ('", "').join(str(x) for x in list(data.values[0])).replace('nan', '')
        #    cur.execute(f'INSERT INTO {table} ({columns_string}) VALUES ("{str_values}")')
        #    conn.commit()
        else:
            # Push everything if less then 10k (SQL Server limit)
            cur.executemany(f'INSERT INTO {table} ({columns_string}) VALUES ({"%s, "*col_num} %s )', data.values.tolist())
            conn.commit()
    #
    elif query_type.upper() == all_query_types[2]: # DELETE
        if table.upper() in sql_query.upper():
            cur.execute(sql_query)
            conn.commit()
        else:
            raise ValueError('Table does not match with SQL query')
    else:
        raise SyntaxError('Unknown query_type, should be as: {all_query_types}')
    #
    #close sql connection
    conn.close()


#############################################################################################################################


## Add zero to int less than 10 and return a string
def add_zero(nb):
    if nb < 10:
        return('0' + str(nb))
    else:
        return(str(nb))

##############################################################################################################################


## Adding One Hot Encoding
def OneHotEncoding(df, colName, drop = True, verbose = False):
    all_values = df[colName].unique()
    #
    for val in all_values:
        if verbose:
            print('Encoding for value: ' + str(val))
        df[colName + '_' + str(val)] = 0
        df[colName + '_' + str(val)][df[colName] == val] = 1
    #
    if drop:
        df = df.drop(columns = [colName])
    return(df)


##############################################################################################################################


## convert an array of values into a dataset matrix: used for LSTM data pre-processing
def create_dataset(dataset, look_back=1):
    dataX, dataY = [], []
    for i in range(len(dataset)-look_back-1):
        a = dataset[i:(i+look_back), 0]
        dataX.append(a)
        dataY.append(dataset[i + look_back, 0])
    return np.array(dataX), np.array(dataY)


##############################################################################################################################


### Put thousand separator
def group(number):
    s = '%d' % number
    groups = []
    while s and s[-1].isdigit():
        groups.append(s[-3:])
        s = s[:-3]
    return s + ','.join(reversed(groups))


##############################################################################################################################


### Transform 0 to '-'
def replace_zero(x):
    if (str(x) == '0'):
        return '-'
    else:
        return(group(x/1000))


##############################################################################################################################


### Get use name (not only login)
def display_name(display='first'):
    """
        display = first / last / all
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
    with open(file, perm) as f:
        f.write(text + end_row)
    if verbose:
        return(len(text))


##############################################################################################################################

# Convert a string to boolean
def str2bool(v):
  return v.lower() in ("yes", "true", "t", "1")


##############################################################################################################################

# WMAPE formula
def wmape(y, yhat):
    y = np.array(y)
    yhat = np.array(yhat)
    mape = sum(np.abs(y-yhat))/sum(y)*100
    return mape



##############################################################################################################################

# MSE formula
def mse(y_estimated, y_actual, root=False):
    y_estimates = np.array(y_estimates)
    y_actuals = np.array(y_actuals)
    if root:
        output = ((sum((y_estimates - y_actuals))**2)/len(y_estimates))**(1/2)
    else:
        output = (sum((y_estimates - y_actuals))**2)/len(y_estimates)
    return output
