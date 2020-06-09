import os
import sys
import getpass

import pandas as pd
import numpy as np
import math
import json
import xlrd
import pyarrow.parquet as pq

import re

from tqdm import tqdm
import datetime

from .misc import write

##############################################################################################################################

# Easy file read
def f_read(path, extension=None, parse=True, remove_comments=True, sep=',', sheet_name=0, engine='pyarrow', **kwargs):
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
        * **\\*\\*kwargs** (:obj:`str`): Arguments to be passed to the engine or values to be formated in the file to load.
        

    :Example:

        >>> pycof.f_read('/path/to/file.sql', country='FR')

    :Returns:
        * :obj:`pandas.DataFrame`: Data frame a string from file read.
    """
    ext = path.split('.')[-1] if extension is None else extension
    data = []

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
        if type(engine) == str:
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
