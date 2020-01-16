import os
import sys
import getpass
import json

import pandas as pd
import numpy as np

from tqdm import tqdm
import datetime

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

# Compute the age of a given file
def file_age(file_path, format='seconds'):
    """
    """
    ttl_sec = (datetime.datetime.now() - datetime.datetime.utcfromtimestamp(os.stat(file_path).st_mtime)).total_seconds()
    if format.upper() in ['SEC', 'SECONDS']:
        return ttl_sec


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

