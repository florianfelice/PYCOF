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
def write(text, file, perm='a', verbose=False, end_row='\n'):
    """Write a line of text into a file (usually .txt).

    .. code-block:: python

        pycof.write(text, file, perm='a', verbose=False, end_row='\\n')

    :Parameters:
        * **text** (:obj:`str`): Line of text to be inserted in the file.
        * **file** (:obj:`str`): File on which to write (/path/to/file.txt). Can be any format, not necessarily txt.
        * **perm** (:obj:`str`): Permission to use when opening file (usually 'a' for appending text, or 'w' to (re)write file).
        * **verbose** (:obj:`bool`): Return the length of the inserted text if set to True (defaults False).
        * **end_row** (:obj:`str`): Character to end the row (defaults '\\n').
    
    :Example:
        >>> pycof.write('This is a test', file='~/pycof_test_write.txt', perm='w')

    :Returns:
        * :obj:`int`: Number of characters inserted if verbose is True.
    """
    with open(file, perm) as f:
        f.write(text + end_row)
    if verbose:
        return(len(text))


##############################################################################################################################

# Compute the age of a given file
def file_age(file_path, format='seconds'):
    """Computes the age of a file. 

    .. code-block:: python

        pycof.file_age(file_path, format='seconds')
    
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
    ttl_sec = (datetime.datetime.now() - datetime.datetime.utcfromtimestamp(os.stat(file_path).st_mtime)).total_seconds()
    if format.lower() in ['s', 'sec', 'second', 'seconds']:
        return ttl_sec
    elif format.lower() in ['m', 'min', 'minute', 'minutes']:
        return ttl_sec/60
    elif format.lower() in ['h', 'hr', 'hour', 'hour']:
        return ttl_sec/3600
    elif format.lower() in ['d', 'day', 'days']:
        return ttl_sec/(24*60*60)
    else:
        raise ValueError(f"Format value is not correct. Can be 'seconds', 'minutes', 'hours' or 'days'. Got '{format}'.")


##############################################################################################################################

## Display tqdm only if argument for verbosity is 1 (works for lists, range and str)

def verbose_display(element, verbose=True, sep=' ', end='\n', return_list=False):
    """Extended print function with tqdm display for loops.
    Also has argument verbose for automated scripts with overall verbisity argument.

    .. code-block:: python

        pycof.verbose_display(element, verbose=True, sep=' ', end='\\n', return_list=False)

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

