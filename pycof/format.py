import os
import sys
import getpass
import warnings

import re
import math

from tqdm import tqdm
import datetime

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from .sqlhelper import _get_config, _get_credentials
from .misc import file_age, verbose_display


#######################################################################################################################

# Send an Email
def send_email(to, subject, body, cc='', credentials={}):
    """Simplified function to send emails.
    Will look at the credentials at :obj:`/etc/config.json`. User can also pass a dictionnary for credentials.

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
        port = str(config.get('EMAIL_PORT'))
    except Exception:
        port = '587'  # Default Google port number
    connection = config.get('EMAIL_SMTP') + ':' + port
    server = smtplib.SMTP(connection)
    server.starttls()
    server.login(user=config.get('EMAIL_USER'), password=config.get('EMAIL_PASSWORD'))

    # Send email
    server.sendmail(config.get('EMAIL_USER'), [to, '', cc], text)
    server.quit()


#######################################################################################################################

# Add zero to int less than 10 and return a string
def add_zero(nb):
    """Converts a number to a string and adds a '0' if less than 10.

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


#######################################################################################################################

# Adding One Hot Encoding
def OneHotEncoding(dataset, column, drop=True, verbose=False):
    """Performs One Hot Encoding (OHE) usally used in Machine Learning.

    .. warning::
        This function will soon be migrated to STATINF.
        Please check `STATINF <https://www.florianfelice.com/statinf/data/process.html#statinf.data.ProcessData.OneHotEncoding?orgn=pycof>`_

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

    deprec_msg = """Deprecation!
    This function is being migrated to STATINF and will soon be removed from PYCOF.
    Please see https://www.florianfelice.com/statinf/data/process.html#statinf.data.ProcessData.OneHotEncoding?orgn=py
    """
    warnings.warn(deprec_msg, Warning)
    all_values = dataset[column].unique()

    for val in all_values:
        if verbose:
            print('Encoding for value: ' + str(val))
        dataset[column + '_' + str(val)] = 0
        dataset[column + '_' + str(val)][dataset[column] == val] = 1

    if drop:
        dataset = dataset.drop(columns=[column])
    return(dataset)


#######################################################################################################################

# convert an array of values into a dataset matrix: used for LSTM data pre-processing
def create_dataset(dataset, look_back=1):
    """Function to convert a DataFrame to array format readable for keras LSTM.

    .. warning::
        This function is being migrated to STATINF and will soon be removed from PYCOF.
        Please check `STATINF <https://www.florianfelice.com/statinf/data/process.html#statinf.data.ProcessData.create_dataset?orgn=pycof>`_


    :Parameters:
        * **dataset** (:obj:`pandas.DataFrame`): DataFrame on which to aply the transformation.
        * **look_back** (:obj:`int`): Number of periods in the past to consider (defaults 1).

    :Example:
        >>> pc.create_dataset(df)

    :Returns:
        * :obj:`numpy.array`: Features X converted for keras LSTM.
        * :obj:`numpy.array`: Dependent variable Y converted for keras LSTM.
    """

    deprec_msg = """Deprecation!
    This function is being migrated to STATINF and will soon be removed from PYCOF.
    Please see https://www.florianfelice.com/statinf/data/process.html#statinf.data.ProcessData.create_dataset?orgn=py
    """
    warnings.warn(deprec_msg, Warning)

    dataX, dataY = [], []
    for i in range(len(dataset) - look_back - 1):
        a = dataset[i:(i + look_back), 0]
        dataX.append(a)
        dataY.append(dataset[i + look_back, 0])
    return np.array(dataX), np.array(dataY)


#######################################################################################################################

# Put thousand separator
def group(nb, digits=0):
    """Transforms a number into a string with a thousand separator.

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
    if math.isnan(nb):
        return('-')
    else:
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


#######################################################################################################################

# Transform 0 to '-'
def replace_zero(nb, digits=0):
    """For a given number, will transform 0 by '-' for display puspose.

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
        return(group(nb / 1000, digits))


#######################################################################################################################

# Get the week (sunday) date
def week_sunday(date=None, return_week_nb=False):
    """For a given date, will return the date from previous sunday or week number.

    :Parameters:
        * **date** (:obj:`datetime.date`): Date from which we extract the week number/sunday date (defaults to today).
        * **return_week_nb** (:obj:`bool`): If True will return week number with sunday basis (defaults False).

    :Example:
        >>> pycof.week_sunday(datetime.date(2020, 4, 15))
        ... datetime.date(2020, 4, 12)
        >>> pycof.week_sunday(datetime.date(2020, 4, 15), return_week_nb = True)
        ... 16

    :Returns:
        * :obj:`int`: Week number (from 1 to 52) if :obj:`return_week_nb` else date format.
    """
    date = datetime.date.today() if date is None else date

    # Get when was the last sunday
    idx = (date.weekday() + 1) % 7  # MON = 0, SUN = 6 -> SUN = 0 .. SAT = 6
    # Get the date
    last_sunday = date - datetime.timedelta(idx)
    if return_week_nb:
        # Return iso week number
        return(last_sunday.isocalendar()[1] + 1)
    else:
        # Return date
        return(last_sunday)


#######################################################################################################################

# Get use name (not only login)
def display_name(display='first'):
    """Displays current user name (either first/last or full name)

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
    except Exception:
        return(getpass.getuser())


#######################################################################################################################

# Convert a string to boolean
def str2bool(value):
    """Convert a string into boolean.

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
