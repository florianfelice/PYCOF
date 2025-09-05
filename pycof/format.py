import base64
import datetime
import email
import getpass
import imaplib
import math
import os
import pickle
import re
import smtplib
import sys
import time
import traceback
import warnings
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import dateparser
import numpy as np
import pandas as pd
import pytz
from bs4 import BeautifulSoup
from dateparser import parse
from dateutil import tz
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm

from .misc import EmailSSHTunnel, _pycof_folders, file_age, verbose_display
from .sqlhelper import _get_config, _get_credentials

# Define default Google API scopes
default_scopes = ["https://mail.google.com/", "https://www.googleapis.com/auth/calendar.readonly"]


#######################################################################################################################


# Send an Email
def send_email(to, subject, body, cc="", credentials={}, connection="auto"):
    """Simplified function to send emails.
    Will look at the credentials at :obj:`/etc/.pycof/config.json`. User can also pass a dictionnary for credentials.

    :Parameters:
        * **to** (:obj:`str`): Recipient of the email.
        * **subject** (:obj:`str`): Subject of the email.
        * **body** (:obj:`str`): Content of the email to be send.
        * **cc** (:obj:`str`): Email address to be copied (defaults None).
        * **credentials** (:obj:`dict`): Credentials to use to connect to the database. You can also provide the credentials path or the json file name from :obj:`/etc/.pycof/` (defaults {}).

    :Configuration: The function requires the below arguments in the configuration file.

        * :obj:`EMAIL_USER`: Email address from which we want to send the email.
        * :obj:`EMAIL_SENDER`: Name to display for the sender.
        * :obj:`EMAIL_PASSWORD`: Password for authentication.
        * :obj:`EMAIL_SMTP`: SMTP host for connection. Default is smtp.gmail.com for Google.
        * :obj:`EMAIL_PORT`: Port for authentication.

        .. code-block:: python

            {
            "EMAIL_USER": "",
            "EMAIL_SENDER": "",
            "EMAIL_PASSWORD": "",
            "EMAIL_SMTP": "smtp.gmail.com",
            "EMAIL_PORT": "587"
            }


    :Example:
        >>> content = "This is a test"
        >>> pycof.send_email(to="test@domain.com", body=content, subject="Hello world!")
    """
    config = _get_config(credentials)

    if connection.lower() == "auto":
        pattern = re.compile("^([0-9]+.[0-9]+.[0-9]+.[0-9]+)+$")
        connection = "direct" if pattern.match(config.get("EMAIL_SMTP")) else "ssh"
    msg = MIMEMultipart()
    msg["From"] = config.get("EMAIL_USER")
    msg["To"] = to
    # msg['Cc'] = '' if cc == '' else cc
    msg["Subject"] = subject

    mail_type = "html" if "</" in body else "plain"
    msg.attach(MIMEText(body, mail_type))

    text = msg.as_string()

    with EmailSSHTunnel(config=config, connection=connection) as tunnel:
        conn = tunnel.connector()
        conn.sendmail(config.get("EMAIL_USER"), [to, "", cc], text)
        conn.quit()


# Send an email from Gmail
class google_email:
    def __init__(self, credentials={}, scopes=default_scopes, token_path=None):
        """Simplified class to send email from a Gmail email address with secured connection with developper API.
        The `Google credentials file <https://developers.google.com/calendar/quickstart/python>`_ needs to be saved as :obj:`/etc/.pycof/google.json`.

        :param credentials: Credentials to use which includes the from email address to display. See Setup, defaults to {}.
        :type credentials: :obj:`dict`, optional
        :param scopes: Targeted permissions required. Check https://developers.google.com/calendar/auth for more details, defaults to ['https://mail.google.com/', 'https://www.googleapis.com/auth/calendar.readonly'].
        :type scopes: :obj:`list`, optional
        :param token_path: Path to the saved `token.pickle` authentication file, defaults to None and saves in the PYCOF temporary data folder.
        :type token_path: :obj:`str`, optional

        :Configuration: The function requires a configuration file stored at :obj:`/etc/.pycof/google.json`.
            This file can be generated at https://developers.google.com/calendar/quickstart/python.
            User will need to enable the Google Calendar API on the account from Step 1.

        :Example:
            >>> _body = '<html><body><h1>This email is a test</h1><p>Hello world!</p></body></html>'
            >>> pc.google_email().send(to='email@example.com', subject='Test', body=_body, cc='friend@example.com')
            >>> # Alternative where we specify token_path
            >>> pc.google_email(token_path='/home/ubuntu/token.pickle').send(to='email@example.com', subject='Test', body=_body, cc='friend@example.com')
        """
        self.scopes = scopes
        self.token_path = os.path.join(_pycof_folders("data"), "token.pickle") if token_path is None else token_path
        self._creds = credentials

    def _get_creds(self):
        """Retreive Google credentials.

        :return: Google calendar credentials.
        :rtype: :obj:`google_auth_oauthlib`
        """
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        creds_path = os.path.join(_pycof_folders("creds"), "google.json")

        if os.path.exists(self.token_path):
            with open(self.token_path, "rb") as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(creds_path, self.scopes)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_path, "wb") as token:
                pickle.dump(creds, token)
        return creds

    def send(self, to, subject, body, cc="", return_status=False):
        """Send the email.

        :param to: Recipient of the email.
        :type to: :obj:`str`
        :param subject: Subject of the email.
        :type subject: :obj:`str`
        :param body: Content of the email to be send.
        :type body: :obj:`str`
        :param cc: Email address to be copied, defaults to ''.
        :type cc: :obj:`str`, optional
        :param return_status: Returns status of the email sent, defaults to False.
        :type return_status: :obj:`bool`, optional

        :return: If `return_status=True`, returns a dictionnary with the status of the email.
        :rtype: :obj:`dict`
        """
        config = _get_config(self._creds)
        try:
            service = build("gmail", "v1", credentials=self._get_creds())
            msg = MIMEMultipart()
            msg["from"] = config.get("EMAIL_SENDER")
            msg["to"] = to
            msg["cc"] = "" if cc == "" else cc
            msg["subject"] = subject

            mail_type = "html" if "</" in body else "plain"
            msg.attach(MIMEText(body, mail_type))

            create_message = {"raw": base64.urlsafe_b64encode(bytes(msg.as_string(), "utf-8")).decode("utf-8")}
            # pylint: disable=E1101
            send_message = service.users().messages().send(userId="me", body=create_message).execute()
        except HttpError as error:
            print(f"An error occurred: {error}")
            send_message = None
        if return_status:
            return send_message

    def getEmails(self, nb_email=1, include_spam=False, _to="me", *args):
        """Get latest emails from your Gmail address.

        :param nb_email: Number of emails to retreive, defaults to 1.
        :type nb_email: :obj:`int`, optional
        :param _to: User ID to use to retreive emails, defaults to 'me'.
        :type _to: :obj:`str`, optional

        :example:
            >>> pycof.google_emails().GetEmails(2)
            ... +----------------------------+-----------------+----------------+---------------+
            ... |                       Date |            From |        Subject |            To |
            ... +----------------------------+-----------------+----------------+---------------+
            ... |  2021-01-01 04:00:03+01:00 | test@domain.com |          Testo |  me@gmail.com |
            ... |  2021-01-01 03:14:09+01:00 | test@domain.com |   Another test |  me@gmail.com |
            ... +----------------------------+-----------------+----------------+---------------+

        :return: Data frame with last emails.
        :rtype: :obj:`pandas.DataFrame`
        """
        creds = self._get_creds()

        # Connect to the Gmail API
        service = build("gmail", "v1", credentials=creds)

        # request a list of all the messages
        result = (
            service.users()
            .messages()
            .list(userId=_to, maxResults=nb_email, includeSpamTrash=include_spam, *args)
            .execute()
        )

        # We can also pass maxResults to get any number of emails. Like this:
        # result = service.users().messages().list(maxResults=200, userId='me').execute()
        messages = result.get("messages")

        # messages is a list of dictionaries where each dictionary contains a message id.

        df = []

        # iterate through all the messages
        for msg in messages[:nb_email]:
            # Get the message from its id
            txt = service.users().messages().get(userId=_to, id=msg["id"]).execute()

            # Use try-except to avoid any Errors
            try:
                # Get value of 'payload' from dictionary 'txt'
                payload = txt["payload"]
                headers = payload["headers"]

                _dest = _to
                _subj = ""
                _from = ""

                # Look for Subject and Sender Email in the headers
                for d in headers:
                    if d["name"].lower() == "subject":
                        _subj = d["value"]
                    if d["name"] == "From":
                        _from = d["value"]
                    if d["name"].lower() == "to":
                        _dest = d["value"]
                    if d["name"] == "Date":
                        ddt = dateparser.parse(d["value"].strip())
                        try:
                            _date = ddt.replace(tzinfo=datetime.timezone.utc).astimezone(tz=tz.tzlocal())
                        except Exception:
                            _date = np.nan

                # Get email attachments
                i = 1
                attch = {}
                # _body = ""
                for part in payload.get("parts"):
                    try:
                        if part["filename"]:
                            att_id = part["body"]["attachmentId"]
                            att = (
                                service.users()
                                .messages()
                                .attachments()
                                .get(userId=_to, messageId=msg["id"], id=att_id)
                                .execute()
                            )
                            data = att["data"]
                            file_data = base64.urlsafe_b64decode(data.encode("UTF-8"))
                            filePath = os.path.join(_pycof_folders("data"), part["filename"])
                            attch.update({f"Attachment {i}": filePath})
                            # Download the attachment
                            with open(filePath, "wb") as f:
                                f.write(file_data)
                            i += 1
                        # else:
                        # data = part["body"]["data"]
                        # data = data.replace("-", "+").replace("_", "/")
                        # decoded_data = base64.b64decode(data)

                        # Now, the data obtained is in lxml. So, we will parse
                        # it with BeautifulSoup library
                        # soup = BeautifulSoup(decoded_data, "lxml")
                        # _body = soup.body()
                    except Exception:
                        pass
                for_df = {"From": _from, "To": _dest, "Date": _date, "Subject": _subj}  # , 'Body': _body
                for_df.update(attch)
                df += [pd.DataFrame(for_df, index=[0])]
            except Exception:
                pass
        return pd.concat(df).sort_values(by="Date", ascending=False).reset_index(drop=True)


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
        return "0" + str(nb)
    else:
        return str(nb)


#######################################################################################################################


# Put thousand separator
def group(nb, digits=0, unit=""):
    """Transforms a number into a string with a thousand separator.

    :Parameters:
        * **nb** (:obj:`float`): Number to be transformed.
        * **digits** (:obj:`int`): Number of digits to round.
        * **unit** (:obj:`str`): Unit to be displayed (defaults to '').

    :Example:
        >>> pycof.group(12345)
        ... '12,345'
        >>> pycof.group(12345.54321, digits=3)
        ... '12,345.543'
        >>> pycof.group(12.54, digits=3, unit='%')
        ... '12.54%'

    :Returns:
        * :obj:`str`: Transformed number.
    """
    if math.isnan(nb):
        return "-"
    elif nb == 0.0:
        return "-"
    else:
        s = "%d" % round(nb, digits)
        groups = []
        if digits > 0:
            dig = "." + str(nb).split(".")[1][:digits]
        else:
            dig = ""
        while s and s[-1].isdigit():
            groups.append(s[-3:])
            s = s[:-3]
        return s + ",".join(reversed(groups)) + dig + unit


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
    if str(nb) == "0":
        return "-"
    else:
        return group(nb / 1000, digits)


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
        return last_sunday.isocalendar()[1] + 1
    else:
        # Return date
        return last_sunday


#######################################################################################################################


# Get use name (not only login)
def display_name(display="first"):
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
        if sys.platform in ["win32"]:
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
            if display == "first":
                return user.split(", ")[1]
            elif display == "last":
                return user.split(", ")[0]
            else:
                return user
        else:
            import pwd

            user = pwd.getpwuid(os.getuid())[4]
            if display == "first":
                return user.split(", ")[1]
            elif display == "last":
                return user.split(", ")[0]
            else:
                return user
    except Exception:
        return getpass.getuser()


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


#######################################################################################################################


# Getting Google Calendar events
class GoogleCalendar:
    def __init__(self, timezone="Europe/Paris", scopes=default_scopes, token_path=None):
        """Get all available events on a Google Calendar.
        The `Google credentials file <https://developers.google.com/calendar/quickstart/python>`_ needs to be saved as :obj:`/etc/.pycof/google.json`.

        :param timezone: Time zone to transform dates, defaults to 'Europe/Paris'.
        :type timezone: :obj:`str`, optional
        :param scopes: Targeted permissions required. Check https://developers.google.com/calendar/auth for more details, defaults to ['https://www.googleapis.com/auth/calendar.readonly'].
        :type scopes: :obj:`list`, optional
        :param token_path: Path to the saved `token.pickle` authentication file, defaults to None and saves in the PYCOF temporary data folder.
        :type token_path: :obj:`str`, optional

        :Configuration: The function requires a configuration file stored at :obj:`/etc/.pycof/google.json`.
            This file can be generated at https://developers.google.com/calendar/quickstart/python.
            User will need to enable the Google Calendar API on the account from Step 1.
        """
        self.timezone = pytz.timezone(timezone)
        self.scopes = scopes
        self.token_path = os.path.join(_pycof_folders("data"), "token.pickle") if token_path is None else token_path

    def _get_creds(self):
        """Retreive Google credentials.

        :return: Google calendar credentials.
        :rtype: :obj:`google_auth_oauthlib`
        """
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        creds_path = os.path.join(_pycof_folders("creds"), "google.json")

        if os.path.exists(self.token_path):
            with open(self.token_path, "rb") as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(creds_path, self.scopes)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_path, "wb") as token:
                pickle.dump(creds, token)
        return creds

    def _events_to_df(self, events):
        """Transform events list into pandas DataFrame for easy manipulation and filtering

        :param events: List containing all events.
        :type events: :obj:`list`
        :return: Data Frame with all retreived events.
        :rtype: :obj:`pandas.DataFrame`
        """
        events_df = pd.DataFrame()

        if not events:
            return events_df
        else:
            events_df["StartDate"] = [
                parse(event["start"].get("dateTime", event["start"].get("date"))) for event in events
            ]
            events_df["EndDate"] = [parse(event["end"].get("dateTime", event["end"].get("date"))) for event in events]
            events_df["EventName"] = [event["summary"] for event in events]
            events_df["EventOrganizer"] = [event["creator"]["email"] for event in events]
            events_df["EventCreationDate"] = [parse(event["created"]).astimezone(self.timezone) for event in events]

            return events_df

    def today_events(self, calendar="primary", singleEvents=True, orderBy="startTime", *args):
        """Retreive all events for current date. See https://developers.google.com/calendar/v3/reference/events/list for details for arguments.

        :param calendar: ID of the targeted calendar. Use the function :py:meth:`get_calendars` to find more calendars, defaults to 'primary'.
        :type calendar: :obj:`str`, optional
        :param singleEvents: Whether to expand recurring events into instances and only return single one-off events and instances of recurring events, but not the underlying recurring events themselves, defaults to True.
        :type singleEvents: :obj:`bool`, optional
        :param orderBy: The order of the events returned in the result, defaults to 'startTime'.
        :type orderBy: :obj:`str`, optional

        :return: Data Frame with all events for today.
        :rtype: :obj:`pandas.DataFrame`
        """
        # Call the Calendar API
        service = build("calendar", "v3", credentials=self._get_creds())

        # print(service.calendarList().list().execute())

        # Set start and end date
        now = datetime.datetime.now().astimezone(self.timezone)
        endtime = now.replace(hour=23)

        events_result = (
            service.events()
            .list(
                calendarId=calendar,
                timeMin=now.isoformat(),
                timeMax=endtime.isoformat(),
                singleEvents=singleEvents,
                orderBy=orderBy,
                *args,
            )
            .execute()
        )
        return self._events_to_df(events_result.get("items", []))

    def next_events(
        self, calendar="primary", maxResults=None, endTime=None, singleEvents=True, orderBy="startTime", *args
    ):
        """Retreive next events. See https://developers.google.com/calendar/v3/reference/events/list for details for arguments.

        :param calendar: ID of the targeted calendar. Use the function :py:meth:`get_calendars` to find more calendars, defaults to 'primary'.
        :type calendar: :obj:`str`, optional
        :param maxResults: Number of future events to retreive, defaults to None.
        :type maxResults: :obj:`int`, optional
        :param endTime: Maximum date for the future events, defaults to None
        :type endTime: :obj:`datetime.datetime`, optional
        :param singleEvents: Whether to expand recurring events into instances and only return single one-off events and instances of recurring events, but not the underlying recurring events themselves, defaults to True.
        :type singleEvents: :obj:`bool`, optional
        :param orderBy: The order of the events returned in the result, defaults to 'startTime'.
        :type orderBy: :obj:`str`, optional

        :return: Data Frame with future events.
        :rtype: :obj:`pandas.DataFrame`
        """
        # Call the Calendar API
        service = build("calendar", "v3", credentials=self._get_creds())

        # Set start and end date
        now = datetime.datetime.now().astimezone(self.timezone)

        endtime = parse(endTime).astimezone(self.timezone).isoformat() if endTime else None

        events_result = (
            service.events()
            .list(
                calendarId=calendar,
                timeMin=now.isoformat(),
                timeMax=endtime,
                maxResults=maxResults,
                singleEvents=singleEvents,
                orderBy=orderBy,
                *args,
            )
            .execute()
        )
        return self._events_to_df(events_result.get("items", []))

    def get_calendars(self):
        """Get list of all available calendars.

        :return: List of all available calendars.
        :rtype: :obj:`list`
        """
        service = build("calendar", "v3", credentials=self._get_creds())

        return service.calendarList().list().execute()


def GetEmails(nb_email=1, email_address="", port=993, credentials={}):
    """Get latest emails from your address.

    :param nb_email: Number of emails to retreive, defaults to 1.
    :type nb_email: :obj:`int`, optional
    :param email_address: Email address to use, defaults to '' and uses :obj:`EMAIL_USER` from config file.
    :type email_address: :obj:`str`, optional
    :param port: Port for IMAP, defaults to 993 for Gmail.
    :type port: :obj:`int`, optional
    :param credentials: Credentials to use. See Setup, defaults to {}.
    :type credentials: :obj:`dict`, optional

    :Configuration: The function requires the below arguments in the configuration file.

        * :obj:`EMAIL_USER`: Email address from which we want to retreive emails. Similar argument as :py:meth:`pycof.format.send_email`.
        * :obj:`EMAIL_PASSWORD`: Password for authentication. Similar argument as :py:meth:`pycof.format.send_email`.
        * :obj:`EMAIL_IMAP`: IMAP host for connection. Default is imap.gmail.com for Google.

        .. code-block:: python

            {
            "EMAIL_USER": "",
            "EMAIL_PASSWORD": "",
            "EMAIL_IMAP": "imap.gmail.com"
            }

    :example:
        >>> pycof.GetEmails(2)
        ... +----------------------------+-----------------+----------------+----------------+
        ... |                       Date |            From |        Subject |             To |
        ... +----------------------------+-----------------+----------------+----------------+
        ... |  2021-01-01 04:00:03+01:00 | test@domain.com |          Testo |  me@domain.com |
        ... |  2021-01-01 03:14:09+01:00 | test@domain.com |   Another test |  me@domain.com |
        ... +----------------------------+-----------------+----------------+----------------+

    :return: Data frame with last emails.
    :rtype: :obj:`pandas.DataFrame`
    """
    # Getting configs
    config = _get_config(credentials)

    FROM_EMAIL = email_address if email_address else config.get("EMAIL_USER")
    FROM_PWD = config.get("EMAIL_PASSWORD")
    smtp_conf = config.get("EMAIL_SMTP")
    SMTP_SERVER = config.get("EMAIL_IMAP") if config.get("EMAIL_IMAP") else smtp_conf.replace("smtp", "imap")
    # SMTP_PORT = port

    try:
        mail = imaplib.IMAP4_SSL(SMTP_SERVER)
        mail.login(FROM_EMAIL, FROM_PWD)
        mail.select("inbox")

        type, data = mail.search(None, "ALL")
        mail_ids = data[0]
        id_list = mail_ids.split()
        # first_email_id = int(id_list[0])
        latest_email_id = int(id_list[-1])

        df = []
        for num in range(latest_email_id, latest_email_id - nb_email, -1):
            typ, data = mail.fetch(str(num), "(RFC822)")
            raw_email = data[0][1]  # converts byte literal to string removing b''
            try:
                raw_email_string = raw_email.decode("utf-8")
            except Exception:
                raw_email_string = ""
            email_message = email.message_from_string(raw_email_string)  # downloading attachments
            # Get email content
            try:
                msg = email.message_from_string(str(raw_email, "utf-8"))
                _subj = msg["subject"]
                _from = msg["From"]
                _to = msg["To"]
                ddt = dateparser.parse(msg["Date"].strip())
            except Exception:
                msg = ""
                ddt = np.nan

            if ddt is None:
                ddt = dateparser.parse(msg["Date"].replace("00 (PST)", " PST").split(",")[1])

            try:
                _date = ddt.replace(tzinfo=datetime.timezone.utc).astimezone(tz=tz.tzlocal())
            except Exception:
                _date = np.nan

            for_df = {"From": _from, "Subject": _subj, "To": _to, "Date": _date}

            # Get email attachments
            i = 1
            for part in email_message.walk():
                fileName = part.get_filename()
                if bool(fileName):
                    filePath = os.path.join(_pycof_folders("data"), fileName)
                    for_df.update({f"Attachment {i}": filePath})
                    i += 1
                    if not os.path.isfile(filePath):
                        fp = open(filePath, "wb")
                        fp.write(part.get_payload(decode=True))
                        fp.close()

            df += [pd.DataFrame(for_df, index=[0])]
        return pd.concat(df).reset_index(drop=True)
    except Exception as e:
        traceback.print_exc()
        print(str(e))
