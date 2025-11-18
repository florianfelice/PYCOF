import datetime
import getpass
import json
import logging
import os
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import BytesIO, StringIO

import boto3
import colorlog
import numpy as np
import pandas as pd
import sshtunnel
from botocore.exceptions import ProfileNotFound
from tqdm import tqdm

########################################################################################################################
# Get config file


def _pycof_folders(output=None, verbose=False):
    pycof_fold = os.environ.get("PYCOF_PATH", os.sep)
    # Define the root folder depending on the OS
    if sys.platform in ["win32", "win64", "cygwin", "msys"]:
        temp_path = os.environ["TEMP"] + os.sep
        home = os.path.expanduser("~")
        creds_fold = os.path.join(home, ".pycof") + os.sep
    else:
        temp_path = os.path.join(pycof_fold, "tmp") + os.sep
        creds_fold = os.path.join(pycof_fold, "etc", ".pycof") + os.sep
        home = os.path.expanduser("~")

    # Initialize the creation count variable
    _created = 0

    # Credentials folder
    if not os.path.exists(creds_fold):
        try:
            os.makedirs(creds_fold)
            _created += 1
        except PermissionError as err:
            raise PermissionError(
                f"""Could not create the PYCOF config folder, permission denied: {creds_fold}.
                    Please create folder with: sudo mkdir {creds_fold} or run your script with super-user: {err}"""
            )

    # Queries temp folder
    folds_q = os.path.join(temp_path, "pycof", "cache", "queries") + os.sep
    if not os.path.exists(folds_q):
        try:
            os.makedirs(folds_q)
            _created += 1
        except PermissionError as err:
            raise PermissionError(
                f"""Could not create the PYCOF temp folder, permission denied: {folds_q}.
                    Please create folder with: sudo mkdir {folds_q} or run your script with super-user: {err}"""
            )

    # Data temp folder
    folds_d = os.path.join(temp_path, "pycof", "cache", "data") + os.sep
    if not os.path.exists(folds_d):
        try:
            os.makedirs(folds_d)
            _created += 1
        except PermissionError as err:
            raise PermissionError(
                f"""Could not create the PYCOF temp data folder, permission denied: {folds_d}.
                    Please create folder with: sudo mkdir {folds_d} or run your script with super-user: {err}"""
            )

    # S3 temp folder
    folds_s3 = os.path.join(temp_path, "pycof", "cache", "s3") + os.sep
    if not os.path.exists(folds_s3):
        try:
            os.makedirs(folds_s3)
            _created += 1
        except PermissionError as err:
            raise PermissionError(
                f"""Could not create the PYCOF temp s3 folder, permission denied: {folds_s3}.
                    Please create folder with: sudo mkdir {folds_s3} or run your script with super-user: {err}"""
            )

    # Models temp folder
    folds_models = os.path.join(temp_path, "pycof", "cache", "models") + os.sep
    if not os.path.exists(folds_models):
        try:
            os.makedirs(folds_models)
            _created += 1
        except PermissionError as err:
            raise PermissionError(
                f"""Could not create the PYCOF temp models folder, permission denied: {folds_models}.
                    Please create folder with: sudo mkdir {folds_models} or run your script with super-user: {err}"""
            )

    # Return path if asked by user
    if output in ["tmp", "temp"]:
        return temp_path
    elif output == "creds":
        return creds_fold
    elif output == "queries":
        return folds_q
    elif output == "data":
        return folds_d
    elif output == "home":
        return home
    elif output == "s3":
        return folds_s3
    elif output == "models":
        return folds_models
    elif verbose:
        print(f"PYCOF folder created: {_created}")


# #######################################################################################################################
# Get config file


def _get_config(credentials={}):
    # ==========
    # Parse credentials argument
    if isinstance(credentials, str):
        if "/" in credentials:
            path = credentials
        else:
            creds = credentials if credentials.endswith(".json") else credentials + ".json"
            path = os.path.join(_pycof_folders(output="creds"), creds)
    elif isinstance(credentials, dict) and credentials == {}:
        path = os.path.join(_pycof_folders(output="creds"), "config.json")
    else:
        path = ""

    # ==========
    # Load credentials
    if path == "":
        config = credentials
    else:
        try:
            with open(path) as config_file:
                config = json.load(config_file)
        except Exception:
            raise ValueError(
                """Could not load config file.
                    Note that from version 1.2.0, config file location has changed. Make sure your file is in {}""".format(
                    _pycof_folders("creds")
                )
            )

    return config


########################################################################################################################
# Write to a txt file


def write(file, path, perm="a", verbose=False, end_row="\n", credentials={}, profile_name=None, **kwargs):
    """Write a line of text into a file (usually .txt) or saves data objects.
    As opposed to Pandas' built-in functions (:obj:`to_csv` or :obj:`to_parquet`), this function allows to pass AWS IAM credentials similar to
    :py:meth:`pycof.sql.remote_execute_sql`.

    :Parameters:
        * **file** (:obj:`str` or :obj:`pandas.DataFrame`): Line of text or object to be inserted in the file.
        * **path** (:obj:`str`): File on which to write (`/path/to/file.txt`). Can be any format, not necessarily txt.
        * **perm** (:obj:`str`): Permission to use when opening file (usually 'a' for appending text, or 'w' to (re)write file).
        * **verbose** (:obj:`bool`): Return the length of the inserted text if set to True (defaults False).
        * **end_row** (:obj:`str`): Character to end the row (defaults '\\n').
        * **credentials** (:obj:`dict`): Credentials to use to connect to AWS S3. You can also provide the credentials path or the json file name from '/etc/' (defaults {}).
        * **profile_name** (:obj:`str`): Profile name of the AWS profile configured with the command `aws configure` (defaults None).
        * **\\*\\*kwargs** (:obj:`str`): Arguments to be passed to pandas function (either :obj:`to_csv` or :obj:`to_parquet`).

    :Example:
        >>> pycof.write('This is a test', path='~/pycof_test_write.txt', perm='w')
        >>> pycof.write(df, path='s3://bucket/path/to/file.parquet', credentials='config.json')

    :Returns:
        * :obj:`int`: Number of characters inserted if verbose is True.
    """
    # Check if path provided is S3
    useIAM = path.startswith("s3://")

    if useIAM:
        # If S3, try AWS cli profile or credentials
        try:
            sess = boto3.session.Session(profile_name=profile_name)
            s3 = sess.client("s3")
            s3_resource = sess.resource("s3")
        except ProfileNotFound:
            config = _get_config(credentials)
            s3 = boto3.client(
                "s3",
                aws_access_key_id=config.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=config.get("AWS_SECRET_ACCESS_KEY"),
                region_name=config.get("REGION"),
            )
            s3_resource = boto3.resource(
                "s3",
                aws_access_key_id=config.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=config.get("AWS_SECRET_ACCESS_KEY"),
                region_name=config.get("REGION"),
            )
        except FileNotFoundError:
            raise ConnectionError(
                "Please run 'aws config' on your terminal and initialize the parameters or profide a correct value for crendetials."
            )

    # Pandas DataFrame
    if isinstance(file, pd.DataFrame):
        # CSV or TXT
        if path.endswith(".csv") or path.endswith(".txt"):
            out_buffer = StringIO() if useIAM else path
            file.to_csv(out_buffer, **kwargs)
        # Parquet
        elif path.endswith(".parquet"):
            out_buffer = BytesIO() if useIAM else path
            file.to_parquet(out_buffer, **kwargs)
        # Json
        elif path.endswith(".json"):
            out_buffer = StringIO() if useIAM else path
            file.to_json(out_buffer, **kwargs)

        # If S3, push to bucket
        if useIAM:
            bucket = path.replace("s3://", "").split("/")[0]
            folder_path = "/".join(path.replace("s3://", "").split("/")[1:])
            s3_resource.Object(bucket, folder_path).put(Body=out_buffer.getvalue())
    # Other input file format
    else:
        if useIAM:
            splitted_path = path.replace("s3://", "").split("/")
            bucket = splitted_path[0]
            folder_path = "/".join(splitted_path[1:])
            file_name = splitted_path[-1]
            data_path = os.path.join(_pycof_folders("temp"), "pycof", "cache", "data") + os.sep
            path = data_path + file_name

        with open(path, perm) as f:
            if path.endswith(".json") or path.endswith(".jsonc"):
                json.dump(file, f)
            else:
                f.write(file + end_row)

        if useIAM:
            s3.upload_file(path, bucket, folder_path)

    if verbose:
        return len(file)


########################################################################################################################
# Compute the age of a given file


def file_age(file_path, format="seconds"):
    """Computes the age of a file.

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
    ttl_sec = (datetime.datetime.now() - datetime.datetime.fromtimestamp(os.stat(file_path).st_mtime)).total_seconds()
    if format.lower() in ["s", "sec", "second", "seconds"]:
        return ttl_sec
    elif format.lower() in ["m", "min", "mins", "minute", "minutes"]:
        return ttl_sec / 60
    elif format.lower() in ["h", "hr", "hrs", "hour", "hours"]:
        return ttl_sec / 3600
    elif format.lower() in ["d", "day", "days"]:
        return ttl_sec / (24 * 60 * 60)
    elif format.lower() in ["w", "wk", "wks", "week", "weeks"]:
        return ttl_sec / (7 * 24 * 60 * 60)
    else:
        raise ValueError(
            f"Format value is not correct. Can be 'seconds', 'minutes', 'hours', 'days' or 'weeks'. Got '{format}'."
        )


########################################################################################################################
# Display tqdm only if argument for verbosity is 1 (works for lists, range and str)


def verbose_display(element, verbose=True, sep=" ", end="\n", return_list=False):
    """Extended print function with tqdm display for loops.
    Also has argument verbose for automated scripts with overall verbisity argument.

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
    if (verbose in [1, True]) & (type(element) in [list, range]) & (return_list is False):
        return tqdm(element)
    elif (verbose in [1, True]) & (type(element) in [list]) & (return_list is True):
        return print(*element, sep=sep, end=end)
    elif (verbose in [1, True]) & (type(element) in [str]) & (return_list is False):
        return print(element, sep=sep, end=end)
    # elif (verbose in [0, False]) & (type(element) in [str, type(None)]):
    #     disp = 0  # we don't display anything
    else:
        return element


# #######################################################################################################################
# Fake SSH tunnel for direct connections


class _fake_tunnel:
    def __init__():
        pass

    def close():
        pass


########################################################################################################################
# SSHTunnel for email
class EmailSSHTunnel:
    def __init__(self, config, connection="direct", engine="default"):
        self.connection = connection.lower()
        self.config = config
        self.engine = engine

    def __enter__(self):
        if self.connection == "ssh":
            try:
                ssh_port = 22 if self.config.get("SSH_PORT") is None else int(self.config.get("SSH_PORT"))
                remote_addr = (
                    "localhost"
                    if self.config.get("EMAIL_REMOTE_HOST") is None
                    else self.config.get("EMAIL_REMOTE_HOST")
                )
                remote_port = (
                    1025 if self.config.get("EMAIL_REMOTE_PORT") is None else int(self.config.get("EMAIL_REMOTE_PORT"))
                )
                # hostname = (
                #     "127.0.0.1" if self.config.get("EMAIL_LOCAL_HOST") is None else self.config.get("EMAIL_LOCAL_HOST")
                # )

                if (self.config.get("SSH_PASSWORD") is None) & (self.config.get("SSH_KEY") is None):
                    # Try to get the default SSH location if neither a password nor a path is provided
                    ssh_path = os.path.join(_pycof_folders("home"), ".ssh", "id_rsa")
                else:
                    ssh_path = self.config.get("SSH_KEY")

                self.tunnel = sshtunnel.SSHTunnelForwarder(
                    (self.config.get("EMAIL_SMTP"), ssh_port),
                    ssh_username=self.config.get("SSH_USER"),
                    ssh_password=self.config.get("SSH_PASSWORD"),
                    ssh_pkey=ssh_path,
                    remote_bind_address=(remote_addr, remote_port),
                )
                self.tunnel.daemon_forward_servers = True
                self.tunnel.connector = self._define_connector
            except AttributeError as e:
                if "DSSKey" in str(e):
                    raise ConnectionError(
                        "SSH connection failed due to incompatible paramiko version. "
                        "Please ensure paramiko<3.0 is installed for DSS key support."
                    )
                else:
                    raise ConnectionError(f"SSH configuration error: {str(e)}")
            except Exception as e:
                raise ConnectionError(f"Failed to establish SSH connection with host: {str(e)}")
        else:
            self.tunnel = _fake_tunnel
            self.tunnel.connector = self._define_connector

        return self.tunnel

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tunnel.close()

    def _define_connector(self):
        """Define the SQL connector for executing SQL.

        :param config: Credentials file containing authentiation information, defaults to {}.
        :type config: :obj:`dict`, optional
        :param engine: SQL engine to use ('Redshift', 'SQLite' or 'MySQL'), defaults to 'default'
        :type engine: str, optional
        :param connection: Connextion type. Cqn either be 'direct' or 'SSH', defaults to 'direct'
        :type connection: str, optional

        :return: Connector, cursor and tunnel
        """
        user = self.config.get("EMAIL_USER")
        password = self.config.get("EMAIL_PASSWORD")
        hostname = self.config.get("EMAIL_SMTP")
        port = self.config.get("EMAIL_PORT")

        if self.connection.lower() == "ssh":
            hostname = (
                "127.0.0.1" if self.config.get("EMAIL_REMOTE_HOST") is None else self.config.get("EMAIL_REMOTE_HOST")
            )
            self.tunnel.start()
            port = self.tunnel.local_bind_port

        try:
            connector = smtplib.SMTP(hostname, port)
            connector.login(user, password)
        except Exception:
            raise ConnectionError("Failed to connect to the email server")

        return connector


########################################################################################################################
# Logging setup
def setup_logging(file=__name__, logging_level=logging.INFO):
    """
    Set up logging configuration.
    """
    # For external libraries using this function, use a consistent logger name
    # to avoid hierarchy issues when multiple modules call this function
    if not file.startswith("pycof"):
        # Extract the top-level package name for external libraries
        package_name = file.split(".")[0]
        logger_name = package_name
    else:
        logger_name = file

    # Create a logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging_level)

    # Check if logger already has handlers to avoid duplicates
    if logger.handlers:
        # Make sure propagation is disabled even for existing loggers
        logger.propagate = False

        # CRITICAL: Ensure root logger is completely disabled on every call
        root_logger = logging.getLogger()
        root_logger.disabled = True

        return logger

    # CRITICAL FIX: Completely disable the root logger
    root_logger = logging.getLogger()
    root_logger.disabled = True

    # Clear any existing handlers from this logger and disable propagation
    logger.handlers.clear()
    logger.propagate = False

    # Also clear handlers from any parent loggers in the same package to prevent interference
    parent_name = ".".join(logger_name.split(".")[:-1]) if "." in logger_name else None
    if parent_name:
        parent_logger = logging.getLogger(parent_name)
        parent_logger.handlers.clear()
        parent_logger.propagate = False

    # Create handlers
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging_level)

    # Create formatters and add them to the handlers
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s [%(levelname)s] %(name)s %(reset)s %(message_log_color)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        reset=True,
        log_colors={
            "DEBUG": "light_black",
            "INFO": "blue",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
        secondary_log_colors={
            "message": {
                "DEBUG": "light_black",
                "INFO": "white",
                "WARNING": "light_yellow",
                "ERROR": "red",
                "CRITICAL": "red",
            }
        },
    )
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(console_handler)

    return logger
