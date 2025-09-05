import csv
import datetime
import getpass
import hashlib
import os
import re
import sqlite3
import sys
import warnings
from types import SimpleNamespace

import boto3
import botocore
import numpy as np
import pandas as pd
import paramiko
import psycopg2
import sqlalchemy as sa
import sshtunnel
from fabric import Connection
from tqdm import tqdm

from .data import read
from .misc import (
    _fake_tunnel,
    _get_config,
    _pycof_folders,
    file_age,
    verbose_display,
    write,
)

# #######################################################################################################################
# Cache data from SQL


def _cache(sql, tunnel, query_type="SELECT", cache_time="24h", cache_file_name=None, cache_folder=None, verbose=False):
    # Parse cache_time value
    if type(cache_time) in [float, int]:
        c_time = cache_time
    else:
        # Force the input to be a string
        str_c_time = str(cache_time).lower().replace(" ", "")
        # Get the numerical part of the input
        c_time = float("".join(re.findall("[^a-z]", str_c_time)))
        # Get the str part of the input - for the format
        age_fmt = "".join(re.findall("[a-z]", str_c_time))

    # Hash the file's name to save the query and the data
    file_name = (
        cache_file_name if cache_file_name else hashlib.sha224(bytes(sql, "utf-8")).hexdigest().replace("-", "m")
    )
    file_name += "" if ".parquet" in file_name else ".parquet"

    # Set the query and data paths
    query_path = _pycof_folders("queries")
    data_path = cache_folder if cache_folder else _pycof_folders("data")

    # Chec if the cached data already exists
    if (query_type.upper() == "SELECT") & (file_name in os.listdir(data_path)):
        # If file exists, checks its age
        age = file_age(os.path.join(data_path, file_name), format=age_fmt)
        size = os.path.getsize(os.path.join(data_path, file_name))
        if (query_type.upper() == "SELECT") & (age < c_time) & (size > 4):
            # If file is younger than c_time, we read the cached data
            verbose_display("Reading cached data", verbose)
            sql_out = read(os.path.join(data_path, file_name))
        else:
            # Else we execute the SQL query and save the ouput + the query
            verbose_display("Execute SQL query and cache the data - updating cache", verbose)
            conn = tunnel.connector()
            warnings.filterwarnings(
                "ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy connectable.*"
            )
            sql_out = pd.read_sql(sql, conn)
            conn.close()
            write(sql, query_path + file_name, perm="w", verbose=verbose)
            write(sql_out, os.path.join(data_path, file_name), index=False)
    else:
        # If the file does not even exist, we execute SQL, save the query and its output
        verbose_display("Execute SQL query and cache the data", verbose)
        conn = tunnel.connector()
        warnings.filterwarnings(
            "ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy connectable.*"
        )
        sql_out = pd.read_sql(sql, conn)
        conn.close()
        write(sql, os.path.join(query_path, file_name), perm="w", verbose=verbose)
        write(sql_out, os.path.join(data_path, file_name), index=False)

    def age(fmt="seconds"):
        return file_age(file_path=os.path.join(data_path, file_name), format=fmt)

    sql_out.meta = SimpleNamespace()
    sql_out.meta.cache = SimpleNamespace()
    sql_out.meta.cache.creation_date = datetime.datetime.now() - datetime.timedelta(seconds=age())
    sql_out.meta.cache.cache_path = os.path.join(data_path, file_name)
    sql_out.meta.cache.query_path = os.path.join(query_path, file_name)
    sql_out.meta.cache._age_format = age_fmt
    sql_out.meta.cache._age_value = c_time
    sql_out.meta.cache._cache_time = cache_time
    sql_out.meta.cache.age = age

    return sql_out


# #######################################################################################################################
# Get DB credentials


def _get_credentials(config, profile_name=None, connection="direct"):

    useIAM = connection.lower() == "iam"

    # Access DB credentials
    # try:
    #     hostname = config.get("DB_HOST")  # Read the host name value from the config dictionnary
    # except Exception:
    #     raise ValueError("Could not get the hostname")

    # port = config.get("DB_PORT")  # Get the port from the config file and convert it to int
    user = config.get("DB_USER")  # Get the user name for connecting to the DB
    # password = config.get("DB_PASSWORD")  # Get the DB

    # AWS and Redshift specific parameters
    database = config.get("DB_DATABASE")  # For Redshift, use the database, for MySQL set it by default to ""
    access_key = config.get("AWS_ACCESS_KEY_ID")
    secret_key = config.get("AWS_SECRET_ACCESS_KEY")
    region = config.get("REGION")
    cluster_name = config.get("CLUSTER_NAME")

    boto_error = """Cannot initialize the boto3 session. Please check your config file and ensure awscli is installed.\n
    To install awcli, please run: \n
    pip install awscli -y && aws configure\n
    Values from `aws configure` command can remain empty.
    """
    # Get AWS credentials with access and secret key
    if (useIAM) & (secret_key in [None, "None", ""]):
        try:
            session = boto3.Session(profile_name=profile_name)
        except Exception:
            raise ConnectionError(boto_error)
    elif useIAM:
        try:
            session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
        except Exception:
            session = boto3.Session(
                profile_name=profile_name,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region,
            )

    if useIAM:
        rd_client = session.client("redshift")
        cluster_creds = rd_client.get_cluster_credentials(
            DbUser=user, DbName=database, ClusterIdentifier=cluster_name, AutoCreate=False
        )
        # Update user and password
        config["DB_USER"] = cluster_creds["DbUser"]
        config["DB_PASSWORD"] = cluster_creds["DbPassword"]

    return config


# #######################################################################################################################
# Get SSH tunnel


class SSHTunnel:
    def __init__(self, config, connection="direct", engine="default"):
        self.connection = connection.lower()
        self.config = config
        self.engine = engine
        self.ssh_client = None
        self.local_port = None
        self.forward_thread = None

    def __enter__(self):
        if self.connection == "ssh":
            try:
                self._setup_ssh_tunnel()
            except Exception as e:
                # Clean up on failure
                if self.ssh_client:
                    try:
                        self.ssh_client.close()
                    except Exception:
                        pass

                # Check if it's the DSSKey issue
                if "DSSKey" in str(e):
                    raise ConnectionError(
                        "SSH connection failed due to Paramiko compatibility issue. "
                        'Either downgrade paramiko to <3.0 (pip install "paramiko<3.0") '
                        "or use an alternative SSH tunnel library."
                    )
                else:
                    raise ConnectionError(f"Failed to establish SSH connection: {str(e)}")
        else:
            self.tunnel = _fake_tunnel
            self.tunnel.connector = self._define_connector

        return self.tunnel

    def _setup_ssh_tunnel(self):
        """Set up SSH tunnel using either sshtunnel or direct paramiko implementation"""

        # Patch DSSKey if it's missing (Paramiko 3.0+ compatibility)
        if not hasattr(paramiko, "DSSKey"):
            # Create a dummy DSSKey class for backward compatibility
            class DummyDSSKey:
                """Dummy DSSKey class for Paramiko 3.0+ compatibility"""

                pass

            paramiko.DSSKey = DummyDSSKey

        # Now use sshtunnel normally
        self._setup_sshtunnel()

    def _setup_sshtunnel(self):
        """Original sshtunnel implementation"""
        ssh_port = 22 if self.config.get("SSH_PORT") is None else int(self.config.get("SSH_PORT"))
        remote_addr = "localhost" if self.config.get("DB_REMOTE_HOST") is None else self.config.get("DB_REMOTE_HOST")
        remote_port = 3306 if self.config.get("DB_REMOTE_PORT") is None else int(self.config.get("DB_REMOTE_PORT"))

        if (self.config.get("SSH_PASSWORD") is None) & (self.config.get("SSH_KEY") is None):
            ssh_path = os.path.join(_pycof_folders("home"), ".ssh", "id_rsa")
        else:
            ssh_path = self.config.get("SSH_KEY")

        self.tunnel = sshtunnel.SSHTunnelForwarder(
            (self.config.get("DB_HOST"), ssh_port),
            ssh_username=self.config.get("SSH_USER"),
            ssh_password=self.config.get("SSH_PASSWORD"),
            ssh_pkey=ssh_path,
            remote_bind_address=(remote_addr, remote_port),
        )
        self.tunnel.daemon_forward_servers = True
        self.tunnel.connector = self._define_connector

    def _start_tunnel(self):
        """Start the port forwarding (called by connector)"""
        # This is handled by sshtunnel now
        pass

    def _close_tunnel(self):
        """Close the SSH tunnel"""
        # This is handled by sshtunnel now
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, "tunnel") and hasattr(self.tunnel, "close"):
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

        hostname = self.config.get("DB_HOST")
        user = self.config.get("DB_USER")
        password = self.config.get("DB_PASSWORD")
        port = self.config.get("DB_PORT")
        database = self.config.get("DB_DATABASE")

        if self.connection.lower() == "ssh":
            hostname = "127.0.0.1"
            self.tunnel.start()
            port = self.tunnel.local_bind_port

        # ### Initiate sql connection to the Database
        # Redshift
        if ("redshift" in hostname.lower().split(".")) or (self.engine.lower() == "redshift"):
            try:
                connector = psycopg2.connect(
                    host=hostname, port=int(port), user=user, password=password, database=database
                )
            except Exception:
                raise ConnectionError("Failed to connect to the Redshfit cluster")
        # SQLite
        elif (
            (hostname.lower().find("sqlite") > -1)
            or (str(port).lower() in ["sqlite", "sqlite3"])
            or (self.engine.lower() in ["sqlite", "sqlite3"])
        ):
            try:
                connector = sqlite3.connect(hostname)
            except Exception:
                raise ConnectionError("Failed to connect to the sqlite database")
        # MySQL
        else:
            try:
                # Add new encoder of numpy.float64
                # pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
                # pymysql.converters.conversions = pymysql.converters.encoders.copy()
                # pymysql.converters.conversions.update(pymysql.converters.decoders)
                # # Create connection
                # connector = pymysql.connect(host=hostname, port=int(port), user=user, password=password)

                params = "{user}:{password}@{hostname}:{port}/".format(
                    user=user, password=password, hostname=hostname, port=port
                )

                connector = sa.create_engine("mysql+pymysql://{}".format(params)).raw_connection()
            except Exception:
                raise ConnectionError("Failed to connect to the MySQL database")

        return connector


# #######################################################################################################################
# Insert data to DB

# #######################################################################################################################
# SSH Tunnel Factory Function (for backward compatibility)


def create_ssh_tunnel(config, connection="direct", engine="default"):
    """
    Factory function to create SSH tunnel (currently just wraps SSHTunnel for compatibility).

    Args:
        config: Configuration dictionary
        connection: Connection type ('direct' or 'ssh')
        engine: Database engine type

    Returns:
        SSH tunnel instance
    """
    return SSHTunnel(config, connection, engine)


# #######################################################################################################################
# Insert data to SQL


def _insert_data(data, table, connector, autofill_nan=False, verbose=False):
    # Check if user defined the table to publish
    if table == "":
        raise SyntaxError("Destination table not defined by user")
    # Create the column string and the number of columns used for push query
    columns_string = (", ").join(list(data.columns))
    col_num = len(list(data.columns)) - 1

    # calculate the size of the dataframe to be pushed
    num = len(data)
    batches = int(num / 10000) + 1

    # #######################################################################################################################
    # Transform date columns to str before loading
    warnings.filterwarnings("ignore")  # Removing filter warning when changing data type
    dt_cols = []
    for col in data.columns:
        if type(data.sample(1).reset_index()[col][0]) in [datetime.date, pd._libs.tslibs.timestamps.Timestamp]:
            try:
                data.loc[:, col] = pd.to_datetime(data[col]).apply(str)
                dt_cols += [col]
            except ValueError:
                pass
        elif data[col].dtype in [np.dtype("<M8[ns]"), np.dtype("datetime64[ns]")]:
            dt_cols += [col]
            data.loc[:, col] = data[col].apply(str)
    warnings.filterwarnings("default")  # Putting warning back

    # #######################################################################################################################
    # Fill Nan values if requested by user
    if autofill_nan:
        """
        For each row of the dataset, we fill the NaN values
        with a specific string that will be replaced by None
        value (converted by NULL in MySQL). This aims at avoiding
        the PyMySQL 1054 error.
        """
        data_load = []
        for ls in [v for v in data.fillna("@@@@EMPTYDATA@@@@").values.tolist()]:
            data_load += [[None if vv == "@@@@EMPTYDATA@@@@" else vv for vv in ls]]
    else:
        data_load = data.values.tolist()

    # #######################################################################################################################
    # Push 10k batches iterativeley and then push the remainder
    if isinstance(connector, sqlite3.Connection):
        insert_string = f'INSERT INTO {table} ({columns_string}) VALUES ({"?, " * col_num} ? )'
    else:
        insert_string = f'INSERT INTO {table} ({columns_string}) VALUES ({"%s, " * col_num} %s )'

    if num == 0:
        raise ValueError("len(data) == 0 -> No data to insert")
    elif num > 10000:
        rg = tqdm(range(0, batches - 1)) if verbose else range(0, batches - 1)
        cursor = connector.cursor()
        for i in rg:
            cursor.executemany(insert_string, data_load[i * 10000 : (i + 1) * 10000])
            connector.commit()
        # Push the remainder
        cursor.executemany(insert_string, data_load[(batches - 1) * 10000 :])
        connector.commit()
    else:
        # Push everything if less then 10k (SQL Server limit)
        cursor = connector.cursor()
        cursor.executemany(insert_string, data_load)
        connector.commit()
