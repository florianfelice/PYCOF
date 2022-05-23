###############
Library updates
###############


This section aims at showing the latest release of the library.
We show most important releases that included new features.
Library versions in between are used to fix bugs and implement improvement suggested by users' feedback.
----

***************************************************************************
1.3.0 - May 23, 2023 - AWS credentials profile prioritized over config file
***************************************************************************

PYCOF now prioritizes AWS cli profiles created through the `aws configure` command over `config.json` file.
For functions :py:meth:`pycof.data.f_read` and :py:meth:`pycof.misc.write`, users no longer to create the `config.json` file.
Only requirement will be to run the command `aws configure` and register the IAM access and private keys.
For the function :py:meth:`pycof.sql.remote_execute_sql`, the `config.json` file may remain required for the case where `connection='IAM'` to connect to a Redshift cluster.
The fallback solution with `config.json` file containing the fields `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` still remains available but may be deprecated in later versions.

This change will allow faster setup and even no setup required on AWS environments (e.g. EC2, SageMaker).


^^^^^^^^^^^^^^
How to use it?
^^^^^^^^^^^^^^

.. code::

    import pycof as pc

    # Load a parquet file from Amazon S3
    df = pc.f_read('s3://bucket/path/to/file.parquet', profile_name='default')
    
    # Write a file on Amazon S3
    pc.write(df, 's3://bucket/path/to/file2.parquet', profile_name='default')
    
    # Run a query on a Redshift cluster
    df2 = pc.remote_execute_sql('/path/to/query.sql', connection='IAM', profile_name='default')


^^^^^^^^^^^^^^^^^^
How to install it?
^^^^^^^^^^^^^^^^^^

.. code::

    pip3 install pycof==1.3.0


See more details: :py:meth:`pycof.data.f_read` / :py:meth:`pycof.misc.write` / :py:meth:`pycof.sql.remote_execute_sql`

----

***********************************************************************
1.2.5 - February 2, 2021 - Emails and Google Calendar are now supported
***********************************************************************

A new module :py:meth:`pycof.format.GoogleCalendar` allows users to retreive events from Google Calendar.
The modules contains a fonction :py:meth:`pycof.format.GoogleCalendar.today_events` to get all events of the running day.
The user can also use :py:meth:`pycof.format.GoogleCalendar.next_events` to find the next events (the number of events is passed in the arguments).

Another module :py:meth:`pycof.format.GetEmails` allows users to retreive the most recent emails from a selected address.
Users can retreive a fixed number of emails and their attachments.

An additional namespace is available in the output of :py:meth:`pycof.sql.remote_execute_sql`.
Metadata have been added when the cache is called and allow users to have information regarding the cache in place, the last run date of the query, the file age, etc...


^^^^^^^^^^^^^^
How to use it?
^^^^^^^^^^^^^^

.. code::

    import pycof as pc

    calendar = pc.GoogleCalendar()
    # Get today events
    todays = calendar.today_events(calendar='primary')
    # Get 10 next events
    next10 = calendar.next_events(calendar='primary', maxResults=10)

    # Retreive last 10 emails
    pycof.GetEmails(10)

    # Check file age of an SQL output
    df = pc.remote_execute_sql(sql, cache='2h')
    df.meta.cache.age()


^^^^^^^^^^^^^^^^^^
How to install it?
^^^^^^^^^^^^^^^^^^

.. code::

    pip3 install pycof==1.2.5


See more details: :py:meth:`pycof.format.GoogleCalendar` / :py:meth:`pycof.format.GetEmails` / :py:meth:`pycof.sql.remote_execute_sql`


----

********************************************************************************************
1.2.0 - December 13, 2020 - SSH tunnels supported in :py:meth:`pycof.sql.remote_execute_sql`
********************************************************************************************

The module :py:meth:`pycof.sql.remote_execute_sql` now supports remote connections with SSH tunneling thanks to the argument :obj:`connection='SSH'`.
Supported for both MySQL and SQLite databases, users will be able to access databases on servers that only expose port 22.
This will allow more secure connections.
If argument :obj:`connection='SSH'` is called but the config file does not have neither a value for :obj:`SSH_KEY` nor for :obj:`SSH_PASSWORD`,
the function will look for the default SSH location (:obj:`/home/user/.ssh/id_rsa` on Linux/MacOS or :obj:`'C://Users/<username>/.ssh/id_rsa` on Windows).

Also, both functions :py:meth:`pycof.sql.remote_execute_sql` and :py:meth:`pycof.data.f_read` can consume argument :obj:`credentials` without '.json' extension.
See `SQL FAQ 6 <../sql/sql.html?orgn=pycof_faq#how-to-query-a-database-with-ssh-tunneling>`_ for more details.

.. warning::

    Note that from version 1.2.0, the pycof credentials folder for Linux and MacOS will need to be :obj:`/etc/.pycof`.
    You can then move you config file with the command: :obj:`sudo mv /etc/config.json /etc/.pycof/config.json`.

The adapted :obj:`config.json` structure is:

.. code-block:: python

   {
   "DB_USER": "",
   "DB_PASSWORD": "",
   "DB_HOST": "",
   "DB_PORT": "3306",
   "DB_DATABASE": "",
   "SSH_USER": ""
   }

Other arguments such as :obj:`SSH_KEY` and :obj:`SSH_PASSWORD` are optional provided that the SSH key is stored in the default folder.



^^^^^^^^^^^^^^
How to use it?
^^^^^^^^^^^^^^

.. code::

    import pycof as pc

    pc.remote_execute_sql('my_example.sql', connection='SSH')


^^^^^^^^^^^^^^^^^^
How to install it?
^^^^^^^^^^^^^^^^^^

.. code::

    pip3 install pycof==1.2.0


See more details: :py:meth:`pycof.sql.remote_execute_sql`


----


****************************************************************************************
1.1.37 - September 30, 2020 - SQLite database on :py:meth:`pycof.sql.remote_execute_sql`
****************************************************************************************

The module :py:meth:`pycof.sql.remote_execute_sql` now supports local `SQLite <https://www.sqlite.org>`_ connections.
Extending from MySQL and AWS Redshift databases, users can now work with local databases thanks to `SQLite <https://www.sqlite.org>`_.
This will allow users to play with infrastructure running on their local machine (overcoming the problem of remote servers and potential cost infrastructure).

The adapted :obj:`config.json` structure is:

.. code-block:: python

   {
   "DB_USER": "",
   "DB_PASSWORD": "",
   "DB_HOST": "/path/to/sqlite.db",
   "DB_PORT": "sqlite3",
   "DB_DATABASE": "",
   }


The module will automatically detect the connection if the keyword `sqlite` appears in the path to the database.
User can also define the port as `sqlite` if the path does not contain the keyword.
A final option is given to force the connection with the argument :obj:`engine='sqlite3'`.

The module will offer the same functionality as the first two connectors.


^^^^^^^^^^^^^^
How to use it?
^^^^^^^^^^^^^^

.. code::

    import pycof as pc

    pc.remote_execute_sql('my_example.sql', engine='sqlite3')


^^^^^^^^^^^^^^^^^^
How to install it?
^^^^^^^^^^^^^^^^^^

.. code::

    pip3 install pycof==1.1.37


See more details: :py:meth:`pycof.sql.remote_execute_sql`


----


***********************************************************************************************
1.1.35 - September 13, 2020 - Connector engine added to :py:meth:`pycof.sql.remote_execute_sql`
***********************************************************************************************

The module :py:meth:`pycof.sql.remote_execute_sql` automaticaly detects a redshift cluster.
The logic consists in checking whether the keyword *redshift* is contained in the hostname of the AWS Redshift cluster.

The module now includes an argument :obj:`engine` which allows to force the Redshift connector.
If you need another engine (neither Redshift nor MySQL), please submit an `issue`_.


.. warning::
    The module :obj:`datamngt` which contained :func:`~OneHotEncoding` and :func:`~create_dataset` is now deprecated.
    To use these modules, please refer to `statinf`_.



^^^^^^^^^^^^^^
How to use it?
^^^^^^^^^^^^^^

.. code::

    import pycof as pc

    pc.remote_execute_sql('my_example.sql', engine='redshift')


^^^^^^^^^^^^^^^^^^
How to install it?
^^^^^^^^^^^^^^^^^^

.. code::

    pip3 install pycof==1.1.35


See more details: :py:meth:`pycof.sql.remote_execute_sql`


----


**********************************************************************************************
1.1.33 - May 17, 2020 - Improved query experience with :py:meth:`pycof.sql.remote_execute_sql`
**********************************************************************************************

We improved querying experience in :py:meth:`pycof.sql.remote_execute_sql` by simplifying the argument :obj:`cache_time`
and by allowing an :obj:`sql_query` as a path.

Usage of argument :obj:`cache_time` has been improved by allowing users to provide a string with units (e.g. :obj:`24h`, :obj:`1.3mins`).
Users still have the possibility to provide an integer representing file age in seconds.

:py:meth:`pycof.sql.remote_execute_sql` also now accepts a path for :obj:`sql_query`.
The extension needs to be :obj:`.sql`.
The path will then be passed to :py:meth:`pycof.data.f_read` to recover the SQL query.


.. warning::
    The module :obj:`datamngt` which contains :func:`~OneHotEncoding` and :func:`~create_dataset` will be moved to `statinf`_.



^^^^^^^^^^^^^^
How to use it?
^^^^^^^^^^^^^^

.. code::

    import pycof as pc

    pc.remote_execute_sql('my_example.sql', cache=True, cache_time='2.3wk')


^^^^^^^^^^^^^^^^^^
How to install it?
^^^^^^^^^^^^^^^^^^

.. code::

    pip3 install pycof==1.1.33


See more details: :py:meth:`pycof.sql.remote_execute_sql`


----


**********************************************************************************
1.1.26 - Mar 20, 2020 - :py:meth:`pycof.data.f_read` now supports json and parquet
**********************************************************************************

We extended the :py:meth:`pycof.data.f_read` extension capabilities to include :obj:`json` and :obj:`parquet` formats.
It aims at loading files to be used as DataFrame or SQL files.
The formats accepted now are: :obj:`csv`, :obj:`txt`, :obj:`xlsx`, :obj:`sql`, :obj:`json`, :obj:`parquet`, :obj:`js`, :obj:`html`.

.. warning::
    The recommended engine is :obj:`pyarrow` since :obj:`fastparquet` has stability and installation issues.
    The dependency on :obj:`fastparquet` will be removed in version 1.1.30.

^^^^^^^^^^^^^^
How to use it?
^^^^^^^^^^^^^^

.. code::

    import pycof as pc

    pc.f_read('example_df.json')


^^^^^^^^^^^^^^^^^^
How to install it?
^^^^^^^^^^^^^^^^^^

.. code::

    pip3 install pycof==1.1.24


See more details: :py:meth:`pycof.data.f_read`


----


*****************************************************************
1.1.21 - Feb 21, 2020 - New function :py:meth:`pycof.data.f_read`
*****************************************************************

PYCOF now provides a function to load files without having to care about the extension.
It aims at loading files to be used as DataFrame or SQL files.
The formats accepted are: :obj:`csv`, :obj:`txt`, :obj:`xlsx`, :obj:`sql`
Soon it will be extended to :obj:`json`, :obj:`parquet`, :obj:`js`, :obj:`html`.

^^^^^^^^^^^^^^
How to use it?
^^^^^^^^^^^^^^

.. code::

    import pycof as pc

    pc.f_read('example_df.csv')


^^^^^^^^^^^^^^^^^^
How to install it?
^^^^^^^^^^^^^^^^^^

.. code::

    pip3 install pycof==1.1.21


See more details: :py:meth:`pycof.data.f_read`


----


****************************************************************
1.1.13 - Dec 21, 2019 - New function :py:meth:`pycof.send_email`
****************************************************************

PYCOF allows to send email from a script with an easy function.
No need to handle SMTP connector, PYCOF does it for you.
The only requirement is the file :obj:`config.json` to be setup once.
See more `setup <../pycof.html#setup>`_ details.


^^^^^^^^^^^^^^
How to use it?
^^^^^^^^^^^^^^

.. code::

    import pycof as pc

    pc.send_email(to="test@domain.com", body="Hello world!", subject="Test")


^^^^^^^^^^^^^^^^^^
How to install it?
^^^^^^^^^^^^^^^^^^

.. code::

    pip3 install pycof==1.1.13

See more details: :py:meth:`pycof.send_email`


----


************************************************************************************
1.1.11 - Dec 10, 2019 - :py:meth:`pycof.sql.remote_execute_sql` now supports caching
************************************************************************************

:py:meth:`pycof.sql.remote_execute_sql` can now cache your SELECT results.
This will avoid querying the database several times when executing the command multiple times.
The function will save the file in a temporary file by hasing your SQL query.
See more `details <../sql/sql.html#caching-the-data>`_.

^^^^^^^^^^^^^^
How to use it?
^^^^^^^^^^^^^^

.. code::

    .. code::

    import pycof as pc

    sql = """
    SELECT *
    FROM schema.table
    """

    pc.remote_execute_sql(sql, cache=True, cache_time=3600)


^^^^^^^^^^^^^^^^^^
How to install it?
^^^^^^^^^^^^^^^^^^

.. code::

    pip3 install pycof==1.1.11


See more details: :py:meth:`pycof.sql.remote_execute_sql`


----


********************************************************************************
1.1.9 - Nov 23, 2019 - :py:meth:`pycof.sql.remote_execute_sql` now supports COPY
********************************************************************************

:py:meth:`pycof.sql.remote_execute_sql` can now execute COPY commands on top of SELECT, INSERT and DELETE.
The only requirement is the file :obj:`config.json` to bet setup once.
See more `setup <../pycof.html#setup>`_ details.


^^^^^^^^^^^^^^
How to use it?
^^^^^^^^^^^^^^

.. code::

    import pycof as pc

    sql_copy = """
    COPY FROM schema.table -
    CREATE SCIENTISTS (EMPLOYEE_ID, EMAIL) -
    USING SELECT EMPLOYEE_ID, EMAIL FROM EMPLOYEES -
    WHERE JOB_ID='SCIENTIST';
    """

    pc.remote_execute_sql(sql_copy, useIAM=True)


^^^^^^^^^^^^^^^^^^
How to install it?
^^^^^^^^^^^^^^^^^^

.. code::

    pip3 install pycof==1.1.9


See more details: :py:meth:`pycof.sql.remote_execute_sql`


----


*******************************************************************************************
1.1.5 - Nov 15, 2019 - :py:meth:`pycof.sql.remote_execute_sql` now supprots IAM credentials
*******************************************************************************************

You can now connect to your database though `IAM <https://aws.amazon.com/iam/features/manage-users/>`_.
The only requirement is the file :obj:`config.json` to bet setup once.
See more `setup <../pycof.html#setup>`_ details and more information for this `feature <../sql/sql.html#query-with-aws-iam-credentials>`_.

^^^^^^^^^^^^^^
How to use it?
^^^^^^^^^^^^^^

.. code::

    import pycof as pc

    sql = """
    SELECT *
    FROM schema.table
    """

    pc.remote_execute_sql(sql, useIAM=True)


^^^^^^^^^^^^^^^^^^
How to install it?
^^^^^^^^^^^^^^^^^^

.. code::

    pip3 install pycof==1.1.5


See more details: :py:meth:`pycof.sql.remote_execute_sql`



.. _git: https://github.com/florianfelice/PYCOF/
.. _issue: https://github.com/florianfelice/PYCOF/issues

.. _statinf: https://www.florianfelice.com/statinf
