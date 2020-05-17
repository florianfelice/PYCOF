###############
Library updates
###############


This section aims at showing the latest release of the library.
We show most important releases that included new features.
Library versions in between are used to fix bugs and implement improvement suggested by users' feedback.


----


******************************************************************************************
1.1.33 - May 17, 2020 - Improved query experience with :py:meth:`pycof.remote_execute_sql`
******************************************************************************************

We improved querying experience in :py:meth:`pycof.remote_execute_sql` by simplifying the argument :obj:`cache_time`
and by allowing an :obj:`sql_query` as a path.

Usage of argument :obj:`cache_time` has been improved by allowing users to provide a string with units (e.g. :obj:`24h`, :obj:`1.3mins`).
Users still have the possibility to provide an integer representing file age in seconds.

:py:meth:`pycof.remote_execute_sql` also now accepts a path for :obj:`sql_query`.
The extension needs to be :obj:`.sql`.
The path will then be passed to :py:meth:`pycof.f_read` to recover the SQL query.


.. warning::
    The module :obj:`datamngt` which contains :func:`~OneHotEncoding` and :func:`~create_dataset` will be moved to `statinf`_.
    It is still



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


See more details: :py:meth:`pycof.remote_execute_sql`


----


*****************************************************************************
1.1.26 - Mar 20, 2020 - :py:meth:`pycof.f_read` now supports json and parquet
*****************************************************************************

We extended the :py:meth:`pycof.f_read` extension capabilities to include :obj:`json` and :obj:`parquet` formats.
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


See more details: :py:meth:`pycof.f_read`


----


************************************************************
1.1.21 - Feb 21, 2020 - New function :py:meth:`pycof.f_read`
************************************************************

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


See more details: :py:meth:`pycof.f_read`


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


********************************************************************************
1.1.11 - Dec 10, 2019 - :py:meth:`pycof.remote_execute_sql` now supports caching
********************************************************************************

:py:meth:`pycof.remote_execute_sql` can now cache your SELECT results.
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


See more details: :py:meth:`pycof.remote_execute_sql`


----


****************************************************************************
1.1.9 - Nov 23, 2019 - :py:meth:`pycof.remote_execute_sql` now supports COPY
****************************************************************************

:py:meth:`pycof.remote_execute_sql` can now execute COPY commands on top of SELECT, INSERT and DELETE.
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


See more details: :py:meth:`pycof.remote_execute_sql`


----


***************************************************************************************
1.1.5 - Nov 15, 2019 - :py:meth:`pycof.remote_execute_sql` now supprots IAM credentials
***************************************************************************************

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


See more details: :py:meth:`pycof.remote_execute_sql`



.. _git: https://github.com/florianfelice/PYCOF/
.. _issue: https://github.com/florianfelice/PYCOF/issues

.. _statinf: https://www.florianfelice.com/statinf
