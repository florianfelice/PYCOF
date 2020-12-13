###
FAQ
###

*****************************************************
I have an error when using a function, what can I do?
*****************************************************

If you encounter an unexpected error (coming from the source code), we recommend to first check if you are using the latest version of PYCOF

.. code::

    pip3 show pycof

If the version is not the latest one, please upgrade it.

.. code::

    pip3 install --upgrade pycof

If you still encounter the same error with the most recent version, please raise an `issue`_.


----


***************************************************************************
I am having issues when installing pycof on Windows, is this a known issue?
***************************************************************************

One of the dependencies for PYCOF is `pyarrow <https://arrow.apache.org/docs/python/>`_ which is not compatible on Windows 32bits.
Check whether your computer is running on 32 or 64 bits (`Windows documentation <https://support.microsoft.com/en-us/help/15056/windows-32-64-bit-faq>`_).
If your computer is 64 bits, check what your default python is using:

.. code:: bash

    C:\Users\username>python
    Python 3.8.6 (tags/v3.8.6:db45529, Sep 23 2020, 15:52:53) [MSC v.1927 32 bit (AMD64)] on win32
    Type "help", "copyright", "credits" or "license" for more information.
    >>>

If you cannot see 64 bits, you may need to change your version of Python and `install <https://www.python.org/downloads/>`_ the 64 bit version.
You may need to reboot your computer to see the changes effective.

Find more details on the `Python documentation <https://docs.python.org/3/using/windows.html>`_ or on `Geek University <https://geek-university.com/python/add-python-to-the-windows-path/>`_ for updating the Python path.

If the problem persists, please raise an `issue`_.



----


*****************************************************************************
What if I change an argument in the SQL query and run with :obj:`cache=True`?
*****************************************************************************

See `SQL FAQ 1 <../sql/sql.html?orgn=pycof_faq#what-if-i-change-an-argument-in-the-sql-query-and-run-with-cache-true>`_.


----


*************************************
How to use different credential sets?
*************************************

See `SQL FAQ 2 <../sql/sql.html?orgn=pycof_faq#how-to-use-different-credential-sets>`_.


----


****************************************
How to execute a query from an SQL file?
****************************************

See `SQL FAQ 3 <../sql/sql.html?orgn=pycof_faq#how-to-execute-a-query-from-an-sql-file>`_.


----


**************************************
How to query with AWS IAM credentials?
**************************************

See `SQL FAQ 4 <../sql/sql.html?orgn=pycof_faq#how-to-query-with-aws-iam-credentials>`_.


----


**********************
How to cache the data?
**********************

See `SQL FAQ 5 <../sql/sql.html?orgn=pycof_faq#how-to-cache-the-data>`_.


----


*******************************************
How to query a database with SSH tunneling?
*******************************************

See `SQL FAQ 6 <../sql/sql.html?orgn=pycof_faq#how-to-query-a-database-with-ssh-tunneling>`_.


----


*********************************************
How can I load a .json file as a dictionnary?
*********************************************

See `Data FAQ 1 <../datamngt/datamngt.html?orgn=pycof_faq#how-can-i-load-a-json-file-as-a-dictionnary>`_.


----

******************************
How can I read .parquet files?
******************************

See `Data FAQ 2 <../datamngt/datamngt.html?orgn=pycof_faq#how-can-i-read-parquet-files>`_.


----


********************************************
How can I get my AWS credentials with boto3?
********************************************

See `Data FAQ 3 <../datamngt/datamngt.html?orgn=pycof_faq#how-can-i-get-my-aws-credentials-with-boto3>`_.


----


.. _git: https://github.com/florianfelice/PYCOF/
.. _issue: https://github.com/florianfelice/PYCOF/issues

.. _statinf: https://www.florianfelice.com/statinf?orgn=pycof_faq
