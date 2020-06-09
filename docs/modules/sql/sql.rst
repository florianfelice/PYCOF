##########
SQL module
##########

.. automodule:: pycof.sql
    :members:
    :undoc-members:
    :show-inheritance:

----

***********
Credentials
***********


Save your credentials locally
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The functions :py:meth:`pycof.sql.remote_execute_sql` and :py:meth:`pycof.format.send_email` will by default look for the credentials located in :obj:`/etc/config.json`.
On Windows, save the config file as :obj:`C:/Windows/config.json`.

The file follows the below structure:

.. code-block:: python

   {
   "DB_USER": "",
   "DB_PASSWORD": "",
   "DB_HOST": "",
   "DB_PORT": "3306",
   "DB_DATABASE": "",
   "__COMMENT_1__": "Email specific, send_email"
   "EMAIL_USER": "",
   "EMAIL_PASSWORD": "",
   "EMAIL_SMTP": "smtp.gmail.com",
   "EMAIL_PORT": "587"
   "__COMMENT_2__": "IAM specific, if useIAM=True in remote_execute_sql",
   "CLUSTER_NAME": "",
   "AWS_ACCESS_KEY_ID": "",
   "AWS_SECRET_ACCESS_KEY": "",
   "REGION": "eu-west-1"
   }


On Unix based system, run:

.. code-block:: console

   sudo nano /etc/config.json

and paste the above json after filling the empty strings (pre-filled values are standard default values).

**Reminder:** To save the file, with nano press :obj:`CTRL + O`, confirm with :obj:`b` then :obj:`CTRL + X` to exit.

On Windows, use the path :obj:`C:/Windows/config.json`.


Pass your credentials in your code
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Though it is highly not recommended, you can pass your credentials locally to the functions with the argument credentials.
You can then create a dictionnary using the same keys as described in previous section.

The preferred option is to provide the json file's path to the credentials argument. 

----

*******
Example
*******

Standard :obj:`SELECT`
^^^^^^^^^^^^^^^^^^^^^^

The function executes a given SQL query with credentials automatically pulled from :obj:`/etc/config.json`.
To execute an SQL query, follow the below steps:

.. code-block:: python

    from pycof as pc

    ## Set up the SQL query
    sql = "SELECT * FROM SCHEMA.TABLE LIMIT 10"

    ## The function will return a pandas dataframe
    df = pc.remote_execute_sql(sql)


----


***
FAQ
***

1 - What if I change an argument in the SQL query and run with :obj:`cache=True`?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The function :py:meth:`pycof.remote_execute_sql` looks at your SQL query as a whole when saving/loading the cache data.
Even a slight change in the query (column name, filter, etc...) will trigger a new run of the new query before being cached again.
You can then safely use caching without worrying about the eventual evolution of your SQL.


2 - How to use different credential sets?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The credentials argument can take the path or json file name into account to load them.

For instance, you can have multiple credential files such as :obj:`/etc/config.json`, :obj:`/etc/MyNewHost.json` and :obj:`/home/OtherHost.json`.
In :py:meth:`pycof.sql.remote_execute_sql` you can play with the credentials argument.

    * To use the :obj:`/etc/config.json` credentials you can use the default arguments by not providing anything.
    * To use :obj:`/etc/MyNewHost.json` you can either pass :obj:`credentials='MyNewHost.json'` or the whole path to use them.
    * To use :obj:`/home/OtherHost.json` you need to pass the whole path.

Example:

.. code-block:: python

    import pycof as pc

    ## Set the SQL query
    sql = "SELECT * FROM SCHEMA.TABLE LIMIT 10"

    ## Run the query
    df = pc.remote_execute_sql(sql, credentials='MyNewHost.json')
    df2 = pc.remote_execute_sql(sql, credentials='/home/OtherHost.json')



3 - How to execute a query from an SQL file?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By providing the path to your sql file in :py:meth:`pycof.sql.remote_execute_sql`, the function will directly load the content of the file.
You can play with the arguments and give wildcards to format in the file.
The function uses :py:meth:`pycof.data.f_read` to load your file, and hence the same arguments can be passed as :obj:`**kwargs`.


Example:

Consider you have a first file :obj:`/path/to/simple_query.sql`:

.. code-block:: sql
    
    SELECT *
    FROM schema.table
    WHERE 1 = 1

Then, you can directly use this file in :py:meth:`pycof.sql.remote_execute_sql` as:

.. code-block:: python

    import pycof as pc

    ## Run the query
    df1 = pc.remote_execute_sql('/path/to/simple_query.sql')


You can also put wildcards in your file that will be replaced when loading the file.
Consider your path file is :obj:`/path/to/query_wildcards.sql`.
The wildcards can have any name you want.
Just make sure you do not use same name as arguments in :py:meth:`pycof.sql.remote_execute_sql` or :py:meth:`pycof.data.f_read`.

.. note::

    TIP: In both functions, we do not use arguments starting with an underscore :obj:`_`.
    You can use this structure for your wilcards, as illustrated below.


.. code-block:: sql
    
    SELECT *
    FROM schema.table
    WHERE column1 = '{_value1}'
        AND column2 = '{_newfilter}'

You can then execute your query:

.. code-block:: python

    import pycof as pc

    ## Run the query
    df2 = pc.remote_execute_sql('/path/to/query_wildcards.sql', _value1='random_value', _newfilter='test')

4 - How to query with AWS IAM credentials?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The function :py:meth:`pycof.sql.remote_execute_sql` can take into account `IAM <https://aws.amazon.com/iam/features/manage-users/>`_ user's credentials.
You need to ensure that your credentials file :obj:`/etc/config.json` includes the IAM access and secret keys with the Redshift cluster information.
The only argument to change when calling the function is to set :obj:`useIAM=True`.

The function will then use the `AWS access and secret keys <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html>`_ to ask AWS to provide the user name and password to connect to the cluster.
This is a much safer approach to connect to a Redshift cluster than using direct cluster's credentials.

.. note::
    TIP: When using PYCOF on EC2 or SageMaker, you can also play with `IAM roles <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html>`_ and not specify the access and secret keys.
    Roles do not depend on credentials. Just make sure your host has the required roles attached.

Example: 

.. code-block:: python

    import pycof as pc

    ## Set the SQL query
    sql = "SELECT * FROM SCHEMA.TABLE LIMIT 10"

    ## Run the query
    df = pc.remote_execute_sql(sql, useIAM=True)


5 - How to cache the data?
^^^^^^^^^^^^^^^^^^^^^^^^^^

:py:meth:`pycof.sql.remote_execute_sql` allows to cache the data with the :obj:`cache` argument and save time for multiple executions.

.. code-block:: python

    ## Cache the results of the query for 30 minutes
    df = pc.remote_execute_sql(sql, cache='30mins')

The cache argument will allow you to save time for the next execution of the same SQL query.
It will then load the cached data and not execute the whole SQL query if the age of the last execution is younger than the :obj:`cache` argument.

