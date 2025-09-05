PYCOF
=======

.. image:: https://pepy.tech/badge/pycof
   :target: https://pepy.tech/project/pycof
.. image:: https://badge.fury.io/py/pycof.svg
   :target: https://pypi.org/project/pycof/
.. image:: https://github.com/fluidicon.png
    :width: 32
    :target: https://github.com/florianfelice/PYCOF

PYCOF is a library to group some useful functions on Python.
The library helps running SQL queries or send emails in a much simpler way.


The library is pip-installable and the source code is available on my `Git <https://github.com/florianfelice/PYCOF>`_.
For any question or suggestion of improvement, please `contact me <mailto:admin@florianfelice.com>`_.


Installation
------------

You can get PYCOF from `PyPI <https://pypi.org/project/pycof/>`_ with:

.. code-block:: console

   pip3 install pycof



The library is supported on Windows, Linux and MacOS.


Setup
-----

1. AWS credentials setup
^^^^^^^^^^^^^^^^^^^^^^^^

Some functions such as :py:meth:`pycof.sql.remote_execute_sql`, :py:meth:`pycof.data.f_read` or :py:meth:`pycof.misc.write` may need to have access to AWS to intereact with different services.
On a AWS instance (e.g. EC2, SageMaker), you may not need to got though these steps as the instance would directly consume the IAM role you assigned to it.

Then, on a local instance (which will rely on a IAM user), run:

.. code-block:: console

   aws configure

and enter your AWS access and private keys.

Note that this setup is required only if you use AWS services such as S3 or Redshift.
These functions also work without credentials for local use.


2. PYCOF configuration setup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The functions :py:meth:`pycof.sql.remote_execute_sql` and :py:meth:`pycof.format.send_email` will, by default, look for the credentials located in :obj:`/etc/.pycof/config.json`.
On Windows, save the config file as :obj:`C:/Users/<username>/.pycof/config.json`.

The file follows the below structure:

.. code-block:: python

   {
   "DB_USER": "",
   "DB_PASSWORD": "",
   "DB_HOST": "",
   "DB_PORT": "3306",
   "DB_DATABASE": "",
   "__COMMENT_1__": "Email specific, send_email",
   "EMAIL_USER": "",
   "EMAIL_SENDER": "",
   "EMAIL_PASSWORD": "",
   "EMAIL_SMTP": "smtp.gmail.com",
   "EMAIL_PORT": "587",
   "__COMMENT_2__": "IAM specific, if connection='SSH' in remote_execute_sql",
   "CLUSTER_NAME": "",
   "__COMMENT_3__": "SSH specific",
   "SSH_USER": "",
   "SSH_KEY": "",
   "SSH_PASSWORD": ""
   }


On Unix based system, run:

.. code-block:: console

   sudo nano /etc/.pycof/config.json

and paste the above json after filling the empty strings (pre-filled values are standard default values).

**Reminder:** To save the file, with nano press :obj:`CTRL + O`, confirm with :obj:`ENTER` then :obj:`CTRL + X` to exit.



Available functions
-------------------

SQL
^^^

.. toctree::

   sql/sql


Data
^^^^

.. toctree::

   datamngt/datamngt


Formatting
^^^^^^^^^^

.. toctree::

   format/format



Release and FAQ
---------------

.. toctree::

   news/updates
   news/faq


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
