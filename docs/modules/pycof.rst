PYCOF
=======

.. image:: https://pepy.tech/badge/pycof
   :target: https://pepy.tech/project/pycof
.. image:: https://badge.fury.io/py/pycof.svg
   :target: https://pypi.org/project/pycof/


PYCOF is a library to group some useful functions on Python.
The library helps running SQL queries or send emails in a much simpler way. 


The library is pip-installable and the source code is available on my `Git <https://github.com/florianfelice/PYCOF>`_.
For any question or suggestion of improvement, please `contact me <mailto:florian.website.mail@gmail.com>`_.


Installation
============

You can get PYCOF from `PyPI <https://pypi.org/project/pycof/>`_ with:

.. code-block:: console

   pip3 install pycof



The library is supported on Windows, Linux and MacOS.


Setup
=====

The functions :py:meth:`pycof.remote_execute_sql` and :py:meth:`pycof.send_email` will by default look for the credentials located in :obj:`/etc/config.json`.
On Windows, save the config file as :obj:`C:/Windows/config.json`.

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
   "EMAIL_PASSWORD": "",
   "EMAIL_SMTP": "smtp.gmail.com",
   "EMAIL_PORT": "587",
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


Available functions
===================

SQL
---

.. toctree::

   sql/sql


Data
----

.. toctree::

   datamngt/datamngt


Formatting
----------

.. toctree::

   format/format



Release and FAQ
===============

.. toctree::

   news/updates
   news/faq


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`



