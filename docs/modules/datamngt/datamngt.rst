###########
Data module
###########


.. automodule:: pycof.data
    :members:
    :undoc-members:
    :show-inheritance:


----


***
FAQ
***

1 - How can I load a .json file as a dictionnary?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The function :py:meth:`pycof.data.f_read` allows to read different formats.
By default it will load as and :obj:`pandas.DataFrame` but you can provide :obj:`engine='json'` to load as :obj:`dict`.

.. code:: python

    import pycof as pc

    pc.f_read('/path/to/file.json', engine='json')



2 - How can I read .parquet files?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Providing a path containing the keyword :obj:`.parquet` to :py:meth:`pycof.data.f_read`, it will by default call the `pyarrow`_ engine.
You can also pass `pyarrow`_ specific argument for loading your file. In particular, you can parallilize the loading step by providing the argument
:obj:`metadata_nthreads` and set it to the number of threads to be used.

.. code:: python

    import pycof as pc

    # Reading a local file
    df1 = pc.f_read('/path/to/file.parquet', metadata_nthreads=32)
    # Reading remote file from S3
    df2 = pc.f_read('s3://bucket/path/to/parquet/folder', extension='parquet', metadata_nthreads=32)


You can also find more details on the `pyarrow read_table documentation <https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html>`_.

.. warning::

    When loading a file from S3, the credentials required to access AWS. See `setup`_ for the config file or `FAQ3 <#how-can-i-get-my-aws-credentials-with-boto3>`_ for `boto3`_.



3 - How can I get my AWS credentials with boto3?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can use `boto3`_ to get your credentials before passing to a function:

.. code:: python

    import boto3

    session = boto3.Session()
    creds = session.get_credentials().get_frozen_credentials()

    config = {"AWS_ACCESS_KEY_ID": creds[0],
              "AWS_SECRET_ACCESS_KEY": creds[1],
              "REGION": "eu-west-3"}



4 - How do I save my AWS credentials?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you work from an AWS EC2 instance or a SageMaker notebook instance, you may not need to setup your credentials.
Just ensure you assigned an IAM role with the correct permissions to your instance.
You should then be able to use PYCOF normally.

If you use it locally, or with an instance not using an AWS IAM role (but user), you then need to run:

.. code-block:: console

    ubuntu@ip-123-45-67-890:~$ aws configure
    AWS Access Key ID [None]: ****
    AWS Secret Access Key [None]: ****
    Default region name [None]: us-east-1
    Default output format [None]: json



.. _pyarrow: https://arrow.apache.org/docs/python/
.. _setup: https://www.florianfelice.com/pycof#setup
.. _boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html