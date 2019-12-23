# PYCOF (PYthon COmmon Functions)

## 1. Installation

You can get pycof from [PyPI](https://pypi.org/project/pycof/) with:

```bash
pip install pycof
```

The library is supported on Windows, Linux and MacOs.

## 2. Usage

### 2.1. Config file for credentials

#### 2.1.1. Save your credentials locally

The function `remote_execute_sql` and `send_email` will by default look for the credentials located in `/etc/config.json`.
On Windows, save the config file as `C:/Windows/config.json`.

The file follows the below structure:

```bash
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
```

On Unix based system, run:
```bash
sudo nano /etc/config.json
```

and paste the above json after filling the empty strings (pre-filled values are standard default values).

*__Reminder:__* To save the file, with nano press `CTRL + O`, confirm with `y` then `CTRL + X` to exit.

On Windows, use the path `C:/Windows/config.json`.


#### 2.1.2. Pass your credentials in your code

Though it is highly not recommended, you can pass your credentials locally to the functions with the argument `credentials`.
You can then create a dictionnary using the same keys as described in [previous section](#211-save-your-credentials-locally).

The preferred option is to provide the json file's path to the `credentials` argument.


### 2.2. Load PYCOF

To load `pycof` in your script, you can use:

```python
# Load pycof
import pycof as pc

# Or, load a specific function or all functions from pycof
from pycof import *
```


### 2.3. Available functions

The current version of the library provides:

* **`verbose_display`**: extended function to [print](https://docs.python.org/3/library/functions.html#print) strings, lists, data frames and progression bar if used as a wrapper in `for` loops.
* **`remote_execute_sql`**: aggragated function for SQL queries to `SELECT`, `INSERT`, `DELETE` or `COPY`.
* **`send_email`**: simple function to send email to contacts in a concise way.
* **`add_zero`**: simple function to convert `int` to `str` by adding a 0 is less than 10.
* **`OneHotEncoding`**: performs [One Hot Encoding](https://en.wikipedia.org/wiki/One-hot) on a dataframe for the provided column names. Will keep the original categorical variables if `drop` is set to `False`.
* **`create_dataset`**: function to format a [Pandas](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html) dataframe for [keras](https://keras.io/) format for LSTM.
* **`group`**: will convert an `int` to a `str` with thousand seperator.
* **`replace_zero`**: will transform 0 values to `-` for display purposes.
* **`week_sunday`**: will return week number of last sunday date of a given date.
* **`display_name`**: displays the current user name. Will display either `first`, `last` or `full` name.
* **`write`**: writes a `str` to a specific file (usually .txt) in one line of code.
* **`str2bool`**: converts string to boolean.
* **`wmape`**: computes the [Weighted Mean Absolute Percentage Error](https://en.wikipedia.org/wiki/Mean_absolute_percentage_error) between two vectors.
* **`mse`**: computes the [Mean Squared Error](https://en.wikipedia.org/wiki/Mean_squared_error) between two vectors. Returns the RMSE (Root MSE) if `root` is set to `True`.


## 3. FAQ

### 3.1. How to use `remote_execute_sql`?

The function executes a given SQL query with credentials automatically pulled from `/etc/config.json`.
To execute an SQL query, follow the below steps:

```python
from pycof as pc

## Set up the SQL query
sql = "SELECT * FROM SCHEMA.TABLE LIMIT 10"

## The function will return a pandas dataframe
df = pc.remote_execute_sql(sql)
```


You can also cache your data by using the `cache` argument:

```python
## Cache the results of the query for 1h (60*60 = 3600 seconds)
df = pc.remote_execute_sql(sql, cache=True, cache_time=3600)
```

The `cache` argument will allow you to save time the next time you execute the same SQL query within the `cache_time` period.
It will then load the cached data and not execute the whole SQL query.
The default value is 1 day.


### 3.2. Can I query a Reshift cluster with IAM user credentials?

The function `remote_execute_sql` can take into account [IAM](https://aws.amazon.com/iam/features/manage-users/) user's credentials. You need to ensure that your credentials file `/etc/config.json` includes the IAM access and secret keys with the Redshift cluster information.
The only argument to change when calling the function is to set `useIAM=True`.

The function will then use the [AWS access and secret keys](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html)
to ask AWS to provide the user name and password to connect to the cluster.
This is a much safer approach to connect to a Redshift cluster than using direct cluster's credentials.

You can also play with IAM roles by not specifying the access and secret keys, provided that you host has the required permissions.

**Example:**
```python
import pycof as pc

## Set the SQL query
sql = "SELECT * FROM SCHEMA.TABLE LIMIT 10"

## Run the query
df = pc.remote_execute_sql(sql, useIAM=True)
```


### 3.3. What if I change an argument in the SQL query and run with `cache=True`?

The function `remote_execute_sql` looks at your SQL query as a whole when saving/loading the cache data. Even a slight change in the query (column name, filter, etc...) will trigger a new run of the new query before being cached again.
You can then safely use caching without worrying about the eventual evolution of your SQL.


### 3.4. How to use different credential sets?

The `credentials` argument can take the path or json file name into account to load them.

For instance, you can have multiple credential files such as `/etc/config.json`, `/etc/MyNewHost.json` and `/home/OtherHost.json`.
In `remote_execute_sql` you can play with the `credentials` argument.

* To use the `/etc/config.json` credentials you can use the default arguments by not providing anything.
* To use `/etc/MyNewHost.json` you can either pass `MyNewHost.json` or the whole path to use them.
* To use `/home/OtherHost.json` you need to pass the whole path.

**Example:**
```python
import pycof as pc

## Set the SQL query
sql = "SELECT * FROM SCHEMA.TABLE LIMIT 10"

## Run the query
df = pc.remote_execute_sql(sql, credentials='MyNewHost.json')
```


### 3.5. How to use `send_email`?

The function `send_email` takes allows to send an email to a given recipient with a specific subject.
Similar to [`remote_execute_sql`](#31-how-to-use-remote_execute_sql), the function will automatically pull the credentials from `/etc/config.json` unless you pass a dictionnary or a path.

**Example:**

```python
import pycof as pc

## Write the content of the email as HTML
content = """
Hello,
<br><br>
This is a test email using <a href = 'https://pypi.org/project/pycof/'>PYCOF</a>, check it out!
"""

## Send the email
pc.send_email(to='test@domain.com', body=content, subject='Hello world!')
```

You can choose to copy some other users with the `cc` argument and choose a different credential set, as explained [above](#34-how-to-use-different-credential-sets).
