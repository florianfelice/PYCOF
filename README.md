# PYCOF (PYthon COmmon Functions)

## 1. Usage

### 1.1 Config file for credentials

#### 1.1.1 Save your credentials locally

The function `remote_execute_sql` will by default look for the credentials located in `/etc/config.json`.

The file follows the below structure:

```bash
{
	"DB_USER": "",
	"DB_PWD": "",
	"DB_HOST": "",
	"DB_PORT": "",
	"DB_DATABASE": ""
}

```

On Unix based server, run:
```bash
sudo nano /etc/config.json
```

and paste the above json after filling the empty strings.

*__Reminder:__* To save the file, with nano press `CTRL + O` and `y` then `CTRL + X` to exit.


#### Pass your credentials in you code

Though it is highly not recommended, you can pass your credentials locally to the `remote_execute_sql` with the argument `credentials`.
You can then create a dictionnary using the same keys as described in [previous section](#111-save-your-credentials-locally).


### 1.2 Load pycof

To load `pycof` in your script, you can use:

```python
# Load pycof
import pycof as pc
# Or, load a specific or all functions from pycof
from pycof import *
```

To execute an SQL query, follow the below steps:

```python
from pycof import remote_execute_sql

## Set up the SQL query
sql = "SELECT * FROM SCHEMA.TABLE LIMIT 10"

## The function will return a pandas dataframe
remote_execute_sql(sql)
```


### 1.3 Available functions

The current version of the library provides:

* `verbose_display`: extended function for `print` that can print strings, lists, data frames and uses tqdm is used in `for` loops.
* `remote_execute_sql`: aggragated function for SQL queries to `SELECT`, `INSERT` or `DELETE`.
* `add_zero`: simple function to convert int to str by adding a 0 is less than 10.
* `OneHotEncoding`: perform One Hot Encoding on a dataframe for the provided column names. Will keep the original categorical variables if `drop` is set to `False`.
* `create_dataset`: function to format a `pandas` dataframe for `keras` format for LSTM.
* `group`: will convert an `int` to a `str` with thousand seperator.
* `replace_zero`: will transform 0 values to `-` for display purposes.
* `display_name`: displays the current user name. Will display either `first`, `last` or `full` name.
* `write`: writes a `str` to a specific file (usually .txt) in one line of code.
* `str2bool`: converts string to boolean.
* `wmape`: computes the [Weighted Mean Absolute Percentage Error](https://en.wikipedia.org/wiki/Mean_absolute_percentage_error) between two columns.
* `mse`: computes the [Mean Squared Error](https://en.wikipedia.org/wiki/Mean_squared_error) between two columns. Returns the RMSE (Root MSE) if `root` is set to `True`.