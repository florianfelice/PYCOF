# PYCOF (PYthon COmmon Functions)

## 1. Installation

You can get pycof from [PyPI](https://pypi.org/project/pycof/) with:

```bash
pip install pycof
```

The library is supported on Windows, Linux and MacOs.

## 2. Usage

### 2.1. Documentation

You can find the full documentation at [https://www.florianfelice.com/pycof](https://www.florianfelice.com/pycof?orgn=github) or by clicking on the name of each function in the next section.
It will redirect you the the function's specific documentation.

### 2.1. Available functions

The current version of the library provides:

* [**`verbose_display`**](https://www.florianfelice.com/pycof/format#verbose_display):
extended function to [print](https://docs.python.org/3/library/functions.html#print) strings, lists, data frames and progression bar if used as a wrapper in `for` loops.
* [**`remote_execute_sql`**](https://www.florianfelice.com/pycof/sql#remote_execute_sql): aggragated function for SQL queries to `SELECT`, `INSERT`, `DELETE` or `COPY`.
* [**`read_sql`**](https://www.florianfelice.com/pycof/data#f_read): read and format an SQL query file.
* [**`send_email`**](https://www.florianfelice.com/pycof/format#send_email): simple function to send email to contacts in a concise way.
* [**`add_zero`**](https://www.florianfelice.com/pycof/format#add_zero): simple function to convert `int` to `str` by adding a 0 is less than 10.
* [**`OneHotEncoding`**](https://www.florianfelice.com/pycof/models#OneHotEncoding): performs [One Hot Encoding](https://en.wikipedia.org/wiki/One-hot) on a dataframe for the provided column names. Will keep the original categorical variables if `drop` is set to `False`.
* [**`create_dataset`**](https://www.florianfelice.com/pycof/models#create_dataset): function to format a [Pandas](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html) dataframe for [keras](https://keras.io/) format for LSTM.
* [**`group`**](https://www.florianfelice.com/pycof/format#group): will convert an `int` to a `str` with thousand seperator.
* [**`replace_zero`**](https://www.florianfelice.com/pycof/format#replace_zero): will transform 0 values to `-` for display purposes.
* [**`week_sunday`**](https://www.florianfelice.com/pycof/format#week_sunday): will return week number of last sunday date of a given date.
* [**`display_name`**](https://www.florianfelice.com/pycof/format#display_name): displays the current user name. Will display either `first`, `last` or `full` name.
* [**`write`**](https://www.florianfelice.com/pycof/data#write): writes a `str` to a specific file (usually .txt) in one line of code.
* [**`file_age`**](https://www.florianfelice.com/pycof/format#file_age): computes the age (in days, hours, ...) of a given local file.
* [**`str2bool`**](https://www.florianfelice.com/pycof/format#str2bool): converts string to boolean.




### 2.2. Config file for credentials

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
