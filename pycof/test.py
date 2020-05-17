import os
import sys
import getpass

sys.path.append(f"/Users/{getpass.getuser()}/Documents/PYCOF/")

from pycof import group
from pycof import remote_execute_sql

group(12345)
