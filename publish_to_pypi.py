#!/usr/bin/env python3
from requests import get
from bs4 import BeautifulSoup

import sys
import os
import getpass
import json

import argparse


# Define
library = 'PYCOF'
desc = "A package for commonly used functions"
requirements = ['pandas>=0.24.1', 'numpy>=1.16.3', 'psycopg2-binary>=2.7.4',
                'pymysql>=0.9.3', 'tqdm>=4.35.0', 'boto3>=1.16.19', 'xlrd>=1.2.0',
                'matplotlib>=3.1.1', 'sshtunnel>=0.3.1', 'dateparser>=1.0.0',
                'google-api-python-client>=1.12.8', 'google-auth-httplib2>=0.0.4',
                'google-auth-oauthlib>=0.4.2', 'google-auth>=1.24.0',
                'httplib2>=0.18.1', 'pyarrow>=11.0.0', 'bs4>=0.0.1'
                ]


# Collect arguments
parser = argparse.ArgumentParser()
parser.add_argument("-v", "--version", default=None, help="New version to load")
parser.add_argument("-t", "--test", action="store_true", help="Publish to PyPi test")
parser.add_argument("-p", "--publish", action="store_true", help="Publish to Git")
parser.add_argument("-m", "--message", default="", help="Message for the Git commit")

args = parser.parse_args()

if sys.platform in ['darwin']:
    path = f'/Users/{getpass.getuser()}/Documents/'
elif sys.platform in ['linux']:
    path = f'/home/{getpass.getuser()}/'
elif sys.platform in ['win32']:
    path = f'C:/Users/{getpass.getuser()}/Documents/'

lib_path = path + library

# Set up working directory
os.chdir(lib_path)


# Define new version number is not provided in arguments
if args.version is None:
    if args.test:
        url = f'https://test.pypi.org/project/{library}/'
    else:
        url = f'https://pypi.org/project/{library}/'

    response = get(url)

    soup = BeautifulSoup(response.text, 'html.parser')

    version_tag = soup.findAll("h1", {"class": "package-header__name"})
    version = str(version_tag[0]).split('\n')[1].split(' ')[-1]
    version_splitted = version.split('.')
    # Define new version
    version_splitted_new = version_splitted.copy()
    version_splitted_new[2] = str(int(version_splitted_new[2]) + 1)
    new_version = '.'.join(version_splitted_new)
else:
    new_version = args.version

# Load the setup template file
with open(lib_path + '/setup_template.py') as f:
    template = f.read()

all_reqs = '"' + '",\n          "'.join(requirements) + '"'

# Update template
template = template.format(library=library.lower(), version=new_version, desc=desc, requirements=all_reqs)

# And write the new setup file
with open(lib_path + '/setup_new.py', "w") as f:
    f.write(template)


# Load the init template file
with open(lib_path + f'/{library.lower()}/init_template.py') as f:
    init_template = f.read()

# Update init_template
init_template = init_template.format(version=new_version)

# And write the new init file
with open(lib_path + f'/{library.lower()}/__init__.py', "w") as f:
    f.write(init_template)


# Delete files to load
os.system(f'rm {lib_path}/dist/*')

# Execute the setup file
os.system(f'python3 {lib_path}/setup_new.py sdist bdist_wheel')


# Load pypi credentials
with open('/etc/.pycof/config.json') as config_file:
    config = json.load(config_file)

user = config.get('PYPI_USER')
pwd = config.get('PYPI_PASSWORD')

# Load to pypi.org
if args.test:
    # If test, we upload on pypi test
    test_dest = '--repository-url https://test.pypi.org/legacy/'
    os.system(f'python3 -m twine upload {test_dest} {lib_path}/dist/*')
else:
    # Else we publish on standard pypi
    os.system(f'python3 -m twine upload {lib_path}/dist/*')


# Commit to git and push
if args.publish:
    os.system("git add --all")
    os.system(f"git tag -a v{new_version} -m 'Version {new_version} on pypi. {args.message}'")
    os.system(f"git commit -a -m 'Upload version {new_version} to pypi. {args.message}'")
    os.system(f"git push origin v{new_version}")
    os.system("git push")
    git_update = 'and changes pushed to git'
else:
    git_update = ""

print(f'\n\n New version {new_version} loaded on PyPi {git_update}')
