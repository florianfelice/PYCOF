import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pycof",
    version="1.5.8",
    author="Florian Felice",
    author_email="admin@florianfelice.com",
    description="A package for commonly used functions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://www.florianfelice.com/pycof",
    packages=setuptools.find_packages(),
    install_requires=[
          "pandas>=0.24.1",
          "numpy>=1.16.3",
          "psycopg2-binary>=2.7.4",
          "pymysql>=0.9.3",
          "tqdm>=4.35.0",
          "boto3>=1.16.19",
          "xlrd>=1.2.0",
          "matplotlib>=3.1.1",
          "sshtunnel>=0.3.1",
          "dateparser>=1.0.0",
          "google-api-python-client>=1.12.8",
          "google-auth-httplib2>=0.0.4",
          "google-auth-oauthlib>=0.4.2",
          "google-auth>=1.24.0",
          "httplib2>=0.18.1",
          "pyarrow>=11.0.0",
          "bs4>=0.0.1"
      ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)