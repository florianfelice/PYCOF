import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pycof",  # Replace with your own username
    version="1.5.0",
    author="Florian Felice",
    author_email="admin@florianfelice.com",
    description="A package for commonly used functions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/florianfelice/PYCOF",
    packages=setuptools.find_packages(),
    install_requires=[
        "pandas>=0.24.1",
        "numpy>=1.16.3",
        "psycopg2-binary>=2.7.4",
        "pymysql>=0.9.3",
        "tqdm>=4.35.0",
        "sshtunnel>=0.3.1",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
