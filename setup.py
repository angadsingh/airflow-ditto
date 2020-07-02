from setuptools import setup

# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='airflow-ditto',
    version='0.0.1.2',
    author="Angad Singh",
    author_email="angad.singh@trufactor.io",
    description="An airflow DAG transformation framework",
    long_description=long_description,
    long_description_content_type='text/markdown',
    license="Apache Software License (http://www.apache.org/licenses/LICENSE-2.0)",
    url="https://github.com/angadsingh/airflow-ditto",
    py_modules=['ditto'],
    install_requires=[
        'azure-mgmt-hdinsight~=1.5.1',
        'msrestazure~=0.6.3',
        'apache-airflow>=1.10.10,<=2.*',
        'azure-storage-blob==2.1.0',
        'azure-storage-common==2.1.0',
        'azure-storage-nspkg==3.1.0',
        'azure-datalake-store',
        'paramiko',
        'sshtunnel'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7'
)