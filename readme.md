# General 

Package containing resuable code components for data pipelines and dags deployed to pypi.

# Usage

- Install/Upgrade locally: 

$ pip3 install pyveb
$ pip3 install pyveb --upgrade

- Import

import pyveb
from pyveb import selenium_client


# Update package

Upload package: https://packaging.python.org/en/latest/tutorials/packaging-projects/ 

1.  update version in ~/pyproject.toml
2.  deploy to pypi


TBD 

We bump the version automatically within the github action and commit the changes back to the branch

    - $ rm -rf dist
    - $ poetry
    - $ python3 -m twine upload dist/*
    - $ rm -rf dist

3.  provide username and password of AWS SSM prd/pypi at prompt

Credentials test: AWS SSM - test/pypi
Credentials PRD: AWS SSM - prd/pypi

username: veb_prd_user
password: *****









