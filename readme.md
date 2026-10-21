# General 

Package containing resuable code components for data pipelines and dags deployed to pypi.

# Usage

Install/Upgrade locally: 

```
$ pip3 install pyveb
$ pip3 install pyveb --upgrade
```

Import in python

```
import pyveb
from pyveb import selenium_client
```

# Update package

Package is automaticly deployed to pypi via github actions. Just commit and open a pull request. During the action workflow, the version will be automatically bumped and updated pyproject.toml is commited back. 

! in case a dependency is added to pyproject.toml, no workflow is started unless there are also changes to src/pyveb/** 

# SQL scripts

Naast het package bevat deze repo ook SQL die manueel uitgevoerd werd, ter traceerbaarheid.
Deze map valt buiten `src/pyveb/**`: ze wordt niet gepackaged en triggert geen deploy.

- `sql/redshift/datashare_prd_to_dev/`: datashare van Redshift PRD naar DEV
  (`terdwh_prd_datashare` read-only, `terdwh_uat` read/write), uitgevoerd op 2026-10-15.
  Scripts in uitvoeringsvolgorde; context, verbeterpunten en next steps staan bovenaan elk script.
