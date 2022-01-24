Structure
=========

Project Structure
-----------------

```
src
|
|---oiflib
|   |
|   |---<theme>
|   |   |
|   |   |---<indicator>
|   |   |   |   <activity>.py
|   |   |   |   <activity>.py
|   |   |   |   ...
|   |   |
|   |   |---<indicator>
|   |   |   |   ...
|   |   |
|   |   |   ...
|   |
|   |---<theme>
|   |   |   ...
|   |
|   |   ...
```

.github/workflows
^^^^^^^^^^^^^^^^^

data
^^^^

docs
^^^^

scr/oiflib
^^^^^^^^^^

This project uses a `src package layout <https://hynek.me/articles/testing-packaging/>_`.

The `oiflib` package is divided into theme modules. These theme modules are divided into indicator modules.

test
^^^^

Indicator Module Structure
--------------------------

These indicator modules are divided into activity modules.

There are five activity modules under each indicator: extract, transform, enrich, schemas, validate.

extract
^^^^^^^

The extract modules contain functions for reading the input data into memory.

transform
^^^^^^^^^

The transform modules contain functions for transforming the input data into a tidy format.

enrich
^^^^^^

The enrich modules contain functions for adding derived columns and combing DataFrames.

schemas
^^^^^^^

The schema modules contain schemas that describe the properties of the DataFrames produced by the extract, transform, and enrich functions.

validate
^^^^^^^^

The validate modules contain functions to validate the DataFrames produced by the extract, transform, and enrich functions against their respective schema.
