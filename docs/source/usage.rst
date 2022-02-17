Usage
=====

The package provides generic `Upload Raw`_, Extract_, Validate_, Format_, and Publish_ functions and theme and indicator
specific Transform_ and Enrich_ functions.

Upload Raw
----------

.. automodule:: oiflib.upload_raw
    :noindex:


Extract
-------

.. automodule:: oiflib.extract
    :noindex:

Transform
---------

The library provides theme and indicator specific functions to convert an extracted
DataFrame into a tidy format by performing a series for pre-defined transformations,
such as selecting columns, filtering rows, and/or unpivoting wide-format DataFrames
into long-format DataFrames.

For example, the Air One :ref:`air_one_transform_module` module defines functions to
filter out unused rows, drop unused columns, clean values, and unpivot the data. These
functions can be called individually but they are intended to be called by a wrapper
function ``transform_air_one`` that calls them in a pre-defined order. The use of a
wrapper function should improve the readability of high-level code and allow future
developers to add additional transformations.

To import the ``transform_air_one`` function run:

>>> from oiflib.air.one import transform_air_one

To use it to transform the extracted Air One DataFrame run:

>>> transformed = transform_air_one(df=extracted)

Enrich
------

Similarly, the library provides theme and indicator specific functions to enrich a
transformed DataFrame by adding derived columns.

For example, the Air One :ref:`air_one_enrich_module` module defines a function to
index emissions to base year. Again this function could be called explicitly but it
is intended to be called via the wrapper ``enrich_air_one``.

To import the ``enrich_air_one`` function run:

>>> from oiflib.air.one import enrich_air_one

To use it to enrich the transformed Air One DataFrame run:

>>> enriched = enrich_air_one(df=transformed)

Validate
--------

.. automodule:: oiflib.validate
    :noindex:

Format
------

.. automodule:: oiflib.format
    :noindex:

Publish
-------

.. automodule:: oiflib.push
    :noindex:


The Full Workflow
-----------------

>>> # import oiflib functions
>>> from oiflib.core import extract, validate
>>> from oiflib.air.one import enrich_air_one, transform_air_one
>>>
>>> # extract and validate air one dataframe
>>> extracted = extract(theme="air", indicator="one")
>>> extracted_validated = validate(theme="air", indicator="one", stage="extracted", df=extracted)
>>>
>>> # transform and validate
>>> transformed = transform_air_one(extracted_validated)
>>> transformed_validated = validate(theme="air", indicator="one", stage="transformed", df=transformed)
>>>
>>> # enrich and validate
>>> enriched = enrich_air_one(transformed_validated)
>>> enriched_validated = validate(theme="air", indicator="one", stage="enriched", df=enriched)
