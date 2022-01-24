"""Functions to validate air one extract, transform, & enrich function outputs."""

from typing import Union

from pandas import DataFrame
from pandera.errors import SchemaError

from oiflib.air.one.schemas import schema_enriched, schema_extracted, schema_transformed


def validate_air_one_extracted(df: DataFrame) -> Union[DataFrame, SchemaError]:
    """Validates a DataFrame against the air_one_extracted schema.

    If the input DataFrame passes the schema validation checks, it is returned.
    However, if it doesn't, an error is returned explaining which checks have
    failed.

    Args:
        df (DataFrame): A DataFrame to be validated.

    Returns:
        Union[DataFrame, SchemaError]: Either a valid DataFrame or, in the case of an
        invalid DataFrame, a SchemaError.
    """
    return schema_extracted(df)


def validate_air_one_transformed(df: DataFrame) -> Union[DataFrame, SchemaError]:
    """Validates a DataFrame against the air_one_transformed schema.

    If the input DataFrame passes the schema validation checks, it is returned.
    However, if it doesn't, an error is returned explaining which checks have
    failed.

    Args:
        df (DataFrame): A DataFrame to be validated.

    Returns:
        Union[DataFrame, SchemaError]: Either a valid DataFrame or, in the case of an
        invalid DataFrame, a SchemaError.
    """
    return schema_transformed(df)


def validate_air_one_enriched(df: DataFrame) -> Union[DataFrame, SchemaError]:
    """Validates a DataFrame against the air_one_enriched schema.

    If the input DataFrame passes the schema validation checks, it is returned.
    However, if it doesn't, an error is returned explaining which checks have
    failed.

    Args:
        df (DataFrame): A DataFrame to be validated.

    Returns:
        Union[DataFrame, SchemaError]: Either a valid DataFrame or, in the case of an
        invalid DataFrame, a SchemaError.
    """
    return schema_enriched(df)
