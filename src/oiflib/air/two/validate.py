"""Validation functions for Air Two DataFrames."""

from typing import Union

from pandas import DataFrame
from pandera.errors import SchemaError

from oiflib.air.two.schemas import schema_enriched, schema_extracted, schema_transformed


def validate_air_two_extracted(df: DataFrame) -> Union[DataFrame, SchemaError]:
    """Validates a DataFrame against the air_two_extracted schema.

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


def validate_air_two_transformed(df: DataFrame) -> Union[DataFrame, SchemaError]:
    """Validates a DataFrame against the air_two_extracted schema.

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


def validate_air_two_enriched(df: DataFrame) -> Union[DataFrame, SchemaError]:
    """Validates a DataFrame against the air_two_extracted schema.

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
