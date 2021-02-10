"""Functions for validating OIF DataFrames."""
from typing import Dict, Union

from dill import load  # noqa: S403 - security warnings n/a
from pandas import DataFrame
from pandera import DataFrameSchema
from pandera.errors import SchemaError


def _dict_from_path(file_path: str) -> Dict[str, Dict[str, Dict[str, DataFrameSchema]]]:
    """Returns dictionary of DataFrameSchema from file path.

    Args:
        file_path (str): Path to file containing dictionary of DataFrameSchema.

    Returns:
        Dict[str, Dict[str, Dict[str, DataFrameSchema]]]: Dictionary of
            DataFrameSchema.
    """
    with open(file=file_path, mode="rb") as file:
        dictionary: Dict[
            str, Dict[str, Dict[str, DataFrameSchema]]
        ] = load(  # noqa: S301 - security warnings n/a
            file
        )
    return dictionary


def _schema_from_dict(
    dict: Dict[str, Dict[str, Dict[str, DataFrameSchema]]],
    theme: str,
    indicator: str,
    stage: str,
) -> DataFrameSchema:
    """Returns DataFrameSchema from dictionary of DataFrameSchema.

    Args:
        dict (Dict[str, Dict[str, Dict[str, DataFrameSchema]]]): The dictionary of
            DataFrameSchema.
        theme (str): Theme name, as a lower case string.
        indicator (str): Indicator number, as a lower case string.
        stage (str): Processing stage, as lower case string.

    Returns:
        DataFrameSchema: DataFrameSchema for given theme, indicator, and stage.


    """
    return dict[theme][indicator][stage]


def validate(
    theme: str,
    indicator: str,
    stage: str,
    df: DataFrame,
    file_path: str = "/home/edfawcetttaylor/repos/oiflib/data/schema.pkl",
) -> Union[DataFrame, SchemaError]:
    """Validates a DataFrame against a DataFrameSchema.

    If the input DataFrame passes the schema validation checks, it is returned.
    However, if it doesn't, an error is returned explaining which checks have
    failed.

    Args:
        theme (str): Theme name, as a lower case string.
        indicator (str): Indicator number, as a lower case string.
        stage (str): Stage in pipeline, as lower case string.
        df (DataFrame): A DataFrame to be validated.
        file_path (str): Path to file containing dictionary of DataFrameSchema.

    Returns:
        Union[DataFrame, SchemaError]: Either a valid DataFrame or, in the case of an
        invalid DataFrame, a SchemaError.
    """
    dict: Dict[str, Dict[str, Dict[str, DataFrameSchema]]] = _dict_from_path(
        file_path=file_path,
    )

    schema: DataFrameSchema = _schema_from_dict(
        dict=dict,
        theme=theme,
        indicator=indicator,
        stage=stage,
    )

    return schema(df)