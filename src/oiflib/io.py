"""Input/output functions."""
from typing import Optional, Union

from pandas import DataFrame

from oiflib.push import _set_data_file_name


def _write_csv_to_local(
    df: DataFrame,
    root: str,
    repo: str,
    data_folder: str,
    data_file_name: str,
) -> str:
    """[summary].

    Args:
        df (DataFrame): [description]
        root (str): [description]
        repo (str): [description]
        data_folder (str): [description]
        data_file_name (str): [description]

    Returns:
        str: [description]
    """
    file_path: str = f"{root}/{repo}/{data_folder}/{data_file_name}"

    df.to_csv(
        path_or_buf=file_path,
        index=False,
    )

    return file_path


def _write_csv_to_s3(
    df: DataFrame,
    bucket: str,
    stage: str,
    data_file_name: str,
    return_object: str = "df",
) -> Union[str, DataFrame]:
    """[summary].

    Args:
        df (DataFrame): [description]
        bucket (str): [description]
        stage (str): [description]
        data_file_name (str): [description]
        return_object (str): [description]. Defaults to "df".

    Returns:
        str: [description]
    """
    file_path: str = f"s3://{bucket}/{stage}/{data_file_name}"

    df.to_csv(
        path_or_buf=file_path,
        index=False,
    )

    if return_object == "df":
        return df
    else:
        return file_path


def save(
    theme: str,
    indicator: str,
    stage: str,
    df: DataFrame,
    return_df: bool = True,
) -> Optional[DataFrame]:
    """[summary].

    Args:
        theme (str): [description]
        indicator (str): [description]
        stage (str): [description]
        df (DataFrame): [description]
        return_df (bool): [description]. Defaults to True.

    Returns:
        DataFrame: [description]
    """
    _data_file_name: str = _set_data_file_name(
        theme=theme,
        indicator=indicator,
    )

    _file_path: str = f"s3://s3-ranch-019/{stage}/{_data_file_name}"

    df.to_csv(
        path_or_buf=_file_path,
        index=False,
        storage_options={
            "s3_additional_kwargs": {
                "ACL": "bucket-owner-full-control",
            },
        },
    )

    if return_df:
        return df
    else:
        return None
