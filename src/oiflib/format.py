"""Functions to format data for OpenSDG data repo."""
from typing import Dict, Optional

from pandas import DataFrame


def format(
    df: DataFrame,
    year_column: str,
    value_column: str,
    disaggregation_column: Optional[str],
    disaggregation_column_new: Optional[str],
) -> DataFrame:
    """Selects and renames columns to fit OpenSDG dataset format.

    The OpenSDG platform expects the first column to be "Year", the last to be "Value"
    and any in between to be disaggregations. This function re-orders and re-names the
    columns of a given DataFrame accordingly. It also gives the caller the opportunity
    to rename the disaggregation column.

    Example:
        >>> air_one_formatted = format(
            df=air_one_enriched_validated,
            year_column="EmissionYear",
            value_column="Index",
            disaggregation_column="ShortPollName",
            disaggregation_column_new="Pollutant",
        )

    Args:
        df (DataFrame): A DataFrame will columns to be re-order and re-named.
        year_column (str): The name of the column containing year data.
        value_column (str): The name of the column containing value data.
        disaggregation_column (Optional[str]): The name of the column containing
            disaggregation data, if one exists.
        disaggregation_column_new (Optional[str]): A new name for the column
            containing disaggregation data, if one is needed.

    Returns:
        DataFrame: A DataFrame with re-ordered and re-named columns.
    """
    columns: Dict[str, str]

    if not disaggregation_column:
        columns = {
            year_column: "Year",
            value_column: "Value",
        }
    elif not disaggregation_column_new:
        columns = {
            year_column: "Year",
            disaggregation_column: disaggregation_column,
            value_column: "Value",
        }
    else:
        columns = {
            year_column: "Year",
            disaggregation_column: disaggregation_column_new,
            value_column: "Value",
        }

    return df[columns.keys()].rename(columns=columns)
