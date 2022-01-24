"""Schema for Air Five DataFrames."""
from typing import Dict, Optional, Tuple, Union

from boto3 import resource
from dill import dump, dumps, load, loads  # noqa: S403 - security warnings n/a
from pandera import DataFrameSchema


def get(
    source: Union[str, Tuple[str, str]] = ("s3-ranch-019", "schemas.pkl"),
) -> Dict[str, Dict[str, Dict[str, DataFrameSchema]]]:
    """[summary].

    >>> oif_schema_dictionary = get()

    Args:
        source (Union[str, Tuple[str, str]]): [description]. Defaults to
            ("s3-ranch-019", "schemas.pkl").

    Raises:
        NotImplementedError: [description]

    Returns:
        Dict[str, Dict[str, Dict[str, DataFrameSchema]]]: [description]
    """
    if isinstance(source, tuple):
        s3_resource = resource("s3")
        return loads(  # noqa: S301 - security warnings n/a
            s3_resource.Object(
                bucket_name=source[0],
                key=source[1],
            )
            .get()["Body"]
            .read()
        )
    elif isinstance(source, str):
        with open(source, "rb") as file:
            return load(file)  # noqa: S301 - security warnings n/a
    else:
        raise NotImplementedError("# TODO")


def update(
    dictionary_one: Dict[
        Optional[str], Dict[Optional[str], Dict[Optional[str], DataFrameSchema]]
    ],
    dictionary_two: Union[
        Dict[Optional[str], Dict[Optional[str], Dict[Optional[str], DataFrameSchema]]],
        Dict[Optional[str], Dict[Optional[str], DataFrameSchema]],
        Dict[Optional[str], DataFrameSchema],
    ],
    theme: Optional[str] = None,
    indicator: Optional[str] = None,
    stage: Optional[str] = None,
) -> Dict[Optional[str], Dict[Optional[str], Dict[Optional[str], DataFrameSchema]]]:
    """[summary].

    To update an indicator within an existing theme or add a new indicator to an
    existing theme:

    Read the existing schema dictionary into memory:
    >>> old_oif_schema_dictionary = get()

    Define a new dictionary for the schema you want to add or update:
    >>> new_indicator_dictionary = {
            {"three":
                "formatted": DataFrameSchema(
                    ...
                ),
            },
        }

    Specify the name of the theme within the update function:
    >>> new_oif_schema_dictionary = update(
        dictionary_one=old_oif_schema_dictionary,
        dictionary_two=new_indicator_dictionary,
        theme="air",
    )

    To update a stage within an existing indicator or add a new stage to an
    existing indicator:

    Read the existing schema dictionary into memory:
    >>> old_oif_schema_dictionary = get()

    Define a new dictionary for the schema you want to add or update:
    >>> new_stage_dictionary = {
            "formatted": DataFrameSchema(
                ...
            ),
        }

    Specify the name of the theme and indicator within the update function:
    >>> new_oif_schema_dictionary = update(
        dictionary_one=old_oif_schema_dictionary,
        dictionary_two=new_indicator_dictionary,
        theme="air",
        indicator="six",
    )

    Args:
        dictionary_one (Dict[str, Dict[str, Dict[str, DataFrameSchema]]]): [description]
        dictionary_two (Union[ Dict[str, Dict[str, Dict[str, DataFrameSchema]]],
            Dict[str, Dict[str, DataFrameSchema]], Dict[str, DataFrameSchema], ]):
            [description]
        theme (Optional[str], optional): [description]. Defaults to None.
        indicator (Optional[str], optional): [description]. Defaults to None.
        stage (Optional[str], optional): [description]. Defaults to None.

    Raises:
        ValueError: [description]

    Returns:
        Dict[str, Dict[str, Dict[str, DataFrameSchema]]]: [description]
    """
    if not any([theme, indicator, stage]):
        dictionary_one.update(dictionary_two)
    elif theme and not any([stage, indicator]):
        dictionary_one[theme].update(dictionary_two)
    elif all([theme, indicator]) and not stage:
        dictionary_one[theme][indicator].update(dictionary_two)
    elif all([theme, indicator, stage]):
        dictionary_one[theme][indicator][stage].update(dictionary_two)
    else:
        raise ValueError("# TODO")

    return dictionary_one


def put(
    dictionary: Dict[str, Dict[str, Dict[str, DataFrameSchema]]],
    destination: Union[str, Tuple[str, str]] = ("s3-ranch-019", "schemas.pkl"),
) -> None:
    """[summary].

    >>> put(
    dictionary=new_dict,
    )

    Args:
        dictionary (Dict[str, Dict[str, Dict[str, DataFrameSchema]]]): [description]
        destination (Union[str, Tuple[str, str]], optional): [description]. Defaults to
            ("s3-ranch-019", "schemas.pkl").

    Raises:
        NotImplementedError: [description]
    """
    if isinstance(destination, tuple):
        s3_resource = resource("s3")

        pickled_obj: bytes = dumps(dictionary)

        s3_resource.Object(bucket_name=destination[0], key=destination[1],).put(
            ACL="bucket-owner-full-control",
            Body=pickled_obj,
        )

    elif isinstance(destination, str):

        with open(destination, "wb") as file:
            dump(dictionary, file)

    else:
        raise NotImplementedError("# TODO")
