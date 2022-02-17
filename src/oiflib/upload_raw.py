"""Upload raw data to S3."""
from functools import singledispatch
from io import StringIO
from os.path import basename
from typing import Dict, Optional, Union

from boto3 import resource
from pandas import DataFrame
from requests import get


@singledispatch
def upload_raw(
    df_or_path: Union[DataFrame, str],
    indicator_code: str,
    year: str,
    bucket_name: str = "s3-ranch-029",
    acl: str = "bucket-owner-full-control",
    filename: Optional[str] = None,
    key: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
) -> None:
    """Upload raw data to S3.

    Examples:

        Get file from `https://url/for/raw/data.xlsx` and upload it to
        `S3://s3-ranch-029/2022_update/raw/A1/data.xlsx`.

        >>> upload_raw(
            indicator_code="A1",
            df_or_path="https://url/for/raw/data.xlsx",
            year="2022",
        )

        Upload a local file to
        `S3://s3-ranch-029/2022_update/raw/A1/data.xlsx`.

        >>> upload_raw(
            indicator_code="A1",
            df_or_path="/path/to/data.xlsx",
            year="2022",
        )

    Args:
        df_or_path (Union[DataFrame, str]): An in-memory DataFrame or the path
            to the data you want to upload. This can be a URL or a local filepath.
        indicator_code (str): Indicator code as a string, e.g. "A1".
        year (str): The publication date of the release that this data will
            feed into, e.g. "2022".
        bucket_name (str): The name of the S3 bucket that you want to upload
            data. Defaults to "s3-ranch-029".
        acl (str): The canned ACL to apply to the object. For more information,
            see `Canned ACL`_. Defaults to "bucket-owner-full-control".
        filename (str, optional): The name you want to give the uploaded file. This
            is required if you're uploading an in-memory DataFrame but can also be
            used if you want to save a file you're uploading from a local path or
            a url as something different. Defaults to None.
        key (str, optional): The object key identifier, the filepath-like string
            used to identify the object. If set to None, this will be constructed
            from other arguments, e.g.
            "{year}_update/raw/{indicator_code}/{filename)}". Defaults to
            None.
        headers (Dict[str, str]], optional): HTTP headers to be passed to the
            `Requests`_ library's get function. If left as None, a `user-agent`_ header
            will be added. For more information see the `Custom Headers`_ section
            of Requests' documentation. Defaults to None.

    Raises:
        ValueError: If you try and upload an in-memory DataFrame without providing a
            filename or full S3 key.

    .. _Canned ACL: https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl
    .. _Requests: https://docs.python-requests.org/en/latest/
    .. _user-agent: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/User-Agent
    .. _Custom Headers: https://docs.python-requests.org/en/latest/user/quickstart/#custom-headers
    """  # noqa: B950, D412, DAR402
    pass


@upload_raw.register
def _(
    df_or_path: DataFrame,
    indicator_code: str,
    year: str,
    bucket_name: str = "s3-ranch-029",
    acl: str = "bucket-owner-full-control",
    key: Optional[str] = None,
    filename: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
) -> None:

    s3_resource = resource("s3")
    bucket = s3_resource.Bucket(bucket_name)

    csv_buffer = StringIO()
    df_or_path.to_csv(csv_buffer)

    if filename is None and key is None:
        raise ValueError("You must provide a filename or a full S3 key.")

    _key = key if key else f"{year}_update/raw/{indicator_code}/{filename}"

    bucket.put_object(
        ACL=acl,
        Body=csv_buffer.getvalue(),
        Key=_key,
    )


@upload_raw.register
def _(
    df_or_path: str,
    indicator_code: str,
    year: str,
    bucket_name: str = "s3-ranch-029",
    acl: str = "bucket-owner-full-control",
    key: Optional[str] = None,
    filename: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
) -> None:

    s3_resource = resource("s3")
    bucket = s3_resource.Bucket(bucket_name)

    _filename = filename if filename else basename(df_or_path)

    _key = key if key else f"{year}_update/raw/{indicator_code}/{_filename}"

    if df_or_path.startswith(("https://", "http://")):

        _headers = (
            headers
            if headers
            else {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36 Edg/95.0.1020.44"  # noqa: B950
            }
        )

        response = get(
            df_or_path,
            headers=_headers,
        )

        response.raise_for_status()

        return bucket.put_object(
            ACL=acl,
            Body=response.content,
            Key=_key,
        )
    else:
        return bucket.upload_file(
            Filename=df_or_path,
            Key=_key,
            ExtraArgs={"ACL": acl},
        )
