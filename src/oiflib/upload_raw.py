"""Upload raw data to S3."""
from os.path import basename
from typing import Dict, Optional

from boto3 import resource
from requests import get


def upload_raw(
    indicator_code: str,
    url: str,
    year: str,
    bucket_name: str = "s3-ranch-029",
    acl: str = "bucket-owner-full-control",
    key: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
) -> None:
    """Upload raw data to S3.

    Example:

    >>> upload_raw(
        indicator_code="A1",
        url="https://url/for/raw/data.xlsx",
        year="2022,
    )

    Args:
        indicator_code (str): Indicator code as a string, e.g. "A1".
        url (str): URL for the data you want to upload.
        year (str): The publication date of the release that this data will
            feed into, e.g. "2022".
        bucket_name (str): The name of the S3 bucket that you want to upload
            data. Defaults to "s3-ranch-029".
        acl (str): The canned ACL to apply to the object. For more information,
            see `Canned ACL`_. Defaults to "bucket-owner-full-control".
        key (str, optional): The object key identifier, the filepath-like string
            used to identify the object. If set to None, this will be constructed
            from other arguments, e.g.
            "{year}_update/raw/{indicator_code}/{basename(url)}". Defaults to
            None.
        headers (Dict[str, str]], optional): HTTP headers to be passed to the
            `Requests`_ library's get function. If left as None, a `user-agent`_ header
            will be added. For more information see the `Custom Headers`_ section
            of Requests' documentation. Defaults to None.

    Returns:
        [type]: [description]

    .. _Canned ACL: https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl
    .. _Requests: https://docs.python-requests.org/en/latest/
    .. _user-agent: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/User-Agent
    .. _Custom Headers: https://docs.python-requests.org/en/latest/user/quickstart/#custom-headers
    """  # noqa: B950
    s3 = resource("s3")
    bucket = s3.Bucket(bucket_name)

    _headers = (
        headers
        if headers
        else {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36 Edg/95.0.1020.44"  # noqa: B950
        }
    )

    response = get(
        url,
        headers=_headers,
    )

    response.raise_for_status()

    _key = key if key else f"{year}_update/raw/{indicator_code}/{basename(url)}"

    return bucket.put_object(
        ACL=acl,
        Body=response.content,
        Key=_key,
    )
