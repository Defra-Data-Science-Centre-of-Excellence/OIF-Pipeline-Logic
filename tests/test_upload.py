"""Tests for upload module functions."""
from pytest import raises
from pytest_cases import parametrize_with_cases

from oiflib.upload import _check_stage

from .test_upload_cases import Stage


@parametrize_with_cases("stage", cases=Stage, glob="*success")
def test__check_stage_success(
    stage: str,
) -> None:
    """# TODO [summary]."""
    assert _check_stage(stage=stage) is None


@parametrize_with_cases("stage", cases=Stage, glob="*failure")
def test__check_stage_failure(
    stage: str,
) -> None:
    """# TODO [summary]."""
    with raises(KeyError):
        _check_stage(stage=stage)
