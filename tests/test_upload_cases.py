"""Cases for testing upload module functions."""
from pytest_cases import parametrize


class Stage:
    """# TODO [summary]."""

    @parametrize(stage=("got", "extracted", "transformed", "enriched"))
    def case_success(self, stage) -> str:
        """# TODO [summary]."""
        return "%s" % stage

    def case_failure(self) -> str:
        """# TODO [summary]."""
        return "failure"
