"""Nox sessions."""
from sys import executable

import nox
from nox_poetry import Session, session

package = "oiflib"
python_versions = "3.10"
nox.options.sessions = "isort", "lint", "safety", "mypy", "tests"
locations = "src", "tests", "noxfile.py", "docs/source/conf.py"


@session(python=python_versions)
def isort(session: Session) -> None:
    """Sort imports with isort."""
    args = session.posargs or locations
    session.install("isort")
    session.run("isort", *args)


@session(python=python_versions)
def black(session: Session) -> None:
    """Run black code formatter."""
    args = session.posargs or locations
    session.install("black")
    session.run("black", *args)


@session(python=python_versions)
def lint(session: Session) -> None:
    """Lint using flake8."""
    args = session.posargs or locations
    session.install(
        "flake8",
        "flake8-annotations",
        "flake8-bandit",
        "flake8-black",
        "flake8-bugbear",
        "flake8-docstrings",
        "flake8-isort",
        "darglint",
    )
    session.run("flake8", *args)


@session(python="3.10")
def safety(session: Session) -> None:
    """Scan dependencies for insecure packages."""
    requirements = session.poetry.export_requirements()
    session.install("safety")
    session.run("safety", "check", "--full-report", f"--file={requirements}")


@session(python=python_versions)
def mypy(session: Session) -> None:
    """Type-check using mypy."""
    args = session.posargs or locations
    deps = [".", "mypy", "pytest"]
    session.install(*deps)
    session.run("mypy", *args)
    if not session.posargs:
        session.run("mypy", f"--python-executable={executable}", "noxfile.py")


# TODO Capture all np DeprecationWarnings in one line
# TODO Move pytest config to pyproject.toml
@nox.session(python="3.8")
def tests(session: Session) -> None:
    """Run the test suite."""
    args = session.posargs or [
        "--cov",
        "-v",
        "-W ignore:`np.complex` is a deprecated:DeprecationWarning",
        "-W ignore:`np.int` is a deprecated:DeprecationWarning",
        "-W ignore:`np.float` is a deprecated:DeprecationWarning",
    ]
    session.install(".")
    session.install(
        "coverage[toml]",
        "poetry",
        "hypothesis",
        "moto",
        "pytest",
        "pytest_cases",
        "pytest-cov",
    )
    session.run("pytest", *args)


@nox.session(python="3.8")
def coverage(session: Session) -> None:
    """Upload coverage data."""
    session.install("coverage[toml]", "codecov")
    session.run("coverage", "xml", "--fail-under=0")
    session.run("codecov", *session.posargs)


@nox.session(python="3.8")
def docs(session: Session) -> None:
    """Build the documentation."""
    session.run("poetry", "install", "--no-dev", external=True)
    session.install("sphinx")
    session.run("sphinx-build", "docs/source", "docs/_build")
