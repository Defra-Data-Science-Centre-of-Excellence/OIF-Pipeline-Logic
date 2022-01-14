"""A function to upload processed data to the GitHub repo.

The OpenSDG platform requires data to be uploaded to the site's GitHub repo as a
CSV. Given a DataFrame and an indicator code, this function saves an appropriately
named CSV and then uploads it to GitHub.

Example:
    >>> publish(
        df=A1_formatted_validated,
        indicator_code="A1",
    )

To do this the :func:`publish` function does the following:

#. it resets the local git repository to match the remote repository.
#. it to convert the OIF alphanumeric code to an OpenSDG compatable three-part numeric
   code.
#. it write the processed DataFrame to the local repository.
#. it adds the file to the local repository's index.
#. it records the changes to the local repository.
#. it pushes those local changes to the remote repostory.

.. note::
    SDG uses a three part Goal-Target-Indicator (GTI) numeric code, so the name of the
    CSV file must follow the format `indicator_#-#-#.csv`, where each # is an integer
    identifying, respectively, the goal, target, and indicator.

    To make OIF fit the SDG convention, this function uses a look-up to convert the
    alphanumeric OIF codes into a three part Theme-Indicator-Chart (TIC) format. For
    example, OIF's A1 indicator would have a TIC code of 1-1-1, whereas, OIF's C1b
    indicator would have a TIC code of 3-1-2.

"""
from logging import INFO, basicConfig, info
from os import environ
from typing import Optional, Tuple, Union

from git import Repo
from pandas import DataFrame

from oiflib._helper import _oiflib_to_sdg_lookup


def _reset_local_branch(
    root: str,
    repo: str,
    remote: str,
) -> None:
    """Overwrites local with remote repository.

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`publish`.

    Example:
        >>> _reset_local_branch(
            root="/home/oif",
            repo="OIF-Dashboard",
            remote="origin",
        )

    Args:
        root (str): The path to the repository. This will be prepended to `repo` to
            create a full path.
        repo (str): The name of the repository. This will be appended to `root` to
            create a full path.
        remote (str): The remote you want to overwrite the local repository with. If
            you want to reset your local repo to match the remote repo on GitHub, this
            will be "origin".
    """
    _repo: Repo = Repo(f"{root}/{repo}")

    _repo.remotes[remote].fetch()

    _repo.head.reset(commit=remote)


def _set_data_file_name(
    indicator_code: str,
) -> str:
    """Given theme and indicator, sets Theme-Indicator-Chart filename.

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`publish`.

    Example:
        >>> file_name = _set_data_file_name(
            indicator_code="A1",
        )
        indicator_1-1-1.csv

    Args:
        indicator_code (str): Indicator code, as a string. E.g. "A1".

    Returns:
        str: Theme-Indicator-Chart filename as string.
    """
    tic_code: str = _oiflib_to_sdg_lookup.get(indicator_code, "0-0-0")
    return f"indicator_{tic_code}.csv"


def _write_to_csv(
    df: DataFrame,
    root: str,
    repo: str,
    data_folder: str,
    data_file_name: str,
) -> str:
    """Writes the DataFrame to local repo and returns path as string.

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`publish`.

    Example:
        >>> file_name = _set_data_file_name(
            indicator_code="A1",
        )

        >>> _write_to_csv(
            df=A1_formatted,
            root="/home/oif",
            repo="OIF-Dashboard",
            data_folder="data",
            data_file_name=file_name,
        )

    Args:
        df (DataFrame): A DataFrame to write.
        root (str): The path to the repository, as a string. This will be prepended to
            `repo` to create a full path.
        repo (str): The name of the repository, as a string. This will be appended to
            `root` to create a full path.
        data_folder (str): The name of the folder containing site data, as a string.
        data_file_name (str): A Theme-Indicator-Chart style filename as a string.

    Returns:
        str: Path to local file as string.
    """
    file_path: str = f"{root}/{repo}/{data_folder}/{data_file_name}"

    df.to_csv(
        path_or_buf=file_path,
        index=False,
    )

    return file_path


def _add(
    root: str,
    repo: str,
    data_folder: str,
    data_file_name: str,
) -> None:
    """Adds a file to the local repo's index.

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`publish`.

    Example:
        >>> file_name = _set_data_file_name(
            indicator_code="A1",
        )

        >>> _write_to_csv(
            df=A1_formatted,
            root="/home/oif",
            repo="OIF-Dashboard",
            data_folder="data",
            data_file_name=file_name,
        )

        >>> _add(
            root="/home/oif",
            repo="OIF-Dashboard",
            data_folder="data",
            data_file_name=file_name,
        )

    Args:
        root (str): The path to the repository, as a string. This will be prepended to
            `repo` to create a full path.
        repo (str): The name of the repository, as a string. This will be appended to
            `root` to create a full path.
        data_folder (str): The name of the folder containing site data, as a string.
        data_file_name (str): A Theme-Indicator-Chart style filename as a string.
    """
    _repo: Repo = Repo(f"{root}/{repo}")

    _repo.index.add(f"{data_folder}/{data_file_name}")


def _commit(
    root: str,
    repo: str,
    indicator_code: str,
    data_commit_message: Optional[str],
) -> None:
    """Commits any changes to the local repo.

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`publish`.

    Example:
        >>> file_name = _set_data_file_name(
            indicator_code="A1",
        )

        >>> _write_to_csv(
            df=A1_formatted,
            root="/home/oif",
            repo="OIF-Dashboard",
            data_folder="data",
            data_file_name=file_name,
        )

        >>> _add(
            root="/home/oif",
            repo="OIF-Dashboard",
            data_folder="data",
            data_file_name=file_name,
        )

        To commit with the default generated message:

        >>> _commit(
            root="/home/oif",
            repo="OIF-Dashboard",
            indicator_code="A1",
        )

        To commit with your own message

        >>> _commit(
            root="/home/oif",
            repo="OIF-Dashboard",
            indicator_code="A1",
            data_commit_message="my commit message"
        )

    Args:
        root (str): The path to the repository, as a string. This will be prepended to
            `repo` to create a full path.
        repo (str): The name of the repository, as a string. This will be appended to
            `root` to create a full path.
        indicator_code (str): Indicator code, as a string. E.g. "A1".
        data_commit_message (Optional[str]): If no message is provided, one will be
            generated using the `indicator_code`, otherwise the provided message will
            be used.
    """
    _repo: Repo = Repo(f"{root}/{repo}")

    if not data_commit_message:
        data_commit_message = f"add data for {indicator_code}"

    _repo.index.commit(data_commit_message)


def _push(
    root: str,
    repo: str,
    remote: str,
    branches: Union[str, Tuple[str, str]],
) -> None:
    """Pushes local changes to the remote repo.

    .. warning::
        This is a private function. It is not intended to be called directly. It is
        called within :func:`publish`.

    Example:
        >>> _reset_local_branch(
            root="/home/oif",
            repo="OIF-Dashboard",
            remote="origin",
        )

        >>> file_name = _set_data_file_name(
            indicator_code="A1",
        )

        >>> _write_to_csv(
            df=A1_formatted,
            root="/home/oif",
            repo="OIF-Dashboard",
            data_folder="data",
            data_file_name=file_name,
        )

        >>> _add(
            root="/home/oif",
            repo="OIF-Dashboard",
            data_folder="data",
            data_file_name=file_name,
        )

        >>> _commit(
            root="/home/oif",
            repo="OIF-Dashboard",
            indicator_code="A1",
        )

        To push the local branch to the same branch on the remote:

        >>> _push(
            root="/home/oif",
            repo="OIF-Dashboard",
            remote="origin",
            branches="develop",
        )

        To push the local branch to a different branch on the remote:

        >>> _push(
            root="/home/oif",
            repo="OIF-Dashboard",
            remote="origin",
            branches=("develop", "main"),
        )

    Args:
        root (str): The path to the repository, as a string. This will be prepended to
            `repo` to create a full path.
        repo (str): The name of the repository, as a string. This will be appended to
            `root` to create a full path.
        remote (str): The remote you want to overwrite the local repository with. If
            you want to reset your local repo to match the remote repo on GitHub, this
            will be "origin".
        branches (Union[str, Tuple[str, str]]): If you want to push the local branch to
            the same branch on the remote, provide the branch name as a string. However,
            if you want push the local branch to a different branch on the remote,
            provide the local and remote branch names as a tuple, e.g.
            ("local-branch-name", "remote-branch-name").
    """
    _repo: Repo = Repo(f"{root}/{repo}")

    _refspec: str

    if isinstance(branches, tuple):
        _refspec = f"{branches[0]}:{branches[1]}"
    else:
        _refspec = f"{branches}:{branches}"

    _repo.remotes[remote].push(refspec=_refspec)


def publish(
    df: DataFrame,
    indicator_code: str,
    remote: str = "origin",
    branches: Union[str, Tuple[str, str]] = "develop",
    repo: str = "OIF-Dashboard",
    data_folder: str = "data",
    root: Optional[str] = None,
    data_commit_message: Optional[str] = None,
) -> None:
    """Publishes an in-memory DataFrame to a remote git repository.

    The OpenSDG platform requires data to be uploaded to the site's GitHub repo as a
    CSV. Given a DataFrame and an indicator code, this function saves an appropriately
    named CSV and then uploads it to GitHub.

    Example:
        >>> publish(
            df=A1_formatted,
            indicator_code="A1",
        )

    Under the hood, this function calls:

    * :func:`_reset_local_branch` to reset the local to match the remote repository.
    * :func:`_write_to_csv` to write the DataFrame to the local repository.
    * :func:`_add` to add the file to the local repository's index.
    * :func:`_commit` to record the changes to the local repository.
    * :func:`_push` to push those local changes to the remote repostory.

    .. note::
        SDG uses a three part Goal-Target-Indicator (GTI) numeric code, so the name of
        the CSV file must follow the format `indicator_#-#-#.csv`, where each # is an
        integer identifying, respectively, the goal, target, and indicator.

        To make OIF fit the SDG convention, this function uses a look-up to convert the
        alphanumeric OIF codes into a three part Theme-Indicator-Chart (TIC) format. For
        example, OIF's A1 indicator would have a TIC code of 1-1-1, whereas, OIF's C1b
        indicator would have a TIC code of 3-1-2.

    Args:
        df (DataFrame): A DataFrame to write.
        indicator_code (str): Indicator code, as a string. E.g. "A1".
        remote (str): The remote you want to overwrite the local repository with. If
            you want to reset your local repo to match the remote repo on GitHub, this
            will be "origin". Defaults to "origin".
        branches (Union[str, Tuple[str, str]]): If you want to push the local branch to
            the same branch on the remote, provide the branch name as a string. However,
            if you want push the local branch to a different branch on the remote,
            provide the local and remote branch names as a tuple, e.g.
            ("local-branch-name", "remote-branch-name"). Defaults to
            "develop".
        repo (str): The name of the repository, as a string. This will be appended to
            `root` to create a full path. Defaults to "OIF-Dashboard".
        data_folder (str): The name of the folder containing site data, as a string.
            Defaults to "data".
        root (Optional[str]): The path to the repository, as a string. This will be
            prepended to `repo` to create a full path. If no `root` is provided, the
            value of the `$HOME` environmental variable will be used. Defaults to None.
        data_commit_message (Optional[str]): If no message is provided, one will be
            generated using the `indicator_code`, otherwise the provided message will
            be used.
    """
    basicConfig(
        level=INFO,
    )

    if not root:
        root = environ["HOME"]

    _reset_local_branch(
        root=root,
        repo=repo,
        remote=remote,
    )

    info("Reset local branch.")

    _data_file_name: str = _set_data_file_name(
        indicator_code=indicator_code,
    )

    info("Set file name.")

    _write_to_csv(
        df=df,
        root=root,
        repo=repo,
        data_folder=data_folder,
        data_file_name=_data_file_name,
    )

    info("Wrote CSV.")

    _add(
        root=root,
        repo=repo,
        data_folder=data_folder,
        data_file_name=_data_file_name,
    )

    info("Added CSV to git index.")

    _commit(
        root=root,
        repo=repo,
        indicator_code=indicator_code,
        data_commit_message=data_commit_message,
    )

    info("Committed CSV to local repository.")

    _push(
        root=root,
        repo=repo,
        remote=remote,
        branches=branches,
    )

    info("Pushed CSV to remote repository.")
