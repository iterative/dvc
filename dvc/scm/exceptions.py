class SCMError(Exception):
    """Base class for source control management errors."""


class GitHookAlreadyExists(SCMError):
    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"Hook '{name}' already exists")


class FileNotInRepoError(SCMError):
    """Thrown when trying to find .gitignore for a file that is not in a scm
    repository.
    """


class MergeConflictError(SCMError):
    pass
