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


class InvalidRemote(SCMError):
    def __init__(self, url: str) -> None:
        self.url = url
        super().__init__(f"'{url}' is not a valid Git remote or URL")


class AuthError(SCMError):
    def __init__(self, url: str) -> None:
        self.url = url
        super().__init__(f"HTTP Git authentication is not supported: '{url}'")


class CloneError(SCMError):
    def __init__(self, url: str, path: str) -> None:
        self.url = url
        self.path = path
        super().__init__(f"Failed to clone repo '{url}' to '{path}'")


class RevError(SCMError):
    pass


class UnsupportedIndexFormat(SCMError):
    pass
