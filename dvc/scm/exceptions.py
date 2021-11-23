class SCMError(Exception):
    """Base class for source control management errors."""


class GitHookAlreadyExists(SCMError):
    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"Hook '{name}' already exists")
