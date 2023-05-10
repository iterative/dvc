from typing import TYPE_CHECKING

from dvc.exceptions import DvcException

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.scm import Git


def pre_commit_install(scm: "Git") -> None:
    import os

    from dvc.utils.serialize import modify_yaml

    config_path = os.path.join(scm.root_dir, ".pre-commit-config.yaml")
    with modify_yaml(config_path) as config:
        entry = {
            "repo": "https://github.com/iterative/dvc",
            "rev": "main",
            "hooks": [
                {
                    "id": "dvc-pre-commit",
                    "additional_dependencies": [".[all]"],
                    "language_version": "python3",
                    "stages": ["commit"],
                },
                {
                    "id": "dvc-pre-push",
                    "additional_dependencies": [".[all]"],
                    "language_version": "python3",
                    "stages": ["push"],
                },
                {
                    "id": "dvc-post-checkout",
                    "additional_dependencies": [".[all]"],
                    "language_version": "python3",
                    "stages": ["post-checkout"],
                    "always_run": True,
                },
            ],
        }

        config["repos"] = config.get("repos", [])
        if entry not in config["repos"]:
            config["repos"].append(entry)


def install_hooks(scm: "Git") -> None:
    from scmrepo.exceptions import GitHookAlreadyExists

    from dvc.utils import format_link

    hooks = ["post-checkout", "pre-commit", "pre-push"]
    for hook in hooks:
        try:
            scm.verify_hook(hook)
        except GitHookAlreadyExists as exc:
            link = format_link("https://man.dvc.org/install")
            raise DvcException(  # noqa: B904
                f"{exc}. Please refer to {link} for more info."
            )

    for hook in hooks:
        scm.install_hook(hook, f"exec dvc git-hook {hook} $@")


def install(self: "Repo", use_pre_commit_tool: bool = False) -> None:
    """Adds dvc commands to SCM hooks for the repo.

    If use_pre_commit_tool is set and pre-commit is installed it will be used
    to install the hooks.
    """
    from dvc.scm import Git

    scm = self.scm
    if not isinstance(scm, Git):
        return

    driver = "dvc git-hook merge-driver --ancestor %O --our %A --their %B "
    scm.install_merge_driver("dvc", "DVC merge driver", driver)

    if use_pre_commit_tool:
        return pre_commit_install(scm)

    return install_hooks(scm)
