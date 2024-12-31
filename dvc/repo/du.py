from typing import Any, Optional, Union


def du(
    url: str,
    path: Optional[str] = None,
    rev: Optional[str] = None,
    summarize: bool = False,
    config: Union[dict[str, Any], str, None] = None,
    remote: Optional[str] = None,
    remote_config: Optional[dict] = None,
):
    from dvc.config import Config

    from . import Repo

    if config and not isinstance(config, dict):
        config_dict = Config.load_file(config)
    else:
        config_dict = None

    with Repo.open(
        url,
        rev=rev,
        subrepos=True,
        uninitialized=True,
        config=config_dict,
        remote=remote,
        remote_config=remote_config,
    ) as repo:
        path = path or ""

        fs = repo.dvcfs

        if summarize or not fs.isdir(path):
            return [(path, fs.du(path, total=True))]

        ret = [
            (entry_path, fs.du(entry_path, total=True)) for entry_path in fs.ls(path)
        ]
        ret.append((path, sum(entry[1] for entry in ret)))
        return ret
