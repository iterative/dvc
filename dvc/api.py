from dvc.repo import Repo


def get_url(path, repo_dir=None, remote=None):
    """Returns an url of `path` in default or specified remote"""
    repo = Repo(repo_dir)
    out, = repo.find_outs_by_path(path)
    remote_obj = repo.cloud.get_remote(remote)
    return str(remote_obj.checksum_to_path_info(out.checksum))


def open(path, repo_dir=None, remote=None, mode="r", encoding=None):
    """Opens a specified resource as a file descriptor"""
    repo = Repo(repo_dir)
    return repo.open(path, remote=remote, mode=mode, encoding=encoding)
