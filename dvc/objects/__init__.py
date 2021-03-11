import itertools
import logging
from concurrent import futures

from dvc.exceptions import DvcException
from dvc.progress import Tqdm

from .stage import get_file_hash

logger = logging.getLogger(__name__)


class ObjectError(DvcException):
    pass


class ObjectFormatError(ObjectError):
    pass


class HashFile:
    def __init__(self, path_info, fs, hash_info):
        self.path_info = path_info
        self.fs = fs
        self.hash_info = hash_info

    @property
    def size(self):
        if not (self.path_info and self.fs):
            return None
        return self.fs.getsize(self.path_info)

    def __len__(self):
        return 1

    def __str__(self):
        return f"object {self.hash_info}"

    def __bool__(self):
        return bool(self.hash_info)

    def __eq__(self, other):
        if not isinstance(other, HashFile):
            return False
        return (
            self.path_info == other.path_info
            and self.fs == other.fs
            and self.hash_info == other.hash_info
        )

    def check(self, odb):
        actual = get_file_hash(
            self.path_info, self.fs, self.hash_info.name, odb.repo.state
        )

        logger.trace(
            "cache '%s' expected '%s' actual '%s'",
            self.path_info,
            self.hash_info,
            actual,
        )

        assert actual.name == self.hash_info.name
        if actual.value.split(".")[0] != self.hash_info.value.split(".")[0]:
            raise ObjectFormatError(f"{self} is corrupted")

    @classmethod
    def load(cls, odb, hash_info):
        return odb.get(hash_info)


def save(odb, obj, **kwargs):
    from .tree import Tree

    if isinstance(obj, Tree):
        for _, entry in Tqdm(obj):
            odb.add(entry.path_info, entry.fs, entry.hash_info, **kwargs)
    odb.add(obj.path_info, obj.fs, obj.hash_info, **kwargs)


def check(odb, obj):
    from .tree import Tree

    odb.check(obj.hash_info)

    if isinstance(obj, Tree):
        for _, entry in obj:
            odb.check(entry.hash_info)


def load(odb, hash_info):
    from .tree import Tree

    if hash_info.isdir:
        return Tree.load(odb, hash_info)
    return odb.get(hash_info)


def _transfer_file(odb, from_fs, from_info):
    from dvc.utils import tmp_fname
    from dvc.utils.stream import HashedStreamReader

    tmp_info = odb.fs.path_info / tmp_fname()
    with from_fs.open(
        from_info, mode="rb", chunk_size=from_fs.CHUNK_SIZE
    ) as stream:
        stream_reader = HashedStreamReader(stream)
        # Since we don't know the hash beforehand, we'll
        # upload it to a temporary location and then move
        # it.
        odb.fs.upload_fobj(
            stream_reader,
            tmp_info,
            total=from_fs.getsize(from_info),
            desc=from_info.name,
        )

    hash_info = stream_reader.hash_info
    return tmp_info, hash_info


def _transfer_directory_contents(odb, from_fs, from_info, jobs, pbar):
    rel_path_infos = {}
    from_infos = from_fs.walk_files(from_info)

    def create_tasks(executor, amount):
        for entry_info in itertools.islice(from_infos, amount):
            pbar.total += 1
            task = executor.submit(
                pbar.wrap_fn(_transfer_file), odb, from_fs, entry_info
            )
            rel_path_infos[task] = entry_info.relative_to(from_info)
            yield task

    pbar.total = 0
    with futures.ThreadPoolExecutor(max_workers=jobs) as executor:
        tasks = set(create_tasks(executor, jobs * 5))

        while tasks:
            done, tasks = futures.wait(
                tasks, return_when=futures.FIRST_COMPLETED
            )
            tasks.update(create_tasks(executor, len(done)))
            for task in done:
                yield rel_path_infos.pop(task), task.result()


def _transfer_directory(odb, from_fs, from_info, jobs, no_progress_bar=False):
    from .tree import Tree

    tree = Tree(None, None, None)

    with Tqdm(total=1, unit="Files", disable=no_progress_bar) as pbar:
        for (
            entry_info,
            (entry_tmp_info, entry_hash),
        ) in _transfer_directory_contents(odb, from_fs, from_info, jobs, pbar):
            obj = HashFile(entry_tmp_info, odb.fs, entry_hash)
            tree.add(entry_info.parts, obj)

    tree.digest()
    save(odb, tree)
    return tree.hash_info


def transfer(odb, from_fs, from_info, jobs=None, no_progress_bar=False):
    jobs = jobs or min((from_fs.jobs, odb.fs.jobs))

    if from_fs.isdir(from_info):
        return _transfer_directory(
            odb,
            from_fs,
            from_info,
            jobs=jobs,
            no_progress_bar=no_progress_bar,
        )
    tmp_info, hash_info = _transfer_file(odb, from_fs, from_info)
    odb.add(tmp_info, odb.fs, hash_info)
    return hash_info
