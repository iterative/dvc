import itertools
import json
import logging
import posixpath
from concurrent import futures

from funcy import cached_property

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


class Tree(HashFile):
    PARAM_RELPATH = "relpath"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dict = {}

    @cached_property
    def trie(self):
        from pygtrie import Trie

        return Trie(self._dict)

    @property
    def size(self):
        try:
            return sum(obj.size for _, obj in self)
        except TypeError:
            return None

    def add(self, key, obj):
        self.__dict__.pop("trie", None)
        self._dict[key] = obj

    def digest(self):
        from dvc.fs.memory import MemoryFileSystem
        from dvc.path_info import PathInfo
        from dvc.utils import tmp_fname

        memfs = MemoryFileSystem(None, {})
        path_info = PathInfo(tmp_fname(""))
        with memfs.open(path_info, "wb") as fobj:
            fobj.write(self.as_bytes())
        self.fs = memfs
        self.path_info = path_info
        self.hash_info = get_file_hash(path_info, memfs, "md5")
        self.hash_info.value += ".dir"
        self.hash_info.size = self.size
        self.hash_info.nfiles = len(self)

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        yield from self._dict.items()

    def as_dict(self):
        return self._dict.copy()

    def as_list(self):
        from operator import itemgetter

        # Sorting the list by path to ensure reproducibility
        return sorted(
            (
                {
                    # NOTE: not using hash_info.to_dict() because we don't want
                    # size/nfiles fields at this point.
                    obj.hash_info.name: obj.hash_info.value,
                    self.PARAM_RELPATH: posixpath.sep.join(parts),
                }
                for parts, obj in self._dict.items()  # noqa: B301
            ),
            key=itemgetter(self.PARAM_RELPATH),
        )

    def as_bytes(self):
        return json.dumps(self.as_list(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_list(cls, lst):
        from dvc.hash_info import HashInfo

        tree = cls(None, None, None)
        for _entry in lst:
            entry = _entry.copy()
            relpath = entry.pop(cls.PARAM_RELPATH)
            parts = tuple(relpath.split(posixpath.sep))
            hash_info = HashInfo.from_dict(entry)
            obj = HashFile(None, None, hash_info)
            tree.add(parts, obj)
        return tree

    @classmethod
    def load(cls, odb, hash_info):

        obj = odb.get(hash_info)

        try:
            with obj.fs.open(obj.path_info, "r") as fobj:
                raw = json.load(fobj)
        except ValueError as exc:
            raise ObjectFormatError(f"{obj} is corrupted") from exc

        if not isinstance(raw, list):
            logger.error(
                "dir cache file format error '%s' [skipping the file]",
                obj.path_info,
            )
            raise ObjectFormatError(f"{obj} is corrupted")

        tree = cls.from_list(raw)
        tree.path_info = obj.path_info
        tree.fs = obj.fs
        tree.hash_info = hash_info

        return tree

    def filter(self, odb, prefix):
        obj = self._dict.get(prefix)
        if obj:
            return obj

        depth = len(prefix)
        tree = Tree(None, None, None)
        try:
            for key, obj in self.trie.items(prefix):
                tree.add(key[depth:], obj)
        except KeyError:
            return None
        tree.digest()
        odb.add(tree.path_info, tree.fs, tree.hash_info)
        return tree


def save(odb, obj, **kwargs):
    if isinstance(obj, Tree):
        for _, entry in Tqdm(obj):
            odb.add(entry.path_info, entry.fs, entry.hash_info, **kwargs)
    odb.add(obj.path_info, obj.fs, obj.hash_info, **kwargs)


def check(odb, obj):
    odb.check(obj.hash_info)

    if isinstance(obj, Tree):
        for _, entry in obj:
            odb.check(entry.hash_info)


def load(odb, hash_info):
    if hash_info.isdir:
        return Tree.load(odb, hash_info)
    return odb.get(hash_info)


def _get_dir_size(odb, tree):
    try:
        return sum(
            odb.fs.getsize(odb.hash_to_path_info(obj.hash_info.value))
            for _, obj in tree
        )
    except FileNotFoundError:
        return None


def _diff(ancestor, other, allow_removed=False):
    from dictdiffer import diff

    from dvc.exceptions import MergeError

    allowed = ["add"]
    if allow_removed:
        allowed.append("remove")

    result = list(diff(ancestor, other))
    for typ, _, _ in result:
        if typ not in allowed:
            raise MergeError(
                "unable to auto-merge directories with diff that contains "
                f"'{typ}'ed files"
            )
    return result


def _merge(ancestor, our, their):
    import copy

    from dictdiffer import patch

    our_diff = _diff(ancestor, our)
    if not our_diff:
        return copy.deepcopy(their)

    their_diff = _diff(ancestor, their)
    if not their_diff:
        return copy.deepcopy(our)

    # make sure there are no conflicting files
    _diff(our, their, allow_removed=True)

    return patch(our_diff + their_diff, ancestor)


def merge(odb, ancestor_info, our_info, their_info):
    assert our_info
    assert their_info

    if ancestor_info:
        ancestor = load(odb, ancestor_info)
    else:
        ancestor = Tree(None, None, None)

    our = load(odb, our_info)
    their = load(odb, their_info)

    merged_dict = _merge(ancestor.as_dict(), our.as_dict(), their.as_dict(),)

    merged = Tree(None, None, None)
    for key, hi in merged_dict.items():
        merged.add(key, hi)
    merged.digest()

    odb.add(merged.path_info, merged.fs, merged.hash_info)
    hash_info = merged.hash_info
    hash_info.size = _get_dir_size(odb, merged)
    return hash_info


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
