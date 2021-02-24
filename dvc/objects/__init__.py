import itertools
import json
import logging
from concurrent import futures

from dvc.dir_info import DirInfo
from dvc.exceptions import DvcException
from dvc.progress import Tqdm

from .stage import get_hash

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

    def __len__(self):
        return 1

    def __str__(self):
        return f"object {self.hash_info}"

    def __bool__(self):
        return bool(self.hash_info)

    def check(self, odb):
        actual = get_hash(self.path_info, self.fs, odb.fs.PARAM_CHECKSUM, odb)

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

    def save(self, odb, **kwargs):
        odb.add(self.path_info, self.fs, self.hash_info, **kwargs)


class File(HashFile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.src = None

    @classmethod
    def stage(cls, odb, path_info, fs, **kwargs):
        hash_info = get_hash(
            path_info, fs, odb.fs.PARAM_CHECKSUM, odb, **kwargs
        )
        raw = odb.get(hash_info)
        obj = cls(raw.path_info, raw.fs, hash_info)
        obj.src = HashFile(path_info, fs, hash_info)
        return obj

    def save(self, odb, **kwargs):
        self.src.save(odb, **kwargs)


class Tree(HashFile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.src_hash_info = None
        self.src_path_info = None
        self.src_fs = None

    def __len__(self):
        return self.hash_info.dir_info.nfiles

    def __iter__(self):
        yield from self.hash_info.dir_info.items()

    @classmethod
    def save_dir_info(cls, odb, dir_info, hash_info=None):
        if hash_info and hash_info.name == odb.fs.PARAM_CHECKSUM:
            try:
                odb.check(hash_info)
                assert hash_info.dir_info == dir_info
                return hash_info
            except (FileNotFoundError, ObjectFormatError):
                pass

        from dvc.fs.memory import MemoryFileSystem
        from dvc.path_info import PathInfo
        from dvc.utils import tmp_fname

        fs = MemoryFileSystem(None, {})
        path_info = PathInfo(tmp_fname(""))
        with fs.open(path_info, "w") as fobj:
            json.dump(dir_info.to_list(), fobj, sort_keys=True)

        tmp_info = odb.fs.path_info / tmp_fname("")
        with fs.open(path_info, "rb") as fobj:
            odb.fs.upload_fobj(fobj, tmp_info)

        hash_info = get_hash(tmp_info, odb.fs, odb.fs.PARAM_CHECKSUM, odb)
        hash_info.value += odb.fs.CHECKSUM_DIR_SUFFIX
        hash_info.dir_info = dir_info
        hash_info.nfiles = dir_info.nfiles

        odb.add(tmp_info, odb.fs, hash_info)

        return hash_info

    @classmethod
    def stage(cls, odb, path_info, fs, **kwargs):
        hash_info = get_hash(
            path_info, fs, odb.fs.PARAM_CHECKSUM, odb, **kwargs
        )
        hi = cls.save_dir_info(odb, hash_info.dir_info, hash_info)
        hi.size = hash_info.size
        raw = odb.get(hi)
        obj = cls(raw.path_info, raw.fs, hi)
        obj.src_hash_info = hash_info
        obj.src_path_info = path_info
        obj.src_fs = fs
        return obj

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

        dir_info = DirInfo.from_list(raw)
        hash_info.dir_info = dir_info
        hash_info.nfiles = dir_info.nfiles

        return cls(obj.path_info, obj.fs, hash_info)

    def save(self, odb, **kwargs):
        assert self.src_hash_info.dir_info
        hi = self.save_dir_info(
            odb, self.src_hash_info.dir_info, self.hash_info
        )
        for entry_key, entry_hash in Tqdm(
            hi.dir_info.items(),
            desc="Saving " + self.src_path_info.name,
            unit="file",
        ):
            entry_info = self.src_path_info.joinpath(*entry_key)
            entry_obj = HashFile(entry_info, self.src_fs, entry_hash)
            entry_obj.save(odb, **kwargs)
        self.src_fs.repo.state.save(self.src_path_info, self.src_fs, hi)

    def filter(self, odb, prefix):
        hash_info = self.hash_info.dir_info.get(prefix)
        if hash_info:
            return load(odb, hash_info)

        depth = len(prefix)
        dir_info = DirInfo()
        try:
            for key, value in self.hash_info.dir_info.trie.items(prefix):
                dir_info.add(key[depth:], value)
        except KeyError:
            return None

        return load(odb, self.save_dir_info(odb, dir_info))


def save(odb, obj, **kwargs):
    obj.save(odb, **kwargs)


def check(odb, obj):
    odb.check(obj.hash_info)

    if isinstance(obj, Tree):
        for _, hash_info in obj:
            odb.check(hash_info)


def load(odb, hash_info):
    if hash_info.isdir:
        return Tree.load(odb, hash_info)
    return File.load(odb, hash_info)


def _get_dir_size(odb, dir_info):
    try:
        return sum(
            odb.fs.getsize(odb.hash_to_path_info(hi.value))
            for _, hi in dir_info.items()
        )
    except FileNotFoundError:
        return None


def merge(odb, ancestor_info, our_info, their_info):
    assert our_info
    assert their_info

    if ancestor_info:
        ancestor = load(odb, ancestor_info).hash_info.dir_info
    else:
        ancestor = DirInfo()

    our = load(odb, our_info).hash_info.dir_info
    their = load(odb, their_info).hash_info.dir_info

    merged = our.merge(ancestor, their)
    hash_info = Tree.save_dir_info(odb, merged)
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
    dir_info = DirInfo()

    with Tqdm(total=1, unit="Files", disable=no_progress_bar) as pbar:
        for (
            entry_info,
            (entry_tmp_info, entry_hash),
        ) in _transfer_directory_contents(odb, from_fs, from_info, jobs, pbar):
            odb.add(entry_tmp_info, odb.fs, entry_hash)
            dir_info.add(entry_info.parts, entry_hash)

    return Tree.save_dir_info(odb, dir_info)


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
