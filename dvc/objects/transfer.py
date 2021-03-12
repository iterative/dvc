import itertools
import logging
from concurrent import futures

from dvc.progress import Tqdm

from . import save
from .file import HashFile
from .tree import Tree

logger = logging.getLogger(__name__)


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
