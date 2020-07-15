import errno
import itertools
import logging
from concurrent.futures import ThreadPoolExecutor

from dvc.progress import Tqdm
from dvc.utils import to_chunks

from .base import Remote

logger = logging.getLogger(__name__)


class SSHRemote(Remote):
    def batch_exists(self, path_infos, callback):
        def _exists(chunk_and_channel):
            chunk, channel = chunk_and_channel
            ret = []
            for path in chunk:
                try:
                    channel.stat(path)
                    ret.append(True)
                except OSError as exc:
                    if exc.errno != errno.ENOENT:
                        raise
                    ret.append(False)
                callback(path)
            return ret

        with self.tree.ssh(path_infos[0]) as ssh:
            channels = ssh.open_max_sftp_channels()
            max_workers = len(channels)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                paths = [path_info.path for path_info in path_infos]
                chunks = to_chunks(paths, num_chunks=max_workers)
                chunks_and_channels = zip(chunks, channels)
                outcome = executor.map(_exists, chunks_and_channels)
                results = list(itertools.chain.from_iterable(outcome))

            return results

    def hashes_exist(self, hashes, jobs=None, name=None):
        """This is older implementation used in remote/base.py
        We are reusing it in RemoteSSH, because SSH's batch_exists proved to be
        faster than current approach (relying on exists(path_info)) applied in
        remote/base.
        """
        if not self.tree.CAN_TRAVERSE:
            return list(set(hashes) & set(self.tree.all()))

        # possibly prompt for credentials before "Querying" progress output
        self.tree.ensure_credentials()

        with Tqdm(
            desc="Querying "
            + ("cache in " + name if name else "remote cache"),
            total=len(hashes),
            unit="file",
        ) as pbar:

            def exists_with_progress(chunks):
                return self.batch_exists(chunks, callback=pbar.update_msg)

            with ThreadPoolExecutor(
                max_workers=jobs or self.tree.JOBS
            ) as executor:
                path_infos = [self.tree.hash_to_path_info(x) for x in hashes]
                chunks = to_chunks(path_infos, num_chunks=self.tree.JOBS)
                results = executor.map(exists_with_progress, chunks)
                in_remote = itertools.chain.from_iterable(results)
                ret = list(itertools.compress(hashes, in_remote))
                return ret
