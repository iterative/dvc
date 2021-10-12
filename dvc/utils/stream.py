import hashlib
import io

from funcy import cached_property

from dvc.hash_info import HashInfo
from dvc.istextfile import DEFAULT_CHUNK_SIZE, istextblock
from dvc.utils import dos2unix


class HashedStreamReader(io.IOBase):

    PARAM_CHECKSUM = "md5"

    def __init__(self, fobj):
        self.fobj = fobj
        self.md5 = hashlib.md5()
        self.total_read = 0
        self.is_text_file = None
        super().__init__()

    def readable(self):
        return True

    def tell(self):
        return self.fobj.tell()

    @cached_property
    def _reader(self):
        if hasattr(self.fobj, "read1"):
            return self.fobj.read1
        return self.fobj.read

    def read(self, n=-1):
        chunk = self._reader(n)
        if self.is_text_file is None:
            self.is_text_file = istextblock(chunk[:DEFAULT_CHUNK_SIZE])

        if self.is_text_file:
            data = dos2unix(chunk)
        else:
            data = chunk
        self.md5.update(data)
        self.total_read += len(data)

        return chunk

    @property
    def hash_info(self):
        return HashInfo(self.PARAM_CHECKSUM, self.md5.hexdigest())
