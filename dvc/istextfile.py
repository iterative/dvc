"""Use heuristics to guess if it is a text file or a binary file."""

from __future__ import unicode_literals

from dvc.utils.compat import is_py3, open

# Based on https://eli.thegreenplace.net/2011/10/19/
# perls-guess-if-file-is-text-or-binary-implemented-in-python


# A function that takes an integer in the 8-bit range and returns
# a single-character byte object in py3 / a single-character string
# in py2.
#
def _int2byte(i):
    if is_py3:
        return bytes((i,))
    return chr(i)


TEXT_CHARS = b"".join(_int2byte(i) for i in range(32, 127)) + b"\n\r\t\f\b"


def istextfile(fname, blocksize=512):
    """ Uses heuristics to guess whether the given file is text or binary,
        by reading a single block of bytes from the file.
        If more than 30% of the chars in the block are non-text, or there
        are NUL ('\x00') bytes in the block, assume this is a binary file.
    """
    with open(fname, "rb") as fobj:
        block = fobj.read(blocksize)

    if not block:
        # An empty file is considered a valid text file
        return True

    if b"\x00" in block:
        # Files with null bytes are binary
        return False

    # Use translate's 'deletechars' argument to efficiently remove all
    # occurrences of TEXT_CHARS from the block
    nontext = block.translate(None, TEXT_CHARS)
    return float(len(nontext)) / len(block) <= 0.30
