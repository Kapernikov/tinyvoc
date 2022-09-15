import hashlib
from pathlib import Path
from typing import Union, IO
# source: https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file

def hash_bytestr_iter(bytesiter, hasher, ashexstr=False):
    for block in bytesiter:
        hasher.update(block)
    return hasher.hexdigest() if ashexstr else hasher.digest()

def file_as_blockiter(afile, blocksize=65536):
        #with afile:
        block = afile.read(blocksize)
        while len(block) > 0:
            yield block
            block = afile.read(blocksize)


def hash_from_fileobj(afile) -> str:
    return hash_bytestr_iter(file_as_blockiter(afile), hashlib.sha256(), True)


def hash_from_file(path: Union[str, IO, Path]) -> str:
    if type(path) == str:
        path = open(path, "rb")
    if type(path) == Path:
        path = open(path.absolute(), "rb")
    return hash_from_fileobj(path)


def hash_from_Str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()