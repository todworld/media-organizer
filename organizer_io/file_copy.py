import shutil
from organizer_io.path_utils import ensure_parent

def copy_stream(src: str, dst: str, chunk_size: int = 1024 * 1024) -> int:
    ensure_parent(dst)
    copied = 0
    with open(src, "rb") as fsrc, open(dst, "wb") as fdst:
        while True:
            buf = fsrc.read(chunk_size)
            if not buf:
                break
            fdst.write(buf)
            copied += len(buf)
    shutil.copystat(src, dst, follow_symlinks=False)
    return copied
