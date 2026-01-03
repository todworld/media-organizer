import os
from io.win_attrs import is_hidden, is_system, is_reparse_point
from domain.constants import EXCLUDED_EXTS
from io.path_utils import norm_abs_path

def iter_files(source_root: str):
    # Ignore symlinks/junctions/shortcuts: skip reparse points at directory level
    for dirpath, dirnames, filenames in os.walk(source_root):
        # prune hidden/system/reparse directories
        pruned = []
        for d in list(dirnames):
            full = os.path.join(dirpath, d)
            if is_hidden(full) or is_system(full) or is_reparse_point(full):
                pruned.append(d)
        for d in pruned:
            dirnames.remove(d)

        for fn in filenames:
            full = os.path.join(dirpath, fn)
            try:
                if is_hidden(full) or is_system(full) or is_reparse_point(full):
                    continue
            except Exception:
                continue
            ext = os.path.splitext(fn)[1].lower()
            if ext in EXCLUDED_EXTS:
                continue
            yield norm_abs_path(full)
