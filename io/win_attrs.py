import ctypes
from ctypes import wintypes

FILE_ATTRIBUTE_HIDDEN = 0x2
FILE_ATTRIBUTE_SYSTEM = 0x4
FILE_ATTRIBUTE_REPARSE_POINT = 0x400

GetFileAttributesW = ctypes.windll.kernel32.GetFileAttributesW
GetFileAttributesW.argtypes = [wintypes.LPCWSTR]
GetFileAttributesW.restype = wintypes.DWORD

INVALID_FILE_ATTRIBUTES = 0xFFFFFFFF

def get_attrs(path: str) -> int:
    attrs = GetFileAttributesW(path)
    if attrs == INVALID_FILE_ATTRIBUTES:
        return 0
    return int(attrs)

def is_hidden(path: str) -> bool:
    return bool(get_attrs(path) & FILE_ATTRIBUTE_HIDDEN)

def is_system(path: str) -> bool:
    return bool(get_attrs(path) & FILE_ATTRIBUTE_SYSTEM)

def is_reparse_point(path: str) -> bool:
    return bool(get_attrs(path) & FILE_ATTRIBUTE_REPARSE_POINT)
