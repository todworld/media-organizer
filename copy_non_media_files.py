import os
import shutil

# -------- CONFIG --------
SOURCE_ROOT = "E:\Amazon Photos Downloads\Yaneth Collection"     
DEST_ROOT   = "C:Download\Yaneth Collection>"

DEST_FOLDER = os.path.join(DEST_ROOT, "non-media")

# Only these extensions are processed
EXCLUDED_EXTS = {
    ".psd",
    ".afphoto",
    ".xmp",
    ".pdf",
    ".webp",
}

DRY_RUN = True   # True = preview only, False = copy

# -------- SCRIPT --------
def ext_folder(ext: str) -> str:
    return ext[1:].lower() if ext.startswith(".") else "no-ext"

def copy_with_suffix(src: str, dst_dir: str) -> str:
    os.makedirs(dst_dir, exist_ok=True)
    base = os.path.basename(src)
    name, ext = os.path.splitext(base)
    dst = os.path.join(dst_dir, base)

    i = 1
    while os.path.exists(dst):
        dst = os.path.join(dst_dir, f"{name} ({i}){ext}")
        i += 1

    if not DRY_RUN:
        shutil.copy2(src, dst)

    return dst

count = 0

print("DRY RUN" if DRY_RUN else "COPY MODE")
print("-" * 60)

for root, _, files in os.walk(SOURCE_ROOT):
    for file in files:
        ext = os.path.splitext(file)[1].lower()
        if ext not in EXCLUDED_EXTS:
            continue

        src_path = os.path.join(root, file)
        dst_dir = os.path.join(DEST_FOLDER, ext_folder(ext))

        try:
            dst_path = copy_with_suffix(src_path, dst_dir)
            count += 1
            print(f"[{'DRY' if DRY_RUN else 'COPY'}] {src_path}")
            print(f"      -> {dst_path}")
        except Exception as e:
            print(f"[ERROR] {src_path} -> {e}")

print("-" * 60)
print(f"Total excluded files {'found' if DRY_RUN else 'copied'}: {count}")
print(f"Destination: {DEST_FOLDER}")
