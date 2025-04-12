import hashlib
from collections import defaultdict
from pathlib import Path


def compute_sha256(file_path):
    """Вычисляет SHA-256 хеш файла."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            sha256.update(chunk)
    return sha256.hexdigest()


def find_duplicates(image_dir):
    """Находит дубликаты изображений в директории."""
    hashes = defaultdict(list)
    image_dir = Path(image_dir)

    for file_path in image_dir.rglob("*"):
        if file_path.suffix.lower() in (".png", ".jpg", ".jpeg"):
            file_hash = compute_sha256(file_path)
            hashes[file_hash].append(file_path)

    duplicates = {k: v for k, v in hashes.items() if len(v) > 1}
    return duplicates


if __name__ == "__main__":
    image_directory = "images"
    duplicates = find_duplicates(image_directory)

    if duplicates:
        print("Найдены дубликаты:")
        for hash_val, files in duplicates.items():
            print(f"Хеш {hash_val}:")
            for file in files:
                print(f"  - {file}")
    else:
        print("Дубликатов не найдено.")
