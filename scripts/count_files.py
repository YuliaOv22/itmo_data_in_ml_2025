from pathlib import Path


# Вычисление количества видео в директории
def count_files(dir: str, file_extensions: set) -> int:
    dir_path = Path(dir)
    count = 0

    for file in dir_path.iterdir():
        if file.suffix.lower() in file_extensions:  # Проверяем расширение файла
            count += 1

    return count
