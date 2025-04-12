import subprocess
from pathlib import Path
import tqdm
from count_files import count_files


# Извлечение кадров из видео и ресайз
def extract_and_resize_frames(
    input_video: Path,
    output_dir: Path,
    count: int,
    target_size: str = "448x448",
    quality: int = 2,
) -> None:

    output_dir.mkdir(parents=True, exist_ok=True)
    if count == 10:
        count = 99

    ffmpeg_cmd = [
        "ffmpeg",
        "-i",
        str(input_video),
        "-vf",
        f"select=not(mod(n\,10)),scale={target_size}",  # Каждый 10-й кадр + Ресайз через фильтр scale
        "-qscale:v",
        str(quality),  # Качество JPEG (2-31, где 2 — лучшее)
        "-vsync",
        "vfr",
        str(output_dir / (f"{count}_" + "frame_%04d.jpg")),
    ]

    subprocess.run(ffmpeg_cmd, check=True)
    print(f"Кадры сохранены в {output_dir}")


if __name__ == "__main__":

    input_dir = "pigs"
    output_dir = "frames"
    video_extensions = {".mkv"}
    image_extensions = {".jpg"}
    count_vids = count_files(input_dir, video_extensions)
    print(f"Количество видео в папке '{input_dir}': {count_vids}")

    # Создаем папку для кадров, если она не существует
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Обрабатываем каждое видео в папке pigs
    for item in tqdm.tqdm(range(0, count_vids)):
        count = item + 1
        file_path = Path(input_dir) / f"Movie_{count}.mkv"
        extract_and_resize_frames(
            input_video=file_path,
            count=count,
            output_dir=Path(output_dir),
            target_size="448x448",  # "448:-1" для сохранения пропорций
        )
        # break

    print(f"Сохранено {count_files(output_dir, image_extensions)} кадров")
