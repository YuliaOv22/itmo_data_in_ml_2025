import cv2
import os
import tqdm
from skimage.metrics import structural_similarity as ssim
from pathlib import Path
import shutil
from count_files import count_files


# Сравнение изображений с использованием SSIM
def compare_images_ssim(image1_path, image2_path, threshold):
    image1 = cv2.imread(image1_path)
    image2 = cv2.imread(image2_path)
    gray1 = cv2.cvtColor(image1, cv2.COLOR_BGR2GRAY)
    gray2 = cv2.cvtColor(image2, cv2.COLOR_BGR2GRAY)
    similarity = ssim(gray1, gray2)
    return similarity < threshold


# Сравнение гистограмм
def compare_histograms(img1_path, img2_path, threshold):
    """
    Сравнивает две гистограммы с помощью корреляции
    Возвращает True, если изображения разные (корреляция < порога)
    """

    # Загружаем изображения с проверкой
    img1 = cv2.imread(str(img1_path))
    img2 = cv2.imread(str(img2_path))

    # Конвертируем в HSV и считаем гистограммы
    img1_hsv = cv2.cvtColor(img1, cv2.COLOR_BGR2HSV)
    img2_hsv = cv2.cvtColor(img2, cv2.COLOR_BGR2HSV)

    # Вычисляем гистограммы для каналов H и S
    hist1 = cv2.calcHist([img1_hsv], [0, 1], None, [50, 60], [0, 180, 0, 256])
    hist2 = cv2.calcHist([img2_hsv], [0, 1], None, [50, 60], [0, 180, 0, 256])

    # Нормализуем гистограммы
    cv2.normalize(hist1, hist1, alpha=0, beta=1, norm_type=cv2.NORM_MINMAX)
    cv2.normalize(hist2, hist2, alpha=0, beta=1, norm_type=cv2.NORM_MINMAX)

    # Сравниваем гистограммы
    similarity = cv2.compareHist(hist1, hist2, cv2.HISTCMP_CORREL)
    return similarity < threshold


# Извлечение уникальных кадров
def extract_diff_frames(
    input_dir: str,
    output_dir: str,
    method: str = "hist",
    hist_threshold: float = 0.9,  # Порог сравнения гистограмм (0-1)
    min_interval: int = 10,  # Минимальный интервал между кадрами (в кадрах)
    ssim_threshold: float = 0.9,  # Порог для SSIM (0-1)
) -> None:

    # Создаем Path объекты
    input_path = Path(input_dir)
    output_path = Path(output_dir)

    # Создаем выходную директорию (со всеми родительскими)
    output_path.mkdir(parents=True, exist_ok=True)

    # Получаем список изображений
    image_files = sorted(
        [
            f
            for f in input_path.iterdir()
            if f.suffix.lower() in {".jpg", ".jpeg", ".png"}
        ]
    )
    print(f"Найдено {len(image_files)} кадров в папке `{input_dir}`")

    # Копируем первый кадр
    prev_image = image_files[0]
    print(f"Первый кадр: {prev_image}")
    assert str(prev_image) == "frames/1_frame_0001.jpg"
    shutil.copy2(prev_image, output_path / prev_image.name)

    # Выбираем функцию сравнения
    if method == "hist":
        print("Используется метод сравнения гистограмм")
        last_saved = 0
        for i, curr_image in tqdm.tqdm(
            enumerate(image_files[1:]), desc="Обработка кадров"
        ):
            # Проверяем минимальный интервал и разницу
            if (i - last_saved >= min_interval) or compare_histograms(
                prev_image, curr_image, hist_threshold
            ):
                shutil.copy2(curr_image, output_path / curr_image.name)
                last_saved = i
                prev_image = curr_image
    elif method == "ssim":
        print("Используется метод сравнения SSIM")
        for curr_image in tqdm.tqdm(image_files, desc="Обработка кадров"):
            if compare_images_ssim(prev_image, curr_image, threshold=ssim_threshold):
                shutil.copy2(curr_image, output_path / curr_image.name)
                prev_image = curr_image


if __name__ == "__main__":
    input_dir = "frames"
    output_dir = "unique_frames"
    image_extensions = {".jpg"}
    hist_threshold = 0.8  # Чем ниже, тем больше кадров будет сохранено
    min_interval = 6  # Гарантированно сохранять минимум каждый 15-й кадр
    ssim_threshold = 0.955  # Пороговое значение для SSIM [0.93 - 0.94]

    extract_diff_frames(
        input_dir, output_dir, "hist", hist_threshold, min_interval, ssim_threshold
    )
    print(f"Сохранено {count_files(output_dir, image_extensions)} кадров")
