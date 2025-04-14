from pathlib import Path

def rename_files_by_pattern(directory):
    dir_path = Path(directory)
    
    # Проверяем, существует ли директория
    if not dir_path.exists() or not dir_path.is_dir():
        print(f"Ошибка: директория {dir_path} не существует или не является директорией")
        return
    
    # Проходим по всем файлам в директории
    for file_path in dir_path.glob('*'):
        if file_path.is_file():
            parts = file_path.name.split('-', 1)
            
            # Если разделение успешно и есть часть после первого '-'
            if len(parts) > 1:
                new_name = parts[1]  # Берем часть после первого '-'
                new_path = file_path.with_name(new_name)
                
                # Проверяем, не существует ли уже файл с таким именем
                if not new_path.exists():
                    file_path.rename(new_path)
                    print(f"Переименован: {file_path.name} -> {new_name}")
                else:
                    print(f"Файл {new_name} уже существует, пропускаем")
            else:
                print(f"Файл {file_path.name} не соответствует шаблону, пропускаем")

if __name__ == "__main__":
    # target_directory = "labels_from_workers/worker_2_Al_obb/labels" 
    # target_directory = "labels_from_workers/worker_3_Ig_obb/labels" 
    target_directory = "labels_from_workers/worker_1_Zh_obb/labels/" 
    # target_directory = "golden_set/labels" 
    rename_files_by_pattern(target_directory)