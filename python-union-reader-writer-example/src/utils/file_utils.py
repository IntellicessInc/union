import os
import shutil


def read_file(file_path: str) -> str:
    with open(file_path, 'r', encoding='utf8') as file:
        return file.read()


def create_textual_file(file_path: str, content: str):
    with open(file_path, 'w', encoding='utf8') as file:
        file.write(content)


def create_binary_file(file_path: str, content: bytes):
    with open(file_path, 'wb') as file:
        file.write(content)


def create_folder(directory_path: str):
    if not is_folder(directory_path):
        os.mkdir(directory_path)


def delete_file_or_directory(file_path: str, retry=0):
    try:
        if os.path.isfile(file_path):
            os.remove(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)
    except FileNotFoundError as e:
        if retry > 3:
            raise e
        delete_file_or_directory(file_path, retry + 1)


def get_all_file_paths_from_folder(folder_path) -> [str]:
    return [folder_path + "/" + file_path for file_path in os.listdir(folder_path)]


def is_folder(file_path):
    return os.path.isdir(file_path)
