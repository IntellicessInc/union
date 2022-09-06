import os


def read_file(file_path: str) -> str:
    with open(file_path, 'r', encoding='utf8') as file:
        return file.read()


def create_file(file_path: str, content: str):
    with open(file_path, 'w', encoding='utf8') as file:
        file.write(content)


def delete_file(file_path: str):
    os.remove(file_path)


def get_all_file_paths_from_folder(folder_path) -> [str]:
    return [folder_path + "/" + file_path for file_path in os.listdir(folder_path)]
