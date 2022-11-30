import json
import logging

import time

from src.utils import config, file_utils, csv_jwlf_converter, jwlf
from src.utils.csv_jwlf_converter import FILENAME_METADATA_KEY
from src.utils.union_client import union_client
from src.utils import csv_jwlf_with_base64_encoded_binaries_converter

HEADER_VALUE_TYPE_MAPPING = {
    "Integer Number 1": jwlf.ValueType.INTEGER,
    "Float Number 2": jwlf.ValueType.FLOAT,
    "String Text A": jwlf.ValueType.STRING,
    "Bit SN": jwlf.ValueType.INTEGER,
    "TFA": jwlf.ValueType.FLOAT,
    "Bit Size": jwlf.ValueType.FLOAT,
    "DI": jwlf.ValueType.INTEGER,
    "DO": jwlf.ValueType.INTEGER,
    "Run Length": jwlf.ValueType.INTEGER,
    "Hours": jwlf.ValueType.FLOAT,
    "ROP": jwlf.ValueType.FLOAT
}

AVAILABLE_FILE_EXTENSIONS = ['.csv', '.json']
BASE64_ENCODED_BINARIES_EXAMPLE_FOLDER_NAME = "base64-encoded-binaries-example"

if __name__ == '__main__':
    config_parser = config.init('./writer_local_config.ini')
    auth_config = dict(config_parser['auth'])
    union_config = dict(config_parser['union'])
    local_config = dict(config_parser['local'])

    union_api_client = union_client.UnionClient(union_url=union_config.get('url', None),
                                                auth_provider_url=auth_config.get('provider.url', None),
                                                auth_provider_realm=auth_config.get('provider.realm', None),
                                                auth_client_id=auth_config.get('client.id', None),
                                                username=auth_config.get('user.username', None),
                                                password=auth_config.get('user.password', None))

    client = union_config['client']
    region = union_config['region']
    asset = union_config['asset']
    folder = union_config['folder']
    writer_folder = local_config['writer.folder']
    encoded_images_example_folder = f"{writer_folder}/"
    file_utils.create_folder(encoded_images_example_folder)

    logging.info("Listening to local writer folder...")
    while True:
        for path in file_utils.get_all_file_paths_from_folder(writer_folder):
            name: str = path.split('/')[-1]
            if path.endswith('.csv'):
                csv_content = file_utils.read_file(path)
                jwlf_log = csv_jwlf_converter.convert_csv_to_jwlf(asset, name, csv_content,
                                                                  header_value_type_mapping=HEADER_VALUE_TYPE_MAPPING)
                saved_log_id = union_api_client.save_jwlf_log(client, region, asset, folder, jwlf_log)
                logging.info(f"JWLF Log with name '{jwlf_log.header.name}' got saved with id={saved_log_id}")
            elif path.endswith('.json'):
                json_content = file_utils.read_file(path)
                jwlf_dict_logs = json.loads(json_content)
                jwlf_logs = [jwlf.JWLFLog.from_dict(jwlf_dict_log) for jwlf_dict_log in jwlf_dict_logs]
                index = 1
                for jwlf_log in jwlf_logs:
                    if not jwlf_log.header.metadata:
                        jwlf_log.header.metadata = {}
                    filename_without_extension = name.replace('.json', '')
                    jwlf_log.header.metadata[FILENAME_METADATA_KEY] = filename_without_extension + str(index)
                    index = index + 1
                saved_log_ids = union_api_client.save_jwlf_logs(client, region, asset, folder, jwlf_logs)
                logging.info(
                    f"JWLF Logs with names='{[log.header.name for log in jwlf_logs]}' got saved with ids={saved_log_ids}")
            elif file_utils.is_folder(path) and name == BASE64_ENCODED_BINARIES_EXAMPLE_FOLDER_NAME:
                jwlf_log = csv_jwlf_with_base64_encoded_binaries_converter.convert_folder_to_jwlf(path,
                                                                                                  header_value_type_mapping=HEADER_VALUE_TYPE_MAPPING)
                if not jwlf_log:
                    continue
                saved_log_id = union_api_client.save_jwlf_log(client, region, asset, folder, jwlf_log)
                logging.info(f"JWLF Log with name='{jwlf_log.header.name}' got saved with id={saved_log_id}")

            file_utils.delete_file_or_directory(path)
        time.sleep(1)
