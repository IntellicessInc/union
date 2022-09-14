import json
import logging

import time

from src.utils import config, file_utils, csv_jwlf_converter, jwlf
from src.utils.union_client import union_client

HEADER_VALUE_TYPE_MAPPING = {
    "Integer Number 1": jwlf.ValueType.INTEGER,
    "Float Number 2": jwlf.ValueType.FLOAT,
    "String Text A": jwlf.ValueType.STRING
}

AVAILABLE_FILE_EXTENSIONS = ['.csv', '.json']

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

    logging.info("Listening to local writer folder...")
    while True:
        for file_path in file_utils.get_all_file_paths_from_folder(writer_folder):
            filename: str = file_path.split('/')[-1]
            if str(file_path).endswith('.csv'):
                csv_content = file_utils.read_file(file_path)
                jwlf_log = csv_jwlf_converter.convert_csv_to_jwlf(asset, filename, csv_content,
                                                                  header_value_type_mapping=HEADER_VALUE_TYPE_MAPPING)
                saved_log_id = union_api_client.save_jwlf_log(client, region, asset, folder, jwlf_log)
                logging.info(f"JWLF Log from file '{filename}' got saved with id={saved_log_id}")
            elif str(file_path).endswith('.json'):
                json_content = file_utils.read_file(file_path)
                jwlf_dict_logs = json.loads(json_content)
                jwlf_logs = [jwlf.JWLFLog.from_dict(jwlf_dict_log) for jwlf_dict_log in jwlf_dict_logs]
                index = 1
                for jwlf_log in jwlf_logs:
                    if not jwlf_log.header.metadata:
                        jwlf_log.header.metadata = {}
                    filename_without_extension = filename.replace('.json', '')
                    jwlf_log.header.metadata['filename'] = filename_without_extension + str(index)
                    index = index + 1
                saved_log_ids = union_api_client.save_jwlf_logs(client, region, asset, folder, jwlf_logs)
                logging.info(f"JWLF Logs from file '{filename}' got saved with ids={saved_log_ids}")
            file_utils.delete_file(file_path)
        time.sleep(1)
