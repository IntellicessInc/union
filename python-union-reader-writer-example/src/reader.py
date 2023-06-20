import json
import logging
import time
from datetime import datetime

from src.utils import config, file_utils, utils
from src.utils import csv_jwlf_with_base64_encoded_binaries_converter
from src.utils.csv_jwlf_converter import FILENAME_METADATA_KEY
from src.utils.csv_jwlf_with_base64_encoded_binaries_converter import BASE64_ENCODED_BINARIES_EXAMPLE_METADATA_KEY
from src.utils.union_client import union_client


if __name__ == '__main__':
    config_parser = config.init('reader_local_config.ini')
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
    reader_folder = local_config['reader.folder']

    # if set to None, it will read all data that was saved so far in given data space (i.e. in client/region/asset/folder)
    inclusive_since_timestamp = utils.date_time_to_milliseconds_timestamp(datetime.now()) - 5000
    logging.info(
        f"Listening to '{client}/{region}/{asset}/{folder}' data space in Union since timestamp='{inclusive_since_timestamp}'...")
    while True:
        logs_with_data = union_api_client.get_new_jwlf_logs_with_data(client, region, asset, folder,
                                                                      inclusive_since_timestamp)
        for new_data in logs_with_data:
            log_with_data_dict = new_data.to_dict()
            jwlf_log_json: str = json.dumps(log_with_data_dict, indent=2)
            filename: str = new_data.header.metadata[FILENAME_METADATA_KEY]
            base64_encoded_binaries_example: bool = True if new_data.header.metadata.get(
                BASE64_ENCODED_BINARIES_EXAMPLE_METADATA_KEY) else False
            if base64_encoded_binaries_example:
                csv_jwlf_with_base64_encoded_binaries_converter.convert_jwlf_to_folder(reader_folder, new_data)
            if not filename.endswith('.json'):
                filename = filename + '.json'
            file_utils.create_textual_file(f"{reader_folder}/{filename}", jwlf_log_json)
            logging.info(f"Pulled '{filename}' and saved in local reader folder")

        time.sleep(1)
