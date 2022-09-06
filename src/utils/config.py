import logging
import os
from configparser import ConfigParser, ExtendedInterpolation

CONFIG_FILE_ENV_VAR_NAME = 'CONFIG_FILE'
LOGGING_LEVEL_ENV_VAR_NAME = 'LOGGING_LEVEL'
ENV_SECTION_NAME = 'env'


def init(local_config_file_path):
    logging_level = logging.INFO
    if LOGGING_LEVEL_ENV_VAR_NAME in os.environ.keys():
        logging_level = logging.getLevelName(os.environ[LOGGING_LEVEL_ENV_VAR_NAME])
    logging.basicConfig(level=logging_level)

    config_file = local_config_file_path
    if CONFIG_FILE_ENV_VAR_NAME in os.environ.keys():
        config_file = os.environ[CONFIG_FILE_ENV_VAR_NAME]
    logging.info(f"File '{config_file}' will be used as config file")

    config_parser = ConfigParser(interpolation=ExtendedInterpolation())
    config_parser.read(config_file)
    config_parser.add_section(ENV_SECTION_NAME)
    for key, value in os.environ.items():
        config_parser.set(ENV_SECTION_NAME, key, value.replace('$', '$$'))
    return config_parser
