import datetime
import enum
import hashlib
import json
import os
import re
import secrets
import shutil
import time
from os.path import exists
import traceback

import pandas
import pytz
import requests
from requests import Response


class RTInputDataTypes(enum.Enum):
    REAL_TIME = 1
    HISTORICAL = 2


class RunTypes(enum.Enum):
    CONSUMER = 1
    PRODUCER = 2


# Constants
BROOK_URL = 'http://dev-brook.eastus.cloudapp.azure.com/api/v1'
DSP_URL = 'https://dev-dsp.southcentralus.cloudapp.azure.com/api/v1'
KEYCLOAK_URL = 'https://dev-intellicess-keycloak.southcentralus.cloudapp.azure.com'
USERNAME = 'client1_admin'
PASSWORD = 'HmHxqYkcSbliyDY8pqb6'
KEYCLOAK_CLIENT_ID = 'dev-client'
KEYCLOAK_CLIENT_SECRET = 'af61c454-e3d5-45c1-a767-e08b5199837d'

CLIENT = 'client1'
REGION = 'texas'
LOG_INPUT_DSP_FOLDER = 'sentinel-cloud_input-folder'
LOG_OUTPUT_DSP_FOLDER = 'sentinel-cloud_output-folder'
GENERAL_DATA_INPUT_DSP_FOLDER = 'sentinel-cloud_input-folder'
GENERAL_DATA_OUTPUT_DSP_FOLDER = 'sentinel-cloud_output-folder'
MAX_GET_BOT_ID_REQUESTS = 300
BOT_ID_REQUESTS_RETRY_SLEEP_SECONDS = 1.0
BATCH_SIZE = 3000
DATE_TIME = datetime.datetime.now(pytz.utc).strftime('%m_%d_%Y_T%H_%M')
INPUT_FILES_LOCAL_FOLDER = "input_files"
SENT_DATA_INPUT_FILES_LOCAL_FOLDER = f"{INPUT_FILES_LOCAL_FOLDER}/sent-data"
CFG_LOCAL_FOLDER = f"cfg"
SENT_EFFECTIVE_PROPERTIES_LOCAL_FOLDER = f"{CFG_LOCAL_FOLDER}/sent-data"
SENT_EFFECTIVE_PROPERTIES_LOCAL_FILE_PATH = f"{SENT_EFFECTIVE_PROPERTIES_LOCAL_FOLDER}/effective-config.properties"
OUTPUT_FILES_LOCAL_FOLDER = "output_files"
BROOK_RT_INPUT_DATA_FILENAME_POSTFIX = "_Second_Data.csv"
MICROSECONDS_IN_SECOND = 1000000
CONFIG_PROPERTIES_PATH = 'cfg/config.properties'
OUTPUT_FILES_SYNCH_TIMESTAMP_FILENAME = 'output_files_synch_timestamp.local'
LOGS_FOLDER = "logs"
OUTPUT_FILES_SYNCH_TIMESTAMP_FILE_PATH = f"{LOGS_FOLDER}/{OUTPUT_FILES_SYNCH_TIMESTAMP_FILENAME}"
UNIT_IN_HEADER_REGEX = re.compile('.*\(.*\)')
PYTHON_BROOK_CLIENT_PROPERTIES_PREFIX = 'brook.python-client'
INPUT_DATA_FILENAME_TO_BE_SENT_PROPERTY_NAME = f"{PYTHON_BROOK_CLIENT_PROPERTIES_PREFIX}.input-data-files-to-be-sent"
OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED_PROPERTY_NAME = f"{PYTHON_BROOK_CLIENT_PROPERTIES_PREFIX}.output_data.column.date-time.enabled"
CONSOLE_LOGGING_INPUT_DATA_CONTENT_PROPERTY_NAME = f"{PYTHON_BROOK_CLIENT_PROPERTIES_PREFIX}.logging.console.input-data.content"
CONSOLE_LOGGING_OUTPUT_DATA_CONTENT_PROPERTY_NAME = f"{PYTHON_BROOK_CLIENT_PROPERTIES_PREFIX}.logging.console.output-data.content"
# lower date raises exception when timestamp() method is called
MIN_DATE_TIME = datetime.datetime.strptime('3/1/1970', '%d/%m/%Y')
# note that “%f” will parse all the way up to nanoseconds
# https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
DATE_FORMATS_ORDERED_BY_PRIORITY = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%Sa",
    "%m/%d/%Y %H:%M:%S a",
    "%m/%d/%Y %H:%M:%S a ",
    "%m/%d/%Y %H:%M:%Sa ",
    "%m/%d/%Y %H:%M:%Sa%z",
    "%m/%d/%Y %H:%M:%S a%z",
    "%m/%d/%Y %H:%M:%S a %z",
    "%m/%d/%Y %H:%M:%Sa %z",
]
# no data for 3 minutes implies consumer running finish
LAST_DATA_FETCH_MAX_DELAY_MILLISECONDS = 180000
SENT_FILE_ID_METADATA_KEY = 'sentFileId'
REAL_TIME_DATA_SENT_FILE_ID = secrets.token_hex(16)
CONTEXTUAL_INPUT_DATA_CHECK_SECONDS_INTERVAL = 3
SENTINEL_CLOUD_CLIENT_LOGS = f"{LOGS_FOLDER}/sentinel-cloud-client.logs"
HEADERS_DICT_SAVE_FILE_PATH = f"{LOGS_FOLDER}/headers_dict_save.local"
ORDERED_CURVES_DICT_SAVE_FILE_PATH = f"{LOGS_FOLDER}/ordered_curves_dict_save.local"
FILE_ORDINAL_PREFIX = '_vol'
SPLIT_BY_BHA_METADATA_KEY = 'splitByBhaPart'
JWLF_TO_SENTINEL_DATA_TYPE_MAPPINGS = {
    'boolean': 'Boolean',
    'string': 'String',
    'integer': 'Integer',
    'float': 'Real',
    'datetime': 'DateTime',
}

# Constants to be loaded based on properties
WELL = None
RT_INPUT_DATA_TYPE = None
CLEAN_START = None
ACTIVE_RUN_TYPES = None
DATA_SYNCH_RATE_IN_SECONDS = None
BROOK_SENT_RT_INPUT_DATA_FILENAME = None
BROOK_SENT_RT_INPUT_DATA_FILE_PATH = None
OUTPUT_DATA_NAMES_FILENAMES_DICT = None
INPUT_DATA_FILENAMES_TO_BE_SENT = None
OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED = None
CONSOLE_LOGGING_INPUT_DATA_CONTENT = None
CONSOLE_LOGGING_OUTPUT_DATA_CONTENT = None
OUTPUT_DATA_ROW_LIMIT = None

# Global variables
properties = None
last_keycloak_realm = ''  # Keycloak realm = client = Json Asset Log Format (JWLF) operator
access_token_creation_time = None
max_access_token_age_seconds = 60
max_number_of_ids_per_request = 25
last_access_token = ''
till_date_time_jwlf = None
till_date_time_general_data = None
refresh_all_output_data = False
_new_output_data_lines_dict = {}
ordered_curves_dict = {}
headers_dict = {}
used_date_formats = []
running_consumer = True
last_data_fetch_datetime = None
contextual_input_data_files_hash_dict = {}
ordinal_files_dict = {}
sentinel_exit = False


def log(str):
    global SENTINEL_CLOUD_CLIENT_LOGS
    log_message = f"{datetime.datetime.now()} {str}"
    print(log_message)
    append_lines_to_file([log_message], SENTINEL_CLOUD_CLIENT_LOGS)


def equals_filename_match(filename_from_property, input_filename):
    return filename_from_property == input_filename


def historical_match(filename_from_property, input_filename):
    return RTInputDataTypes.HISTORICAL == RT_INPUT_DATA_TYPE and filename_from_property == input_filename


def bha_filename_match(filename_from_property, input_filename):
    return input_filename.startswith(filename_from_property.replace('.csv', ''))


INPUT_DATA_TYPES_DICT = {
    'HISTORICAL': {
        'priority': 0,
        'filename_config': {
            'property': 'inputdatatimebased.filename',
            'property_value_index': 0,
            'filename_match_strategy': historical_match
        },
        'metadata_headers_count': 0,
        'sent_data_log_filename': f"sent_historical-data.csv"
    },
    'BHA': {
        'priority': 1,
        'filename_config': {
            'property': 'inputdatabha.filename',
            'property_value_index': 0,
            'filename_match_strategy': bha_filename_match
        },
        'metadata_headers_count': 1,
        'sent_data_log_filename': f"sent_bha-data.csv"
    },
    'DEPTH_REFERENCE': {
        'priority': 1,
        'filename_config': {
            'property': 'inputdatadepthbased.filename',
            'property_value_index': 1,
            'filename_match_strategy': equals_filename_match
        },
        'metadata_headers_count': 0,
        'sent_data_log_filename': f"sent_depth-reference-data.csv"
    },
    'MORNING_REPORT': {
        'priority': 1,
        'filename_config': {
            'property': 'inputdatatimebased.filename',
            'property_value_index': 1,
            'filename_match_strategy': equals_filename_match
        },
        'metadata_headers_count': 0,
        'sent_data_log_filename': f"sent_morning-report-data.csv"
    },
    'CASING': {
        'priority': 1,
        'filename_config': {
            'property': 'inputdatacasing.filename',
            'property_value_index': 0,
            'filename_match_strategy': equals_filename_match
        },
        'metadata_headers_count': 1,
        'sent_data_log_filename': f"sent_casing-data.csv"
    },
    'CASING_KEY_DEPTHS': {
        'priority': 1,
        'filename_config': {
            'property': 'inputdatacasing.keyDepthFileName',
            'property_value_index': 0,
            'filename_match_strategy': equals_filename_match
        },
        'metadata_headers_count': 0,
        'sent_data_log_filename': f"sent_casing-key-depths-data.csv"
    },
    'CONE_CONFIGURATION': {
        'priority': 1,
        'filename_config': {
            'property': 'inputdata.drillingconefilename',
            'property_value_index': 0,
            'filename_match_strategy': equals_filename_match
        },
        'metadata_headers_count': 0,
        'sent_data_log_filename': f"sent_cone-configuration-data.csv"
    },
    'DRILLING_MEMO': {
        'priority': 1,
        'filename_config': {
            'property': 'inputdatatimebased.filename',
            'property_value_index': 2,
            'filename_match_strategy': equals_filename_match
        },
        'metadata_headers_count': 0,
        'sent_data_log_filename': f"sent_drilling-memo-data.csv"
    },
    'ROP_RECOMMENDATION': {
        'priority': 1,
        'filename_config': {
            'property': 'inputdata.ropfilename',
            'property_value_index': 0,
            'filename_match_strategy': equals_filename_match
        },
        'metadata_headers_count': 0,
        'sent_data_log_filename': f"sent_rop-recommendation-data.csv"
    },
}

NOT_MATCHED_OUTPUT_FILENAME_POSTFIX = '_Not_Matched_Filename.csv'
OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX = f"{DATE_TIME}.csv"


def map_log_json_to_log_format(lines):
    global sentinel_exit

    json_str = "".join(lines)
    json_value = json.loads(json_str)
    log_formatted_value = ""
    if 'dateTime' in json_value.keys():
        log_formatted_value += f"{json_value['dateTime']}"
    if 'level' in json_value.keys():
        log_formatted_value += f" {json_value['level']}"
    if 'message' in json_value.keys():
        log_formatted_value += f" {json_value['message']}"
    if 'stackTrace' in json_value.keys():
        log_formatted_value += f"\n{json_value['stackTrace']}"

    if 'Sentinel exit' in log_formatted_value:
        log('Sentinel Cloud exited!')
        sentinel_exit = True

    return [log_formatted_value]


def get_access_token(realm):
    global access_token_creation_time
    if access_token_creation_time:
        access_token_age_seconds = (datetime.datetime.utcnow() - access_token_creation_time).total_seconds()
    else:
        access_token_age_seconds = max_access_token_age_seconds

    if realm == last_keycloak_realm and access_token_age_seconds < max_access_token_age_seconds:
        return last_access_token
    url = f"{KEYCLOAK_URL}/auth/realms/{CLIENT}/protocol/openid-connect/token"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    payload = {"client_id": KEYCLOAK_CLIENT_ID, "grant_type": "password", "username": USERNAME, "password": PASSWORD,
               "client_secret": KEYCLOAK_CLIENT_SECRET}
    res = requests.post(url, data=payload, headers=headers)
    if res.status_code >= 300:
        raise Exception(
            f"Access token retrieval failure with http response status code={res.status_code} and response={res.text}")
    access_token_creation_time = datetime.datetime.utcnow()
    return res.json()["access_token"]


def read_properties_dict_from_file():
    properties_dict = {}
    with open(CONFIG_PROPERTIES_PATH, 'r', encoding='utf8') as f:
        for line in f:
            line = line.rstrip()

            if "=" not in line:
                continue
            if line.startswith("#"):
                continue

            k, v = line.split("=", 1)
            properties_dict[k.strip()] = v.strip()
    return properties_dict


def send(client, region, well, folder, jwlf, access_token):
    url = f"{DSP_URL}/well-logs/{client}/{region}/{well}/{folder}"
    res: Response = requests.post(url, json=jwlf, headers={"Authorization": f"Bearer {access_token}"})
    if res.status_code >= 300:
        raise Exception(
            f"Sending data failure with http response status code={res.status_code} and response={res.text}")
    return res.json()["id"]


def get_data_entries_from_dsp(url, access_token, since_date_time):
    query_params = {}
    since_date_time_milliseconds = date_time_to_milliseconds_timestamp(since_date_time)
    if since_date_time:
        query_params['sinceTimestamp'] = since_date_time_milliseconds
    query_params['tillTimestamp'] = since_date_time_milliseconds

    stable_timestamp_response: Response = requests.get(
        url, params=query_params, headers={"Authorization": f"Bearer {access_token}"})
    if stable_timestamp_response.status_code >= 300:
        raise Exception(
            f"Retrieving data from DSP failure with"
            f" http response status code={stable_timestamp_response.status_code}"
            f" and response={stable_timestamp_response.text}")

    stable_timestamp_response_json = stable_timestamp_response.json()
    stable_data_timestamp = stable_timestamp_response_json['stableDataTimestamp']
    till_timestamp = max(since_date_time_milliseconds, stable_data_timestamp + 1)
    query_params['tillTimestamp'] = till_timestamp
    till_date_time = milliseconds_timestamp_to_date_time(till_timestamp)

    little_data_elements_response: Response = requests.get(
        url, params=query_params, headers={"Authorization": f"Bearer {access_token}"})
    if little_data_elements_response.status_code >= 300:
        raise Exception(
            f"Retrieving data from DSP failure with"
            f" http response status code={little_data_elements_response.status_code}"
            f" and response={little_data_elements_response.text}")

    ids_response_json = little_data_elements_response.json()

    little_data_elements = ids_response_json['list']
    all_ids = [element['id'] for element in little_data_elements]
    if len(all_ids) == 0:
        return {'entries': [], 'till_date_time': till_date_time}

    ids_packages = [[]]
    id_package_index = 0
    id_index_in_package = 0
    for id in all_ids:
        if id_index_in_package >= max_number_of_ids_per_request:
            id_package_index += 1
            ids_packages.append([])
            id_index_in_package = 0
        ids_packages[id_package_index].append(id)
        id_index_in_package += 1

    entries = []
    all_ids_num = len(all_ids)
    for ids in ids_packages:
        query_params['id'] = ids
        full_data_elements_response: Response = requests.get(
            url, params=query_params, headers={"Authorization": f"Bearer {access_token}"})
        if full_data_elements_response.status_code >= 300:
            raise Exception(
                f"Retrieving data from DSP failure with"
                f" http response status code={full_data_elements_response.status_code}"
                f" and response={full_data_elements_response.text}")
        data_json = full_data_elements_response.json()
        entries.extend(data_json['list'])
        if len(ids_packages) > 1:
            log(f"Receiving new output data packets {(len(entries) / all_ids_num) * 100.0}%")

    return {'entries': entries, 'till_date_time': till_date_time}


def date_time_to_milliseconds_timestamp(date_time):
    return int(float(date_time.timestamp()) * 1000.0)


def milliseconds_timestamp_to_date_time(milliseconds_timestamp):
    return datetime.datetime.fromtimestamp(milliseconds_timestamp / 1000.0)


def convert_to_datetime(value):
    formats = []
    formats.extend(used_date_formats)
    formats.extend(DATE_FORMATS_ORDERED_BY_PRIORITY)
    for format in formats:
        try:
            result = pandas.to_datetime(value, format=format)
            if result != 'NaT':
                if format not in used_date_formats:
                    used_date_formats.append(format)
                    DATE_FORMATS_ORDERED_BY_PRIORITY.remove(format)
                return result
        except:
            pass
    raise Exception('NaT')


def create_jwlf_curves(headers_line, data_example):
    headers = headers_line.split(',')
    curves = []
    i = 0
    while i < len(headers):
        header = headers[i]
        unit = None
        if UNIT_IN_HEADER_REGEX.match(header):
            unit = header \
                .rsplit("(", maxsplit=1)[-1] \
                .split(")", maxsplit=1)[0] \
                .strip()

        data_point = data_example[i]
        data_type = 'string'
        if str(type(data_point)) == "<class 'int'>":
            data_type = 'integer'
        elif str(type(data_point)) == "<class 'float'>":
            data_type = 'float'
        elif str(type(data_point)) == "<class 'bool'>":
            data_type = 'boolean'
        elif str(type(data_point)) == "<class 'str'>":
            try:
                convert_to_datetime(data_point)
                data_type = 'datetime'
            except:
                pass
        curves.append({
            "name": header,
            "valueType": data_type,
            "unit": unit,
            "dimensions": 1
        })
        i += 1

    return curves


def create_data_points(line):
    values = line.split(',')
    data_points = []
    for value in values:
        result = None
        if result is None and value.lower() in ['true', 'false']:
            result = bool(value.lower().capitalize())
        if result is None and value.replace("-", "").isnumeric():
            result = int(value)
        if result is None and value.replace(".", "").replace("-", "").isnumeric():
            result = float(value)
        if result is None:
            try:
                result = convert_to_datetime(value)
                result = result.replace(tzinfo=pytz.utc)
                result = result.isoformat()
            except:
                result = None
        if result is None:
            result = str(value)
        data_points.append(result)

    return data_points


def convert_to_zoned_date_time(value):
    return value.replace(' ', 'T') + 'Z'


def send_input_data(input_data_type, properties):
    global contextual_input_data_files_hash_dict, CONSOLE_LOGGING_INPUT_DATA_CONTENT, CLIENT, REGION, WELL

    input_data_config = INPUT_DATA_TYPES_DICT[input_data_type]
    file_paths = get_existing_file_paths_for_input_data_type(input_data_type, properties)
    for file_path in file_paths:
        file_path_relative_to_input_files = file_path.replace(f"{INPUT_FILES_LOCAL_FOLDER}/", '')
        if INPUT_DATA_FILENAMES_TO_BE_SENT is not None and file_path_relative_to_input_files not in INPUT_DATA_FILENAMES_TO_BE_SENT:
            log(
                f"Input file '{file_path_relative_to_input_files}' is not included in non-empty '{INPUT_DATA_FILENAME_TO_BE_SENT_PROPERTY_NAME}' param so this file will not be sent.")
            log(
                f"To allow this file to be sent, add its name to '{INPUT_DATA_FILENAME_TO_BE_SENT_PROPERTY_NAME}' property or clear the property.")
            time.sleep(2)
            continue

        current_hash = generate_file_sha256(file_path)
        last_hash = None
        if file_path in contextual_input_data_files_hash_dict:
            last_hash = contextual_input_data_files_hash_dict[file_path]
        if current_hash == last_hash:
            continue

        contextual_input_data_files_hash_dict[file_path] = current_hash
        num_of_lines = get_num_of_lines_in_file(file_path)
        with open(file_path, 'r', encoding='utf8') as file:
            metadata_headers_count = input_data_config['metadata_headers_count']
            metadata_header_lines = []
            headers_line = None
            input_data_lines = []
            lineIndex = 0
            for line in file:
                line = line.replace("\n", "")
                if lineIndex < metadata_headers_count:
                    metadata_header_lines.append(line)
                elif lineIndex == metadata_headers_count:
                    headers_line = line
                else:
                    input_data_lines.append(line)
                if len(input_data_lines) >= BATCH_SIZE or lineIndex + 1 == num_of_lines:
                    metadata = {
                        "endOfFile": lineIndex + 1 >= num_of_lines,
                        "jwlfHeaderMetadataLines": metadata_header_lines,
                        SENT_FILE_ID_METADATA_KEY: current_hash
                    }
                    access_token = get_access_token(CLIENT)
                    input_jwlf = create_jwlf(input_data_type, headers_line, input_data_lines, metadata)
                    run_request_with_retries(
                        lambda: send(CLIENT, REGION, WELL, LOG_INPUT_DSP_FOLDER, input_jwlf, access_token))
                    input_data_config = INPUT_DATA_TYPES_DICT[input_data_type]
                    if 'sent_data_log_filename' in input_data_config.keys():
                        sent_data_log_file_path = SENT_DATA_INPUT_FILES_LOCAL_FOLDER + "/" + input_data_config[
                            'sent_data_log_filename']
                        log_input_data(sent_data_log_file_path, metadata_header_lines, headers_line, input_data_lines,
                                       input_data_type)
                    input_data_lines = []
                    log(
                        f"Sending input data from '{file_path}': {((lineIndex + 1) / num_of_lines) * 100}%")
                    if CONSOLE_LOGGING_INPUT_DATA_CONTENT and lineIndex + 1 == num_of_lines:
                        time.sleep(1)
                lineIndex += 1


def create_jwlf(name, headers_line, lines, metadata={SENT_FILE_ID_METADATA_KEY: REAL_TIME_DATA_SENT_FILE_ID}):
    data = [create_data_points(line) for line in lines]
    curves = create_jwlf_curves(headers_line, data[0])
    date = datetime.datetime.now(pytz.utc).isoformat()
    if curves[0]['valueType'] == 'datetime':
        date = data[-1][0]
    headers = {
        "name": name,
        "well": WELL,
        "operator": CLIENT,
        "date": date,
        "metadata": metadata
    }

    jwlf = {
        "header": headers,
        "curves": curves,
        "data": data
    }
    return jwlf


def get_new_output_data_lines(client, region, well, access_token):
    global till_date_time_jwlf, till_date_time_general_data
    global sent
    global _new_output_data_lines_dict

    since_date_time_jwlf = till_date_time_jwlf
    entries_data = run_request_with_retries(
        fun=lambda: get_data_entries_from_dsp(f"{DSP_URL}/well-logs/{client}/{region}/{well}/{LOG_OUTPUT_DSP_FOLDER}",
                                              access_token, since_date_time_jwlf),
        default_response={'entries': [], 'till_date_time': MIN_DATE_TIME})
    new_jwlfs = entries_data['entries']
    till_date_time_jwlf = entries_data['till_date_time']

    since_date_time_general_data = till_date_time_general_data
    general_data_entries = run_request_with_retries(
        fun=lambda: get_data_entries_from_dsp(
            f"{DSP_URL}/general-data-entries/{client}/{region}/{well}/{GENERAL_DATA_OUTPUT_DSP_FOLDER}",
            access_token, since_date_time_general_data),
        default_response={'entries': [], 'till_date_time': MIN_DATE_TIME}
    )
    new_general_data_entries = general_data_entries['entries']
    till_date_time_general_data = general_data_entries['till_date_time']

    new_output_data_lines_dict = {}
    new_output_data_lines_dict.update(general_data_entries_to_new_output_data_lines_dict(new_general_data_entries))
    new_output_data_lines_dict.update(jwlfs_to_new_output_data_lines_dict(new_jwlfs))

    return new_output_data_lines_dict


def general_data_entries_to_new_output_data_lines_dict(new_general_data_entries):
    global _new_output_data_lines_dict
    _new_output_data_lines_dict = {}
    if len(new_general_data_entries) > 0:
        for entry in new_general_data_entries:
            data_name = entry['name']
            new_output_data_line = entry['content']
            if "<class 'dict'>" == str(type(new_output_data_line)):
                new_output_data_line = json.dumps(new_output_data_line, indent=3)
            if data_name not in _new_output_data_lines_dict.keys():
                _new_output_data_lines_dict[data_name] = [[]]
            _new_output_data_lines_dict[data_name][0].append([new_output_data_line])

    return _new_output_data_lines_dict


def jwlfs_to_new_output_data_lines_dict(new_jwlfs):
    global _new_output_data_lines_dict, headers_dict, ordered_curves_dict, SPLIT_BY_BHA_METADATA_KEY

    _new_output_data_lines_dict = {}
    if len(new_jwlfs) > 0:
        for new_jwlf in new_jwlfs:
            jwlf_name = new_jwlf['header']['name']
            new_output_data_lines = []
            if jwlf_name not in ordered_curves_dict.keys() or \
                    hash_of_curves_ignoring_order(ordered_curves_dict[jwlf_name]) != hash_of_curves_ignoring_order(
                new_jwlf['curves']):
                ordered_curves_dict[jwlf_name] = new_jwlf['curves']
                csv_header = create_csv_header(jwlf_name, ordered_curves_dict)
                if jwlf_name not in headers_dict:
                    headers_dict[jwlf_name] = []
                headers_dict[jwlf_name].append(csv_header)

            headers_index = len(headers_dict[jwlf_name]) - 1
            new_output_data_lines.extend(
                [convert_jwlf_to_csv_line(jwlf_data_points, new_jwlf, ordered_curves_dict[jwlf_name]) for
                 jwlf_data_points
                 in new_jwlf['data']]
            )
            if "metadata" in new_jwlf['header'] and SPLIT_BY_BHA_METADATA_KEY in new_jwlf['header']["metadata"]:
                if jwlf_name not in _new_output_data_lines_dict.keys():
                    _new_output_data_lines_dict[jwlf_name] = {}
                split_by_bha = new_jwlf['header']["metadata"][SPLIT_BY_BHA_METADATA_KEY]
                if split_by_bha not in _new_output_data_lines_dict[jwlf_name]:
                    _new_output_data_lines_dict[jwlf_name][split_by_bha] = []
                while len(_new_output_data_lines_dict[jwlf_name][split_by_bha]) <= headers_index:
                    _new_output_data_lines_dict[jwlf_name][split_by_bha].append([])
                _new_output_data_lines_dict[jwlf_name][split_by_bha][headers_index].append(new_output_data_lines)
            else:
                if jwlf_name not in _new_output_data_lines_dict.keys():
                    _new_output_data_lines_dict[jwlf_name] = []
                while len(_new_output_data_lines_dict[jwlf_name]) <= headers_index:
                    _new_output_data_lines_dict[jwlf_name].append([])
                _new_output_data_lines_dict[jwlf_name][headers_index].append(new_output_data_lines)

    return _new_output_data_lines_dict


def create_csv_header(jwlf_name, ordered_curves_dict):
    global OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED, OUTPUT_DATA_NAMES_FILENAMES_DICT
    three_lines_header = False
    if 'three-lines-header' in OUTPUT_DATA_NAMES_FILENAMES_DICT[jwlf_name]:
        three_lines_header = OUTPUT_DATA_NAMES_FILENAMES_DICT[jwlf_name]['three-lines-header']
    first_line_prefix = 'date time,' if OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED else ''
    if three_lines_header:
        second_line_prefix = ',' if OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED else ''
        third_line_prefix = 'DateTime,' if OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED else ''
        csv_header = "\n".join([
            first_line_prefix + ",".join(
                [ordered_curve['name'] for ordered_curve in ordered_curves_dict[jwlf_name]]),
            second_line_prefix + ",".join(
                [ordered_curve['unit'] if 'unit' in ordered_curve else '' for ordered_curve in
                 ordered_curves_dict[jwlf_name]]),
            third_line_prefix + ",".join(
                [map_jwlf_to_sentinel_value_type(ordered_curve['valueType']) if 'valueType' in ordered_curve else '' for
                 ordered_curve in ordered_curves_dict[jwlf_name]])
        ])
    else:
        csv_header = first_line_prefix + ",".join(
            [ordered_curve['name'] + get_jwlf_curve_unit_header_part(ordered_curve)
             for ordered_curve in ordered_curves_dict[jwlf_name]])
    return csv_header


def get_jwlf_curve_unit_header_part(ordered_curve):
    if 'unit' in ordered_curve:
        return ' [' + ordered_curve['unit'] + ']'
    return ''


def map_jwlf_to_sentinel_value_type(jwlf_value_type):
    global JWLF_TO_SENTINEL_DATA_TYPE_MAPPINGS
    if jwlf_value_type in JWLF_TO_SENTINEL_DATA_TYPE_MAPPINGS:
        return JWLF_TO_SENTINEL_DATA_TYPE_MAPPINGS[jwlf_value_type]
    return jwlf_value_type


def convert_jwlf_to_csv_line(jwlf_data_points, jwlf, ordered_curves):
    ordered_data_points = []
    ordered_curves_index_dict = {}
    actual_curves = jwlf['curves']
    for actual_index in range(len(ordered_curves)):
        ordered_data_points.append('')
        ordered_curve = ordered_curves[actual_index]
        ordered_curves_index_dict[ordered_curve['name']] = actual_index

    for actual_index in range(len(actual_curves)):
        actual_curve = actual_curves[actual_index]
        if actual_curve['name'] not in ordered_curves_index_dict.keys():
            ordered_curves.append(actual_curve)
            raise Exception(
                f"Actual curve '{actual_curve['name']}' not found in ordered curves '{ordered_curves_index_dict.keys()}'")
        ordered_index = ordered_curves_index_dict[actual_curve['name']]
        value = jwlf_data_points[actual_index]
        if value is not None:
            str_value = str(value)
            if type(value) is bool:
                str_value = str_value.lower()
            ordered_data_points[ordered_index] = str_value
    prefix = ''
    if OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED:
        date = ''
        if jwlf['header']['date']:
            date = jwlf['header']['date']
        prefix += date + ','
    return prefix + ",".join(ordered_data_points)


def hash_of_curves_ignoring_order(curves):
    return hash(frozenset([json.dumps(curve, sort_keys=True, default=str) for curve in curves]))


def flat(list):
    return [y for x in list for y in x]


def save_headers_dict():
    global headers_dict, ordered_curves_dict, HEADERS_DICT_SAVE_FILE_PATH, ORDERED_CURVES_DICT_SAVE_FILE_PATH
    overwrite_file([json.dumps(headers_dict)], HEADERS_DICT_SAVE_FILE_PATH)
    overwrite_file([json.dumps(ordered_curves_dict)], HEADERS_DICT_SAVE_FILE_PATH)


def log_output_data(lines_dict):
    global CONSOLE_LOGGING_OUTPUT_DATA_CONTENT, headers_dict

    for data_name, entry in lines_dict.items():
        if type(entry) is dict:
            for split_by_bha, lines_entries_with_different_headers in entry.items():
                for headers_index in range(0, len(lines_entries_with_different_headers)):
                    lines_entries = lines_entries_with_different_headers[headers_index]
                    headers = get_headers(data_name, headers_index)
                    log_output_data_lines(lines_entries, data_name, split_by_bha, headers)
        elif type(entry) is list:
            lines_entries_with_different_headers = entry
            for headers_index in range(0, len(lines_entries_with_different_headers)):
                lines_entries = lines_entries_with_different_headers[headers_index]
                headers = get_headers(data_name, headers_index)
                log_output_data_lines(lines_entries, data_name, '', headers)
        if data_name in headers_dict:
            headers_dict[data_name] = [headers_dict[data_name][-1]]
    save_headers_dict()


def get_headers(data_name, headers_index):
    global headers_dict
    headers = []
    if data_name in headers_dict.keys() and headers_index < len(headers_dict[data_name]):
        headers = [headers_dict[data_name][headers_index]]
    return headers


def log_output_data_lines(lines_entries, data_name, split_by_bha, headers):
    output_data_file_path = resolve_output_data_file_path(data_name, split_by_bha)
    appended = True
    if data_name in OUTPUT_DATA_NAMES_FILENAMES_DICT.keys():
        output_data_config = OUTPUT_DATA_NAMES_FILENAMES_DICT[data_name]
        if 'value_mapper' in output_data_config.keys():
            lines_entries = [output_data_config['value_mapper'](lines_entry) for lines_entry in lines_entries]
        appended = OUTPUT_DATA_NAMES_FILENAMES_DICT[data_name]['appended']
    if not lines_entries:
        return
    if appended:
        lines = flat(lines_entries)
        if CONSOLE_LOGGING_OUTPUT_DATA_CONTENT:
            for line in lines:
                log(f"OUTPUT DATA FOR '{data_name}': {line}")
        else:
            lines_num = len(lines)
            if lines_num == 0:
                log(f"Received headers of '{output_data_file_path}'")
            else:
                lines_str = 'lines'
                if lines_num == 1:
                    lines_str = 'line'
                log(f"Received {lines_num} new {lines_str} of '{output_data_file_path}'")
        append_lines_to_file(lines, output_data_file_path, headers=headers)
    else:
        lines = lines_entries[-1]
        if headers:
            new_lines = []
            new_lines.extend(headers)
            new_lines.extend(lines)
            lines = new_lines

        if CONSOLE_LOGGING_OUTPUT_DATA_CONTENT:
            for line in lines:
                log(f"OUTPUT DATA FOR '{data_name}': {line}")
        overwrite_file(lines, output_data_file_path)


def resolve_output_data_file_path(data_name, split_by_bha):
    global OUTPUT_DATA_NAMES_FILENAMES_DICT

    output_data_file_path = f"{OUTPUT_FILES_LOCAL_FOLDER}/{data_name + '_' + split_by_bha + '_' + NOT_MATCHED_OUTPUT_FILENAME_POSTFIX}"
    if data_name in OUTPUT_DATA_NAMES_FILENAMES_DICT.keys():
        output_data_config = OUTPUT_DATA_NAMES_FILENAMES_DICT[data_name]
        file_path = None
        if 'output_data_full_file_path' in output_data_config.keys():
            file_path = output_data_config['output_data_full_file_path']
        else:
            file_path = f"{OUTPUT_FILES_LOCAL_FOLDER}/{output_data_config['filename']}"
        extension = ""
        file_path_without_extension = file_path
        if "." in file_path:
            parts = file_path.rsplit(".", 1)
            file_path_without_extension = parts[0]
            extension = "." + parts[1]
        if split_by_bha:
            split_by_bha = "_" + split_by_bha
        output_data_file_path = file_path_without_extension + split_by_bha + extension
    return output_data_file_path


def log_input_data(sent_data_log_file_path, metadata_header_lines=[], headers_line=None, input_data_lines=[],
                   input_data_type='REAL_TIME'):
    global CONSOLE_LOGGING_INPUT_DATA_CONTENT

    log_lines = []
    for data_line in input_data_lines:
        log_lines.append(data_line)

    if CONSOLE_LOGGING_INPUT_DATA_CONTENT:
        for log_line in log_lines:
            log(f"INPUT '{input_data_type}' DATA: {log_line}")
    headers_lines = []
    headers_lines.extend(metadata_header_lines)
    if headers_line is not None:
        headers_lines.append(headers_line)
    append_lines_to_file(log_lines, sent_data_log_file_path, headers=headers_lines)


def log_input_header(header):
    global CONSOLE_LOGGING_INPUT_DATA_CONTENT

    if CONSOLE_LOGGING_INPUT_DATA_CONTENT:
        log(f"INPUT HEADER: {header}")
    append_lines_to_file([], BROOK_SENT_RT_INPUT_DATA_FILE_PATH, headers=[header])


def is_completely_new_file(file_path):
    file_path_parts = file_path.split("/")
    completely_new_file = file_path_parts[-1] not in os.listdir("/".join(file_path_parts[0:-1]))
    return completely_new_file


def are_headers_matched(file_path, headers):
    if is_completely_new_file(file_path):
        return True
    headers = [header_with_new_line.replace('\n', '') for header_with_new_line in
               flat([header.split('\n') for header in headers])]
    lines = []
    with open(file_path, 'r', encoding='utf8') as f:
        for line in f:
            if len(headers) <= len(lines):
                break
            lines.append(line.replace('\n', ''))
    result = True
    for i in range(0, len(lines)):
        result = result and lines[i] == headers[i]
    return result


def append_lines_to_file(lines, file_path, active_rowlimit=True, headers=[]):
    global OUTPUT_DATA_ROW_LIMIT, ordinal_files_dict

    if active_rowlimit:
        if file_path in ordinal_files_dict:
            ordinal_file_data = ordinal_files_dict[file_path]
            current_ordinal_file_path = ordinal_file_data['current_ordinal_file_path']
            next_ordinal_file_path = ordinal_file_data['next_ordinal_file_path']
            remaining_number_of_lines = ordinal_file_data['remaining_number_of_lines']
        else:
            current_ordinal_file_path, next_ordinal_file_path, remaining_number_of_lines \
                = get_current_order_file_path(file_path, OUTPUT_DATA_ROW_LIMIT)
        if not are_headers_matched(current_ordinal_file_path, headers):
            remaining_number_of_lines = 0

        current_ordinal_file_lines = lines[0:min(len(lines), remaining_number_of_lines)]
        append_lines_to_file(current_ordinal_file_lines, current_ordinal_file_path, False, headers)
        if len(lines) > remaining_number_of_lines:
            next_ordinal_file_lines = lines[remaining_number_of_lines:]
            append_lines_to_file(next_ordinal_file_lines, next_ordinal_file_path, False, headers)
            current_ordinal_file_path, next_ordinal_file_path, remaining_number_of_lines \
                = get_current_order_file_path(file_path, OUTPUT_DATA_ROW_LIMIT)
        elif len(lines) <= remaining_number_of_lines:
            remaining_number_of_lines = remaining_number_of_lines - len(current_ordinal_file_lines)

        ordinal_files_dict[file_path] = {
            'current_ordinal_file_path': current_ordinal_file_path,
            'next_ordinal_file_path': next_ordinal_file_path,
            'remaining_number_of_lines': remaining_number_of_lines
        }
    else:
        completely_new_file = is_completely_new_file(file_path)
        all_lines = []
        if completely_new_file:
            all_lines.extend(headers)
        all_lines.extend(lines)
        with open(file_path, 'a', encoding='utf8') as f:
            for line in all_lines:
                new_line = '\n'
                if completely_new_file:
                    new_line = ''
                    completely_new_file = False
                f.write(new_line + line)


def get_current_order_file_path(file_path, row_limit):
    global ordinal_files_dict

    file_path_parts = file_path.rsplit('/', 1)
    folder = file_path_parts[0]
    filename = file_path_parts[1]
    filename_parts = filename.rsplit('.', 1)
    file_name_without_extension = filename_parts[0].split(FILE_ORDINAL_PREFIX, 1)[0]
    extension = filename_parts[1]
    current_ordinal = 1
    for folder_filename in os.listdir(folder):
        ordinal = folder_filename \
            .replace(file_name_without_extension, '') \
            .replace(FILE_ORDINAL_PREFIX, '') \
            .replace('.' + extension, '')
        try:
            ordinal = int(ordinal)
            if re.match(f"{file_name_without_extension}{FILE_ORDINAL_PREFIX}\d*.{extension}", folder_filename) \
                    and current_ordinal < ordinal:
                current_ordinal = ordinal
        except:
            pass

    current_ordinal_file_path = f"{folder}/{file_name_without_extension}{FILE_ORDINAL_PREFIX}{current_ordinal}.{extension}"
    next_ordinal_file_path = f"{folder}/{file_name_without_extension}{FILE_ORDINAL_PREFIX}{current_ordinal + 1}.{extension}"
    num_of_lines = get_num_of_lines_in_file(current_ordinal_file_path)
    remaining_number_of_lines = max(row_limit - num_of_lines, 0)
    return current_ordinal_file_path, next_ordinal_file_path, remaining_number_of_lines


def get_all_partitions_of_file(file_path):
    global ordinal_files_dict

    file_path_parts = file_path.rsplit('/', 1)
    folder = file_path_parts[0]
    filename = file_path_parts[1]
    filename_parts = filename.rsplit('.', 1)
    file_name_without_extension = filename_parts[0].split(FILE_ORDINAL_PREFIX, 1)[0]
    extension = filename_parts[1]
    current_ordinal = 0
    for folder_filename in os.listdir(folder):
        ordinal = folder_filename \
            .replace(file_name_without_extension, '') \
            .replace(FILE_ORDINAL_PREFIX, '') \
            .replace('.' + extension, '')
        try:
            ordinal = int(ordinal)
            if re.match(f"{file_name_without_extension}{FILE_ORDINAL_PREFIX}\d*.{extension}", folder_filename) \
                    and current_ordinal < ordinal:
                current_ordinal = ordinal
        except:
            pass
    return [f"{folder}/{file_name_without_extension}{FILE_ORDINAL_PREFIX}{i}.{extension}" for i in
            range(1, current_ordinal + 1)]


def overwrite_file(lines, filename):
    with open(filename, 'w', encoding='utf8') as f:
        for line in lines:
            f.write(line + '\n')


def clear_file(filename):
    with open(filename, 'w', encoding='utf8') as f:
        pass


def create_well_and_return_id(client_name):
    global WELL, REGION
    url = f"{BROOK_URL}/clients/{client_name}/assets"
    headers = {
        'Content-Type': 'application/json'
    }
    payload = {"assetName": WELL, "region": REGION}
    res = requests.post(url, json=payload, headers=headers)
    if res.status_code >= 300:
        raise Exception(
            f"Brook well creation failure with http response status code={res.status_code} and response={res.text}")
    return res.json()["id"]


def delete_well(client_name, well_id):
    url = f"{BROOK_URL}/clients/{client_name}/assets/{well_id}"
    res = requests.delete(url)
    if res.status_code >= 300:
        raise Exception(
            f"Brook well creation failure with http response status code={res.status_code} and response={res.text}")


def register_well_if_doesnt_exist(client_name):
    url = f"{BROOK_URL}/clients/{client_name}/assets"
    headers = {
        'Content-Type': 'application/json'
    }
    res = requests.get(url, headers=headers)
    if res.status_code >= 300:
        raise Exception(
            f"Assets from Brook retrieval failure with http response status code={res.status_code} and response={res.text}")
    found_matched_wells = [well for well in res.json() if well["name"] == WELL]
    if len(found_matched_wells) == 1:
        return found_matched_wells[0]["id"]
    return create_well_and_return_id(client_name)


def delete_well_if_exists(client_name):
    url = f"{BROOK_URL}/clients/{client_name}/assets"
    headers = {
        'Content-Type': 'application/json'
    }
    res = requests.get(url, headers=headers)
    if res.status_code >= 300:
        raise Exception(
            f"Assets from Brook retrieval failure with http response status code={res.status_code} and response={res.text}")
    found_matched_wells = [well for well in res.json() if well["name"] == WELL]
    if len(found_matched_wells) == 1:
        delete_well(client_name, found_matched_wells[0]["id"])


def delete_well_data_from_dsp(client_name, region, well, access_token):
    url = f"{DSP_URL}/well-logs/{client_name}/{region}/{well}"
    headers = {
        'Content-Type': 'application/json',
        "Authorization": f"Bearer {access_token}"
    }
    res = requests.delete(url, headers=headers)
    if res.status_code >= 300:
        raise Exception(
            f"Well Logs deletion failure with http response status code={res.status_code} and response={res.text}")

    url = f"{DSP_URL}/general-data-entries/{client_name}/{region}/{well}"
    headers = {
        'Content-Type': 'application/json',
        "Authorization": f"Bearer {access_token}"
    }
    res = requests.delete(url, headers=headers)
    if res.status_code >= 300:
        raise Exception(
            f"General data entries deletion failure with http response status code={res.status_code} and response={res.text}")


def get_bot_id(client_name):
    retry_number = 0
    while retry_number < MAX_GET_BOT_ID_REQUESTS:
        url = f"{BROOK_URL}/clients/{client_name}/bots?assetName={WELL}"
        headers = {
            'Content-Type': 'application/json'
        }
        res = requests.get(url, headers=headers)
        bot_id = -1
        if 200 <= res.status_code < 300:
            matched_bots_ids = [bot["id"] for bot in res.json() if bot["type"] == "Sentinel Cloud"]
            if len(matched_bots_ids) == 1:
                bot_id = matched_bots_ids[0]
        if bot_id != -1:
            return bot_id
    raise Exception("Brook bot id retrieval failure")


def update_sentinel_cloud_properties(client_name, properties_dict):
    bot_id = get_bot_id(client_name)
    url = f"{BROOK_URL}/clients/{client_name}/bots/{bot_id}/environment-variables"
    headers = {
        'Content-Type': 'application/json'
    }
    payload = {
        "brookBotEnvironmentVariables": [
            {"name": property_key, "value": properties_dict[property_key], "secret": False}
            for property_key in properties_dict.keys()
        ]
    }
    res = requests.put(url, json=payload, headers=headers)
    if res.status_code >= 300:
        raise Exception(
            f"Brook bot environment variables update failure with http response status code={res.status_code} and response={res.text}")


def get_existing_file_paths_for_input_data_type(input_data_type, properties):
    input_data_config = INPUT_DATA_TYPES_DICT[input_data_type]
    filename_config = input_data_config['filename_config']
    property_name = filename_config['property']
    file_paths = []
    if property_name in properties:
        property_value = properties[property_name]
        property_value_index = filename_config['property_value_index']
        filename_match_strategy = filename_config['filename_match_strategy']
        filename_from_property = property_value.split(",")[property_value_index]
        for input_filename in get_all_files_including_subdirectories(INPUT_FILES_LOCAL_FOLDER):
            if filename_match_strategy(INPUT_FILES_LOCAL_FOLDER + "/" + filename_from_property, input_filename):
                file_paths.append(input_filename)
    return file_paths


def get_all_files_including_subdirectories(main_path):
    list_of_file = os.listdir(main_path)
    all_files = []
    for entry in list_of_file:
        full_path = main_path + "/" + entry
        if os.path.isdir(full_path):
            all_files.extend(get_all_files_including_subdirectories(full_path))
        else:
            all_files.append(full_path)

    return all_files


def get_ordered_active_input_data_types(properties):
    active_input_data_types_dict = {}
    for (input_data_type, input_data_config) in INPUT_DATA_TYPES_DICT.items():
        if RTInputDataTypes.HISTORICAL == RT_INPUT_DATA_TYPE or input_data_type != 'HISTORICAL':
            if len(get_existing_file_paths_for_input_data_type(input_data_type, properties)) != 0:
                priority = input_data_config['priority']
                if priority not in active_input_data_types_dict.keys():
                    active_input_data_types_dict[priority] = []
                active_input_data_types_dict[priority].append(input_data_type)

    sorted_keys = sorted(active_input_data_types_dict.keys(), reverse=True)
    ordered_active_input_data_types = []
    for key in sorted_keys:
        ordered_active_input_data_types.extend(active_input_data_types_dict[key])
    return ordered_active_input_data_types


def generate_file_sha256(file_path, block_size=2 ** 20):
    md = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            buffer = f.read(block_size)
            if not buffer:
                break
            md.update(buffer)
    return md.hexdigest()


def get_required_property(properties_dict, property_name):
    if property_name not in properties_dict:
        raise Exception(f'{property_name} property is required')
    return properties_dict[property_name]


def load_global_constants(properties_dict):
    global WELL, RT_INPUT_DATA_TYPE, CLEAN_START, ACTIVE_RUN_TYPES, DATA_SYNCH_RATE_IN_SECONDS, \
        BROOK_SENT_RT_INPUT_DATA_FILENAME, BROOK_SENT_RT_INPUT_DATA_FILE_PATH, OUTPUT_DATA_NAMES_FILENAMES_DICT, \
        INPUT_DATA_FILENAMES_TO_BE_SENT, OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED, CONSOLE_LOGGING_INPUT_DATA_CONTENT, \
        CONSOLE_LOGGING_OUTPUT_DATA_CONTENT, OUTPUT_DATA_ROW_LIMIT

    WELL = get_required_property(properties_dict, 'well.name')

    RT_INPUT_DATA_TYPE = RTInputDataTypes[
        get_required_property(properties_dict, 'brook.python-client.rt-input-data-type')]

    CLEAN_START = True if get_required_property(properties_dict, 'brook.python-client.clean-start') == 'YES' else False

    ACTIVE_RUN_TYPES = [RunTypes[value] for value in
                        get_required_property(properties_dict, 'brook.python-client.active-run-types').split(",")]

    DATA_SYNCH_RATE_IN_SECONDS = float(
        get_required_property(properties_dict, 'brook.python-client.data-synch-rate-in-seconds'))

    BROOK_SENT_RT_INPUT_DATA_FILENAME = f"sent-Sample_{DATA_SYNCH_RATE_IN_SECONDS}{BROOK_RT_INPUT_DATA_FILENAME_POSTFIX}"

    BROOK_SENT_RT_INPUT_DATA_FILE_PATH = f"{SENT_DATA_INPUT_FILES_LOCAL_FOLDER}/{BROOK_SENT_RT_INPUT_DATA_FILENAME}"

    if INPUT_DATA_FILENAME_TO_BE_SENT_PROPERTY_NAME in properties_dict:
        INPUT_DATA_FILENAMES_TO_BE_SENT = get_required_property(properties_dict,
                                                                INPUT_DATA_FILENAME_TO_BE_SENT_PROPERTY_NAME).split(",")
    OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED = True if get_required_property(properties_dict,
                                                                         OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED_PROPERTY_NAME) == 'YES' else False
    CONSOLE_LOGGING_INPUT_DATA_CONTENT = True if get_required_property(properties_dict,
                                                                       CONSOLE_LOGGING_INPUT_DATA_CONTENT_PROPERTY_NAME) == 'YES' else False
    CONSOLE_LOGGING_OUTPUT_DATA_CONTENT = True if get_required_property(properties_dict,
                                                                        CONSOLE_LOGGING_OUTPUT_DATA_CONTENT_PROPERTY_NAME) == 'YES' else False
    OUTPUT_DATA_ROW_LIMIT = int(get_required_property(properties_dict, 'outputdata.rowlimit'))

    OUTPUT_DATA_NAMES_FILENAMES_DICT = {
        'REAL_TIME': {
            'filename': f"{WELL}_Sentinel_Time_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': True,
            'jwlf': True,
            'three-lines-header': True
        },
        'TORQUE_DRAG_FIELD': {
            'filename': f"{WELL}_Sentinel_TD_Field_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': True,
            'jwlf': True,
        },
        'TORQUE_DRAG_BROOMSTICK': {
            'filename': f"{WELL}_Sentinel_TD_Broomstick_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': False,
            'jwlf': True,
        },
        'DRILLING_DEPTH': {
            'filename': f"{WELL}_Sentinel_Depth_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': True,
            'jwlf': True,
        },
        'SWAB_AND_SURGE_PLOT': {
            'filename': f"{WELL}_Sentinel_SwabSurge_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': False,
            'jwlf': True,
        },
        'COMPOSITE_TORQUE_DRAG_FIELD': {
            'filename': f"{WELL}_Sentinel_TD_Field_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': True,
            'jwlf': True,
        },
        'RIG_STATE_DATA': {
            'filename': f"{WELL}_Sentinel_RigState_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': True,
            'jwlf': True,
        },
        'W2W_WEIGHT_TO_WEIGHT': {
            'filename': f"{WELL}_Sentinel_W2W_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': True,
            'jwlf': True,
        },
        'S2S_SLIP_TO_SLIP_TEMPORARY': {
            'filename': f"{WELL}_Sentinel_S2STemp_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': True,
            'jwlf': True,
        },
        'S2S_SLIP_TO_SLIP': {
            'filename': f"{WELL}_Sentinel_S2S_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': False,
            'jwlf': True,
        },
        'DRILLING_STAND_DATA': {
            'filename': f"{WELL}_Sentinel_RealTime_KPIs_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': True,
            'jwlf': True,
        },
        'PIPE_ROCKING_HEAT_MAP': {
            'filename': f"{WELL}_Sentinel_PipeRockingHeatMap_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': True,
            'jwlf': True,
        },
        'DATA_QUALITY_SUMMARY': {
            'filename': f"{WELL}_Sentinel_DataQuality_Summary_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': False,
            'jwlf': True,
        },
        'SURVEY_REPORT': {
            'filename': f"{WELL}_Sentinel_Directional_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': False,
            'jwlf': True,
        },
        'BIT_DEPTH_METADATA': {
            'filename': f"{WELL}_Sentinel_BitDepth_Metadata_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': True,
            'jwlf': True,
        },
        'PUMP_SWEEP_DATA': {
            'filename': f"{WELL}_Sentinel_Pump_Sweep_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': True,
            'jwlf': True,
        },
        'DOWNLINK_DATA': {
            'filename': f"{WELL}_Sentinel_Downlink_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': True,
            'jwlf': True,
        },
        'TORQUE_DRAG_BROOMSTICK_RERUN': {
            'filename': f"{WELL}_Sentinel_TD_Broomstick_Rerun_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': False,
            'jwlf': True,
        },
        'SWAB_AND_SURGE_RERUN_PLOT': {
            'filename': f"{WELL}_Sentinel_SwabSurge_Rerun_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
            'appended': False,
            'jwlf': True,
        },
        'EVENT_TRACKER': {
            'filename': f"{WELL}_Sentinel_Event_Tracker.json",
            'appended': False,
            'jwlf': False,
        },
        'BROOK_DATA_PROCESSING_NOTE': {
            'output_data_full_file_path': f"{LOGS_FOLDER}/{WELL}_Sentinel_Processing_Notes.txt",
            'appended': True,
            'jwlf': False,
        },
        'LOGS': {
            'appended': True,
            'jwlf': False,
            'output_data_full_file_path': f"{LOGS_FOLDER}/{WELL}_logs.log",
            'value_mapper': map_log_json_to_log_format
        },
    }


def load_headers_dict():
    global headers_dict, ordered_curves_dict, HEADERS_DICT_SAVE_FILE_PATH, ORDERED_CURVES_DICT_SAVE_FILE_PATH
    if exists(HEADERS_DICT_SAVE_FILE_PATH):
        with open(HEADERS_DICT_SAVE_FILE_PATH, 'r', encoding='utf8') as f:
            headers_dict = json.load(f)
    if exists(ORDERED_CURVES_DICT_SAVE_FILE_PATH):
        with open(ORDERED_CURVES_DICT_SAVE_FILE_PATH, 'r', encoding='utf8') as f:
            ordered_curves_dict = json.load(f)


def create_send_effective_properties(properties):
    properties_lines = [key + ' =' + (' ' + value if value else '') + "\n" for key, value in properties.items()]
    with open(SENT_EFFECTIVE_PROPERTIES_LOCAL_FILE_PATH, 'w', encoding='utf8') as f:
        f.writelines(properties_lines)


def load_properties():
    global properties
    if properties is None:
        properties_dict = read_properties_dict_from_file()
        load_global_constants(properties_dict)
        ordered_active_input_data_types = get_ordered_active_input_data_types(properties_dict)
        if 'brook.data.read.init-data' not in properties_dict.keys() and len(ordered_active_input_data_types) > 0:
            properties_dict['brook.data.read.init-data'] = ",".join(ordered_active_input_data_types)
        if RTInputDataTypes.HISTORICAL == RT_INPUT_DATA_TYPE:
            properties_dict['brook.data.read.timeseries'] = 'NO'
        properties = properties_dict


def register_sentinel_cloud_instance(access_token):
    global LOG_INPUT_DSP_FOLDER, LOG_OUTPUT_DSP_FOLDER, GENERAL_DATA_INPUT_DSP_FOLDER, GENERAL_DATA_OUTPUT_DSP_FOLDER, properties, \
        REGION, WELL
    client_name = CLIENT
    region = REGION
    well = WELL
    if CLEAN_START:
        delete_well_if_exists(client_name)
        delete_well_data_from_dsp(client_name, region, well, access_token)
    well_id = register_well_if_doesnt_exist(client_name)

    log(f"LOG_INPUT_DSP_FOLDER={LOG_INPUT_DSP_FOLDER}")
    log(f"LOG_OUTPUT_DSP_FOLDER={LOG_OUTPUT_DSP_FOLDER}")
    log(f"GENERAL_DATA_INPUT_DSP_FOLDER={GENERAL_DATA_INPUT_DSP_FOLDER}")
    log(f"GENERAL_DATA_OUTPUT_DSP_FOLDER={GENERAL_DATA_OUTPUT_DSP_FOLDER}")
    log(f"Brook well with id={well_id} is registered")

    properties_dict = {}
    for key in properties.keys():
        if not key.startswith(PYTHON_BROOK_CLIENT_PROPERTIES_PREFIX):
            properties_dict[key] = properties[key]

    update_sentinel_cloud_properties(client_name, properties_dict)
    log(f"Sentinel Cloud custom properties applied")


def remove_local_data():
    if INPUT_FILES_LOCAL_FOLDER in os.listdir() \
            and SENT_DATA_INPUT_FILES_LOCAL_FOLDER.split("/")[-1] in os.listdir(INPUT_FILES_LOCAL_FOLDER):
        shutil.rmtree(SENT_DATA_INPUT_FILES_LOCAL_FOLDER)
    if OUTPUT_FILES_LOCAL_FOLDER in os.listdir():
        shutil.rmtree(OUTPUT_FILES_LOCAL_FOLDER)
    if LOGS_FOLDER in os.listdir():
        shutil.rmtree(LOGS_FOLDER)
    if SENT_EFFECTIVE_PROPERTIES_LOCAL_FOLDER.split("/")[-1] in os.listdir(CFG_LOCAL_FOLDER):
        shutil.rmtree(SENT_EFFECTIVE_PROPERTIES_LOCAL_FOLDER)


def rename_sent_input_files():
    filenames = [filename for filename in os.listdir(SENT_DATA_INPUT_FILES_LOCAL_FOLDER) if
                 filename.endswith(BROOK_RT_INPUT_DATA_FILENAME_POSTFIX)]
    if len(filenames) == 1:
        os.rename(f"{SENT_DATA_INPUT_FILES_LOCAL_FOLDER}/{filenames[0]}", BROOK_SENT_RT_INPUT_DATA_FILE_PATH)


def get_rt_input_data_offset():
    offset = 0
    num_of_lines_list = [get_num_of_lines_in_file(file_path) for file_path in
                         get_all_partitions_of_file(BROOK_SENT_RT_INPUT_DATA_FILE_PATH)]
    for num in num_of_lines_list:
        offset += num
    return offset


def update_output_files_synch_timestamps():
    global till_date_time_jwlf, till_date_time_general_data
    data = {}
    if till_date_time_jwlf:
        data['till_date_time_jwlf'] = str(date_time_to_milliseconds_timestamp(till_date_time_jwlf))
    if till_date_time_general_data:
        data['till_date_time_general_data'] = str(date_time_to_milliseconds_timestamp(till_date_time_general_data))
    with open(OUTPUT_FILES_SYNCH_TIMESTAMP_FILE_PATH, 'w', encoding='utf8') as file:
        json_data = json.dumps(data)
        file.write(json_data)


def load_last_till_date_time():
    global till_date_time_jwlf, till_date_time_general_data
    file_path_parts = OUTPUT_FILES_SYNCH_TIMESTAMP_FILE_PATH.split("/")
    data = {}
    if not CLEAN_START and file_path_parts[-1] in os.listdir("/".join(file_path_parts[0:-1])):
        with open(OUTPUT_FILES_SYNCH_TIMESTAMP_FILE_PATH, 'r', encoding='utf8') as file:
            try:
                data_json = "\n".join(file.readlines())
                data = json.loads(data_json)
            except Exception:
                traceback.print_exc()
                print(f"Loading last till date times failure. Setting default values...")

    if 'till_date_time_jwlf' in data:
        till_date_time_jwlf = milliseconds_timestamp_to_date_time(int(data['till_date_time_jwlf']))
    else:
        till_date_time_jwlf = MIN_DATE_TIME

    if 'till_date_time_general_data' in data:
        till_date_time_general_data = milliseconds_timestamp_to_date_time(int(data['till_date_time_general_data']))
    else:
        till_date_time_general_data = MIN_DATE_TIME


def create_data_directories():
    global properties
    if CLEAN_START:
        remove_local_data()
    if INPUT_FILES_LOCAL_FOLDER not in os.listdir():
        os.makedirs(INPUT_FILES_LOCAL_FOLDER)
    if SENT_DATA_INPUT_FILES_LOCAL_FOLDER.split("/")[-1] not in os.listdir(INPUT_FILES_LOCAL_FOLDER):
        os.makedirs(SENT_DATA_INPUT_FILES_LOCAL_FOLDER)
    if OUTPUT_FILES_LOCAL_FOLDER not in os.listdir():
        os.makedirs(OUTPUT_FILES_LOCAL_FOLDER)
    if LOGS_FOLDER not in os.listdir():
        os.makedirs(LOGS_FOLDER)
    if SENT_EFFECTIVE_PROPERTIES_LOCAL_FOLDER.split("/")[-1] not in os.listdir(CFG_LOCAL_FOLDER):
        os.makedirs(SENT_EFFECTIVE_PROPERTIES_LOCAL_FOLDER)
    create_send_effective_properties(properties)


def run_request_with_retries(fun, default_response=None, max_retries=24 * 3600):
    global sent
    sent = False
    response = default_response
    request_index = 0
    while not sent and request_index < max_retries:
        try:
            response = fun()
            sent = True
        except requests.exceptions.RequestException as e:
            sent = False
            log(f"Error occured: {e}")
            time.sleep(1)
    return response


def synch_output_data(access_token):
    global _new_output_data_lines_dict, refresh_all_output_data, last_keycloak_realm, last_access_token, \
        last_data_fetch_datetime, CLIENT, REGION, WELL

    _new_output_data_lines_dict = get_new_output_data_lines(CLIENT, REGION, WELL, access_token)
    log_output_data(_new_output_data_lines_dict)
    update_output_files_synch_timestamps()

    if len(_new_output_data_lines_dict) > 0:
        last_data_fetch_datetime = datetime.datetime.utcnow()


def get_num_of_lines_in_file(file_path):
    num_of_lines = 0
    try:
        with open(file_path, 'r', encoding='utf8') as source_data_file:
            for _ in source_data_file:
                num_of_lines += 1
    except FileNotFoundError:
        pass
    return num_of_lines


def send_contextual_input_data():
    global properties
    ordered_active_input_data_types = get_ordered_active_input_data_types(properties)
    for active_input_data_type in ordered_active_input_data_types:
        send_input_data(active_input_data_type, properties)


load_properties()
create_data_directories()
load_headers_dict()
rename_sent_input_files()
rt_input_data_offset = get_rt_input_data_offset()
load_last_till_date_time()
access_token = get_access_token(CLIENT)
register_sentinel_cloud_instance(access_token)

if RunTypes.PRODUCER in ACTIVE_RUN_TYPES:
    active_input_data_types = get_ordered_active_input_data_types(properties)
    if RTInputDataTypes.HISTORICAL == RT_INPUT_DATA_TYPE and len(active_input_data_types) == 0:
        log(f"Historical mode is on but no input data to send was found!")
        time.sleep(3)
    else:
        send_contextual_input_data()

if RTInputDataTypes.REAL_TIME == RT_INPUT_DATA_TYPE:
    if RunTypes.PRODUCER in ACTIVE_RUN_TYPES:
        source_data_filename = properties["inputdatatimebased.filename"].split(",")[0]
        source_data_file_path = f"{INPUT_FILES_LOCAL_FOLDER}/{source_data_filename}"
        with open(source_data_file_path, 'r', encoding='utf8') as source_data_file:
            input_header_line = ''
            input_data_index = 0
            num_of_lines = get_num_of_lines_in_file(source_data_file_path)

            input_data_lines = []
            for input_data_line in source_data_file:
                start_datetime = datetime.datetime.utcnow()
                input_data_index += 1
                input_data_line = input_data_line.replace('\n', '')
                if input_header_line == '':
                    input_header_line = input_data_line
                    log_input_header(input_data_line)
                    continue
                if rt_input_data_offset >= input_data_index:
                    continue

                input_data_lines.append(input_data_line)
                input_jwlf = create_jwlf(RT_INPUT_DATA_TYPE.name, input_header_line, input_data_lines)
                access_token = get_access_token(CLIENT)
                if input_jwlf:
                    run_request_with_retries(
                        lambda: send(CLIENT, REGION, WELL, LOG_INPUT_DSP_FOLDER, input_jwlf, access_token))
                    log_input_data(BROOK_SENT_RT_INPUT_DATA_FILE_PATH, input_data_lines=input_data_lines)
                    if not CONSOLE_LOGGING_INPUT_DATA_CONTENT:
                        log(f"New line of real time data was sent to Sentinel Cloud")
                    input_data_lines = []

                if RunTypes.CONSUMER in ACTIVE_RUN_TYPES:
                    synch_output_data(access_token)

                if sentinel_exit:
                    break
                last_access_token = access_token
                last_keycloak_realm = CLIENT

                send_contextual_input_data()
                sleep_time = max(
                    DATA_SYNCH_RATE_IN_SECONDS - (datetime.datetime.utcnow() - start_datetime).microseconds / 1000000,
                    0)
                time.sleep(sleep_time)

if RunTypes.CONSUMER in ACTIVE_RUN_TYPES:
    log("Receiving data from Sentinel Cloud...")
    while running_consumer:
        start_datetime = datetime.datetime.utcnow()
        access_token = get_access_token(CLIENT)
        synch_output_data(access_token)
        if sentinel_exit:
            break
        if last_data_fetch_datetime is not None:
            running_consumer = datetime.datetime.utcnow() - datetime.timedelta(
                milliseconds=LAST_DATA_FETCH_MAX_DELAY_MILLISECONDS) < last_data_fetch_datetime

        last_access_token = access_token
        last_keycloak_realm = CLIENT

        if RunTypes.PRODUCER in ACTIVE_RUN_TYPES:
            send_contextual_input_data()
        sleep_time = max(
            DATA_SYNCH_RATE_IN_SECONDS - (datetime.datetime.utcnow() - start_datetime).microseconds / 1000000, 0)
        time.sleep(sleep_time)
    log("Finished receiving data from Sentinel Cloud")
