import base64
import re
from typing import Optional

from src.utils import file_utils
from src.utils import jwlf
from src.utils.csv_jwlf_converter import get_from_dict, FILENAME_METADATA_KEY
from src.utils.jwlf import JWLFLog, ValueType

BINARIES_DATA_CURVE_NAME = "Binaries data"
BINARIES_NAMES_CURVE_NAME = "Binaries names"
BASE64_ENCODED_BINARIES_EXAMPLE_METADATA_KEY = "base64EncodedBinariesExample"


def convert_folder_to_jwlf(folder_path: str, header_value_type_mapping: Optional[dict]) -> Optional[JWLFLog]:
    jwlf_header_name = None
    jwlf_header_values_dict = dict()
    jwlf_curves = []
    data_entry = []
    encoded_binary_files_contents = []
    encoded_binary_files_names = []
    for path in file_utils.get_all_file_paths_from_folder(folder_path):
        if file_utils.is_folder(path):
            continue
        name: str = path.split('/')[-1]
        if name.endswith(".csv"):
            jwlf_header_name = name.replace(".csv", "")
            csv_content = file_utils.read_file(path)
            lines = csv_content.split("\n")
            if len(lines) != 2:
                return
            headers = lines[0].split(",")
            data_elements = lines[1].split(",")
            for i in range(0, len(headers)):
                header = headers[i]
                curve_unit = None
                data = data_elements[i]
                jwlf_header_value_matches = re.findall("\\[.+]", header)
                if len(jwlf_header_value_matches) > 0:
                    field_name = jwlf_header_value_matches[0].replace("[", "").replace("]", "")
                    jwlf_header_values_dict[field_name] = data
                    continue

                curve_unit_matches = re.findall("\\(.+\\)", header)
                if len(curve_unit_matches) > 0:
                    curve_unit = curve_unit_matches[0].replace("(", "").replace(")", "")
                    header = header.replace(curve_unit_matches[0], "")
                jwlf_curves.append(
                    jwlf.JWLFCurve(name=header, value_type=get_from_dict(header_value_type_mapping, header),
                                   dimensions=1, unit=curve_unit))
                data_entry.append(data)
        else:
            with open(path, "rb") as image2string:
                base64_encoded_content = base64.b64encode(image2string.read()).decode("ascii")
            encoded_binary_files_names.append(name)
            encoded_binary_files_contents.append(base64_encoded_content)

    if not jwlf_header_name:
        return None
    jwlf_header_values_dict["name"] = jwlf_header_name
    jwlf_header_values_dict["metadata"] = {
        FILENAME_METADATA_KEY: jwlf_header_name.replace(".csv", ""),
        BASE64_ENCODED_BINARIES_EXAMPLE_METADATA_KEY: True
    }
    jwlf_header = jwlf.JWLFHeader.from_dict(jwlf_header_values_dict)

    jwlf_curves.append(jwlf.JWLFCurve(name=BINARIES_NAMES_CURVE_NAME, value_type=ValueType.STRING,
                                      dimensions=len(encoded_binary_files_names)))
    data_entry.append(encoded_binary_files_names)
    jwlf_curves.append(jwlf.JWLFCurve(name=BINARIES_DATA_CURVE_NAME, value_type=ValueType.STRING,
                                      dimensions=len(encoded_binary_files_contents)))
    data_entry.append(encoded_binary_files_contents)

    jwlf_data = list()
    jwlf_data.append(data_entry)

    return jwlf.JWLFLog(jwlf_header, jwlf_curves, jwlf_data)


def convert_jwlf_to_folder(base_path: str, log: JWLFLog):
    folder_path = f"{base_path}/{log.header.name} files"
    file_utils.delete_file_or_directory(folder_path)
    file_utils.create_folder(folder_path)
    binaries_names_curve_indices = [i for i in range(0, len(log.curves)) if log.curves[i].name == BINARIES_NAMES_CURVE_NAME]
    if len(binaries_names_curve_indices) != 1:
        return
    binaries_data_curve_indices = [i for i in range(0, len(log.curves)) if log.curves[i].name == BINARIES_DATA_CURVE_NAME]
    if len(binaries_data_curve_indices) != 1:
        return
    binaries_names_curve_index = binaries_names_curve_indices[0]
    binaries_data_curve_index = binaries_data_curve_indices[0]

    binaries_names = log.data[0][binaries_names_curve_index]
    binaries_data_elements = log.data[0][binaries_data_curve_index]
    for i in range(0, len(binaries_data_elements)):
        file_path = folder_path + "/" + binaries_names[i]
        base64_encoded_data = binaries_data_elements[i]
        decoded_data: bytes = base64.b64decode(bytes(base64_encoded_data, "ascii"))
        file_utils.create_binary_file(file_path, decoded_data)
