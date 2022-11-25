from typing import Optional

from src.utils import jwlf
from src.utils.jwlf import JWLFLog

FILENAME_METADATA_KEY = "filename"


def get_from_dict(dictionary, key) -> Optional:
    if dictionary and key and key in dictionary.keys():
        return dictionary[key]
    return None


def cast_value_to_right_type(jwlf_curve, value):
    jwlf_value = None
    if value:
        value_type = jwlf_curve.value_type
        jwlf_value = None
        if value_type is jwlf.ValueType.STRING or value_type is jwlf.ValueType.DATETIME:
            jwlf_value = str(value)
        elif value_type is jwlf.ValueType.FLOAT:
            jwlf_value = float(value)
        elif value_type is jwlf.ValueType.INTEGER:
            jwlf_value = int(value)
        elif value_type is jwlf.ValueType.BOOLEAN:
            lower_value = str(value).lower()
            if lower_value in ['yes', 'true', 'on']:
                jwlf_value = True
            elif lower_value in ['no', 'false', 'off']:
                jwlf_value = False
        else:
            jwlf_value = value
    return jwlf_value


def to_jwlf_data_row(data_line, jwlf_curves):
    values = data_line.split(",")
    jwlf_values = []
    index = 0
    for value in values:
        jwlf_curve = jwlf_curves[index]
        jwlf_value = cast_value_to_right_type(jwlf_curve, value)
        jwlf_values.append(jwlf_value)
        index += 1
    return jwlf_values


def convert_csv_to_jwlf(well: str, filename: str, content: str,
                        header_value_type_mapping: Optional[dict]) -> Optional[JWLFLog]:
    filename_without_extension = filename.replace(".csv", "")
    jwlf_header = jwlf.JWLFHeader(name=filename_without_extension, well=well,
                                  metadata={FILENAME_METADATA_KEY: filename_without_extension.replace(".csv", "")})
    if not content:
        return jwlf.JWLFLog(jwlf_header)

    lines: [str] = content.split('\n')
    title_line = lines[0]
    headers = title_line.split(",")
    jwlf_curves = [
        jwlf.JWLFCurve(name=header, value_type=get_from_dict(header_value_type_mapping, header), dimensions=1)
        for header in headers
    ]
    if len(lines) == 1:
        return jwlf.JWLFLog(jwlf_header, jwlf_curves)

    data_lines = lines[1:]
    jwlf_data = [to_jwlf_data_row(data_line, jwlf_curves) for data_line in data_lines if data_line.strip() != '']
    return jwlf.JWLFLog(jwlf_header, jwlf_curves, jwlf_data)
