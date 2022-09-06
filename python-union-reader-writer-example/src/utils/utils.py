import pandas

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


def convert_to_datetime(value):
    formats = []
    formats.extend(DATE_FORMATS_ORDERED_BY_PRIORITY)
    for format in formats:
        try:
            result = pandas.to_datetime(value, format=format)
            if result != 'NaT':
                return result
        except:
            pass
    return None


def date_time_to_milliseconds_timestamp(date_time):
    return int(float(date_time.timestamp()) * 1000.0)
