import enum


class ValueType(enum.Enum):
    BOOLEAN = 'boolean'
    STRING = 'string'
    INTEGER = 'integer'
    FLOAT = 'float'
    DATETIME = 'datetime'

    @staticmethod
    def of_value(value):
        for value_type in list(ValueType):
            if value_type.value == value:
                return value_type
        return None


class JWLFCurve(object):
    def __init__(self, name: str = None, description: str = None, quantity: str = None, unit: str = None,
                 value_type: ValueType = None, dimensions: int = None,
                 axis: [str] = None, max_size: int = None):
        self.name = name
        self.description = description
        self.quantity = quantity
        self.unit = unit
        self.value_type = value_type
        self.dimensions = dimensions
        self.axis = axis
        self.max_size = max_size

    @staticmethod
    def from_dict(obj):
        if obj is None:
            return None

        return JWLFCurve(name=obj.get('name', None),
                         description=obj.get('description', None),
                         quantity=obj.get('quantity', None),
                         unit=obj.get('unit', None),
                         value_type=ValueType.of_value(obj.get('valueType', None)),
                         dimensions=obj.get('dimensions', None),
                         axis=obj.get('axis', None),
                         max_size=obj.get('maxSize', None))

    def to_dict(self):
        return {
            'name': self.name,
            'description': self.description,
            'quantity': self.quantity,
            'unit': self.unit,
            'valueType': str(self.value_type.value) if self.value_type else None,
            'dimensions': self.dimensions,
            'axis': self.axis,
            'maxSize': self.max_size
        }


class JWLFHeader(object):
    def __init__(self, name: str = None, description: str = None, well: str = None, wellbore: str = None,
                 field: str = None, country: str = None, date: str = None, operator: str = None,
                 service_company: str = None, run_number: str = None, elevation: float = None, source: str = None,
                 start_index: float = None, end_index: float = None, step: float = None, data_uri: str = None,
                 metadata=None):
        if metadata is None:
            metadata = {}
        self.name = name
        self.description = description
        self.well = well
        self.wellbore = wellbore
        self.field = field
        self.country = country
        self.date = date
        self.operator = operator
        self.service_company = service_company
        self.run_number = run_number
        self.elevation = elevation
        self.source = source
        self.start_index = start_index
        self.end_index = end_index
        self.step = step
        self.data_uri = data_uri
        self.metadata = metadata

    @staticmethod
    def from_dict(obj):
        if obj is None:
            return None

        return JWLFHeader(name=obj.get('name', None),
                          description=obj.get('description', None),
                          well=obj.get('well', None),
                          wellbore=obj.get('wellbore', None),
                          field=obj.get('field', None),
                          country=obj.get('country', None),
                          date=obj.get('date', None),
                          operator=obj.get('operator', None),
                          service_company=obj.get('serviceCompany', None),
                          run_number=obj.get('runNumber', None),
                          elevation=obj.get('elevation', None),
                          source=obj.get('source', None),
                          start_index=obj.get('startIndex', None),
                          end_index=obj.get('endIndex', None),
                          step=obj.get('step', None),
                          data_uri=obj.get('dataUri', None),
                          metadata=obj.get('metadata', None))

    def to_dict(self):
        return {
            'name': self.name,
            'description': self.description,
            'well': self.well,
            'wellbore': self.wellbore,
            'field': self.field,
            'country': self.country,
            'date': self.date,
            'operator': self.operator,
            'serviceCompany': self.service_company,
            'runNumber': self.run_number,
            'elevation': self.elevation,
            'source': self.source,
            'startIndex': self.start_index,
            'endIndex': self.end_index,
            'step': self.step,
            'dataUri': self.data_uri,
            'metadata': self.metadata
        }


class JWLFLog(object):
    def __init__(self, header: JWLFHeader = None, curves: [JWLFCurve] = None, data: [[object]] = None):
        self.header = header
        self.curves = curves
        self.data = data

    def to_dict(self):
        return {
            'header': self.header.to_dict(),
            'curves': [curve.to_dict() for curve in self.curves],
            'data': self.data
        }

    @staticmethod
    def from_dict(obj):
        if obj is None:
            return None

        return JWLFLog(header=JWLFHeader.from_dict(obj.get('header', None)),
                       curves=[JWLFCurve.from_dict(curve) for curve in obj.get('curves', [])],
                       data=obj.get('data', []))
