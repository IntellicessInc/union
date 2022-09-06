from src.utils.jwlf import JWLFHeader, JWLFCurve


class SavedJWLFLog(object):
    def __init__(self, union_id: str, client: str, region: str, asset: str, folder: str, creation_timestamp: int,
                 header: JWLFHeader = None, curves: [JWLFCurve] = None, data: [[object]] = None):
        self.union_id = union_id
        self.client = client
        self.region = region
        self.asset = asset
        self.folder = folder
        self.creation_timestamp = creation_timestamp
        self.header = header
        self.curves = curves
        self.data = data

    def to_dict(self):
        return {
            'id': self.union_id,
            'client': self.client,
            'region': self.region,
            'asset': self.asset,
            'folder': self.folder,
            'creationTimestamp': self.creation_timestamp,
            'header': self.header.to_dict(),
            'curves': [curve.to_dict() for curve in self.curves],
            'data': self.data
        }

    @staticmethod
    def from_dict(obj):
        if obj is None:
            return None

        return SavedJWLFLog(union_id=obj.get('id', None),
                            client=obj.get('client', None),
                            region=obj.get('region', None),
                            asset=obj.get('asset', None),
                            folder=obj.get('folder', None),
                            creation_timestamp=obj.get('creationTimestamp', None),
                            header=JWLFHeader.from_dict(obj.get('header', None)),
                            curves=[JWLFCurve.from_dict(curve) for curve in obj.get('curves', [])],
                            data=obj.get('data', None))
