import apache_beam as beam
from typing import Any, Iterable, Dict, List
import json

DEFAULT_HOST = 'https://api.tinybird.co'


class InvalidElementType(TypeError):
    """Error Class for when an Element is not the correct type"""


class _Insert(beam.DoFn):
    """An internal DoFn to be used in a PTransform. Not for external use.
    """
    def __init__(self, data_source: str, host: str, token: str, columns: List):
        beam.DoFn.__init__(self)
        self.host = host
        self.token = token
        self.data_source = data_source
        self.columns = columns

    def process(self, element: Iterable[Dict[str, Any]]) -> None:
        try:
            if not isinstance(element, Iterable):
                raise InvalidElementType(f"Wrong element type. Expected Iterable[Dict[str, Any]], received {type(element)}")

            # imports need to be here for serialization purposes
            import requests
            import csv

            from io import StringIO
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            mode = 'append'
            datasource = self.data_source
            token = self.token

            url = f'{self.host}/v0/datasources?mode={mode}&name={datasource}'

            retry = Retry(total=5, backoff_factor=10)
            adapter = HTTPAdapter(max_retries=retry)
            _session = requests.Session()
            _session.mount('http://', adapter)
            _session.mount('https://', adapter)

            csv_chunk = StringIO()
            writer = csv.writer(csv_chunk, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)

            for row in element:
                rr = []
                for column in self.columns:
                    rr.append(row[column])
                writer.writerow(rr)

            data = csv_chunk.getvalue()
            headers = {
                'Authorization': f'Bearer {token}',
                'X-TB-Client': 'beam-0.1',
            }

            response = _session.post(url, headers=headers, files=dict(csv=data))
            ok = response.status_code < 400
            if not ok:
                raise Exception(json.dumps(response.json()))
        except Exception as e:
            raise e


class WriteToTinybird(beam.PTransform):
    def __init__(self, host: str, token: str, data_source: str, columns: List, **kwargs):
        beam.PTransform.__init__(self)
        self.host = host or DEFAULT_HOST
        self.token = token
        self.data_source = data_source
        self.columns = columns
        self.kwargs = kwargs

    def expand(self, pcoll):
        return (
            pcoll
            | beam.ParDo(_Insert(self.data_source, self.host, self.token, self.columns))
        )


class WriteToTinybirdBatch(beam.PTransform):
    def __init__(self, host: str, token: str, data_source: str, columns: List, batch_size: int = 1000, **kwargs):
        beam.PTransform.__init__(self)
        self.host = host
        self.token = token
        self.data_source = data_source
        self.batch_size = batch_size
        self.columns = columns
        self.kwargs = kwargs

    def expand(self, pcoll):
        return (
            pcoll
            | beam.BatchElements(min_batch_size=self.batch_size, max_batch_size=self.batch_size, **self.kwargs)
            | beam.ParDo(_Insert(self.data_source, self.host, self.token, self.columns))
        )
