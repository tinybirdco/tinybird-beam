# tinybird-beam

A Tinybird Apache Beam connector to ingest from an Apache Beam pipeline in DataFlow, Flink or Spark to a [Tinybird](https://www.tinybird.co/) Data Source

Directories:

- `tinybird`: Contains the source code for the Apache Beam connector
- `dataflow`: Pipeline definition to stream data from Google PubSub to Tinybird using the Apache Beam connector

## Installation

Install the `tinybird-beam` [PyPI package](https://pypi.org/project/tinybird-beam/) in your Python project. See the `dataflow` directory for a usage example.

```sh
pip install tinybird-beam
```

## Development

```sh
python3 -m venv env
source env/bin/activate
pip install -e .
```

## DataFlow example