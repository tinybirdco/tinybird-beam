import argparse
import logging
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from tinybird.beam import WriteToTinybird


class GroupWindowsIntoBatches(beam.PTransform):
    def __init__(self, batch_key, batch_size, max_buffering_duration_secs=60):
        self.batch_key = batch_key
        self.batch_size = int(batch_size)
        self.max_buffering_duration_secs = int(max_buffering_duration_secs)

    def expand(self, pcoll):
        return (
            pcoll | "Generate tuple" >> beam.Map(lambda x: (x.get(self.batch_key, None), x))
                  | "Group into batches"
                    >> beam.transforms.util.GroupIntoBatches(self.batch_size, self.max_buffering_duration_secs)
                  | "Abandon Dummy Key" >> beam.MapTuple(lambda _, val: val)
        )


def run(input_topic=None,
        batch_size=None,
        batch_seconds=None,
        batch_key=None,
        project=None,
        temp_location=None,
        region=None,
        bq_table=None,
        tb_host=None,
        tb_token=None,
        tb_datasource=None,
        tb_columns=None):
    # `save_main_session` is set to true because some DoFn's rely on
    # globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args,
        streaming=True,
        save_main_session=True,
        setup_file='./setup.py',
        project=project,
        temp_location=temp_location,
        region=region,
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        out = (
            pipeline
            | "Read From PubSub" >> beam.io.ReadFromPubSub(topic=input_topic)
            | "Remove PII from Input" >> beam.Map(lambda payload: json.loads(payload))
        )

        out | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=f'{project}:{bq_table}',
                # schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                # create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=temp_location
            )

        (out | "Group into batches" >> GroupWindowsIntoBatches(batch_key, batch_size, batch_seconds)
             | "Stream to Tinybird" >> WriteToTinybird(tb_host, tb_token, tb_datasource, tb_columns)
        )


if __name__ == "__main__":  # noqa
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help="The Cloud Pub/Sub topic to read from.\n"
        '"projects/<PROJECT_NAME>/topics/<TOPIC_NAME>".',
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=1000,
        help="Number of elements to batch. If batch_key is provided, batches are per key",
    )
    parser.add_argument(
        "--batch_seconds",
        type=int,
        default=30,
        help="Number of seconds to batch if batch_size is not completed",
    )
    parser.add_argument(
        "--batch_key",
        type=str,
        default=None,
        help="Key of each element to partition the data. Leave to None to not partition",
    )
    parser.add_argument(
        '--temp_location',
        dest='temp_location')
    parser.add_argument(
        '--bq_table',
        dest='bq_table',
        help='Bigquery table path (should already exist). Ex: dataset.table')
    parser.add_argument(
        '--project',
        dest='project',
        help='Google Cloud Platform project ID')
    parser.add_argument(
        '--region',
        dest='region',
        help='GCS region')
    parser.add_argument(
        '--tb_host',
        dest='tb_host',
        help='Tinybird host')
    parser.add_argument(
        '--tb_token',
        dest='tb_token',
        help='Tinybird token')
    parser.add_argument(
        '--tb_datasource',
        dest='tb_datasource',
        help='Tinybird Data Source name')
    parser.add_argument(
        '--tb_columns',
        dest='tb_columns',
        help='Comma separated list of columns in the destination Data Source')
    known_args, pipeline_args = parser.parse_known_args()

    # import ipdb; ipdb.set_trace(context=30)
    run(
        input_topic=known_args.input_topic,
        batch_size=known_args.batch_size,
        batch_seconds=known_args.batch_seconds,
        batch_key=known_args.batch_key,
        project=known_args.project,
        temp_location=known_args.temp_location,
        region=known_args.region,
        bq_table=known_args.bq_table,
        tb_host=known_args.tb_host,
        tb_token=known_args.tb_token,
        tb_datasource=known_args.tb_datasource,
        tb_columns=known_args.tb_columns.split(',')
    )
