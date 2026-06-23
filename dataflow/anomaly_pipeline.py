import argparse
import logging
import sys
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.window import FixedWindows

# Add the generated python directory to the path to import telemetry_pb2
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'gen', 'python'))
from v1 import telemetry_pb2

class ParseMessage(beam.DoFn):
    def process(self, element):
        try:
            frame = telemetry_pb2.MetricFrame()
            frame.ParseFromString(element)
            yield frame
        except Exception as e:
            logging.error(f"Error parsing protobuf message: {e}")

class ExtractMetrics(beam.DoFn):
    def process(self, frame):
        # Extract relevant fields from protobuf
        agent_id = frame.agent_id
        fleet_id = frame.fleet_id
        cpu = frame.system.cpu_utilization_pct if frame.HasField('system') else 0.0
        
        # Key by fleet_id to aggregate anomalies by fleet
        if fleet_id:
            yield (fleet_id, {'agent_id': agent_id, 'cpu': cpu, 'count': 1})

class AggregateAnomalies(beam.CombineFn):
    def create_accumulator(self):
        return {'total_cpu': 0.0, 'anomaly_count': 0}

    def add_input(self, accumulator, input):
        accumulator['total_cpu'] += input['cpu']
        accumulator['anomaly_count'] += input['count']
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = {'total_cpu': 0.0, 'anomaly_count': 0}
        for acc in accumulators:
            merged['total_cpu'] += acc['total_cpu']
            merged['anomaly_count'] += acc['anomaly_count']
        return merged

    def extract_output(self, accumulator):
        if accumulator['anomaly_count'] == 0:
            return {'avg_cpu': 0, 'anomaly_count': 0}
        return {
            'avg_cpu': accumulator['total_cpu'] / accumulator['anomaly_count'],
            'anomaly_count': accumulator['anomaly_count']
        }

def format_for_bigquery(element, window=beam.DoFn.WindowParam):
    fleet_id, metrics = element
    return {
        'fleet_id': fleet_id,
        'window_start': window.start.to_rfc3339(),
        'window_end': window.end.to_rfc3339(),
        'anomaly_count': metrics['anomaly_count'],
        'avg_cpu_during_anomalies': float(metrics['avg_cpu'])
    }

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True, help='Input PubSub topic')
    parser.add_argument('--output_table', required=True, help='Output BigQuery table')
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (p 
         | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
         | 'ParseJSON' >> beam.ParDo(ParseMessage())
         | 'ExtractMetrics' >> beam.ParDo(ExtractMetrics())
         | 'Window' >> beam.WindowInto(FixedWindows(60)) # 60-second tumbling windows
         | 'AggregatePerFleet' >> beam.CombinePerKey(AggregateAnomalies())
         | 'FormatForBQ' >> beam.Map(format_for_bigquery)
         | 'WriteToBQ' >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema='fleet_id:STRING, window_start:TIMESTAMP, window_end:TIMESTAMP, anomaly_count:INTEGER, avg_cpu_during_anomalies:FLOAT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
