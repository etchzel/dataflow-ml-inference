import argparse
import logging
import sys
import apache_beam as beam
from modules.drawer import Drawer
from modules.object_detection import ObjectDetectionHandler
from modules.options import UserOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.ml.inference.base import RunInference

class GetImageURI(beam.DoFn):
  def __init__(self, bucket_name, prefix):
    self.bucket_name = bucket_name
    self.prefix = prefix

  def process(self, element):
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(self.bucket_name.get())
    for blob in bucket.list_blobs(match_glob=f'{self.prefix.get()}/*?'):
      yield {"bucket": blob.bucket.name, "name": blob.name}
  
def main(known_args, pipeline_args):
  runner = known_args.runner
  pipeline_options = PipelineOptions(pipeline_args, streaming=False, runner=runner)

  with beam.Pipeline(options=pipeline_options) as pipeline:
    user_options = pipeline_options.view_as(UserOptions)
    predict = (
      pipeline
      | "Initialize" >> beam.Create(['init'])
      | "Get Image URLs" >> beam.ParDo(GetImageURI(user_options.bucket_name, user_options.prefix))
      | "Inference" >> RunInference(model_handler=ObjectDetectionHandler())
      | "Draw Bounding Boxes" >> beam.ParDo(Drawer(user_options.bucket_name, user_options.output))
      | "Print Result" >> beam.Map(print)
    )

if __name__ == "__main__":
  # Configure logging
  log = logging.getLogger()
  log.setLevel(logging.INFO)
  stream_handler = logging.StreamHandler(sys.stdout)
  stream_handler.setLevel(logging.INFO)
  log.addHandler(stream_handler)

  # Use arguments
  parser = argparse.ArgumentParser()
  parser.add_argument(
    "--runner", default="DataflowRunner"
  )
  parser.add_argument(
    "--staging_location", default='gs://trainer_gcs_001/dataflow/staging'
  )
  parser.add_argument(
    "--temp_location", default='gs://trainer_gcs_001/dataflow/temp'
  )
  parser.add_argument(
    "--template_location", default="gs://trainer_gcs_001/dataflow/templates/batch-online-predict"
  )
  parser.add_argument(
    "--requirements_file", default='requirements.txt'
  )
  parser.add_argument(
    "--setup_file", default="./setup.py"
  )
  parser.add_argument(
    "--region", default="us-central1"
  )
  parser.add_argument(
    "--project", default='engineering-training-413102'
  )
  known_args, pipeline_args = parser.parse_known_args()
  if known_args.runner=="DataflowRunner":
    pipeline_args.extend(
      ["--staging_location="+known_args.staging_location]
    )
    pipeline_args.extend(
      ["--temp_location="+known_args.temp_location]
    )
    pipeline_args.extend(
      ["--template_location="+known_args.template_location]
    )
    pipeline_args.extend(
      ["--requirements_file="+known_args.requirements_file]
    )
    pipeline_args.extend(
      ["--setup_file="+known_args.setup_file]
    )
    pipeline_args.extend(
      ["--region="+known_args.region]
    )
    pipeline_args.extend(
      ["--project="+known_args.project]
    )
  print(known_args, pipeline_args)
  main(known_args, pipeline_args)