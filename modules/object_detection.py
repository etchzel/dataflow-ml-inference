from typing import Any, Dict, Iterable, Sequence
from apache_beam.ml.inference.base import ModelHandler

class ObjectDetectionHandler(ModelHandler):
  """
  Handler for object detection model.
  This DoFn accepts a batch of images as bytearray and sends them to the Cloud Vision API
  for inference.
  """
  def load_model(self) -> Any:
    """
    Initiate Cloud Vision API client
    """
    from google.cloud import vision_v1
    client = vision_v1.ImageAnnotatorClient()

    return client
  
  def run_inference(
    self, 
    batch: Sequence, 
    model: Any, 
    inference_args: Dict[str, Any] | None = None
  ) -> Iterable:
    """
    Run inference on a batch of images by sending them to the Cloud Vision API.
    """
    from google.cloud import vision_v1
    from google.cloud.vision_v1.types import Feature, ImageSource

    # Create feature
    feature = Feature(type_=Feature.Type.OBJECT_LOCALIZATION)

    # List images to run inference
    batch_images = []
    for image_blob in batch:
      image_uri = f"gs://{image_blob.get('bucket')}/{image_blob.get('name')}"
      image = vision_v1.Image(source=ImageSource(image_uri=image_uri))
      image_request = vision_v1.AnnotateImageRequest(image=image, features=[feature])
      batch_images.append(image_request)

    # Create batch request
    request = vision_v1.BatchAnnotateImagesRequest(requests=batch_images)

    # Run inference
    responses = model.batch_annotate_images(request=request).responses

    output = [
      {
        "bucket": image_blob.get('bucket'),
        "image_blob": image_blob.get('name'),
        "objects": response
      }
      for image_blob, response in zip(batch, responses)
    ]

    return output