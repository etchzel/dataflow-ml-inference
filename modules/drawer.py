from apache_beam import DoFn

class Drawer(DoFn):
  def __init__(self):
    self.color_palette = [
      (255, 0, 0),
      (0, 255, 0),
      (0, 0, 255),
      (255, 255, 0),
      (0, 255, 255),
      (255, 0, 255)
    ]

  def setup(self):
    import json
    import os
    from google.cloud import storage

    self.client = storage.Client()
    self.bucket = self.client.bucket(json.loads(os.environ['PIPELINE_OPTIONS'])['options']['bucket_name'])
  
  def process(self, element):
    import cv2
    import numpy as np
    from datetime import date
    from modules.utils.Point import Point

    image_blob = element.get('image_blob')
    objects = element.get('objects')
    
    # Process image blob to cv2 format
    blob = self.bucket.get_blob(image_blob)
    img = cv2.imdecode(
      np.frombuffer(blob.download_as_bytes(), np.uint8),
      cv2.IMREAD_COLOR
    )

    # Get image size to denormalize vertices
    img_height, img_width, _ = img.shape

    palette_count = 0
    for index, obj in enumerate(objects.localized_object_annotations):
      # Reset palette again if run out
      if index >= 6:
        palette_count = 0

      name = obj.name
      score = obj.score
      label = f"{name}: {int(score * 100)}%"

      # Define Bounding Box
      top_left = Point(
        x = obj.bounding_poly.normalized_vertices[0].x,
        y = obj.bounding_poly.normalized_vertices[0].y,
        denormalize=True,
        image_width=img_width,
        image_height=img_height
      )
      bottom_right = Point(
        x = obj.bounding_poly.normalized_vertices[2].x,
        y = obj.bounding_poly.normalized_vertices[2].y,
        denormalize=True,
        image_width=img_width,
        image_height=img_height
      )

      # Draw Bounding Box
      img = cv2.rectangle(img, top_left, bottom_right, color=self.color_palette[palette_count], thickness=3)

      # Background Text Space
      (w, h), _ = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)

      # Draw Text Field
      img = cv2.rectangle(img, top_left.adjust_y(-20), top_left.adjust_x(w), color=self.color_palette[palette_count], thickness=-1)
      img = cv2.putText(img, label, top_left.adjust_y(-5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255,255,255), 1)

      # Rotate over color palettes
      palette_count += 1


    # Convert image to binary again
    img = cv2.imencode(".jpg", img)[1]
    img = np.array(img)
    img = img.tobytes()

    # Upload bounded image to GCS
    try:
      filename = image_blob.split('/')[-1]
      output_blob = self.bucket.blob(f"output_image/{date.today()}/{filename}")
      output_blob.upload_from_string(img, content_type="image/jpeg")
      result = f"Succesfully uploaded {filename} with bounding boxes to GCS"
    except:
      result = f"Failed to upload {filename} to GCS"

    yield result