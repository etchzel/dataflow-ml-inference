from typing import NamedTuple

class PointBase(NamedTuple):
  x: int
  y: int

  def adjust_x(self, x: int):
    return (self.x + x, self.y)

  def adjust_y(self, y: int):
    return (self.x, self.y + y)

class Point(PointBase):
  __slots__ = ()
  def __new__(cls, x, y, denormalize=False, **kwargs):
    if denormalize:
      new_x = int(x * kwargs.get('image_width'))
      new_y = int(y * kwargs.get('image_height'))
    else:
      new_x = int(x)
      new_y = int(y)
    return super().__new__(cls, new_x, new_y)