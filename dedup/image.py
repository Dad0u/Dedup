import cv2
import numpy as np
from PIL import Image as PILImage


def make_signature(img):
  h,w,_ = img.shape
  Y,X = 3,3 # The size of the evaluation grid
  sig = np.empty((Y,X,3),dtype=np.uint16)
  for c in range(3):
    for i in range(Y):
      for j in range(X):
        sig[i,j,c] = np.average(img[
            int(h*i/Y):int(h*(i+1)/Y),
            int(w*j/X):int(w*(j+1)/X),c])*256
  return sig


class Image:
  def __init__(self,path,**kwargs):
    self.path = path
    for kw in ['_height','_width','brightness','signature']:
      setattr(self,kw,kwargs.pop(kw.strip('_'),None))
    if kwargs:
      raise AttributeError(f"Unknown kwargs in Image.__init__: {kwargs}")
    if self.brightness is None:
      self.brightness = (0,0,0)

  def compute_attr(self):
    try:
      img = PILImage.open(self.path)
      self._width,self._height = img.size
    except Exception as e:
      print(f"[Image] Error processing {self.path}: {type(e).__name__}: {e}")
      self._height, self._width = 0,0

  def compute_signature(self):
    try:
      img = cv2.imread(self.path,cv2.IMREAD_COLOR)
      self._height,self._width,_ = img.shape
      self.brightness = tuple(np.average(img,axis=(0,1)))
      self.signature = make_signature(img)
    except Exception as e:
      print(f"[Image] Error processing {self.path}: {type(e).__name__}: {e}")
      self._height, self._width = 0,0
      self.brightness = (0,0,0)
      self.signature = np.zeros((3,3,3),dtype=np.unit16)

  @property
  def height(self):
    if self._height is None:
      self.compute_attr()
    return self._height

  @property
  def width(self):
    if self._width is None:
      self.compute_attr()
    return self._width

  @property
  def r(self):
    return self.brightness[0]

  @property
  def g(self):
    return self.brightness[1]

  @property
  def b(self):
    return self.brightness[2]
