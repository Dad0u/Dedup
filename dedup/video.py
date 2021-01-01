import os
import numpy as np
import ffmpeg

Y,X = 27,48
fps = 2


def mkarr(v):
  """
  Computes and returns the Numpy fingerprint of a video
  Warning ! It will be entirely saved to RAM, so make sure it can fit...
  """
  h,w,_ = get_res(v)
  out,_ = ffmpeg.input(v)\
  .output('pipe:', format='rawvideo', pix_fmt='rgb24',s=f'{X}x{Y}',r=2)\
  .run(capture_stdout=True)
  v = np.frombuffer(out, np.uint8).reshape([-1,Y,X,3])
  return v


def to1d(a,factor=5):
  """
  Turns the (t,27,48,3) array into a (t//10,3) 1d array
  """
  b = np.average(a,axis=(1,2))
  t = b.shape[0]
  return np.average(
      b[:t-t%factor].reshape(-1,factor,3),axis=1).astype(np.uint8)


def get_res(v):
  """
  Read the resolution and length of the video at the given path
  """
  p = ffmpeg.probe(v)
  vs = next((stream for stream in
    p['streams'] if stream['codec_type'] == 'video'),None)
  return (int(vs['height']),int(vs['width']),int(float(vs['duration'])))


class Video:
  def __init__(self,path,**kwargs):
    self.path = path
    for kw in ['_height','_width','_length','sigrgb','signature_path']:
      setattr(self,kw,kwargs.pop(kw.strip('_'),None))
    if kwargs:
      raise AttributeError(f"Unknown kwargs in Video.__init__: {kwargs}")
    self._signature = None

  def compute_attrs(self):
    try:
      self._height,self._width,self._length = get_res(self.path)
    except Exception as e:
      print(f"[Video] Error processing {self.path}: {type(e).__name__}: {e}")
      self._height, self._width = 0,0
      self._length = 0

  def compute_sig(self):
    self._signature = mkarr(self.path)
    #d = vid_library_path+os.path.dirname(self.path)
    #os.makedirs(d,exist_ok=True)
    #np.save(d+os.basename(self.path)+'.npy')
    self.sigrgb = to1d(self._signature)

  @property
  def height(self):
    if self._height is None:
      self.compute_attrs()
    return self._height

  @property
  def width(self):
    if self._width is None:
      self.compute_attrs()
    return self._width

  @property
  def length(self):
    if self._length is None:
      self.compute_attrs()
    return self._length

  @property
  def signature(self):
    if self._signature is None and os.path.exists(self.signature_file):
      self._signature = np.load(self.signature_file)
    return self._signature
