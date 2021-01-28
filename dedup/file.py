import os
from .hashing import quick_hash_file, hash_file

NOMEDIA = 0
IMAGE = 1
VIDEO = 2


def human_readable(size):
  """
  Takes a size in Bytes, returns a human readable string
  """
  if size < 1024:
    return f"{size} B"
  for i,prefix in enumerate('KMGTP',1):
    if size/1024**i < 1024:
      return f"{size/1024**i:.2f} {prefix}iB"
  return f"{size/1024**i:.2f} {prefix}iB"


class File:
  """
  Object representing a file in the context of the Database
  """
  def __init__(self,path,**kwargs):
    self.path = path
    for kw in ['_qhash','_size','hash']:
      setattr(self,kw,kwargs.pop(kw.strip('_'),None))
    # Do not forget to raise an Exception if we have invalid kwargs
    # If we make a media, the media constructor will handle it
    if kwargs:
      raise AttributeError(f"Unknown kwargs in File.__init__: {kwargs}")
    self.type = NOMEDIA

  @property
  def qhash(self):
    if self._qhash is None:
      self._qhash = quick_hash_file(self.path)
      if self.size < 3*1024*1024:
        self.hash = self._qhash
    return self._qhash

  @property
  def size(self):
    if self._size is None:
      self._size = os.path.getsize(self.path)
    return self._size

  def compute_hash(self,recompute=False):
    if recompute or self.hash is None:
      self.hash = hash_file(self.path)

  def __repr__(self):
    t = {NOMEDIA:'NOMEDIA',IMAGE:'IMAGE',VIDEO:'VIDEO'}[self.type]
    return f"<File:{t}> {self.path} ({human_readable(self.size)})"
