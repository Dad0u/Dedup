import hashlib
import os

M = 1048576 # 1024**2


def quick_hash_file(fname,bs=M):
  """
  Returns a quicker hash of the file at the given location
  Collisions can happen easily, always perform a full hash in case of collision

  Warning! Changing the bs will change the value of the hash
  """
  size = os.path.getsize(fname)
  if size < 3*bs:
    return hash_file(fname,bs)
  h = hashlib.md5()
  with open(fname,'rb') as f:
    h.update(f.read(bs))
    f.seek(size//2,0)
    h.update(f.read(bs))
    f.seek(-bs,2)
    h.update(f.read(bs))
  return h.digest()


def hash_file(fname,bs=M):
  """
  Returns the hash of the file at the given location
  """
  h = hashlib.md5()
  with open(fname,'rb') as f:
    chunk = f.read(bs)
    while chunk:
      h.update(chunk)
      chunk = f.read(bs)
    return h.digest()
