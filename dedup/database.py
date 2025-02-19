import os
from shutil import rmtree
from typing import List
from multiprocessing import Pool,cpu_count
import numpy as np
import sqlite3

from .file import File,IMAGE,VIDEO,NOMEDIA
from .video import Video
from .image import Image
from .config import Config


# Functions to be called in a multiprocess fashion
def mkargs_file(f):
  return (f.path,f.qhash,f.size,f.type,f.hash)


def mkargs_img(f):
  return (f.path,f.height,f.width,int(f.r*256),
    int(f.g*256),int(f.b*256),
    None if f.signature is None else f.signature.tobytes())


def mkargs_vid(f):
  # Updating the signature will not be done when adding many files
  # (RAM would be limiting, so useless)
  #if f.signature is not None:
  #  os.makedirs(os.path.dirname(f.signature_path),exist_ok=True)
  #  np.save(f.signature_path,f.signature)
  return (f.path,f.height,f.width,f.length,
    None if f.sigrgb is None else f.sigrgb.tobytes())


def mk_video_sig(f):
  f.compute_signatures()
  return f


def mk_image_sig(f):
  f.compute_signature()
  return f


def get_all_files(d):
  """
  Get all the files in the folder (recursively)
  """
  r = []
  absd = os.path.abspath(d)
  l = os.listdir(absd)
  for f in l:
    full = os.path.join(absd,f)
    if os.path.isdir(full):
      r += get_all_files(full)
    else:
      r.append(full)
  return r


class Database:
  """
  The core of this program: represents a collection of files

  Can process the files given in the directory specified inconfig file,
  compare them and find duplicates in many different ways
  """
  def __init__(self,config_file):
    self.config_file = config_file
    self.cfg = Config(config_file)
    self.db = sqlite3.connect(self.cfg.db_file)

  def reset(self):
    """
    To create or completely wipe the database
    """
    if input("Reset database? ALL DATA WILL BE LOST ").strip().lower() != 'y':
      print("Cancelled")
      raise Exception("Aborted")
    print("Erasing...",end='',flush=True)
    if os.path.exists(self.cfg.vid_library):
      rmtree(self.cfg.vid_library)
    os.makedirs(self.cfg.vid_library)
    cur = self.db.cursor()
    cur.execute("DROP TABLE IF EXISTS files")
    cur.execute("DROP TABLE IF EXISTS img")
    cur.execute("DROP TABLE IF EXISTS vid")
    cur.execute("DROP TABLE IF EXISTS known_diff")
    # == TABLE files: contains all the files ==
    # path: Path of the file
    # qhash: quick hash of the file (mandatory)
    # Type: image, video or none (0: non-media, 1: image, 2: video)
    # hash: complete hash
    cur.execute("""CREATE TABLE files(id INTEGER PRIMARY KEY,
    path TEXT UNIQUE,
    qhash BLOB NOT NULL,
    size INT,
    type INT NOT NULL,
    hash BLOB);""")
    # == TABLE img : Contains metadata specific for images ==
    # id: id of the corresponding file
    # height, width: resolution of the image
    # size: product height*width UNUSED
    # r: Average value of the r channel
    # g: Average value of the g channel
    # b: Average value of the b channel
    #   Note : r,g and b are non-integer values between 0 and 255.
    #          They are stored as int(256*val)
    # signature: image signature for quick comparison
    cur.execute("""CREATE TABLE img(id INTEGER PRIMARY KEY,
    height INT,
    width INT,
    r INT,
    g INT,
    b INT,
    signature BLOB);""")
    # == TABLE vid : Contains metadata specific for videos ==
    # id: id of the corresponding file
    # height, width: resolution of the video
    # length: length in seconds
    # sigrgb: (t,3) array of color over time
    cur.execute("""CREATE TABLE vid(id INTEGER PRIMARY KEY,
    height INT,
    width INT,
    length INT,
    sigrgb BLOB);""")
    # == TABLE known_diff: To allow the user to specify manually files that
    # are known to be different
    cur.execute("""CREATE TABLE known_diff(
    f1 INTEGER CHECK (f1 < f2),
    f2 INTEGER CHECK (f1 < f2),
    UNIQUE (f1,f2));""")
    print("OK!")

  def _get_npy_path(self,fname):
    """
    Returns the path of the npy file containing the video signature
    """
    return self.cfg.vid_library+fname[
        len(os.path.abspath(self.cfg.root_dir)):]+'.npy'

  def new_file(self,fname):
    """
    Returns a new File object

    Will NOT check if it exists in the DB
    """
    t = self.get_type(fname)
    if t == NOMEDIA:
      return File(fname)
    elif t == IMAGE:
      return Image(fname)
    elif t == VIDEO:
      return Video(fname,signature_path=self._get_npy_path(fname))

  def get_file(self,fname):
    """
    Returns a File object from the DB given a name

    Returns None if not found
    """
    cur = self.db.cursor()
    cur.execute("SELECT id,qhash,size,type,hash FROM files WHERE path = ?",
        (fname,))
    r = cur.fetchone()
    if not r:
      return
    d = dict((k,v) for k,v in zip(['qhash','size','type','hash'],r[1:]))
    t = d.pop('type')
    if t == IMAGE:
      cur.execute("""SELECT * FROM img WHERE id =
      (SELECT id FROM files WHERE path = ?)""",(fname,))
      r = cur.fetchone()
      h,w,r,g,b,s = r[1:]
      sig = s if s is None else np.frombuffer(
          s,dtype=np.uint16).reshape(3,3,3)
      d['height'],d['width'] = h,w
      d['brightness'] = (r,g,b)
      d['signature'] = sig
      return Image(fname,**d)
    elif t == VIDEO:
      cur.execute("""SELECT * FROM vid WHERE id =
      (SELECT id FROM files WHERE path = ?)""",(fname,))
      r = cur.fetchone()
      h,w,l,s = r[1:]
      sigrgb = s if s is None else np.frombuffer(
          s,dtype=np.uint8).reshape(-1,3)
      d['height'],d['width'] = h,w
      d['length'] = l
      d['sigrgb'] = sigrgb
      return Video(fname,signature_path=self._get_npy_path(fname),**d)

  def get_type(self,fname:str):
    """
    Returns the type of a file given its name based on its extension
    """
    for ext in self.cfg.img_ext:
      if fname.endswith('.'+ext):
        return IMAGE
    for ext in self.cfg.vid_ext:
      if fname.endswith('.'+ext):
        return VIDEO
    return NOMEDIA

  def add_files(self,l:List[File]):
    """
    Adds a list of files to the db
    """
    if not l:
      print("Nothing to add")
      return
    print("Processing files")
    self._insert_files(l)
    img = [f for f in l if f.type == IMAGE]
    if img:
      print("Processing images")
      self._insert_images(img)
    else:
      print("No images")
    vid = [f for f in l if f.type == VIDEO]
    if vid:
      print("Processing videos")
      self._insert_videos(vid)
    else:
      print("No videos")
    self.db.commit()

  def _insert_files(self,l):
    """
    Internal method used to update the tables db
    """
    toadd = []
    for i,v in enumerate(Pool(cpu_count()).imap_unordered(mkargs_file,l),1):
      print(f"\r{i}/{len(l)} ({100*i/len(l):.2f}%)",end='')
      toadd.append(v)
    cur = self.db.cursor()
    print("\nAdding to db...",end='',flush=True)
    cur.executemany("""INSERT INTO files (path,qhash,size,type,hash)
    VALUES (?,?,?,?,?)""", toadd)
    print("OK")

  def _insert_images(self,l):
    toadd = []
    for i,v in enumerate(Pool(cpu_count()).imap_unordered(mkargs_img,l),1):
      print(f"\r{i}/{len(l)} ({100*(i)/len(l):.2f}%)",end='')
      toadd.append(v)
    cur = self.db.cursor()
    print("\nAdding to db...",end='',flush=True)
    cur.executemany("""INSERT INTO img (id,height,width,r,g,b,signature)
    VALUES ((SELECT id FROM files WHERE path = ?),?,?,?,?,?,?)""",toadd)
    print("OK")

  def _insert_videos(self,l):
    toadd = []
    for i,v in enumerate(Pool(cpu_count()).imap_unordered(mkargs_vid,l),1):
      print(f"\r{i}/{len(l)} ({100*i/len(l):.2f}%)",end='')
      toadd.append(v)
    cur = self.db.cursor()
    print("\nAdding to db...",end='',flush=True)
    cur.executemany("""INSERT INTO vid (id,height,width,length,sigrgb)
    VALUES ((SELECT id FROM files WHERE path = ?),?,?,?,?)""",toadd)
    print("OK")

  #def get_file_id(self,fname):
  #  """
  #  Gets the id of a file given its path
  #  """
  #  cur = self.db.cursor()
  #  cur.execute("SELECT id FROM files WHERE path = ?",(fname,))
  #  try:
  #    return cur.fetchone()[0]
  #  except IndexError:
  #    return None

  def update_file(self,f:File):
    """
    Update an entry of the db
    """
    cur = self.db.cursor()
    cur.execute("""UPDATE files SET
      qhash = ?, size = ?, type = ?, hash = ? WHERE path = ?""",
      (f.qhash, f.size,f.type, f.hash, f.path))
    if f.type == IMAGE:
      cur.execute("""UPDATE img SET
        height = ?, width = ?, r = ?, g = ?, b = ?, signature = ?
      WHERE id = (SELECT id FROM files WHERE path = ?)""",
      (f.height,f.width,
        int(f.r*256), int(f.g*256), int(f.b*256),
        None if f.signature is None else f.signature.tobytes(),
        f.path))
    elif f.type == VIDEO:
      cur.execute("""UPDATE vid SET height = ?, width = ?, length = ?,
      sigrgb = ? WHERE id = (SELECT id FROM files WHERE path = ?)""",
      (f.height, f.width,
      f.length, f.sigrgb, f.path))
      if f.signature is not None:
        os.makedirs(os.path.dirname(self._get_npy_path(f.path)),exist_ok=True)
        np.save(self._get_npy_path(f.path),f.signature)
    self.db.commit()

  def remove(self,fname:str):
    """
    Removes the file of the given name from the db
    """
    cur = self.db.cursor()
    cur.execute("""DELETE FROM img WHERE id =
        (SELECT id FROM files WHERE path = ?)""",(fname,))
    cur.execute("""DELETE FROM vid WHERE id =
        (SELECT id FROM files WHERE path = ?)""",(fname,))
    cur.execute("DELETE FROM files WHERE path = ?",(fname,))
    try:
      os.remove(self._get_npy_path(fname))
    except FileNotFoundError:
      pass
    self.db.commit()

  def remove_many(self,l:List[str]):
    """
    Removes a list of path from the db
    """
    cur = self.db.cursor()
    cur.execute(f"""DELETE FROM img WHERE id IN
        (SELECT id FROM files WHERE path IN
        ({','.join(['?' for f in l])}))""",l)
    cur.execute(f"""DELETE FROM vid WHERE id IN
        (SELECT id FROM files WHERE path IN
        ({','.join(['?' for f in l])}))""",l)
    cur.execute(f"""DELETE FROM files WHERE path IN
        ({','.join(['?' for f in l])})""",l)
    for name in l:
      try:
        os.remove(self._get_npy_path(name))
      except FileNotFoundError:
        pass
    self.db.commit()

  def detect_and_add(self):
    """
    Detect all the files in the root_dir and add them to the db
    """
    flist = get_all_files(self.cfg.root_dir)
    cur = self.db.cursor()
    cur.execute("SELECT path FROM files")
    r = [t[0] for t in cur.fetchall()]
    new = [self.new_file(f) for f in flist if f not in r]
    self.add_files(new)

  def cleanup(self):
    """
    Remove all the files from the db that are not in the root_dir
    """
    dblist = get_all_files(self.cfg.root_dir)
    cur = self.db.cursor()
    cur.execute("SELECT path FROM files")
    flist = [t[0] for t in cur.fetchall()]
    torm = [f for f in dblist if f not in flist]
    if not torm:
      print("Nothing to do")
      return
    print(f"{len(torm)} entries to remove...",end="",flush=True)
    self.remove_many(torm)
    print("Ok.")

  def compute_video_signature(self,l=None):
    """
    Compute the video signatures of the file names in the list

    If no list is given, compute all the missing vids
    """
    if l is None:
      cur = self.db.cursor()
      cur.execute("""SELECT path FROM files WHERE id IN
      (SELECT id FROM vid WHERE sigrgb IS NULL)""")
      l = [t[0] for t in cur.fetchall()]
    if not l:
      print("No video signature to compute")
      return
    for i,f in enumerate(
        Pool(4).imap_unordered(mk_video_sig,
          [self.get_file(name) for name in l]),1):
      print(f"\r{i}/{len(l)} ({100*i/len(l):.2f}%)")
      self.update_file(f)

  def compute_image_signature(self,l=None):
    """
    Compute the image signatures of the file names in the list

    If no list is given, compute all the missing signatures
    """
    if l is None:
      cur = self.db.cursor()
      cur.execute("""SELECT path FROM files WHERE id IN
      (SELECT id FROM img WHERE signature IS NULL)""")
      l = [t[0] for t in cur.fetchall()]
    if not l:
      print("No image signature to compute")
      return
    for i,f in enumerate(
        Pool(4).imap_unordered(mk_image_sig,
          [self.get_file(name) for name in l]),1):
      print(f"\r{i}/{len(l)} ({100*i/len(l):.2f}%)")
      self.update_file(f)

  def check_integrity(self): # TODO
    """
    Check if the database is coherent, remove unused entries

    Will check vid_library too
    """
    pass

  def filter_matching(self,key,source=None):
    """
    Takes a list of tuple files
    Assuming each tuple in the list contains files that must be compared
    together. If source=None, all the files from the db are compared

    Returns a list of tuples only containing matching files based on the key
    tuples of 1 are removed

    Examples : key='size', source = [(f1,f2),(f3,f4)],
    all files are the same size except f4
    out = [(f1,f2)]

    key = 'qhash', source = None
    out will contain tuples of groups of all the files with the same qhash

    key = qhash, source = [(fa1,fa2,fb1,fb2,fc)] where the files
    with the same letter have the same qhash
    out = [(fa1,fa2),(fb1,fb2)]
    """
    assert key in ['size','qhash','hash']
    cur = self.db.cursor()
    if source is None:
      cur.execute("SELECT") # TODO
