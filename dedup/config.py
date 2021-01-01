def read_config(fname):
  with open(fname,'r') as f:
    data = f.read().split('\n')
  data = [l.strip() for l in data if l.strip()]
  return eval(f'dict({",".join(data)})')


class Config:
  def __init__(self,config_file='config.cfg',
      expected_param=['root_dir','db_file','vid_library','vid_ext','img_ext']):
    try:
      config = read_config(config_file)
    except Exception:
      print("Could not parse config file!")
      raise
    for k in expected_param:
      try:
        setattr(self,k,config.pop(k))
      except KeyError:
        print(f"Missing parameter in config: {k}")
        raise
    if not self.root_dir.endswith("/"):
      self.root_dir += "/"
    if not self.vid_library.endswith("/"):
      self.vid_library += "/"
    if config:
      raise AttributeError("Unexpected config parameter(s): {config}")
    for param in (self.root_dir,self.db_file,self.vid_library):
      assert isinstance(param,str),f"Invalid config parameter: {param}"
    for param in (self.vid_ext,self.img_ext):
      assert isinstance(param,list),f"Invalid config parameter: {param}"
      for ext in param:
        assert isinstance(ext,str),f"Invalid config parameter: {param}"
