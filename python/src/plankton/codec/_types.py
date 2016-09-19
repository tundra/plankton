"""
Plankton-related type declarations.
"""


from abc import abstractmethod, ABCMeta


__all__ = ["Seed", "Struct"]


class Seed(object):

  def __init__(self, header, fields):
    self.header = header
    self.fields = fields


class Struct(object):

  def __init__(self, fields):
    self.fields = fields

  def __str__(self):
    return "#<Struct {}>".format(self.fields)


class Visitor(object):
  __metaclass__ = ABCMeta

  @abstractmethod
  def on_invalid_instruction(self, code):
    pass

  @abstractmethod
  def on_int(self, value):
    pass

  @abstractmethod
  def on_singleton(self, value):
    pass

  @abstractmethod
  def on_id(self, data):
    pass

  @abstractmethod
  def on_blob(self, data):
    pass

  @abstractmethod
  def on_string(self, data, encoding):
    pass

  @abstractmethod
  def on_begin_array(self, length, ref_key):
    pass

  @abstractmethod
  def on_begin_map(self, length, ref_key):
    pass

  @abstractmethod
  def on_begin_seed(self, field_count, ref_key):
    pass

  @abstractmethod
  def on_begin_struct(self, tags, ref_key):
    pass

  @abstractmethod
  def on_get_ref(self, offset):
    pass
