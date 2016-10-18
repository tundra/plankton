import hashlib
import io

from plankton.codec._binary import BinaryEncoder
from plankton.schema import _id


__all__ = [
  "Schema",
  "Template",
]


class Parameter(object):

  def __init__(self, index):
    self._index = index


class Placeholder(object):

  def __init__(self, index):
    self._index = index


class Template(object):

  def __init__(self, name, params, body):
    self._name = name
    self._params = params
    self._body = body


class Schema(object):

  def __init__(self):
    self._templates = {}
    self._id = None
    self._encoded = None

  @property
  def id(self):
    if self._id is None:
      self._id = self._calc_id()
    return self._id

  def _calc_id(self):
    type = _id.IdType.default()
    hasher = type.new_hasher()
    hasher.update(self.encoded)
    return _id.Id(type, hasher.digest())

  @property
  def encoded(self):
    if self._encoded is None:
      self._encoded = self._calc_encoded()
    return self._encoded

  def _calc_encoded(self):
    out = io.BytesIO()
    encoder = BinaryEncoder(out)
    self.write(encoder)
    return out.getvalue()

  def write(self, encoder):
    encoder.on_begin_schema(len(self._templates))

  def add_template(self, template):
    self._templates[template._name] = template


class Fingerprint(object):

  def __init__(self, type, bytes):
    self.type = type
    self.bytes = bytes
