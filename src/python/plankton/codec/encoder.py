from abc import abstractmethod, ABCMeta
import io
import uuid

from plankton.codec import shared


__all__ = ["encode"]


class EncodeError(Exception):
  pass


class SharedStructureDetected(Exception):
  pass


class Encoder(shared.Codec):
  """
  An abstract value encoder. Most of the work of encoding takes place here, then
  the subclasses tweak various semantics.
  """
  __metaclass__ = ABCMeta

  def __init__(self, out):
    self.refs = {}
    self.out = out
    self.ref_count = 0

  @abstractmethod
  def _preprocess(self, value):
    """Called before this encoder starts serializing the given value."""
    pass

  @abstractmethod
  def _should_add_ref(self, value):
    """
    Returns true iff the subclass determines that we should create a ref to the
    given value at this point in the stream.
    """
    pass

  @abstractmethod
  def _get_backref(self, value):
    """
    If at this point we should make a reference back to a previous occurrence of
    the given value, return of absolute offset of that occurrence.
    """

  def write(self, value):
    """Write the given value to this encoder's output stream."""
    self._preprocess(value)
    self._encode(value)

  def _encode(self, value):
    """Writes a value or subvalue to this encoder's output stream."""
    if value is None:
      self._write_tag(shared.Codec.SINGLETON_NULL_TAG)
    elif isinstance(value, bool):
      if value:
        self._write_tag(shared.Codec.SINGLETON_TRUE_TAG)
      else:
        self._write_tag(shared.Codec.SINGLETON_FALSE_TAG)
    elif isinstance(value, (int, long)):
      self._encode_int(value)
    elif self._is_array(value):
      self._encode_array(value)
    elif self._is_map(value):
      self._encode_map(value)
    elif isinstance(value, uuid.UUID):
      self._encode_id(value)
    else:
      raise EncodeError()

  def _encode_int(self, value):
    if value == 0:
      self._write_tag(shared.Codec.INT_0_TAG)
    elif value == 1:
      self._write_tag(shared.Codec.INT_1_TAG)
    elif value == 2:
      self._write_tag(shared.Codec.INT_2_TAG)
    elif value == -1:
      self._write_tag(shared.Codec.INT_M1_TAG)
    elif value < 0:
      self._write_tag(shared.Codec.INT_M_TAG)
      self._write_unsigned_int(-(value+1))
    else:
      self._write_tag(shared.Codec.INT_P_TAG)
      self._write_unsigned_int(value)

  def _encode_array(self, value):
    if self._should_add_ref(value):
      self._add_ref()
    else:
      ref = self._get_backref(value)
      if not ref is None:
        return self._get_ref(ref)
    if len(value) == 0:
      self._write_tag(shared.Codec.ARRAY_0_TAG)
    elif len(value) == 1:
      self._write_tag(shared.Codec.ARRAY_1_TAG)
    elif len(value) == 2:
      self._write_tag(shared.Codec.ARRAY_2_TAG)
    elif len(value) == 3:
      self._write_tag(shared.Codec.ARRAY_3_TAG)
    else:
      self._write_tag(shared.Codec.ARRAY_N_TAG)
      self._write_unsigned_int(len(value))
    for elm in value:
      self._encode(elm)

  def _encode_map(self, value):
    if self._should_add_ref(value):
      self._add_ref()
    else:
      ref = self._get_backref(value)
      if not ref is None:
        return self._get_ref(ref)
    if len(value) == 0:
      self._write_tag(shared.Codec.MAP_0_TAG)
    elif len(value) == 1:
      self._write_tag(shared.Codec.MAP_1_TAG)
    elif len(value) == 2:
      self._write_tag(shared.Codec.MAP_2_TAG)
    elif len(value) == 3:
      self._write_tag(shared.Codec.MAP_3_TAG)
    else:
      self._write_tag(shared.Codec.MAP_N_TAG)
      self._write_unsigned_int(len(value))
    for (key, value) in value.items():
      self._encode(key)
      self._encode(value)

  def _encode_id(self, value):
    ivalue = value.int
    if ivalue >= 2**64:
      self._write_tag(shared.Codec.ID_128_TAG)
      self._write_bytes(value.bytes)
    elif ivalue >= 2**32:
      self._write_tag(shared.Codec.ID_64_TAG)
      self._write_bytes(value.bytes[8:16])
    elif ivalue >= 2**16:
      self._write_tag(shared.Codec.ID_32_TAG)
      self._write_bytes(value.bytes[12:16])
    else:
      assert ivalue < 2**16
      self._write_tag(shared.Codec.ID_16_TAG)
      self._write_bytes(value.bytes[14:16])

  def _get_ref(self, ref_offset):
    distance = self.ref_count - ref_offset - 1
    self._write_tag(shared.Codec.GET_REF_TAG)
    self._write_unsigned_int(distance)

  def _add_ref(self):
    self._write_tag(shared.Codec.ADD_REF_TAG)
    self.ref_count += 1

  def _write_unsigned_int(self, value):
    assert value >= 0
    while value >= 0x80:
      self._write_byte((value & 0x7F) | 0x80)
      value = (value >> 7) - 1
    self._write_byte(value)

  def _write_bytes(self, data):
    self.out.write(data)

  def _write_byte(self, byte):
    self.out.write(bytearray([byte]))

  def _write_tag(self, byte):
    self._write_byte(byte)

  @staticmethod
  def _is_array(value):
    """Is the given value one we'll consider an array?"""
    return isinstance(value, (list, tuple))

  @staticmethod
  def _is_map(value):
    """Is the given value one we'll consider a map?"""
    return isinstance(value, dict)



class TreeEncoder(Encoder):
  """
  An encoder that assumes that the input is strictly tree shaped (that is, there
  are no cycles or shared substructures) and throws an exception if that
  assumption turns out not to hold.
  """

  def __init__(self, out):
    super(TreeEncoder, self).__init__(out)
    self.ids_seen = set()

  def _preprocess(self, value):
    pass

  def _get_backref(self, value):
    """
    We never actually return backrefs but we use this call to track whether
    we've seen a value before.
    """
    value_id = id(value)
    if value_id in self.ids_seen:
      raise SharedStructureDetected()
    self.ids_seen.add(value_id)

  def _should_add_ref(self, value):
    """
    We don't encode shared structure we only detect it so we never add refs.
    """
    return False


class ReferenceTrackingEncoder(Encoder):
  """
  An encoder that keeps track of shared subexpressions and inserts references
  appropriately to represent them in the output.
  """

  def __init__(self, out):
    super(ReferenceTrackingEncoder, self).__init__(out)
    self.has_seen_once = set()
    self.has_seen_twice = set()
    self.ref_offsets = {}

  def _preprocess(self, value):
    self._look_for_shared_structure(value)

  def _look_for_shared_structure(self, value):
    if self._is_array(value):
      if self._check_if_already_seen(value):
        return
      for elm in value:
        self._look_for_shared_structure(elm)
    elif self._is_map(value):
      if self._check_if_already_seen(value):
        return
      for (k, v) in value.items():
        self._look_for_shared_structure(k)
        self._look_for_shared_structure(v)

  def _check_if_already_seen(self, value):
    """
    Checks whether we've previously seen the given value. Returns true iff that
    is the case.
    """
    value_id = id(value)
    if value_id in self.has_seen_once:
      self.has_seen_twice.add(value_id)
      return True
    else:
      self.has_seen_once.add(value_id)
      return False

  def _get_backref(self, value):
    return self.ref_offsets.get(id(value))

  def _should_add_ref(self, value):
    value_id = id(value)
    if value_id in self.ref_offsets:
      # We've already made a reference to this value so don't make one again.
      return False
    elif value_id in self.has_seen_twice:
      # Preprocessing says we'll want to make a reference to this and we haven't
      # seen it before so this looks like the time to make the reference.
      self.ref_offsets[value_id] = self.ref_count
      return True
    else:
      return False


def _encode_with_encoder(encoder_type, value):
    out = io.BytesIO()
    encoder_type(out).write(value)
    return out.getvalue()


def encode(value):
  try:
    # Assume the value is tree-shaped and try encoding it as such.
    return _encode_with_encoder(TreeEncoder, value)
  except SharedStructureDetected:
    # It wasn't tree shaped -- fall back on reference tracking then.
    return _encode_with_encoder(ReferenceTrackingEncoder, value)
