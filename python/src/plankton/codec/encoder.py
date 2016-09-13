from abc import abstractmethod, ABCMeta
import io
import sys
import uuid

from plankton.codec import shared


__all__ = ["encode", "Encoder"]


class EncodeError(Exception):
  pass


class SharedStructureDetected(Exception):
  pass


if sys.version_info < (3,):
  _INT_TYPES = (int, long)
  _BASESTRING_TYPE = basestring
else:
  _INT_TYPES = (int,)
  _BASESTRING_TYPE = str


class Encoder(object):
  """
  An abstract value encoder. Most of the work of encoding takes place here, then
  the subclasses tweak various semantics.
  """
  __metaclass__ = ABCMeta

  def __init__(self, out):
    self._out = out
    self._default_string_encoding = "utf-8"

  def on_int(self, value):
    if value == 0:
      self._write_tag(shared.INT_0_TAG)
    elif value == 1:
      self._write_tag(shared.INT_1_TAG)
    elif value == 2:
      self._write_tag(shared.INT_2_TAG)
    elif value == -1:
      self._write_tag(shared.INT_M1_TAG)
    elif value < 0:
      self._write_tag(shared.INT_M_TAG)
      self._write_unsigned_int(-(value+1))
    else:
      self._write_tag(shared.INT_P_TAG)
      self._write_unsigned_int(value)

  def on_singleton(self, value):
    if value is None:
      self._write_tag(shared.SINGLETON_NULL_TAG)
    elif value is True:
      self._write_tag(shared.SINGLETON_TRUE_TAG)
    else:
      assert value is False
      self._write_tag(shared.SINGLETON_FALSE_TAG)

  def on_string(self, bytes, encoding):
    if len(bytes) == 0:
      self._write_tag(shared.DEFAULT_STRING_0_TAG)
    elif len(bytes) == 1:
      self._write_tag(shared.DEFAULT_STRING_1_TAG)
    elif len(bytes) == 2:
      self._write_tag(shared.DEFAULT_STRING_2_TAG)
    elif len(bytes) == 3:
      self._write_tag(shared.DEFAULT_STRING_3_TAG)
    elif len(bytes) == 4:
      self._write_tag(shared.DEFAULT_STRING_4_TAG)
    elif len(bytes) == 5:
      self._write_tag(shared.DEFAULT_STRING_5_TAG)
    elif len(bytes) == 6:
      self._write_tag(shared.DEFAULT_STRING_6_TAG)
    elif len(bytes) == 7:
      self._write_tag(shared.DEFAULT_STRING_7_TAG)
    else:
      self._write_tag(shared.DEFAULT_STRING_N_TAG)
      self._write_unsigned_int(len(bytes))
    self._write_bytes(bytes)

  def on_begin_array(self, length):
    if length == 0:
      self._write_tag(shared.ARRAY_0_TAG)
    elif length == 1:
      self._write_tag(shared.ARRAY_1_TAG)
    elif length == 2:
      self._write_tag(shared.ARRAY_2_TAG)
    elif length == 3:
      self._write_tag(shared.ARRAY_3_TAG)
    else:
      self._write_tag(shared.ARRAY_N_TAG)
      self._write_unsigned_int(length)

  def on_begin_map(self, length):
    if length == 0:
      self._write_tag(shared.MAP_0_TAG)
    elif length == 1:
      self._write_tag(shared.MAP_1_TAG)
    elif length == 2:
      self._write_tag(shared.MAP_2_TAG)
    elif length == 3:
      self._write_tag(shared.MAP_3_TAG)
    else:
      self._write_tag(shared.MAP_N_TAG)
      self._write_unsigned_int(length)

  def on_id(self, bytes):
    ivalue = uuid.UUID(bytes=bytes).int
    if ivalue >= 2**64:
      self._write_tag(shared.ID_128_TAG)
      self._write_bytes(bytes)
    elif ivalue >= 2**32:
      self._write_tag(shared.ID_64_TAG)
      self._write_bytes(bytes[8:16])
    elif ivalue >= 2**16:
      self._write_tag(shared.ID_32_TAG)
      self._write_bytes(bytes[12:16])
    else:
      assert ivalue < 2**16
      self._write_tag(shared.ID_16_TAG)
      self._write_bytes(bytes[14:16])

  def on_blob(self, value):
    self._write_tag(shared.BLOB_N_TAG)
    self._write_unsigned_int(len(value))
    self._write_bytes(value)

  def on_begin_seed(self, length):
    if length == 0:
      self._write_tag(shared.SEED_0_TAG)
    elif length == 1:
      self._write_tag(shared.SEED_1_TAG)
    elif length == 2:
      self._write_tag(shared.SEED_2_TAG)
    elif length == 3:
      self._write_tag(shared.SEED_3_TAG)
    else:
      self._write_tag(shared.SEED_N_TAG)
      self._write_unsigned_int(length)

  def on_begin_struct(self, tags):
    if tags == []:
      self._write_tag(shared.STRUCT_LINEAR_0_TAG)
    elif tags == [0]:
      self._write_tag(shared.STRUCT_LINEAR_1_TAG)
    elif tags == [0, 1]:
      self._write_tag(shared.STRUCT_LINEAR_2_TAG)
    elif tags == [0, 1, 2]:
      self._write_tag(shared.STRUCT_LINEAR_3_TAG)
    elif tags == [0, 1, 2, 3]:
      self._write_tag(shared.STRUCT_LINEAR_4_TAG)
    elif tags == [0, 1, 2, 3, 4]:
      self._write_tag(shared.STRUCT_LINEAR_5_TAG)
    elif tags == [0, 1, 2, 3, 4, 5]:
      self._write_tag(shared.STRUCT_LINEAR_6_TAG)
    elif tags == [0, 1, 2, 3, 4, 5, 6]:
      self._write_tag(shared.STRUCT_LINEAR_7_TAG)
    else:
      self._write_tag(shared.STRUCT_N_TAG)
      self._write_unsigned_int(len(tags))
      self._write_struct_tags(tags)

  def _write_struct_tags(self, tags):
    if len(tags) == 0:
      return
    top_nibble = [None]
    def add_nibble(nibble):
      if top_nibble[0] is None:
        top_nibble[0] = nibble
      else:
        byte = (top_nibble[0] << 4) | nibble
        self._write_byte(byte)
        top_nibble[0] = None
    def add_value(value):
      while value >= 0x8:
        add_nibble((value & 0x7) | 0x8)
        value = (value >> 3) - 1
      add_nibble(value)
    last_value = tags[0]
    add_value(last_value)
    index = 1
    while index < len(tags):
      tag = tags[index]
      if tag == last_value:
        end_index = index + 1
        while end_index < len(tags) and tags[end_index] == last_value:
          end_index += 1
        add_value(0)
        add_value(end_index - index)
        index = end_index
      else:
        delta = tag - last_value
        add_value(delta)
        last_value = tag
        index += 1
    add_nibble(0)

  def on_get_ref(self, distance):
    self._write_tag(shared.GET_REF_TAG)
    self._write_unsigned_int(distance)

  def on_add_ref(self):
    self._write_tag(shared.ADD_REF_TAG)

  def _write_unsigned_int(self, value):
    assert value >= 0
    while value >= 0x80:
      self._write_byte((value & 0x7F) | 0x80)
      value = (value >> 7) - 1
    self._write_byte(value)

  def _write_bytes(self, data):
    self._out.write(data)

  def _write_byte(self, byte):
    self._out.write(bytearray([byte]))

  def _write_tag(self, byte):
    self._write_byte(byte)

  def on_invalid_value(self, value):
    raise Exception("Invalid value {}".format(value))


class ObjectGraphDecoder(object):

  def __init__(self, visitor):
    self._visitor = visitor

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

  def decode(self, value):
    self._preprocess(value)
    return self._decode(value)

  def _decode(self, value):
    if (value is None) or isinstance(value, bool):
      return self._visitor.on_singleton(value)
    elif isinstance(value, _INT_TYPES):
      return self._visitor.on_int(value)
    elif isinstance(value, _BASESTRING_TYPE):
      return self._visitor.on_string(value.encode("utf-8"), None)
    elif self._is_array(value):
      return self._decode_array(value)
    elif self._is_map(value):
      return self._decode_map(value)
    elif isinstance(value, uuid.UUID):
      return self._visitor.on_id(value.bytes)
    elif isinstance(value, bytearray):
      return self._visitor.on_blob(value)
    elif self._is_seed(value):
      return self._decode_seed(value)
    elif self._is_struct(value):
      return self._decode_struct(value)
    else:
      return self._visitor.on_invalid_value(value)

  def _decode_array(self, array):
    should_add_ref = self._should_add_ref(array)
    if not should_add_ref:
      ref = self._get_backref(array)
      if not ref is None:
        return self._visitor.on_get_ref(ref)
    self._visitor.on_begin_array(len(array))
    if should_add_ref:
      self._visitor.on_add_ref()
    for value in self._traverse_composite(array):
      self._decode(value)

  def _decode_map(self, map):
    should_add_ref = self._should_add_ref(map)
    if not should_add_ref:
      ref = self._get_backref(map)
      if not ref is None:
        return self._visitor.on_get_ref(ref)
    self._visitor.on_begin_map(len(map))
    if should_add_ref:
      self._visitor.on_add_ref()
    for value in self._traverse_composite(map):
      self._decode(value)

  def _decode_seed(self, seed):
    should_add_ref = self._should_add_ref(seed)
    if not should_add_ref:
      ref = self._get_backref(seed)
      if not ref is None:
        return self._visitor.on_get_ref(seed)
    self._visitor.on_begin_seed(len(seed.fields))
    if should_add_ref:
      self._visitor.on_add_ref()
    self._decode(seed.header)
    for value in self._traverse_composite(seed):
      self._decode(value)

  def _decode_struct(self, struct):
    should_add_ref = self._should_add_ref(struct)
    if not should_add_ref:
      ref = self._get_backref(struct)
      if not ref is None:
        return self._visitor.on_get_ref(struct)
    self._visitor.on_begin_struct([t for (t, v) in struct.fields])
    if should_add_ref:
      self._visitor.on_add_ref()
    for value in self._traverse_composite(struct):
      self._decode(value)

  @staticmethod
  def _is_array(value):
    """Is the given value one we'll consider an array?"""
    return isinstance(value, (list, tuple))

  @staticmethod
  def _is_map(value):
    """Is the given value one we'll consider a map?"""
    return isinstance(value, dict)

  @staticmethod
  def _is_struct(value):
    """Is the given value one we'll consider a struct?"""
    return isinstance(value, shared.Struct)

  @staticmethod
  def _is_seed(value):
    """Is the given value one we'll consider a seed?"""
    return isinstance(value, shared.Seed)

  @classmethod
  def _is_composite(cls, value):
    return (cls._is_array(value)
        or cls._is_map(value)
        or cls._is_struct(value)
        or cls._is_seed(value))

  @classmethod
  def _traverse_composite(cls, value):
    """
    For a given composite value generates all the sub-values one at at time.
    Note that for the composites where values come in pairs (like key-value for
    maps) this generates them alternatingly the same way they'll appear in the
    encoded format.
    """
    if cls._is_array(value):
      for elm in value:
        yield elm
    elif cls._is_map(value):
      for (k, v) in value.items():
        yield k
        yield v
    elif cls._is_struct(value):
      for (t, v) in value.fields:
        yield v
    else:
      assert cls._is_seed(value)
      for (f, v) in value.fields.items():
        yield f
        yield v


class ObjectTreeDecoder(ObjectGraphDecoder):
  """
  An encoder that assumes that the input is strictly tree shaped (that is, there
  are no cycles or shared substructures) and throws an exception if that
  assumption turns out not to hold.
  """

  def __init__(self, visitor):
    super(ObjectTreeDecoder, self).__init__(visitor)
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


class ReferenceTrackingObjectGraphDecoder(ObjectGraphDecoder):
  """
  An encoder that keeps track of shared subexpressions and inserts references
  appropriately to represent them in the output.
  """

  def __init__(self, visitor):
    super(ReferenceTrackingObjectGraphDecoder, self).__init__(visitor)
    self.has_seen_once = set()
    self.has_seen_twice = set()
    self.ref_offsets = {}
    self._ref_count = 0

  def _preprocess(self, value):
    self._look_for_shared_structure(value)

  def _look_for_shared_structure(self, value):
    if self._is_composite(value):
      if self._check_if_already_seen(value):
        return
      for elm in self._traverse_composite(value):
        self._look_for_shared_structure(elm)

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
    offset = self.ref_offsets.get(id(value))
    if offset is None:
      return None
    else:
      return self._ref_count - offset - 1

  def _should_add_ref(self, value):
    value_id = id(value)
    if value_id in self.ref_offsets:
      # We've already made a reference to this value so don't make one again.
      return False
    elif value_id in self.has_seen_twice:
      # Preprocessing says we'll want to make a reference to this and we haven't
      # seen it before so this looks like the time to make the reference.
      self.ref_offsets[value_id] = self._ref_count
      self._ref_count += 1
      return True
    else:
      return False


def _encode_with_decoder(decoder_type, value):
    out = io.BytesIO()
    encoder = Encoder(out)
    decoder_type(encoder).decode(value)
    return out.getvalue()


def encode(value):
  try:
    # Assume the value is tree-shaped and try encoding it as such.
    return _encode_with_decoder(ObjectTreeDecoder, value)
  except SharedStructureDetected:
    # It wasn't tree shaped -- fall back on reference tracking then.
    return _encode_with_decoder(ReferenceTrackingObjectGraphDecoder, value)
