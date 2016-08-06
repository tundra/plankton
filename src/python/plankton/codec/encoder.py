from abc import abstractmethod, ABCMeta
import io
import uuid

from plankton.codec import shared


__all__ = ["encode", "Encoder"]


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
    self.default_string_encoding = "utf-8"

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
    elif isinstance(value, basestring):
      self._encode_string(value)
    elif self._is_array(value):
      self._encode_array(value)
    elif self._is_map(value):
      self._encode_map(value)
    elif isinstance(value, uuid.UUID):
      self._encode_id(value)
    elif isinstance(value, bytearray):
      self._encode_blob(value)
    elif isinstance(value, shared.Seed):
      self._encode_seed(value)
    elif isinstance(value, shared.Struct):
      self._encode_struct(value)
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

  def _encode_string(self, value):
    bytes = value.encode(self.default_string_encoding)
    if len(bytes) == 0:
      self._write_tag(shared.Codec.DEFAULT_STRING_0_TAG)
    elif len(bytes) == 1:
      self._write_tag(shared.Codec.DEFAULT_STRING_1_TAG)
    elif len(bytes) == 2:
      self._write_tag(shared.Codec.DEFAULT_STRING_2_TAG)
    elif len(bytes) == 3:
      self._write_tag(shared.Codec.DEFAULT_STRING_3_TAG)
    elif len(bytes) == 4:
      self._write_tag(shared.Codec.DEFAULT_STRING_4_TAG)
    elif len(bytes) == 5:
      self._write_tag(shared.Codec.DEFAULT_STRING_5_TAG)
    elif len(bytes) == 6:
      self._write_tag(shared.Codec.DEFAULT_STRING_6_TAG)
    elif len(bytes) == 7:
      self._write_tag(shared.Codec.DEFAULT_STRING_7_TAG)
    else:
      self._write_tag(shared.Codec.DEFAULT_STRING_N_TAG)
      self._write_unsigned_int(len(bytes))
    self._write_bytes(bytes)

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

  def _encode_blob(self, value):
    self._write_tag(shared.Codec.BLOB_N_TAG)
    self._write_unsigned_int(len(value))
    self._write_bytes(value)

  def _encode_seed(self, seed):
    if len(seed.fields) == 0:
      self._write_tag(shared.Codec.SEED_0_TAG)
    elif len(seed.fields) == 1:
      self._write_tag(shared.Codec.SEED_1_TAG)
    elif len(seed.fields) == 2:
      self._write_tag(shared.Codec.SEED_2_TAG)
    elif len(seed.fields) == 3:
      self._write_tag(shared.Codec.SEED_3_TAG)
    else:
      self._write_tag(shared.Codec.SEED_N_TAG)
      self._write_unsigned_int(len(seed.fields))
    self._encode(seed.header)
    for (field, value) in seed.fields.items():
      self._encode(field)
      self._encode(value)

  def _encode_struct(self, struct):
    tags = [t for (t, v) in struct.fields]
    if tags == []:
      self._write_tag(shared.Codec.STRUCT_LINEAR_0_TAG)
    elif tags == [0]:
      self._write_tag(shared.Codec.STRUCT_LINEAR_1_TAG)
    elif tags == [0, 1]:
      self._write_tag(shared.Codec.STRUCT_LINEAR_2_TAG)
    elif tags == [0, 1, 2]:
      self._write_tag(shared.Codec.STRUCT_LINEAR_3_TAG)
    elif tags == [0, 1, 2, 3]:
      self._write_tag(shared.Codec.STRUCT_LINEAR_4_TAG)
    elif tags == [0, 1, 2, 3, 4]:
      self._write_tag(shared.Codec.STRUCT_LINEAR_5_TAG)
    elif tags == [0, 1, 2, 3, 4, 5]:
      self._write_tag(shared.Codec.STRUCT_LINEAR_6_TAG)
    elif tags == [0, 1, 2, 3, 4, 5, 6]:
      self._write_tag(shared.Codec.STRUCT_LINEAR_7_TAG)
    else:
      header = self._encode_struct_tags(tags)
      self._write_tag(shared.Codec.STRUCT_N_TAG)
      self._write_unsigned_int(len(header))
      self._write_bytes(header)
    for (tag, value) in struct.fields:
      self._encode(value)

  @staticmethod
  def _encode_struct_tags(tags):
    result = bytearray()
    if len(tags) > 0:
      top_nibble = [None]
      def add_nibble(nibble):
        if top_nibble[0] is None:
          top_nibble[0] = nibble
        else:
          byte = (top_nibble[0] << 4) | nibble
          result.append(byte)
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
    return result

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
