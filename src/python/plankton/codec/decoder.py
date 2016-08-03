import collections
import io
import uuid

from plankton.codec import shared


__all__ = ["decode", "DefaultDataFactory"]


_ATOMIC_READERS = [None] * 256
_COMPOSITE_CONSTRUCTORS = [None] * 256
_COMPOSITE_READERS = [None] * 256


def atomic_reader(instr):
  """Marks a reader for a type that can't be referenced."""
  def register_reader(method):
    _ATOMIC_READERS[instr] = method
    return method
  return register_reader


def composite_constructor(*instrs):
  """Marks a constructor for a type that can be referenced."""
  def register_constructor(method):
    for instr in instrs:
      _COMPOSITE_CONSTRUCTORS[instr] = method
    return method
  return register_constructor


def composite_reader(instr):
  """
  Marks the reader that populates an already constructed value for a type that
  can be referenced.
  """
  def register_handler(method):
    constr = _COMPOSITE_CONSTRUCTORS[instr]
    def atomic_reader(self):
      value = constr(self)
      method(self, value)
      return value
    _ATOMIC_READERS[instr] = atomic_reader
    _COMPOSITE_READERS[instr] = method
    return method
  return register_handler


class DefaultDataFactory(object):
  """
  The default data factory that constructs plain, boring, python data for the
  different composite types.
  """

  def new_array(self):
    return []

  def new_map(self):
    return collections.OrderedDict()

  def new_id(self, bytes):
    return uuid.UUID(bytes=bytes)


class Decoder(shared.Codec):

  def __init__(self, input, factory=None):
    self.input = input
    self.current = None
    self.has_more = True
    self.refs = []
    self.factory = factory or DefaultDataFactory()
    self.default_string_encoding = "utf-8"

  def _advance(self):
    s = self.input.read(1)
    if s:
      self.current = ord(s)
    else:
      self.current = None
      self.has_more = False

  def _advance_and_read_block(self, count):
    result = self.input.read(count)
    self._advance()
    return result

  def _read_block(self, count):
    result = bytearray(count)
    if count == 0:
      return result
    else:
      result[0] = self.current
      if count > 1:
        result[1:] = self.input.read(count-1)
    self._advance()
    return result

  def read(self):
    self._advance()
    return self._decode()

  def _decode(self):
    return _ATOMIC_READERS[self.current](self)

  @atomic_reader(shared.Codec.INT_P_TAG)
  def _int_p(self):
    self._advance()
    return self._read_unsigned_int()

  @atomic_reader(shared.Codec.INT_M1_TAG)
  def _int_m1(self):
    self._advance()
    return -1

  @atomic_reader(shared.Codec.INT_0_TAG)
  def _int_0(self):
    self._advance()
    return 0

  @atomic_reader(shared.Codec.INT_1_TAG)
  def _int_1(self):
    self._advance()
    return 1

  @atomic_reader(shared.Codec.INT_2_TAG)
  def _int_2(self):
    self._advance()
    return 2

  @atomic_reader(shared.Codec.INT_M_TAG)
  def _int_m(self):
    self._advance()
    return -(self._read_unsigned_int() + 1)

  @atomic_reader(shared.Codec.SINGLETON_NULL_TAG)
  def _singleton_null(self):
    self._advance()
    return None

  @atomic_reader(shared.Codec.SINGLETON_TRUE_TAG)
  def _singleton_true(self):
    self._advance()
    return True

  @atomic_reader(shared.Codec.SINGLETON_FALSE_TAG)
  def _singleton_false(self):
    self._advance()
    return False

  @atomic_reader(shared.Codec.ID_16_TAG)
  def _id_16(self):
    data = self._advance_and_read_block(2)
    return self.factory.new_id(b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0" + data)

  @atomic_reader(shared.Codec.ID_32_TAG)
  def _id_32(self):
    data = self._advance_and_read_block(4)
    return self.factory.new_id(b"\0\0\0\0\0\0\0\0\0\0\0\0" + data)

  @atomic_reader(shared.Codec.ID_64_TAG)
  def _id_64(self):
    data = self._advance_and_read_block(8)
    return self.factory.new_id(b"\0\0\0\0\0\0\0\0" + data)

  @atomic_reader(shared.Codec.ID_128_TAG)
  def _id_128(self):
    data = self._advance_and_read_block(16)
    return self.factory.new_id(data)

  @composite_constructor(
    shared.Codec.ARRAY_N_TAG,
    shared.Codec.ARRAY_0_TAG,
    shared.Codec.ARRAY_1_TAG,
    shared.Codec.ARRAY_2_TAG,
    shared.Codec.ARRAY_3_TAG)
  def _new_array(self):
    return self.factory.new_array()

  @composite_reader(shared.Codec.ARRAY_N_TAG)
  def _array_n(self, array):
    self._advance()
    length = self._read_unsigned_int()
    for i in range(0, length):
      array.append(self._decode())

  @composite_reader(shared.Codec.ARRAY_0_TAG)
  def _array_0(self, array):
    self._advance()

  @composite_reader(shared.Codec.ARRAY_1_TAG)
  def _array_1(self, array):
    self._advance()
    array.append(self._decode())

  @composite_reader(shared.Codec.ARRAY_2_TAG)
  def _array_2(self, array):
    self._advance()
    array.append(self._decode())
    array.append(self._decode())

  @composite_reader(shared.Codec.ARRAY_3_TAG)
  def _array_3(self, array):
    self._advance()
    array.append(self._decode())
    array.append(self._decode())
    array.append(self._decode())

  @composite_constructor(
    shared.Codec.MAP_N_TAG,
    shared.Codec.MAP_0_TAG,
    shared.Codec.MAP_1_TAG,
    shared.Codec.MAP_2_TAG,
    shared.Codec.MAP_3_TAG)
  def _new_map(self):
    return self.factory.new_map()

  @composite_reader(shared.Codec.MAP_N_TAG)
  def _map_n(self, map):
    self._advance()
    length = self._read_unsigned_int()
    for i in range(0, length):
      key = self._decode()
      value = self._decode()
      map[key] = value

  @composite_reader(shared.Codec.MAP_0_TAG)
  def _map_0(self, map):
    self._advance()

  @composite_reader(shared.Codec.MAP_1_TAG)
  def _map_1(self, map):
    self._advance()
    key = self._decode()
    value = self._decode()
    map[key] = value

  @composite_reader(shared.Codec.MAP_2_TAG)
  def _map_2(self, map):
    self._advance()
    key = self._decode()
    value = self._decode()
    map[key] = value
    key = self._decode()
    value = self._decode()
    map[key] = value

  @composite_reader(shared.Codec.MAP_3_TAG)
  def _map_3(self, map):
    self._advance()
    key = self._decode()
    value = self._decode()
    map[key] = value
    key = self._decode()
    value = self._decode()
    map[key] = value
    key = self._decode()
    value = self._decode()
    map[key] = value

  @atomic_reader(shared.Codec.BLOB_N_TAG)
  def _blob_n(self):
    self._advance()
    length = self._read_unsigned_int()
    return self._read_block(length)

  @atomic_reader(shared.Codec.DEFAULT_STRING_0_TAG)
  def _default_string_0(self):
    self._advance()
    return ""

  def _read_fixed_string(self, length):
    bytes = self._advance_and_read_block(length)
    return bytes.encode(self.default_string_encoding)

  @atomic_reader(shared.Codec.DEFAULT_STRING_1_TAG)
  def _default_string_1(self):
    return self._read_fixed_string(1)

  @atomic_reader(shared.Codec.DEFAULT_STRING_2_TAG)
  def _default_string_2(self):
    return self._read_fixed_string(2)

  @atomic_reader(shared.Codec.DEFAULT_STRING_3_TAG)
  def _default_string_3(self):
    return self._read_fixed_string(3)

  @atomic_reader(shared.Codec.DEFAULT_STRING_4_TAG)
  def _default_string_4(self):
    return self._read_fixed_string(4)

  @atomic_reader(shared.Codec.DEFAULT_STRING_5_TAG)
  def _default_string_5(self):
    return self._read_fixed_string(5)

  @atomic_reader(shared.Codec.DEFAULT_STRING_6_TAG)
  def _default_string_6(self):
    return self._read_fixed_string(6)

  @atomic_reader(shared.Codec.DEFAULT_STRING_7_TAG)
  def _default_string_7(self):
    return self._read_fixed_string(7)

  @atomic_reader(shared.Codec.DEFAULT_STRING_N_TAG)
  def _default_string_n(self):
    self._advance()
    length = self._read_unsigned_int()
    return self._read_block(length).decode(self.default_string_encoding)

  @atomic_reader(shared.Codec.ADD_REF_TAG)
  def _add_ref(self):
    self._advance()
    constructor = _COMPOSITE_CONSTRUCTORS[self.current]
    value = constructor(self)
    self.refs.append(value)
    _COMPOSITE_READERS[self.current](self, value)
    return value

  @atomic_reader(shared.Codec.GET_REF_TAG)
  def _get_ref(self):
    self._advance()
    offset = self._read_unsigned_int()
    index = len(self.refs) - offset - 1
    return self.refs[index]

  def _read_unsigned_int(self):
    value = self.current & 0x7F
    offset = 7
    while self.current >= 0x80:
      self._advance()
      payload = (self.current & 0x7F) + 1
      value += (payload << offset)
      offset += 7
    self._advance()
    return value


def decode(input, factory=None):
  """Decode the given input as plankton data."""
  if isinstance(input, bytearray):
    input = io.BytesIO(input)
  return Decoder(input, factory).read()
