"""
Utilities for working with binary plankton: parsing binary input and encoding
data in the binary format.
"""


import math
import struct
import uuid

from plankton.codec import _types


__all__ = ["BinaryDecoder", "BinaryEncoder"]


# Value building instructions

INT_0_TAG = 0x00
INT_1_TAG = 0x01
INT_2_TAG = 0x02
INT_3_TAG = 0x03
INT_4_TAG = 0x04
INT_5_TAG = 0x05
INT_P_TAG = 0x08
INT_M_TAG = 0x09
INT_M1_TAG = 0x0f
INT_M2_TAG = 0x0e
INT_M3_TAG = 0x0d

SINGLETON_NULL_TAG = 0x10
SINGLETON_TRUE_TAG = 0x11
SINGLETON_FALSE_TAG = 0x12

ID_16_TAG = 0x14
ID_32_TAG = 0x15
ID_64_TAG = 0x16
ID_128_TAG = 0x17

FLOAT_32_TAG = 0x1a
FLOAT_64_TAG = 0x1b

ARRAY_0_TAG = 0x20
ARRAY_1_TAG = 0x21
ARRAY_2_TAG = 0x22
ARRAY_3_TAG = 0x23
ARRAY_N_TAG = 0x28

MAP_0_TAG = 0x30
MAP_1_TAG = 0x31
MAP_2_TAG = 0x32
MAP_3_TAG = 0x33
MAP_N_TAG = 0x38

BLOB_N_TAG = 0x48

DEFAULT_STRING_0_TAG = 0x50
DEFAULT_STRING_1_TAG = 0x51
DEFAULT_STRING_2_TAG = 0x52
DEFAULT_STRING_3_TAG = 0x53
DEFAULT_STRING_4_TAG = 0x54
DEFAULT_STRING_5_TAG = 0x55
DEFAULT_STRING_6_TAG = 0x56
DEFAULT_STRING_7_TAG = 0x57
DEFAULT_STRING_N_TAG = 0x58

SEED_0_TAG = 0x60
SEED_1_TAG = 0x61
SEED_2_TAG = 0x62
SEED_3_TAG = 0x63
SEED_N_TAG = 0x68

STRUCT_LINEAR_0_TAG = 0x80
STRUCT_LINEAR_1_TAG = 0x81
STRUCT_LINEAR_2_TAG = 0x82
STRUCT_LINEAR_3_TAG = 0x83
STRUCT_LINEAR_4_TAG = 0x84
STRUCT_LINEAR_5_TAG = 0x85
STRUCT_LINEAR_6_TAG = 0x86
STRUCT_LINEAR_7_TAG = 0x87
STRUCT_N_TAG = 0x88

ADD_REF_TAG = 0xa0
GET_REF_TAG = 0xa1


# Stream instructions

STREAM_OP = 0xf0

DEFINE_SCHEMA = 0xc0


# Secure (sometimes quote-unquote) hash instructions. There's no particular
# order to these, intentionally, for any new one pull a random number not
# already used.

SHA_224 = 0x1d

_HASH_NAMES = [
  (SHA_224, "sha224"),
]

_INSTRUCTION_DECODERS = [None] * 256


def decoder(instr):
  """Marks a reader for a type that can't be referenced."""
  def register_decoder(method):
    _INSTRUCTION_DECODERS[instr] = method
    return method
  return register_decoder


class BinaryDecoder(object):

  def __init__(self, input, visitor):
    self._input = input
    self._visitor = visitor
    self._current = None
    self._has_more = True
    self._next_ref_offset = 0
    self._advance()

  def _advance(self):
    s = self._input.read(1)
    if s:
      self._current = ord(s)
    else:
      self._current = None
      self._has_more = False

  def _advance_and_read_block(self, count):
    result = self._input.read(count)
    self._advance()
    return result

  def _read_block(self, count):
    result = bytearray(count)
    if count == 0:
      return result
    else:
      result[0] = self._current
      if count > 1:
        result[1:] = self._input.read(count-1)
    self._advance()
    return result

  def _read_unsigned_int(self):
    value = self._current & 0x7F
    offset = 7
    while self._current >= 0x80:
      self._advance()
      payload = (self._current & 0x7F) + 1
      value += (payload << offset)
      offset += 7
    self._advance()
    return value

  def decode_next(self, ref_key):
    assert self._has_more
    decoder = _INSTRUCTION_DECODERS[self._current]
    if decoder:
      return decoder(self, ref_key)
    else:
      return self._visitor.on_invalid_instruction(self._current)

  @decoder(INT_P_TAG)
  def _int_p(self, ref_key):
    self._advance()
    return self._visitor.on_int(self._read_unsigned_int())

  @decoder(INT_M1_TAG)
  def _int_m1(self, ref_key):
    self._advance()
    return self._visitor.on_int(-1)

  @decoder(INT_M2_TAG)
  def _int_m2(self, ref_key):
    self._advance()
    return self._visitor.on_int(-2)

  @decoder(INT_M3_TAG)
  def _int_m3(self, ref_key):
    self._advance()
    return self._visitor.on_int(-3)

  @decoder(INT_0_TAG)
  def _int_0(self, ref_key):
    self._advance()
    return self._visitor.on_int(0)

  @decoder(INT_1_TAG)
  def _int_1(self, ref_key):
    self._advance()
    return self._visitor.on_int(1)

  @decoder(INT_2_TAG)
  def _int_2(self, ref_key):
    self._advance()
    return self._visitor.on_int(2)

  @decoder(INT_3_TAG)
  def _int_3(self, ref_key):
    self._advance()
    return self._visitor.on_int(3)

  @decoder(INT_4_TAG)
  def _int_4(self, ref_key):
    self._advance()
    return self._visitor.on_int(4)

  @decoder(INT_5_TAG)
  def _int_5(self, ref_key):
    self._advance()
    return self._visitor.on_int(5)

  @decoder(INT_M_TAG)
  def _int_m(self, ref_key):
    self._advance()
    return self._visitor.on_int(-(self._read_unsigned_int() + 1))

  @decoder(SINGLETON_NULL_TAG)
  def _singleton_null(self, ref_key):
    self._advance()
    return self._visitor.on_singleton(None)

  @decoder(SINGLETON_TRUE_TAG)
  def _singleton_true(self, ref_key):
    self._advance()
    return self._visitor.on_singleton(True)

  @decoder(SINGLETON_FALSE_TAG)
  def _singleton_null(self, ref_key):
    self._advance()
    return self._visitor.on_singleton(False)

  @decoder(ID_16_TAG)
  def _id_16(self, ref_key):
    data = self._advance_and_read_block(2)
    return self._visitor.on_id(b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0" + data)

  @decoder(ID_32_TAG)
  def _id_32(self, ref_key):
    data = self._advance_and_read_block(4)
    return self._visitor.on_id(b"\0\0\0\0\0\0\0\0\0\0\0\0" + data)

  @decoder(ID_64_TAG)
  def _id_64(self, ref_key):
    data = self._advance_and_read_block(8)
    return self._visitor.on_id(b"\0\0\0\0\0\0\0\0" + data)

  @decoder(ID_128_TAG)
  def _id_128(self, ref_key):
    data = self._advance_and_read_block(16)
    return self._visitor.on_id(data)

  @decoder(FLOAT_32_TAG)
  def _float_32(self, ref_key):
    data = self._advance_and_read_block(4)
    (value,) = struct.unpack("<f", data)
    return self._visitor.on_float(value)

  @decoder(FLOAT_64_TAG)
  def _float_64(self, ref_key):
    data = self._advance_and_read_block(8)
    (value,) = struct.unpack("<d", data)
    return self._visitor.on_float(value)

  @decoder(ARRAY_0_TAG)
  def _array_0(self, ref_key):
    self._advance()
    return self._visitor.on_begin_array(0, ref_key)

  @decoder(ARRAY_1_TAG)
  def _array_1(self, ref_key):
    self._advance()
    return self._visitor.on_begin_array(1, ref_key)

  @decoder(ARRAY_2_TAG)
  def _array_2(self, ref_key):
    self._advance()
    return self._visitor.on_begin_array(2, ref_key)

  @decoder(ARRAY_3_TAG)
  def _array_3(self, ref_key):
    self._advance()
    return self._visitor.on_begin_array(3, ref_key)

  @decoder(ARRAY_N_TAG)
  def _array_n(self, ref_key):
    self._advance()
    length = self._read_unsigned_int()
    return self._visitor.on_begin_array(length, ref_key)

  @decoder(MAP_0_TAG)
  def _map_0(self, ref_key):
    self._advance()
    return self._visitor.on_begin_map(0, ref_key)

  @decoder(MAP_1_TAG)
  def _map_1(self, ref_key):
    self._advance()
    return self._visitor.on_begin_map(1, ref_key)

  @decoder(MAP_2_TAG)
  def _map_2(self, ref_key):
    self._advance()
    return self._visitor.on_begin_map(2, ref_key)

  @decoder(MAP_3_TAG)
  def _map_3(self, ref_key):
    self._advance()
    return self._visitor.on_begin_map(3, ref_key)

  @decoder(MAP_N_TAG)
  def _map_n(self, ref_key):
    self._advance()
    length = self._read_unsigned_int()
    return self._visitor.on_begin_map(length, ref_key)

  @decoder(BLOB_N_TAG)
  def _blob_n(self, ref_key):
    self._advance()
    length = self._read_unsigned_int()
    data = self._read_block(length)
    return self._visitor.on_blob(data)

  @decoder(DEFAULT_STRING_0_TAG)
  def _default_string_0(self, ref_key):
    self._advance()
    return self._visitor.on_string(b"", None)

  @decoder(DEFAULT_STRING_1_TAG)
  def _default_string_1(self, ref_key):
    bytes = self._advance_and_read_block(1)
    return self._visitor.on_string(bytes, None)

  @decoder(DEFAULT_STRING_2_TAG)
  def _default_string_2(self, ref_key):
    bytes = self._advance_and_read_block(2)
    return self._visitor.on_string(bytes, None)

  @decoder(DEFAULT_STRING_3_TAG)
  def _default_string_3(self, ref_key):
    bytes = self._advance_and_read_block(3)
    return self._visitor.on_string(bytes, None)

  @decoder(DEFAULT_STRING_4_TAG)
  def _default_string_4(self, ref_key):
    bytes = self._advance_and_read_block(4)
    return self._visitor.on_string(bytes, None)

  @decoder(DEFAULT_STRING_5_TAG)
  def _default_string_5(self, ref_key):
    bytes = self._advance_and_read_block(5)
    return self._visitor.on_string(bytes, None)

  @decoder(DEFAULT_STRING_6_TAG)
  def _default_string_6(self, ref_key):
    bytes = self._advance_and_read_block(6)
    return self._visitor.on_string(bytes, None)

  @decoder(DEFAULT_STRING_7_TAG)
  def _default_string_7(self, ref_key):
    bytes = self._advance_and_read_block(7)
    return self._visitor.on_string(bytes, None)

  @decoder(DEFAULT_STRING_N_TAG)
  def _default_string_n(self, ref_key):
    self._advance()
    length = self._read_unsigned_int()
    bytes = self._read_block(length)
    return self._visitor.on_string(bytes, None)

  @decoder(SEED_0_TAG)
  def _seed_0(self, ref_key):
    self._advance()
    return self._visitor.on_begin_seed(0, ref_key)

  @decoder(SEED_1_TAG)
  def _seed_1(self, ref_key):
    self._advance()
    return self._visitor.on_begin_seed(1, ref_key)

  @decoder(SEED_2_TAG)
  def _seed_2(self, ref_key):
    self._advance()
    return self._visitor.on_begin_seed(2, ref_key)

  @decoder(SEED_3_TAG)
  def _seed_3(self, ref_key):
    self._advance()
    return self._visitor.on_begin_seed(3, ref_key)

  @decoder(SEED_N_TAG)
  def _seed_n(self, ref_key):
    self._advance()
    length = self._read_unsigned_int()
    return self._visitor.on_begin_seed(length, ref_key)

  @decoder(STRUCT_LINEAR_0_TAG)
  def _struct_linear_0(self, ref_key):
    self._advance()
    return self._visitor.on_begin_struct([], ref_key)

  @decoder(STRUCT_LINEAR_1_TAG)
  def _struct_linear_1(self, ref_key):
    self._advance()
    return self._visitor.on_begin_struct([0], ref_key)

  @decoder(STRUCT_LINEAR_2_TAG)
  def _struct_linear_2(self, ref_key):
    self._advance()
    return self._visitor.on_begin_struct([0, 1], ref_key)

  @decoder(STRUCT_LINEAR_3_TAG)
  def _struct_linear_3(self, ref_key):
    self._advance()
    return self._visitor.on_begin_struct([0, 1, 2], ref_key)

  @decoder(STRUCT_LINEAR_4_TAG)
  def _struct_linear_4(self, ref_key):
    self._advance()
    return self._visitor.on_begin_struct([0, 1, 2, 3], ref_key)

  @decoder(STRUCT_LINEAR_5_TAG)
  def _struct_linear_5(self, ref_key):
    self._advance()
    return self._visitor.on_begin_struct([0, 1, 2, 3, 4], ref_key)

  @decoder(STRUCT_LINEAR_6_TAG)
  def _struct_linear_6(self, ref_key):
    self._advance()
    return self._visitor.on_begin_struct([0, 1, 2, 3, 4, 5], ref_key)

  @decoder(STRUCT_LINEAR_7_TAG)
  def _struct_linear_7(self, ref_key):
    self._advance()
    return self._visitor.on_begin_struct([0, 1, 2, 3, 4, 5, 6], ref_key)

  @decoder(STRUCT_N_TAG)
  def _struct_n(self, ref_key):
    self._advance()
    length = self._read_unsigned_int()
    tags = self._read_struct_tags(length)
    return self._visitor.on_begin_struct(tags, ref_key)

  @decoder(ADD_REF_TAG)
  def _add_ref(self, ref_key):
    self._advance()
    offset = self._next_ref_offset
    self._next_ref_offset += 1
    return self.decode_next(ref_key=offset)

  @decoder(GET_REF_TAG)
  def _get_ref(self, ref_key):
    self._advance()
    offset = self._read_unsigned_int()
    return self._visitor.on_get_ref(self._next_ref_offset - offset - 1)

  def _decode_nibbles(self, nibbles):
    value = nibbles[nibble_offset] & 0x7
    value_offset = 3
    while nibbles[nibble_offset] >= 0x8:
      nibble_offset += 1
      payload = (nibbles[nibble_offset] & 0x7) + 1
      value += (payload << value_offset)
      value_offset += 3
    nibble_offset += 1
    yield value

  class NibbleReader(object):

    def __init__(self, decoder):
      self._decoder = decoder
      self._current = None
      self._next = None

    def next_unsigned_int(self):
      self._advance()
      value = self._current & 0x7
      value_offset = 3
      while self._current >= 0x8:
        self._advance()
        payload = (self._current & 0x7) + 1
        value += (payload << value_offset)
        value_offset += 3
      return value

    def _advance(self):
      if self._next is None:
        self._current = (self._decoder._current >> 4) & 0xF
        self._next = self._decoder._current & 0xF
        self._decoder._advance()
      else:
        self._current = self._next
        self._next = None

  def _read_struct_tags(self, length):
    reader = self.NibbleReader(self)
    result = []
    last_value = None
    repeat_next_time = False
    while len(result) < length:
      current_delta = reader.next_unsigned_int()
      if last_value is None:
        last_value = current_delta
        result.append(last_value)
      elif repeat_next_time:
        result += [last_value] * current_delta
        repeat_next_time = False
      elif current_delta == 0:
        repeat_next_time = True
      else:
        result.append(last_value + current_delta)
        last_value += current_delta
        repeat_next_time = False
    return result


class BinaryEncoder(_types.Visitor):
  """
  An abstract value encoder. Most of the work of encoding takes place here, then
  the subclasses tweak various semantics.
  """

  def __init__(self, out):
    self._out = out
    self._default_string_encoding = "utf-8"
    self._ref_count = 0

  def on_invalid_instruction(self, code):
    raise Exception("Invalid instruction 0x{:x}".format(code))

  def on_int(self, value):
    if value == 0:
      self._write_tag(INT_0_TAG)
    elif value == 1:
      self._write_tag(INT_1_TAG)
    elif value == 2:
      self._write_tag(INT_2_TAG)
    elif value == 3:
      self._write_tag(INT_3_TAG)
    elif value == 4:
      self._write_tag(INT_4_TAG)
    elif value == 5:
      self._write_tag(INT_5_TAG)
    elif value == -1:
      self._write_tag(INT_M1_TAG)
    elif value == -2:
      self._write_tag(INT_M2_TAG)
    elif value == -3:
      self._write_tag(INT_M3_TAG)
    elif value < 0:
      self._write_tag(INT_M_TAG)
      self._write_unsigned_int(-(value + 1))
    else:
      self._write_tag(INT_P_TAG)
      self._write_unsigned_int(value)

  def on_float(self, value):
    bytes = self._encode_float(value)
    if len(bytes) == 4:
      self._write_tag(FLOAT_32_TAG)
    else:
      assert len(bytes) == 8
      self._write_tag(FLOAT_64_TAG)
    self._write_bytes(bytes)

  def on_singleton(self, value):
    if value is None:
      self._write_tag(SINGLETON_NULL_TAG)
    elif value is True:
      self._write_tag(SINGLETON_TRUE_TAG)
    else:
      assert value is False
      self._write_tag(SINGLETON_FALSE_TAG)

  def on_string(self, bytes, encoding):
    if len(bytes) == 0:
      self._write_tag(DEFAULT_STRING_0_TAG)
    elif len(bytes) == 1:
      self._write_tag(DEFAULT_STRING_1_TAG)
    elif len(bytes) == 2:
      self._write_tag(DEFAULT_STRING_2_TAG)
    elif len(bytes) == 3:
      self._write_tag(DEFAULT_STRING_3_TAG)
    elif len(bytes) == 4:
      self._write_tag(DEFAULT_STRING_4_TAG)
    elif len(bytes) == 5:
      self._write_tag(DEFAULT_STRING_5_TAG)
    elif len(bytes) == 6:
      self._write_tag(DEFAULT_STRING_6_TAG)
    elif len(bytes) == 7:
      self._write_tag(DEFAULT_STRING_7_TAG)
    else:
      self._write_tag(DEFAULT_STRING_N_TAG)
      self._write_unsigned_int(len(bytes))
    self._write_bytes(bytes)

  def on_begin_array(self, length, ref_key):
    self._maybe_add_ref(ref_key)
    if length == 0:
      self._write_tag(ARRAY_0_TAG)
    elif length == 1:
      self._write_tag(ARRAY_1_TAG)
    elif length == 2:
      self._write_tag(ARRAY_2_TAG)
    elif length == 3:
      self._write_tag(ARRAY_3_TAG)
    else:
      self._write_tag(ARRAY_N_TAG)
      self._write_unsigned_int(length)

  def on_begin_map(self, length, ref_key):
    self._maybe_add_ref(ref_key)
    if length == 0:
      self._write_tag(MAP_0_TAG)
    elif length == 1:
      self._write_tag(MAP_1_TAG)
    elif length == 2:
      self._write_tag(MAP_2_TAG)
    elif length == 3:
      self._write_tag(MAP_3_TAG)
    else:
      self._write_tag(MAP_N_TAG)
      self._write_unsigned_int(length)

  def on_id(self, bytes):
    ivalue = uuid.UUID(bytes=bytes).int
    if ivalue >= 2**64:
      self._write_tag(ID_128_TAG)
      self._write_bytes(bytes)
    elif ivalue >= 2**32:
      self._write_tag(ID_64_TAG)
      self._write_bytes(bytes[8:16])
    elif ivalue >= 2**16:
      self._write_tag(ID_32_TAG)
      self._write_bytes(bytes[12:16])
    else:
      assert ivalue < 2**16
      self._write_tag(ID_16_TAG)
      self._write_bytes(bytes[14:16])

  def on_blob(self, value):
    self._write_tag(BLOB_N_TAG)
    self._write_unsigned_int(len(value))
    self._write_bytes(value)

  def on_begin_seed(self, length, ref_key):
    self._maybe_add_ref(ref_key)
    if length == 0:
      self._write_tag(SEED_0_TAG)
    elif length == 1:
      self._write_tag(SEED_1_TAG)
    elif length == 2:
      self._write_tag(SEED_2_TAG)
    elif length == 3:
      self._write_tag(SEED_3_TAG)
    else:
      self._write_tag(SEED_N_TAG)
      self._write_unsigned_int(length)

  def on_begin_struct(self, tags, ref_key):
    self._maybe_add_ref(ref_key)
    if tags == []:
      self._write_tag(STRUCT_LINEAR_0_TAG)
    elif tags == [0]:
      self._write_tag(STRUCT_LINEAR_1_TAG)
    elif tags == [0, 1]:
      self._write_tag(STRUCT_LINEAR_2_TAG)
    elif tags == [0, 1, 2]:
      self._write_tag(STRUCT_LINEAR_3_TAG)
    elif tags == [0, 1, 2, 3]:
      self._write_tag(STRUCT_LINEAR_4_TAG)
    elif tags == [0, 1, 2, 3, 4]:
      self._write_tag(STRUCT_LINEAR_5_TAG)
    elif tags == [0, 1, 2, 3, 4, 5]:
      self._write_tag(STRUCT_LINEAR_6_TAG)
    elif tags == [0, 1, 2, 3, 4, 5, 6]:
      self._write_tag(STRUCT_LINEAR_7_TAG)
    else:
      self._write_tag(STRUCT_N_TAG)
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

  def on_get_ref(self, key):
    self._write_tag(GET_REF_TAG)
    self._write_unsigned_int(self._ref_count - key - 1)

  def on_define_schema(self, id):
    self._write_tag(STREAM_OP)
    self._write_tag(DEFINE_SCHEMA)
    self._write_byte(id.type.code())
    bytes = id.sum
    assert (len(bytes) % 4) == 0
    self._write_unsigned_int(len(bytes) // 4)
    self._write_bytes(bytes)

  def on_begin_schema(self, length):
    self._write_unsigned_int(length)

  def _maybe_add_ref(self, ref_key):
    if not ref_key is None:
      self._write_tag(ADD_REF_TAG)
      self._ref_count += 1

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

  @staticmethod
  def _encode_float(value):
    if value == 0.0 or math.isinf(value) or math.isnan(value):
      return struct.pack("<f", value)
    double_packed = struct.pack('<d', value)
    # Unpack the double and inspect the components.
    (double_raw,) = struct.unpack('Q', double_packed)
    significand = double_raw & 0xFFFFFFFFFFFFF
    biased_exponent = (double_raw & 0x7FF0000000000000) >> 52
    exponent = biased_exponent - 1023
    if (-126 <= exponent) and (exponent <= 127) and ((significand & 0x000001FFFFFFF) == 0):
      # If the exponent and significand fit within what a single precision float
      # provides we encode it as that.
      return struct.pack("<f", value)
    else:
      # Otherwise we need the full double precision representation.
      return double_packed
