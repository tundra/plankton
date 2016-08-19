import collections
import io
import uuid
import itertools

from plankton.codec import shared


__all__ = ["decode", "DefaultDataFactory", "Decoder"]


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

  def new_seed(self):
    return shared.Seed(None, collections.OrderedDict())

  def new_struct(self):
    return shared.Struct([])


class Decoder(shared.Codec):

  def __init__(self, input, factory=None, default_string_encoding="utf-8"):
    self._decoder = InstructionStreamDecoder(input)
    self._factory = factory or DefaultDataFactory()
    self._default_string_encoding = default_string_encoding
    self._refs = []
    # The stack of values we've seen so far but haven't packed into a composite
    # value of some sort.
    self._value_stack = []
    # A stack of info about how to pack values into composites when we've
    # collected enough values.
    self._pending_ends = []
    # The last value we've seen but only if a ref can be created to it, if the
    # last value was atomic this will be None. Note that since None itself is
    # atomic there is no ambiguity between "holding no reffable value" and
    # "holding a reffable value that happens to be None" because None is never
    # reffable.
    self._last_reffable = None
    # The final result of parsing. It's totally valid for this to be None since
    # that's a valid parsing result.
    self._result = None

  def read(self):
    # Schedule an end that doesn't do anything but that ensures that we don't
    # have to explicitly check for the bottom of the pending ends.
    self._schedule_end(2, 1, None, None)
    # Schedule an end that stores the result in the _result field.
    self._schedule_end(1, self._store_result, None, None)

    # Keep running as long as the store-result end is still on the pending end
    # stack.
    while len(self._pending_ends) > 1:
      self._decoder.decode_next(self)

    # Check that the value and pending end stacks look like we expect at this
    # point.
    assert [None] == self._value_stack
    assert len(self._pending_ends) == 1

    return self._result

  def _store_result(self, total_count, open_result, values, data):
    [self._result] = values
    self._push(None)

  def on_invalid_instruction(self, code):
    raise Exception("Invalid instruction 0x{:x}".format(code))

  def on_int(self, value):
    self._last_reffable = None
    self._push(value)

  def on_singleton(self, value):
    self._last_reffable = None
    self._push(value)

  def on_id(self, data):
    self._last_reffable = None
    self._push(self._factory.new_id(data))

  def on_begin_array(self, length):
    array = self._last_reffable = self._factory.new_array()
    if length == 0:
      self._push(array)
    else:
      self._schedule_end(length, self._end_array, array, None)

  def _end_array(self, total_length, array, values, unused):
    array[:] = values
    self._push(array)

  def on_begin_map(self, length):
    map = self._last_reffable = self._factory.new_map()
    if length == 0:
      self._push(map)
    else:
      self._schedule_end(2 * length, self._end_map, map, None)

  def _end_map(self, total_length, map, values, unused):
    for i in range(0, total_length, 2):
      map[values[i]] = values[i+1]
    self._push(map)

  def on_blob(self, data):
    self._last_reffable = None
    self._push(data)

  def on_string(self, data, encoding):
    if not encoding:
      encoding = self._default_string_encoding
    self._push(data.decode(encoding))

  def on_begin_seed(self, field_count):
    seed = self._last_reffable = self._factory.new_seed()
    self._schedule_end(1, self._end_seed_header, seed, field_count)

  def _end_seed_header(self, one, seed, header_values, field_count):
    [header] = header_values
    seed.header = header
    if field_count == 0:
      self._push(seed)
    else:
      self._schedule_end(2 * field_count, self._end_seed, seed, None)

  def _end_seed(self, total_count, seed, field_values, unused):
    for i in range(0, total_count, 2):
      seed.fields[field_values[i]] = field_values[i+1]
    self._push(seed)

  def on_begin_struct(self, tags):
    struct = self._last_reffable = self._factory.new_struct()
    tag_count = len(tags)
    if tag_count == 0:
      self._push(struct)
    else:
      self._schedule_end(tag_count, self._end_struct, struct, tags)

  def _end_struct(self, total_length, struct, values, tags):
    for i in range(0, len(tags)):
      struct.fields.append((tags[i], values[i]))
    self._push(struct)

  def on_add_ref(self):
    assert not self._last_reffable is None
    self._refs.append(self._last_reffable)
    # Once you've added one ref you can't add any more so clear last_reffable.
    self._last_reffable = None

  def on_get_ref(self, offset):
    self._last_reffable = None
    index = len(self._refs) - offset - 1
    self._push(self._refs[index])

  def _push(self, value):
    """
    Push a value on top of the value stack and execute any pending ends that
    are now ready to be executed.
    """
    self._value_stack.append(value)
    self._pending_ends[-1][1] += 1
    if self._pending_ends[-1][0] == self._pending_ends[-1][1]:
      [total_count, added_count, callback, open_result, data] = self._pending_ends.pop()
      values = self._value_stack[-total_count:]
      del self._value_stack[-total_count:]
      # The callback may or may not push a value which will cause this to be
      # called again and then we'll deal with any pending ends further down
      # the pending end stack.
      callback(total_count, open_result, values, data)

  def _schedule_end(self, total_count, callback, open_result, data):
    """
    Schedule the given callback to be called after total_count values have
    become available. The count has to be > 0 because that simplifies things
    and also, if the number of remaining values is 0 the caller can just
    create the result immediately.
    """
    assert total_count > 0
    assert callback
    self._pending_ends.append([total_count, 0, callback, open_result, data])


_INSTRUCTION_DECODERS = [None] * 256


def decoder(instr):
  """Marks a reader for a type that can't be referenced."""
  def register_decoder(method):
    _INSTRUCTION_DECODERS[instr] = method
    return method
  return register_decoder


class InstructionStreamDecoder(object):

  def __init__(self, input):
    self.input = input
    self.current = None
    self.has_more = True
    self._advance()

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

  def decode_next(self, callback):
    assert self.has_more
    decoder = _INSTRUCTION_DECODERS[self.current]
    if decoder:
      return decoder(self, callback)
    else:
      return callback.on_invalid_instruction(self.current)

  @decoder(shared.Codec.INT_P_TAG)
  def _int_p(self, callback):
    self._advance()
    return callback.on_int(self._read_unsigned_int())

  @decoder(shared.Codec.INT_M1_TAG)
  def _int_m1(self, callback):
    self._advance()
    return callback.on_int(-1)

  @decoder(shared.Codec.INT_0_TAG)
  def _int_0(self, callback):
    self._advance()
    return callback.on_int(0)

  @decoder(shared.Codec.INT_1_TAG)
  def _int_1(self, callback):
    self._advance()
    return callback.on_int(1)

  @decoder(shared.Codec.INT_2_TAG)
  def _int_2(self, callback):
    self._advance()
    return callback.on_int(2)

  @decoder(shared.Codec.INT_M_TAG)
  def _int_m(self, callback):
    self._advance()
    return callback.on_int(-(self._read_unsigned_int() + 1))

  @decoder(shared.Codec.SINGLETON_NULL_TAG)
  def _singleton_null(self, callback):
    self._advance()
    return callback.on_singleton(None)

  @decoder(shared.Codec.SINGLETON_TRUE_TAG)
  def _singleton_true(self, callback):
    self._advance()
    return callback.on_singleton(True)

  @decoder(shared.Codec.SINGLETON_FALSE_TAG)
  def _singleton_null(self, callback):
    self._advance()
    return callback.on_singleton(False)

  @decoder(shared.Codec.ID_16_TAG)
  def _id_16(self, callback):
    data = self._advance_and_read_block(2)
    return callback.on_id(b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0" + data)

  @decoder(shared.Codec.ID_32_TAG)
  def _id_32(self, callback):
    data = self._advance_and_read_block(4)
    return callback.on_id(b"\0\0\0\0\0\0\0\0\0\0\0\0" + data)

  @decoder(shared.Codec.ID_64_TAG)
  def _id_64(self, callback):
    data = self._advance_and_read_block(8)
    return callback.on_id(b"\0\0\0\0\0\0\0\0" + data)

  @decoder(shared.Codec.ID_128_TAG)
  def _id_128(self, callback):
    data = self._advance_and_read_block(16)
    return callback.on_id(data)

  @decoder(shared.Codec.ARRAY_0_TAG)
  def _array_0(self, callback):
    self._advance()
    return callback.on_begin_array(0)

  @decoder(shared.Codec.ARRAY_1_TAG)
  def _array_1(self, callback):
    self._advance()
    return callback.on_begin_array(1)

  @decoder(shared.Codec.ARRAY_2_TAG)
  def _array_2(self, callback):
    self._advance()
    return callback.on_begin_array(2)

  @decoder(shared.Codec.ARRAY_3_TAG)
  def _array_3(self, callback):
    self._advance()
    return callback.on_begin_array(3)

  @decoder(shared.Codec.ARRAY_N_TAG)
  def _array_n(self, callback):
    self._advance()
    length = self._read_unsigned_int()
    return callback.on_begin_array(length)

  @decoder(shared.Codec.MAP_0_TAG)
  def _map_0(self, callback):
    self._advance()
    return callback.on_begin_map(0)

  @decoder(shared.Codec.MAP_1_TAG)
  def _map_1(self, callback):
    self._advance()
    return callback.on_begin_map(1)

  @decoder(shared.Codec.MAP_2_TAG)
  def _map_2(self, callback):
    self._advance()
    return callback.on_begin_map(2)

  @decoder(shared.Codec.MAP_3_TAG)
  def _map_3(self, callback):
    self._advance()
    return callback.on_begin_map(3)

  @decoder(shared.Codec.MAP_N_TAG)
  def _map_n(self, callback):
    self._advance()
    length = self._read_unsigned_int()
    return callback.on_begin_map(length)

  @decoder(shared.Codec.BLOB_N_TAG)
  def _blob_n(self, callback):
    self._advance()
    length = self._read_unsigned_int()
    data = self._read_block(length)
    return callback.on_blob(data)

  @decoder(shared.Codec.DEFAULT_STRING_0_TAG)
  def _default_string_0(self, callback):
    self._advance()
    return callback.on_string(b"", None)

  @decoder(shared.Codec.DEFAULT_STRING_1_TAG)
  def _default_string_1(self, callback):
    bytes = self._advance_and_read_block(1)
    return callback.on_string(bytes, None)

  @decoder(shared.Codec.DEFAULT_STRING_2_TAG)
  def _default_string_2(self, callback):
    bytes = self._advance_and_read_block(2)
    return callback.on_string(bytes, None)

  @decoder(shared.Codec.DEFAULT_STRING_3_TAG)
  def _default_string_3(self, callback):
    bytes = self._advance_and_read_block(3)
    return callback.on_string(bytes, None)

  @decoder(shared.Codec.DEFAULT_STRING_4_TAG)
  def _default_string_4(self, callback):
    bytes = self._advance_and_read_block(4)
    return callback.on_string(bytes, None)

  @decoder(shared.Codec.DEFAULT_STRING_5_TAG)
  def _default_string_5(self, callback):
    bytes = self._advance_and_read_block(5)
    return callback.on_string(bytes, None)

  @decoder(shared.Codec.DEFAULT_STRING_6_TAG)
  def _default_string_6(self, callback):
    bytes = self._advance_and_read_block(6)
    return callback.on_string(bytes, None)

  @decoder(shared.Codec.DEFAULT_STRING_7_TAG)
  def _default_string_7(self, callback):
    bytes = self._advance_and_read_block(7)
    return callback.on_string(bytes, None)

  @decoder(shared.Codec.DEFAULT_STRING_N_TAG)
  def _default_string_n(self, callback):
    self._advance()
    length = self._read_unsigned_int()
    bytes = self._read_block(length)
    return callback.on_string(bytes, None)

  @decoder(shared.Codec.SEED_0_TAG)
  def _seed_0(self, callback):
    self._advance()
    return callback.on_begin_seed(0)

  @decoder(shared.Codec.SEED_1_TAG)
  def _seed_1(self, callback):
    self._advance()
    return callback.on_begin_seed(1)

  @decoder(shared.Codec.SEED_2_TAG)
  def _seed_2(self, callback):
    self._advance()
    return callback.on_begin_seed(2)

  @decoder(shared.Codec.SEED_3_TAG)
  def _seed_3(self, callback):
    self._advance()
    return callback.on_begin_seed(3)

  @decoder(shared.Codec.SEED_N_TAG)
  def _seed_n(self, callback):
    self._advance()
    length = self._read_unsigned_int()
    return callback.on_begin_seed(length)

  @decoder(shared.Codec.STRUCT_LINEAR_0_TAG)
  def _struct_linear_0(self, callback):
    self._advance()
    return callback.on_begin_struct([])

  @decoder(shared.Codec.STRUCT_LINEAR_1_TAG)
  def _struct_linear_1(self, callback):
    self._advance()
    return callback.on_begin_struct([0])

  @decoder(shared.Codec.STRUCT_LINEAR_2_TAG)
  def _struct_linear_2(self, callback):
    self._advance()
    return callback.on_begin_struct([0, 1])

  @decoder(shared.Codec.STRUCT_LINEAR_3_TAG)
  def _struct_linear_3(self, callback):
    self._advance()
    return callback.on_begin_struct([0, 1, 2])

  @decoder(shared.Codec.STRUCT_LINEAR_4_TAG)
  def _struct_linear_4(self, callback):
    self._advance()
    return callback.on_begin_struct([0, 1, 2, 3])

  @decoder(shared.Codec.STRUCT_LINEAR_5_TAG)
  def _struct_linear_5(self, callback):
    self._advance()
    return callback.on_begin_struct([0, 1, 2, 3, 4])

  @decoder(shared.Codec.STRUCT_LINEAR_6_TAG)
  def _struct_linear_6(self, callback):
    self._advance()
    return callback.on_begin_struct([0, 1, 2, 3, 4, 5])

  @decoder(shared.Codec.STRUCT_LINEAR_7_TAG)
  def _struct_linear_7(self, callback):
    self._advance()
    return callback.on_begin_struct([0, 1, 2, 3, 4, 5, 6])

  @decoder(shared.Codec.STRUCT_N_TAG)
  def _struct_n(self, callback):
    self._advance()
    length = self._read_unsigned_int()
    tags = self._read_struct_tags(length)
    return callback.on_begin_struct(tags)

  @decoder(shared.Codec.ADD_REF_TAG)
  def _add_ref(self, callback):
    self._advance()
    return callback.on_add_ref()

  @decoder(shared.Codec.GET_REF_TAG)
  def _get_ref(self, callback):
    self._advance()
    offset = self._read_unsigned_int()
    return callback.on_get_ref(offset)

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
      self.decoder = decoder
      self.current = None
      self.next = None

    def next_unsigned_int(self):
      self._advance()
      value = self.current & 0x7
      value_offset = 3
      while self.current >= 0x8:
        self._advance()
        payload = (self.current & 0x7) + 1
        value += (payload << value_offset)
        value_offset += 3
      return value

    def _advance(self):
      if self.next is None:
        self.current = (self.decoder.current >> 4) & 0xF
        self.next = self.decoder.current & 0xF
        self.decoder._advance()
      else:
        self.current = self.next
        self.next = None

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


def decode(input, factory=None):
  """Decode the given input as plankton data."""
  if isinstance(input, bytearray):
    input = io.BytesIO(input)
  return Decoder(input, factory).read()
