import itertools

from plankton.codec import _types


__all__ = [
  "TextDecoder",
  "TextEncoder",
]


class SyntaxError(Exception):
  pass


class Token(object):

  def is_atomic(self):
    return False

  def is_int(self):
    return False

  def is_string(self):
    return False

  def is_singleton(self):
    return False

  def is_punctuation(self, type=None):
    return False

  def is_reference(self):
    return False


class Number(Token):

  def __init__(self, value):
    self._value = value

  def is_atomic(self):
    return True

  def is_int(self):
    return True


class String(Token):

  def __init__(self, value):
    self._value = value

  def is_atomic(self):
    return True

  def is_string(self):
    return True


class Singleton(Token):

  def __init__(self, value):
    self._value = value

  def is_atomic(self):
    return True

  def is_singleton(self):
    return True


class Punctuation(Token):

  def __init__(self, value):
    self._value = value

  def is_punctuation(self, type=None):
    return (type is None) or (type == self._value)


class Reference(Token):

  def __init__(self, name):
    self._name = name

  def is_reference(self):
    return True


class Tokenizer(object):

  def __init__(self, input):
    self._input = input
    self._cursor = 0
    self._current = " "
    self._skip_spaces()

  def _skip_spaces(self):
    while self._has_more() and self._current.isspace():
      self._advance()

  def _has_more(self):
    return not (self._current is None)

  def _advance(self):
    if self._cursor < len(self._input):
      self._current = self._input[self._cursor]
      self._cursor += 1
    elif not self._current is None:
      self._cursor += 1
      self._current = None

  def tokenize(self):
    while self._has_more():
      yield self._read_next()
      self._skip_spaces()

  def _read_next(self):
    if self._is_digit_start(self._current):
      return self._read_number()
    elif self._current == "%":
      return self._read_singleton()
    elif self._current == "$":
      return self._read_reference()
    elif self._current in "[],{}:":
      char = self._current
      self._advance()
      return Punctuation(char)
    elif self._current == '"':
      return self._read_string()
    else:
      raise SyntaxError(self._current)

  def _read_number(self):
    start = self._cursor
    while self._has_more() and self._is_digit_part(self._current):
      self._advance()
    result = self._input[start-1:self._cursor-1].replace("_", "")
    return Number(int(result))

  @staticmethod
  def _is_digit_start(chr):
    return chr.isdigit() or chr in "-"

  @classmethod
  def _is_digit_part(cls, chr):
    return cls._is_digit_start(chr) or chr == "_"

  def _read_singleton(self):
    assert self._current == "%"
    self._advance()
    if not self._has_more():
      raise SyntaxError()
    if self._current == "n":
      self._advance()
      return Singleton(None)
    elif self._current == "t":
      self._advance()
      return Singleton(True)
    elif self._current == "f":
      self._advance()
      return Singleton(False)

  def _read_string(self):
    assert self._current == '"'
    start = self._cursor
    self._advance()
    while self._has_more() and self._current != '"':
      self._advance()
    result = self._input[start:self._cursor-1]
    self._advance()
    return String(result)

  def _read_reference(self):
    assert self._current == "$"
    start = self._cursor
    self._advance()
    while self._has_more() and self._current.isalnum():
      self._advance()
    result = self._input[start:self._cursor-1]
    return Reference(result)


class TokenStream(object):

  def __init__(self, tokens):
    self._tokens = tokens
    self._offset = 0

  @property
  def current(self):
    return self._tokens[self._offset]

  def _advance(self):
    self._offset += 1


class TextDecoder(object):

  def __init__(self, visitor):
    self._visitor = visitor
    self._lengths = {}

  def decode(self, input):
    tokens = list(Tokenizer(input).tokenize())
    self._pre_parse(TokenStream(tokens))
    return self._parse(TokenStream(tokens))

  def _pre_parse(self, tokens):
    if tokens.current.is_atomic():
      tokens._advance()
    elif tokens.current.is_reference():
      return self._pre_parse_reference(tokens)
    elif tokens.current.is_punctuation("["):
      return self._pre_parse_array(tokens)
    elif tokens.current.is_punctuation("{"):
      return self._pre_parse_map(tokens)
    else:
      raise SyntaxError()

  def _parse(self, tokens, ref_name=None):
    if tokens.current.is_atomic():
      assert ref_name is None
      if tokens.current.is_int():
        self._visitor.on_int(tokens.current._value)
      elif tokens.current.is_singleton():
        self._visitor.on_singleton(tokens.current._value)
      elif tokens.current.is_string():
        self._visitor.on_string(tokens.current._value.encode("utf-8"), None)
      else:
        raise AssertionError()
      tokens._advance()
    elif tokens.current.is_reference():
      return self._parse_reference(tokens)
    elif tokens.current.is_punctuation("["):
      return self._parse_array(tokens, ref_name)
    elif tokens.current.is_punctuation("{"):
      return self._parse_map(tokens, ref_name)
    else:
      raise SyntaxError()

  def _pre_parse_array(self, tokens):
    assert tokens.current.is_punctuation("[")
    offset = tokens._offset
    tokens._advance()
    length = 0
    while not tokens.current.is_punctuation("]"):
      length += 1
      self._pre_parse(tokens)
      if tokens.current.is_punctuation(","):
        tokens._advance()
      else:
        break
    if not tokens.current.is_punctuation("]"):
      raise SyntaxError()
    self._lengths[offset] = length
    tokens._advance()

  def _parse_array(self, tokens, ref_key):
    assert tokens.current.is_punctuation("[")
    offset = tokens._offset
    tokens._advance()
    length = self._lengths[offset]
    self._visitor.on_begin_array(length, ref_key)
    while not tokens.current.is_punctuation("]"):
      self._parse(tokens)
      if tokens.current.is_punctuation(","):
        tokens._advance()
      else:
        break
    tokens._advance()

  def _pre_parse_map(self, tokens):
    assert tokens.current.is_punctuation("{")
    offset = tokens._offset
    tokens._advance()
    length = 0
    while not tokens.current.is_punctuation("}"):
      length += 1
      self._pre_parse(tokens)
      assert tokens.current.is_punctuation(":")
      tokens._advance()
      self._pre_parse(tokens)
      if tokens.current.is_punctuation(","):
        tokens._advance()
      else:
        break
    if not tokens.current.is_punctuation("}"):
      raise SyntaxError()
    self._lengths[offset] = length
    tokens._advance()

  def _parse_map(self, tokens, ref_key):
    assert tokens.current.is_punctuation("{")
    offset = tokens._offset
    tokens._advance()
    length = self._lengths[offset]
    self._visitor.on_begin_map(length, ref_key)
    while not tokens.current.is_punctuation("}"):
      self._parse(tokens)
      tokens._advance()
      self._parse(tokens)
      if tokens.current.is_punctuation(","):
        tokens._advance()
      else:
        break
    tokens._advance()

  def _pre_parse_reference(self, tokens):
    assert tokens.current.is_reference()
    tokens._advance()
    if tokens.current.is_punctuation(":"):
      tokens._advance()
      self._pre_parse(tokens)

  def _parse_reference(self, tokens):
    assert tokens.current.is_reference()
    name = tokens.current._name
    tokens._advance()
    if tokens.current.is_punctuation(":"):
      tokens._advance()
      self._parse(tokens, name)
    else:
      self._visitor.on_get_ref(name)


class TextEncoder(_types.StackingBuilder):

  def __init__(self, out):
    super(TextEncoder, self).__init__()
    self._out = out
    self._default_string_encoding = "utf-8"
    self._schedule_end(1, self._flush, None, None)

  def _flush(self, total_count, unused_1, values, unused_2):
    """Store the value currently on the stack in the result field."""
    [result] = values
    self._out.write(result.decode(self._default_string_encoding))

  def on_invalid_instruction(self, code):
    raise Exception("Invalid instruction 0x{:x}".format(code))

  def on_int(self, value):
    self._push(str(value))

  def on_singleton(self, value):
    if value is None:
      self._push("%n")
    elif value is True:
      self._push("%t")
    else:
      assert value is False
      self._push("%f")

  def on_string(self, bytes, encoding):
    self._push('"%s"' % bytes)

  def on_begin_array(self, length, ref_key):
    if length == 0:
      self._push(self._maybe_add_ref(ref_key, "[]"))
    else:
      self._schedule_end(length, self._end_array, ref_key, None)

  def _end_array(self, total_length, ref_key, values, unused):
    self._push(self._maybe_add_ref(ref_key, "[%s]" % (", ".join(values))))

  def on_begin_map(self, length, ref_key):
    if length == 0:
      self._push(self._maybe_add_ref(ref_key, "{}"))
    else:
      self._schedule_end(2 * length, self._end_map, ref_key, None)

  def _end_map(self, total_length, ref_key, values, unused):
    pairs = ["%s: %s" % (values[i], values[i+1]) for i in range(0, total_length, 2)]
    self._push(self._maybe_add_ref(ref_key, "{%s}" % (", ".join(pairs))))

  def on_id(self, bytes):
    pass

  def on_blob(self, value):
    pass

  def on_begin_seed(self, length, ref_key):
    pass

  def on_begin_struct(self, tags, ref_key):
    pass

  def on_get_ref(self, key):
    self._push("$%s" % key)

  def _maybe_add_ref(self, ref_key, text):
    if ref_key is None:
      return text
    else:
      return "$%s:%s" % (ref_key, text)

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
