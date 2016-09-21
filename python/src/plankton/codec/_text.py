import itertools
import uuid
import base64

from plankton.codec import _types


__all__ = [
  "SyntaxError",
  "TextDecoder",
  "TextEncoder",
]


class SyntaxError(Exception):

  def __init__(self, token):
    self._token = token

  @property
  def offset(self):
    return self._token._offset

  @property
  def token(self):
    return self._token



class Token(object):

  def __init__(self, offset):
    self._offset = offset

  def is_atomic(self):
    return False

  def is_int(self):
    return False

  def is_string(self):
    return False

  def is_blob(self):
    return False

  def is_marker(self, type=None):
    return False

  def is_id(self):
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


class Blob(Token):

  def __init__(self, value):
    self._value = value

  def is_atomic(self):
    return True

  def is_blob(self):
    return True


class Marker(Token):

  def __init__(self, offset, value):
    super(Marker, self).__init__(offset)
    self._value = value

  def is_marker(self, type=None):
    return (type is None) or (type == self._value)

  def __str__(self):
    return "%%%s" % self._value


class Id(Token):

  def __init__(self, value):
    self._value = value

  def is_atomic(self):
    return True

  def is_id(self):
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


class End(Token):
  pass


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
    yield End(len(self._input))

  def _read_next(self):
    if self._is_digit_start(self._current):
      return self._read_number()
    elif self._current == "%":
      return self._read_marker()
    elif self._current == "$":
      return self._read_reference()
    elif self._current == "&":
      return self._read_id()
    elif self._current in "[],{}:@()":
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

  def _read_marker(self):
    assert self._current == "%"
    offset = self._cursor
    self._advance()
    while self._has_more() and self._current.isalnum():
      self._advance()
    if self._has_more() and self._current in "[{":
      self._advance()
    result = self._input[offset:self._cursor-1]
    if result in ["[", "x[", "u["]:
      return self._read_blob(result, offset)
    elif result in ["n", "t", "f"]:
      return Marker(offset, result)
    else:
      raise SyntaxError(Marker(offset, result))

  def _read_blob(self, marker, offset):
    start = self._cursor
    while self._has_more() and self._current != "]":
      self._advance()
    result = self._input[start-1:self._cursor-1]
    self._advance()
    if marker == "x[":
      data = bytearray.fromhex(result)
    else:
      data = base64.b64decode(result.replace("\n", ""))
    return Blob(data)

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

  def _read_id(self):
    assert self._current == "&"
    start = self._cursor
    self._advance()
    while self._has_more() and self._is_hex(self._current):
      self._advance()
    short_bytes = self._input[start:self._cursor-1]
    padded_bytes = "0" * (32 - len(short_bytes)) + short_bytes
    return Id(bytes(bytearray.fromhex(padded_bytes)))

  @staticmethod
  def _is_hex(chr):
    return chr in "0123456789abcdefABCDEF"


class TokenStream(object):

  def __init__(self, tokens):
    self._tokens = tokens
    self._offset = 0

  @property
  def current(self):
    return self._tokens[self._offset]

  def _advance(self):
    self._offset += 1

  def _expect_punctuation(self, value):
    if not self.current.is_punctuation(value):
      self._syntax_error()
    self._advance()

  def _expect_reference(self):
    if not self.current.is_reference():
      self._syntax_error()
    name = self.current._name
    self._advance()
    return name

  def _expect_marker(self, type=None):
    if not self.current.is_marker(type):
      self._syntax_error()
    self._advance()

  def _syntax_error(self):
    raise SyntaxError(self.current)


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
    elif tokens.current.is_punctuation("@"):
      return self._pre_parse_seed(tokens)
    elif tokens.current.is_marker():
      return self._pre_parse_marker(tokens)
    else:
      raise SyntaxError()

  def _parse(self, tokens, ref_name=None):
    if tokens.current.is_atomic():
      if not ref_name is None:
        tokens._syntax_error()
      if tokens.current.is_int():
        self._visitor.on_int(tokens.current._value)
      elif tokens.current.is_string():
        self._visitor.on_string(tokens.current._value.encode("utf-8"), None)
      elif tokens.current.is_blob():
        self._visitor.on_blob(tokens.current._value)
      elif tokens.current.is_id():
        self._visitor.on_id(tokens.current._value)
      else:
        raise AssertionError()
      tokens._advance()
    elif tokens.current.is_reference():
      return self._parse_reference(tokens)
    elif tokens.current.is_punctuation("["):
      return self._parse_array(tokens, ref_name)
    elif tokens.current.is_punctuation("{"):
      return self._parse_map(tokens, ref_name)
    elif tokens.current.is_punctuation("@"):
      return self._parse_seed(tokens, ref_name)
    elif tokens.current.is_marker():
      return self._parse_marker(tokens)
    else:
      tokens._syntax_error()

  def _pre_parse_array(self, tokens):
    offset = tokens._offset
    tokens._expect_punctuation("[")
    length = 0
    while not tokens.current.is_punctuation("]"):
      length += 1
      self._pre_parse(tokens)
      if tokens.current.is_punctuation(","):
        tokens._advance()
      else:
        break
    tokens._expect_punctuation("]")
    self._lengths[offset] = length

  def _parse_array(self, tokens, ref_key):
    length = self._lengths[tokens._offset]
    tokens._expect_punctuation("[")
    self._visitor.on_begin_array(length, ref_key)
    while not tokens.current.is_punctuation("]"):
      self._parse(tokens)
      if tokens.current.is_punctuation(","):
        tokens._advance()
      else:
        break
    tokens._expect_punctuation("]")

  def _pre_parse_map(self, tokens):
    offset = tokens._offset
    tokens._expect_punctuation("{")
    length = 0
    while not tokens.current.is_punctuation("}"):
      length += 1
      self._pre_parse(tokens)
      tokens._expect_punctuation(":")
      self._pre_parse(tokens)
      if tokens.current.is_punctuation(","):
        tokens._expect_punctuation(",")
      else:
        break
    tokens._expect_punctuation("}")
    self._lengths[offset] = length

  def _parse_map(self, tokens, ref_key):
    length = self._lengths[tokens._offset]
    tokens._expect_punctuation("{")
    self._visitor.on_begin_map(length, ref_key)
    while not tokens.current.is_punctuation("}"):
      self._parse(tokens)
      tokens._expect_punctuation(":")
      self._parse(tokens)
      if tokens.current.is_punctuation(","):
        tokens._expect_punctuation(",")
      else:
        break
    tokens._expect_punctuation("}")

  def _pre_parse_seed(self, tokens):
    offset = tokens._offset
    tokens._expect_punctuation("@")
    header = self._pre_parse(tokens)
    length = 0
    if tokens.current.is_punctuation("("):
      tokens._advance()
      while not tokens.current.is_punctuation(")"):
        length += 1
        self._pre_parse(tokens)
        tokens._expect_punctuation(":")
        self._pre_parse(tokens)
        if tokens.current.is_punctuation(","):
          tokens._expect_punctuation(",")
        else:
          break
      tokens._expect_punctuation(")")
    self._lengths[offset] = length

  def _parse_seed(self, tokens, ref_key):
    length = self._lengths[tokens._offset]
    tokens._expect_punctuation("@")
    self._visitor.on_begin_seed(length, ref_key)
    self._parse(tokens)
    if tokens.current.is_punctuation("("):
      tokens._advance()
      while not tokens.current.is_punctuation(")"):
        self._parse(tokens)
        tokens._expect_punctuation(":")
        self._parse(tokens)
        if tokens.current.is_punctuation(","):
          tokens._expect_punctuation(",")
        else:
          break
      tokens._expect_punctuation(")")

  def _pre_parse_reference(self, tokens):
    tokens._expect_reference()
    if tokens.current.is_punctuation(":"):
      tokens._expect_punctuation(":")
      self._pre_parse(tokens)

  def _parse_reference(self, tokens):
    name = tokens._expect_reference()
    if tokens.current.is_punctuation(":"):
      tokens._expect_punctuation(":")
      self._parse(tokens, name)
    else:
      self._visitor.on_get_ref(name)

  def _pre_parse_marker(self, tokens):
    if tokens.current.is_marker("n"):
      tokens._expect_marker("n")
    elif tokens.current.is_marker("t"):
      tokens._expect_marker("t")
    elif tokens.current.is_marker("f"):
      tokens._expect_marker("f")
    else:
      tokens._syntax_error()

  def _parse_marker(self, tokens):
    if tokens.current.is_marker("n"):
      tokens._expect_marker("n")
      self._visitor.on_singleton(None)
    elif tokens.current.is_marker("t"):
      tokens._expect_marker("t")
      self._visitor.on_singleton(True)
    elif tokens.current.is_marker("f"):
      tokens._expect_marker("f")
      self._visitor.on_singleton(False)
    else:
      assert False


# Hey no problem, I have so much time I don't mind wasting tons of it on
# unforced incompatibilities between python versions. It's my pleasure.
try:
  to_unicode = unicode
except NameError:
  to_unicode = lambda x: x


class TextEncoder(_types.StackingBuilder):

  def __init__(self, out):
    super(TextEncoder, self).__init__()
    self._out = out
    self._default_string_encoding = "utf-8"
    self._schedule_end(1, self._flush, None, None)

  def _flush(self, total_count, unused_1, values, unused_2):
    """Store the value currently on the stack in the result field."""
    [result] = values
    self._out.write(to_unicode(result))

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
    self._push('"%s"' % bytes.decode(self._default_string_encoding))

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
    basic = "{%s}" % (", ".join(pairs))
    self._push(self._maybe_add_ref(ref_key, basic))

  def on_id(self, bytes):
    ivalue = uuid.UUID(bytes=bytes).int
    if ivalue >= 2**64:
      self._push("&%032x" % ivalue)
    elif ivalue >= 2**32:
      self._push("&%016x" % ivalue)
    elif ivalue >= 2**16:
      self._push("&%08x" % ivalue)
    elif ivalue >= 2**8:
      self._push("&%04x" % ivalue)
    else:
      assert ivalue < 2**8
      self._push("&%x" % ivalue)

  def on_blob(self, value):
    encoded = base64.b64encode(value)
    self._push("%%[%s]" % encoded.decode("ascii"))

  def on_begin_seed(self, length, ref_key):
    self._schedule_end(1, self._end_seed_header, ref_key, length)

  def _end_seed_header(self, total_length, ref_key, values, field_count):
    if field_count == 0:
      [header] = values
      self._push(self._maybe_add_ref(ref_key, "@%s" % header))
    else:
      self._schedule_end(2 * field_count, self._end_seed, ref_key, values)

  def _end_seed(self, total_length, ref_key, values, headers):
    [header] = headers
    pairs = ["%s: %s" % (values[i], values[i+1]) for i in range(0, total_length, 2)]
    self._push(self._maybe_add_ref(ref_key, "@%s(%s)" % (header, ", ".join(pairs))))

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
