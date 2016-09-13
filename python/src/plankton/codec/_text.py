import itertools


__all__ = [
  "TextDecoder",
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

  def _parse_array(self, tokens, ref_name):
    assert tokens.current.is_punctuation("[")
    offset = tokens._offset
    tokens._advance()
    length = self._lengths[offset]
    self._visitor.on_begin_array(length)
    if not ref_name is None:
      self._visitor.on_add_ref(ref_name)
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

  def _parse_map(self, tokens, ref_name):
    assert tokens.current.is_punctuation("{")
    offset = tokens._offset
    tokens._advance()
    length = self._lengths[offset]
    self._visitor.on_begin_map(length)
    if not ref_name is None:
      self._visitor.on_add_ref(ref_name)
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
