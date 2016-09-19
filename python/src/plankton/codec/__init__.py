import io

from plankton.codec._binary import *
from plankton.codec._object import *
from plankton.codec._text import *
from plankton.codec._types import *


def decode_binary(input, factory=None, default_string_encoding=None):
  """Decode the given input as plankton data."""
  if isinstance(input, bytearray):
    input = io.BytesIO(input)
  builder = ObjectBuilder(factory, default_string_encoding)
  decoder = BinaryDecoder(input, builder)
  while not builder.has_result:
    decoder.decode_next(ref_key=None)
  return builder.result


def decode_text(input, factory=None, default_string_encoding=None):
  builder = ObjectBuilder(factory, default_string_encoding)
  decoder = TextDecoder(builder)
  decoder.decode(input)
  return builder.result


def _encode_binary_with_decoder(decoder_type, value):
  out = io.BytesIO()
  encoder = BinaryEncoder(out)
  decoder_type(encoder).decode(value)
  return out.getvalue()


def encode_binary(value):
  try:
    # Assume the value is tree-shaped and try encoding it as such.
    return _encode_binary_with_decoder(ObjectTreeDecoder, value)
  except SharedStructureDetected:
    # It wasn't tree shaped -- fall back on reference tracking then.
    return _encode_binary_with_decoder(ObjectGraphDecoder, value)


def _encode_text_with_decoder(decoder_type, value):
  out = io.StringIO()
  encoder = TextEncoder(out)
  decoder_type(encoder).decode(value)
  return out.getvalue()


def encode_text(value):
  try:
    # Assume the value is tree-shaped and try encoding it as such.
    return _encode_text_with_decoder(ObjectTreeDecoder, value)
  except SharedStructureDetected:
    # It wasn't tree shaped -- fall back on reference tracking then.
    return _encode_text_with_decoder(ObjectGraphDecoder, value)
