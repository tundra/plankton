import io

from plankton.codec._types import *
from plankton.codec._binary import *
from plankton.codec._object import *


def decode_binary(input, factory=None, default_string_encoding=None):
  """Decode the given input as plankton data."""
  if isinstance(input, bytearray):
    input = io.BytesIO(input)
  builder = ObjectBuilder(factory, default_string_encoding)
  decoder = InstructionStreamDecoder(input)
  while not builder.has_result:
    decoder.decode_next(builder)
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
