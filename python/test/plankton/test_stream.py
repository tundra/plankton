import io
import math
import unittest

from plankton.schema import _schema
from plankton.stream import _stream
from test.plankton import spectest
import plankton.codec
import plankton.schema
import plankton.stream


def load_tests(loader, tests, pattern):
  """Called by the test framework to produce the test suite to run."""
  return spectest.build_test_suite(loader, tests, pattern, _make_stream_test_class)


def _make_stream_test_class(test_file, test_name, spec_test):
  if len(spec_test.streams) == 0:
    return None
  class StreamTest(AbstractStreamTest):
    def get_test_file(self):
      return test_file
    def get_test_name(self):
      return test_name
    def get_spec_test(self):
      return spec_test
  StreamTest.__qualname__ = StreamTest.__name__ = "StreamTest({})".format(test_name)
  return StreamTest


class AbstractStreamTest(spectest.SpecTestCase):
  """A codec test case. Concrete tests are created by _make_codec_test_class."""

  def setUp(self):
    self.test_case = self.get_spec_test()

  def test_stream_to_canonical_bton(self):
    for stream_block in self.test_case.streams:
      out = io.BytesIO()
      encoder = plankton.codec.BinaryEncoder(out)
      stream = _stream.StreamWriter(encoder)
      for (type, data) in stream_block.generate():
        if type == "schema":
          stream.define_schema(data)
        else:
          raise Exception("Unknown op %s" % type)
      encoded = out.getvalue()
      for bton in self.test_case.btons:
        if bton.is_canonical():
          self.assertEqual(bton.data, encoded)
