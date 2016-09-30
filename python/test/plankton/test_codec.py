import unittest

from test.plankton import spectest
import plankton.codec


def load_tests(loader, tests, pattern):
  """Called by the test framework to produce the test suite to run."""
  return spectest.build_test_suite(loader, tests, pattern, _make_codec_test_class)


def _make_codec_test_class(test_file, test_name, spec_test):
  if len(spec_test.datas) == 0:
    return None
  class CodecTest(AbstractCodecTest):
    def get_test_file(self):
      return test_file
    def get_test_name(self):
      return test_name
    def get_spec_test(self):
      return spec_test
  CodecTest.__qualname__ = CodecTest.__name__ = "CodecTest({})".format(test_name)
  return CodecTest


class AbstractCodecTest(spectest.SpecTestCase):
  """A codec test case. Concrete tests are created by _make_codec_test_class."""

  def setUp(self):
    self.test_case = self.get_spec_test()

  def test_data_to_canonical_bton(self):
    """
    Test that encoding the hardcoded data yields the expected bytes.
    """
    for data_block in self.test_case.datas:
      data = data_block.eval()
      encoded = plankton.codec.encode_binary(data)
      for bton in self.test_case.btons:
        if bton.is_canonical():
          self.assertEqual(bton.data, encoded)

  def test_all_btons_to_data(self):
    """
    Test that decoding the hardcoded bytes yields the expected data.
    """
    for data_block in self.test_case.datas:
      data = data_block.eval()
      for bton in self.test_case.btons:
        decoded = plankton.codec.decode_binary(bton.data)
        self.assertStructurallyEqual(data, decoded)

  def test_data_object_clone(self):
    """
    Test that traversing the data with the decoder yields a new value that is
    identical to the input, a clone.
    """
    builder = plankton.codec.ObjectBuilder()
    decoder = plankton.codec.ObjectGraphDecoder(builder)
    for data_block in self.test_case.datas:
      data = data_block.eval()
      decoder.decode(data)
      cloned = builder.result
      self.assertStructurallyEqual(data, cloned)

  def test_all_ttons_to_data(self):
    """
    Test that decoding the tton sections yields the expected data.
    """
    for data_block in self.test_case.datas:
      data = data_block.eval()
      for tton in self.test_case.ttons:
        decoded = plankton.codec.decode_text(tton.source)
        self.assertStructurallyEqual(data, decoded)

  def test_data_to_canonical_tton(self):
    for data_block in self.test_case.datas:
      data = data_block.eval()
      tton = self.test_case.canonical_tton
      if tton:
        encoded = plankton.codec.encode_text(data)
        self.assertEqual(tton.source, encoded)
