import unittest

import plankton.codec
from plankton.codec import _binary


def is_single_precision(value):
  encoded = _binary.BinaryEncoder._encode_float(value)
  return len(encoded) == 4


class FloatTest(unittest.TestCase):

  def test_integers(self):
    self.assertTrue(is_single_precision(0))
    self.assertTrue(is_single_precision(1))
    self.assertTrue(is_single_precision(2))
    self.assertTrue(is_single_precision(3))
    self.assertTrue(is_single_precision(-1))
    self.assertTrue(is_single_precision(-2))
    self.assertTrue(is_single_precision(-3))
    self.assertTrue(is_single_precision(0x1000000))
    self.assertFalse(is_single_precision(0x1000001))
    self.assertTrue(is_single_precision(-0x1000000))
    self.assertFalse(is_single_precision(-0x1000001))

  def test_decimals(self):
    self.assertTrue(is_single_precision(0.5))
    self.assertTrue(is_single_precision(0.25))
    self.assertTrue(is_single_precision(0.125))

    self.assertFalse(is_single_precision(1.0 / 3.0))
    self.assertFalse(is_single_precision(0.1))
    self.assertFalse(is_single_precision(0.2))
    self.assertFalse(is_single_precision(0.3))
    self.assertFalse(is_single_precision(0.4))
    self.assertTrue(is_single_precision(1000.125))
    self.assertFalse(is_single_precision(1000.05))
    self.assertTrue(is_single_precision(3.1414947509765625))

    i16 = 1.0 / 16
    self.assertTrue(is_single_precision(0x100000 - i16))
    self.assertFalse(is_single_precision(0x100000 + i16))

    i256 = 1.0 / 256
    self.assertTrue(is_single_precision(0x10000 - i256))
    self.assertFalse(is_single_precision(0x10000 + i256))
