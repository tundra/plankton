import unittest
import math

import plankton.codec
from plankton.schema import _id


class IdTest(unittest.TestCase):

  def test_sha224(self):
    self.run_type_test(_id.Sha224,
      "4dc677cd" "13e5ce41" "4e6c9a03" "b43eabe3" "88954c44" "19294310"
      "7f1eb560")
    self.run_type_test(_id.Sha512,
      "9b1e8460" "1036804d" "21cca384" "bcafac8d" "6b96f897" "1e5d2ac3"
      "73b02770" "d3c5c6ce" "8a71bda0" "6d881971" "0364d3ef" "eea8b78a"
      "eb0771e8" "2a103935" "f050dd40" "a85ff387")

  def run_type_test(self, type, expected):
    hasher = type.new_hasher()
    hasher.update(bytearray.fromhex("fabaceae"))
    self.assertEqual(bytearray.fromhex(expected), hasher.digest())
