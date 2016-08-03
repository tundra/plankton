from collections import OrderedDict
import glob
import itertools
import os
import os.path
import re
import unittest
import uuid

import plankton.codec


def _gen_test_case_files():
  """Generates the absolute path to all the test cases under data/."""
  data_root = os.path.join(os.path.dirname(__file__), '..', '..', 'spec')
  assert os.path.exists(data_root)
  test_cases = []
  for (dirpath, dirnames, filenames) in os.walk(os.path.abspath(data_root)):
    for filename in filenames:
      if filename.endswith(".txt"):
        test_cases.append(os.path.join(dirpath, filename))
  for test_case in test_cases:
    yield test_case


class TestCase(object):
  """Holds the parsed information about a test case from data/."""

  def __init__(self, instrs, bton, tton):
    self.instrs = instrs
    self.bton = bton
    self.tton = tton

  @property
  def data(self):
    vars = {}
    for line in self.instrs:
      yield_result = False
      save_result = None
      yield_match = re.match(r"yield (.*)", line)
      if yield_match:
        expr = yield_match.group(1)
        yield_result = True
      var_match = re.match(r"\$([a-z0-9]+) = (.*)", line)
      if var_match:
        save_result = var_match.group(1)
        expr = var_match.group(2)
      value = self._eval(vars, expr, save_result)
      if yield_result:
        return value
      if not save_result is None:
        vars[save_result] = value

  def _eval(self, vars, expr, dest=None):
    parts = expr.strip().split(" ")
    if len(parts) == 1:
      word = parts[0]
      if word == "null":
        return None
      elif word == "true":
        return True
      elif word == "false":
        return False
      elif word.startswith("$"):
        return vars[word[1:]]
      (instr, arg) = word.split(":")
      if instr == "int":
        return int(arg)
      elif instr == "id":
        return uuid.UUID(arg.zfill(32))
      elif instr == "blob":
        return bytearray.fromhex(arg)
      elif instr == "str":
        return arg
    (instr, arg) = parts[0].split(":")
    if instr == "array":
      count = int(arg)
      result = []
      if not dest is None:
        vars[dest] = result
      for part in parts[1:]:
        result.append(self._eval(vars, part))
      assert count == len(result)
      return result
    elif instr == "map":
      count = int(arg)
      result = OrderedDict()
      if not dest is None:
        vars[dest] = result
      for i in range(0, count):
        key = self._eval(vars, parts[1 + 2 * i])
        value = self._eval(vars, parts[2 + 2 * i])
        result[key] = value
      return result
    raise Exception("Unexpected expression {}".format(expr))

  @classmethod
  def parse(cls, source):
    lines = list(cls._strip_lines(source.splitlines()))
    offset = 0
    while re.match(r"---+ data ---+", lines[offset]) == None:
      offset += 1
    offset += 1
    instrs = []
    while re.match(r"---+ bton ---+", lines[offset]) == None:
      instrs.append(lines[offset])
      offset += 1
    offset += 1
    bton_lines = []
    while re.match(r"---+ tton ---+", lines[offset]) == None:
      bton_lines.append(lines[offset])
      offset += 1
    bton = bytearray.fromhex("".join(bton_lines))
    tton = "\n".join(lines[offset+1:])
    return TestCase(instrs, bton, tton)

  @staticmethod
  def _strip_lines(lines):
    for line in lines:
      stripped = line.strip()
      if stripped:
        yield stripped


def _parse_test_case(filename):
  with open(filename, "rt") as file:
    source = file.read()
  return TestCase.parse(source)


class TestCodecMeta(type):

  def __new__(mcs, name, bases, dict):
    def encode_test_case_method(test_case):
      return lambda self: self._run_encode_test_case(test_case)
    def decode_test_case_method(test_case):
      return lambda self: self._run_decode_test_case(test_case)

    for test_case_file in _gen_test_case_files():
      plankton_path = os.path.join("plankton", "data")
      split = test_case_file.find(plankton_path) + len(plankton_path)
      relative_path = test_case_file[split+1:-4]
      test_base_name = re.sub(r"[^a-zA-Z0-9]", "_", relative_path)
      test_case = _parse_test_case(test_case_file)

      dict["test_encode_{}".format(test_base_name)] = encode_test_case_method(test_case)
      dict["test_decode_{}".format(test_base_name)] = decode_test_case_method(test_case)
    return type.__new__(mcs, name, bases, dict)


class TestCodec(unittest.TestCase):
  __metaclass__ = TestCodecMeta

  def _run_encode_test_case(self, test_case):
    data = test_case.data
    encoded = plankton.codec.encode(data)
    self.assertEqual(test_case.bton, encoded)

  def _run_decode_test_case(self, test_case):
    data = test_case.data
    decoded = plankton.codec.decode(test_case.bton)
    self.assertStructurallyEqual(data, decoded)

  def assertStructurallyEqual(self, a, b):
    self.assertTrue(self.is_structurally_equal(a, b), "{} == {}".format(a, b))

  @classmethod
  def is_structurally_equal(cls, a, b, assumed_equivs=set()):
    if isinstance(a, (list, tuple)):
      # Checks that don't traverse a or b.
      if not isinstance(b, (list, tuple)):
        return False
      if not len(a) == len(b):
        return False

      # Are we just assuming that the two are equal?
      equiv = (id(a), id(b))
      if equiv in assumed_equivs:
        return True

      # If we see these two again assume they're equal. If they're not then the
      # traversal will detect it.
      assumed_equivs.add(equiv)

      # Traverse the array.
      for i in range(0, len(a)):
        if not cls.is_structurally_equal(a[i], b[i], assumed_equivs):
          return False

      return True
    elif isinstance(a, dict):
      # Checks that don't traverse a or b.
      if not isinstance(b, dict):
        return False
      if not len(a) == len(b):
        return False

      # Are we just assuming that the two are equal?
      equiv = (id(a), id(b))
      if equiv in assumed_equivs:
        return True

      # If we see these two again assume they're equal. If they're not then the
      # traversal will detect it.
      assumed_equivs.add(equiv)

      # Traverse the array.
      for k in a.keys():
        if not k in b:
          return False
        if not cls.is_structurally_equal(a[k], b[k], assumed_equivs):
          return False

      return True
    else:
      return a == b
