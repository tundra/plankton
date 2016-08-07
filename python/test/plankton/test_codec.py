from collections import OrderedDict
import glob
import itertools
import os
import os.path
import re
import unittest
import uuid
import io
import pprint

import plankton.codec


def load_tests(loader, tests, pattern):
  """Generates the absolute path to all the test cases under data/."""
  suite = unittest.TestSuite()
  data_root = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'spec')
  assert os.path.exists(data_root)
  test_files = []
  absroot = os.path.abspath(data_root)
  for (dirpath, dirnames, filenames) in os.walk(absroot):
    for filename in filenames:
      if filename.endswith(".txt"):
        test_file = os.path.join(dirpath, filename)
        test_name = test_file[len(absroot)+1:]
        test_class = _make_test_class(test_file, test_name)
        suite.addTests(loader.loadTestsFromTestCase(test_class))
  return suite


def _make_test_class(test_file, test_name):
  class CodecTest(AbstractCodecTest):
    def get_test_file(self):
      return test_file
    def get_test_name(self):
      return test_name
  CodecTest.__qualname__ = CodecTest.__name__ = "CodecTest({})".format(test_name)
  return CodecTest


class BtonBlock(object):

  def __init__(self, config, data):
    self.config = config
    self.data = data

  def is_canonical(self):
    return self.config.get("canonical", "true").lower() == "true"


class TestCase(object):
  """Holds the parsed information about a test case from data/."""

  def __init__(self, config, instrs, btons):
    self.config = config
    self.instrs = instrs
    self.btons = btons

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
    elif instr == "seed":
      count = int(arg)
      header = self._eval(vars, parts[1])
      fields = OrderedDict()
      result = plankton.codec.Seed(header, fields)
      if not dest is None:
        vars[dest] = result
      for i in range(0, count):
        field = self._eval(vars, parts[2 + 2 * i])
        value = self._eval(vars, parts[3 + 2 * i])
        fields[field] = value
      return result
    elif instr == "struct":
      count = int(arg)
      fields = []
      result = plankton.codec.Struct(fields)
      for i in range(0, count):
        tag = int(parts[1 + 2 * i])
        value = self._eval(vars, parts[2 + 2 * i])
        fields.append((tag, value))
      return result
    raise Exception("Unexpected expression {}".format(expr))

  @classmethod
  def parse(cls, source):
    lines = list(cls._strip_lines(source.splitlines()))
    offset = 0
    blocks = {}
    while offset < len(lines):
      # Scan until we find a block.
      while offset < len(lines):
        header_match = re.match(r"---+ (.*) ---+", lines[offset])
        offset += 1
        if header_match:
          header = header_match.group(1)
          break
      config = {}
      while offset < len(lines):
        config_match = re.match(r"^%\s*([\w_]+)\s*:(.*)$", lines[offset])
        if not config_match:
          break
        config[config_match.group(1).strip()] = config_match.group(2).strip()
        offset += 1
      block_lines = []
      while offset < len(lines) and (re.match(r"---+ (.*) ---+", lines[offset]) == None):
        block_lines.append(lines[offset])
        offset += 1
      if not header in blocks:
        blocks[header] = []
      blocks[header].append((config, block_lines))
    instrs = blocks["data"][0][1]
    btons = []
    for (config, bton_lines) in blocks["bton"]:
      bton = bytearray.fromhex("".join(bton_lines))
      btons.append(BtonBlock(config, bton))
    return TestCase(config, instrs, btons)

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


class AbstractCodecTest(unittest.TestCase):

  def setUp(self):
    self.test_case = _parse_test_case(self.get_test_file())

  def test_encode(self):
    data = self.test_case.data
    encoded = plankton.codec.encode(data)
    for bton in self.test_case.btons:
      if bton.is_canonical():
        self.assertEqual(bton.data, encoded)

  def test_decode(self):
    data = self.test_case.data
    for bton in self.test_case.btons:
      decoded = plankton.codec.decode(bton.data)
      self.assertStructurallyEqual(data, decoded)

  def assertStructurallyEqual(self, a, b):
    self.assertTrue(self.is_structurally_equal(a, b, set()))

  @classmethod
  def is_structurally_equal(cls, a, b, assumed_equivs):
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

    elif isinstance(a, plankton.codec.Seed):
      # Checks that don't traverse a or b.
      if not isinstance(b, plankton.codec.Seed):
        return False
      if len(a.fields) != len(b.fields):
        return False

      equiv = (id(a), id(b))
      if equiv in assumed_equivs:
        return True

      assumed_equivs.add(equiv)
      if not cls.is_structurally_equal(a.header, b.header, assumed_equivs):
        return False

      for f in a.fields.keys():
        if not f in b.fields:
          return False
        if not cls.is_structurally_equal(a.fields[f], b.fields[f], assumed_equivs):
          return False

      return True

    elif isinstance(a, plankton.codec.Struct):
      # Checks that don't traverse a or b.
      if not isinstance(b, plankton.codec.Struct):
        return False
      if len(a.fields) != len(b.fields):
        return False

      equiv = (id(a), id(b))
      if equiv in assumed_equivs:
        return True

      assumed_equivs.add(equiv)
      for i in range(0, len(a.fields)):
        (ta, va) = a.fields[i]
        (tb, vb) = b.fields[i]
        if ta != tb:
          return False
        if not cls.is_structurally_equal(va, vb, assumed_equivs):
          return False

      return True

    else:
      return a == b
