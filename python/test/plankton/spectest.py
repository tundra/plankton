import collections
import os
import re
import struct
import unittest
import uuid

import plankton.codec
import plankton.schema


class SpecTestBlock(object):
  """An individual section of a spec test."""

  def __init__(self, config):
    self.config = config

  def is_canonical(self):
    return self.config.get("canonical", "true").lower() == "true"

  @staticmethod
  def parse(header, config, lines):
    return _SPEC_TEST_BLOCKS[header]._parse_concrete(config, lines)


class ExpressionBlock(SpecTestBlock):

  def _eval_composite(self, vars, expr, dest=None):
    """Evaluates a possibly composite expression."""
    parts = expr.strip().split(" ")
    if len(parts) == 1:
      try:
        # Try parsing as an atom; may fail which is okay, there's more cases we
        # can try below.
        return self._eval_atom(vars, parts[0])
      except NotAnAtom:
        pass
    (instr, arg) = parts[0].split(":")
    if instr == "array":
      count = int(arg)
      result = []
      if not dest is None:
        vars[dest] = result
      for part in parts[1:]:
        result.append(self._eval_atom(vars, part))
      assert count == len(result)
      return result
    elif instr == "map":
      count = int(arg)
      result = collections.OrderedDict()
      if not dest is None:
        vars[dest] = result
      for i in range(0, count):
        key = self._eval_atom(vars, parts[1 + 2 * i])
        value = self._eval_atom(vars, parts[2 + 2 * i])
        result[key] = value
      return result
    elif instr == "seed":
      count = int(arg)
      header = self._eval_atom(vars, parts[1])
      fields = collections.OrderedDict()
      result = plankton.codec.Seed(header, fields)
      if not dest is None:
        vars[dest] = result
      for i in range(0, count):
        field = self._eval_atom(vars, parts[2 + 2 * i])
        value = self._eval_atom(vars, parts[3 + 2 * i])
        fields[field] = value
      return result
    elif instr == "struct":
      count = int(arg)
      fields = []
      result = plankton.codec.Struct(fields)
      if not dest is None:
        vars[dest] = result
      for i in range(0, count):
        tag = int(parts[1 + 2 * i])
        value = self._eval_atom(vars, parts[2 + 2 * i])
        fields.append((tag, value))
      return result
    raise Exception("Unexpected expression {}".format(expr))

  def _eval_atom(self, vars, word):
    """
    Parses a string as an atomic expression. If we can't recognize the string as
    a valid atom NotAnAtom is raised.
    """
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
    elif instr == "float":
      return float(arg)
    elif instr == "id":
      return uuid.UUID(arg.zfill(32))
    elif instr == "blob":
      return bytearray.fromhex(arg)
    elif instr == "str":
      return arg
    else:
      raise NotAnAtom()


class DataBlock(ExpressionBlock):

  def __init__(self, config, instrs):
    super(DataBlock, self).__init__(config)
    self._instrs = instrs

  def eval(self):
    """Returns the parsed data/object this test case represents."""
    vars = {}
    for line in self._instrs:
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
      value = self._eval_composite(vars, expr, save_result)
      if yield_result:
        return value
      if not save_result is None:
        vars[save_result] = value

  @staticmethod
  def _parse_concrete(config, instrs):
    return DataBlock(config, instrs)


class StreamBlock(ExpressionBlock):

  def __init__(self, config, instrs):
    super(StreamBlock, self).__init__(config)
    self._instrs = instrs

  def generate(self):
    schema = plankton.schema.Schema()
    for line in self._instrs:
      template_match = re.match(r"-\s+([^(]+)\(\)\s+=>\s+(.*)", line)
      if template_match:
        name = self._eval_composite(None, template_match.group(1), None)
        value = self._eval_composite(None, template_match.group(2), None)
        template = plankton.schema.Template(name, None, value)
        schema.add_template(template)
      elif line == "! schema":
        yield ("schema", schema)
        schema = plankton.schema.Schema()

  @staticmethod
  def _parse_concrete(config, instrs):
    return StreamBlock(config, instrs)


class BtonBlock(SpecTestBlock):
  """A binary plankton block."""

  def __init__(self, config, data):
    super(BtonBlock, self).__init__(config)
    self.data = data

  @staticmethod
  def _parse_concrete(config, lines):
    bton = bytearray.fromhex("".join(lines))
    return BtonBlock(config, bton)


class TtonBlock(SpecTestBlock):
  """A text plankton block."""

  def __init__(self, config, source):
    super(TtonBlock, self).__init__(config)
    self.source = source

  @staticmethod
  def _parse_concrete(config, lines):
    tton = "".join(lines)
    return TtonBlock(config, tton)


_SPEC_TEST_BLOCKS = {
  "bton": BtonBlock,
  "data": DataBlock,
  "stream": StreamBlock,
  "tton": TtonBlock,
}


class NotAnAtom(Exception):
  pass


class ParsedSpecTest(object):
  """Holds the partially parsed information about a test case from spec/."""

  def __init__(self, blocks):
    self.blocks = blocks

  @property
  def datas(self):
    return self.blocks.get("data", [])

  @property
  def btons(self):
    return self.blocks.get("bton", [])

  @property
  def ttons(self):
    return self.blocks.get("tton", [])

  @property
  def canonical_tton(self):
    for tton in self.ttons:
      if tton.is_canonical:
        return tton

  @property
  def streams(self):
    return self.blocks.get("stream", [])

  @classmethod
  def parse(cls, source):
    """Parses an entire test case file into blocks."""
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
      blocks[header].append(SpecTestBlock.parse(header, config, block_lines))
    return ParsedSpecTest(blocks)

  @staticmethod
  def _strip_lines(lines):
    """Strips spaces from the given lines, skipping empty lines."""
    for line in lines:
      stripped = line.strip()
      if stripped:
        yield stripped


def _read_spec_test(filename):
  """Given a file name, returns the parsed test case contained in the file."""
  with open(filename, "rt") as file:
    source = file.read()
  return ParsedSpecTest.parse(source)


def build_test_suite(loader, tests, pattern, test_case_factory):
  """Called by the test framework to produce the test suite to run."""
  suite = unittest.TestSuite()
  data_root = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'spec')
  assert os.path.exists(data_root)
  test_files = []
  absroot = os.path.abspath(data_root)
  for (dirpath, dirnames, filenames) in os.walk(absroot):
    for filename in filenames:
      if filename.endswith(".txt"):
        test_file = os.path.join(dirpath, filename)
        test_files.append(test_file)
  test_files.sort()
  for test_file in test_files:
    test_name = test_file[len(absroot)+1:]
    spec_test = _read_spec_test(test_file)
    test_class = test_case_factory(test_file, test_name, spec_test)
    if test_class:
      suite.addTests(loader.loadTestsFromTestCase(test_class))
  return suite


class SpecTestCase(unittest.TestCase):
  """Adds a few utilities that are useful to spec tests."""

  def assertStructurallyEqual(self, a, b):
    """Assertion that fails unless a and b are structurally equal."""
    if not is_structurally_equal(a, b):
      message = "%s !~= %s" % (repr(a), repr(b))
      self.fail(message)


def is_structurally_equal(a, b, assumed_equivs=None):
  """
  Returns true iff a and b are structurally equal, that is, atomic values are
  equal, composite values have the same size and elements, and values reference
  each other (by object identity) in the same way. Floats are structurally equal
  if they are exactly equal -- so for instance NaN is structurally equal to NaN
  and -0 and 0 are not equal.

  Assumed_equivs is a set of pairs (id1, id2) where, if we encounter two objects
  where one has id1 and the other has id2 then we will just accept them as equal
  without explicitly checking, because then we'll either already have checked
  that they're equal elsewhere or we'll be in the middle of checking them right
  now and so doing it recursively will cause the comparison to diverge.
  """
  if assumed_equivs is None:
    assumed_equivs = set()

  if isinstance(a, (list, tuple)):
    # Checks that don't traverse a or b.
    if not isinstance(b, (list, tuple)):
      return False
    if not len(a) == len(b):
      return False

    if _are_assumed_equal(a, b, assumed_equivs):
      return True

    # Traverse the array.
    for i in range(0, len(a)):
      if not is_structurally_equal(a[i], b[i], assumed_equivs):
        return False

    return True

  elif isinstance(a, dict):
    # Checks that don't traverse a or b.
    if not isinstance(b, dict):
      return False
    if not len(a) == len(b):
      return False

    if _are_assumed_equal(a, b, assumed_equivs):
      return True

    # Traverse the array.
    for k in a.keys():
      if not k in b:
        return False
      if not is_structurally_equal(a[k], b[k], assumed_equivs):
        return False

    return True

  elif isinstance(a, plankton.codec.Seed):
    # Checks that don't traverse a or b.
    if not isinstance(b, plankton.codec.Seed):
      return False
    if len(a.fields) != len(b.fields):
      return False

    if _are_assumed_equal(a, b, assumed_equivs):
      return True

    if not is_structurally_equal(a.header, b.header, assumed_equivs):
      return False

    for f in a.fields.keys():
      if not f in b.fields:
        return False
      if not is_structurally_equal(a.fields[f], b.fields[f], assumed_equivs):
        return False

    return True

  elif isinstance(a, plankton.codec.Struct):
    # Checks that don't traverse a or b.
    if not isinstance(b, plankton.codec.Struct):
      return False
    if len(a.fields) != len(b.fields):
      return False

    if _are_assumed_equal(a, b, assumed_equivs):
      return True

    for i in range(0, len(a.fields)):
      (ta, va) = a.fields[i]
      (tb, vb) = b.fields[i]
      if ta != tb:
        return False
      if not is_structurally_equal(va, vb, assumed_equivs):
        return False

    return True

  elif isinstance(a, float):
    if not isinstance(b, float):
      return False

    # Float comparison is tricky and magical and what we're really after is
    # whether the two values are *exactly* the same. So we compare the binary
    # representation instead of the values.
    a_bytes = struct.pack("<d", a)
    b_bytes = struct.pack("<d", b)
    return a_bytes == b_bytes

  else:
    return a == b


def _are_assumed_equal(a, b, assumed_equivs):
  """
  Returns true iff a and b are already assumed to be equal. Otherwise adds an
  equivalence of a and b to the set and returns false.
  """

  # Are we just assuming that the two are equal?
  equiv = (id(a), id(b))
  if equiv in assumed_equivs:
    return True

  # If we see these two again assume they're equal. If they're not then the
  # traversal will detect it.
  assumed_equivs.add(equiv)
  return False
