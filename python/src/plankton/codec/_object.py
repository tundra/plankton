"""
Utilities for working with object structures: building them based on encoded
input and traversing them.
"""


from abc import abstractmethod, ABCMeta
import collections
import sys
import uuid

from plankton.codec import _types


__all__ = [
  "ObjectBuilder",
  "ObjectGraphDecoder",
  "ObjectTreeDecoder",
  "SharedStructureDetected",
]


if sys.version_info < (3,):
  _INT_TYPES = (int, long)
  _BASESTRING_TYPE = basestring
else:
  _INT_TYPES = (int,)
  _BASESTRING_TYPE = str


class DefaultDataFactory(object):
  """
  The default data factory that constructs plain, boring, python data for the
  different composite types.
  """

  def new_array(self):
    return []

  def new_map(self):
    return collections.OrderedDict()

  def new_id(self, bytes):
    return uuid.UUID(bytes=bytes)

  def new_seed(self):
    return _types.Seed(None, collections.OrderedDict())

  def new_struct(self):
    return _types.Struct([])


class ObjectBuilder(_types.Visitor):
  """
  An instruction stream visitor that builds up an object graph based on the
  incoming instructions.
  """

  def __init__(self, factory=None, default_string_encoding=None):
    self._factory = factory or DefaultDataFactory()
    self._default_string_encoding = default_string_encoding or "utf-8"
    self._refs = {}
    # The stack of values we've seen so far but haven't packed into a composite
    # value of some sort.
    self._value_stack = []
    # A stack of info about how to pack values into composites when we've
    # collected enough values.
    self._pending_ends = []
    # The final result of parsing. It's totally valid for this to be None since
    # that's a valid parsing result.
    self._result = None
    self._init()

  def _init(self):
    # Schedule an end that doesn't do anything but that ensures that we don't
    # have to explicitly check for the bottom of the pending ends.
    self._schedule_end(2, 1, None, None)
    # Schedule an end that stores the result in the _result field.
    self._schedule_end(1, self._store_result, None, None)

  @property
  def has_result(self):
    """Has this builder completed building the object graph?"""
    return len(self._pending_ends) == 1

  @property
  def result(self):
    """If this builder has completed the object graph, yields the value."""
    assert self.has_result
    assert [None] == self._value_stack
    return self._result

  def _store_result(self, total_count, open_result, values, data):
    [self._result] = values
    self._push(None)

  def on_invalid_instruction(self, code):
    raise Exception("Invalid instruction 0x{:x}".format(code))

  def on_int(self, value):
    self._push(value)

  def on_singleton(self, value):
    self._push(value)

  def on_id(self, data):
    self._push(self._factory.new_id(data))

  def on_begin_array(self, length, ref_key):
    array = self._factory.new_array()
    self._maybe_add_ref(ref_key, array)
    if length == 0:
      self._push(array)
    else:
      self._schedule_end(length, self._end_array, array, None)

  def _end_array(self, total_length, array, values, unused):
    array[:] = values
    self._push(array)

  def on_begin_map(self, length, ref_key):
    map = self._factory.new_map()
    self._maybe_add_ref(ref_key, map)
    if length == 0:
      self._push(map)
    else:
      self._schedule_end(2 * length, self._end_map, map, None)

  def _end_map(self, total_length, map, values, unused):
    for i in range(0, total_length, 2):
      map[values[i]] = values[i+1]
    self._push(map)

  def on_blob(self, data):
    self._push(data)

  def on_string(self, data, encoding):
    if not encoding:
      encoding = self._default_string_encoding
    self._push(data.decode(encoding))

  def on_begin_seed(self, field_count, ref_key):
    seed = self._factory.new_seed()
    self._maybe_add_ref(ref_key, seed)
    self._schedule_end(1, self._end_seed_header, seed, field_count)

  def _end_seed_header(self, one, seed, header_values, field_count):
    [header] = header_values
    seed.header = header
    if field_count == 0:
      self._push(seed)
    else:
      self._schedule_end(2 * field_count, self._end_seed, seed, None)

  def _end_seed(self, total_count, seed, field_values, unused):
    for i in range(0, total_count, 2):
      seed.fields[field_values[i]] = field_values[i+1]
    self._push(seed)

  def on_begin_struct(self, tags, ref_key):
    struct = self._factory.new_struct()
    self._maybe_add_ref(ref_key, struct)
    tag_count = len(tags)
    if tag_count == 0:
      self._push(struct)
    else:
      self._schedule_end(tag_count, self._end_struct, struct, tags)

  def _end_struct(self, total_length, struct, values, tags):
    for i in range(0, len(tags)):
      struct.fields.append((tags[i], values[i]))
    self._push(struct)

  def on_get_ref(self, key):
    self._push(self._refs[key])

  def _maybe_add_ref(self, ref_key, value):
    if not ref_key is None:
      self._refs[ref_key] = value

  def _push(self, value):
    """
    Push a value on top of the value stack and execute any pending ends that
    are now ready to be executed.
    """
    self._value_stack.append(value)
    self._pending_ends[-1][1] += 1
    if self._pending_ends[-1][0] == self._pending_ends[-1][1]:
      [total_count, added_count, callback, open_result, data] = self._pending_ends.pop()
      values = self._value_stack[-total_count:]
      del self._value_stack[-total_count:]
      # The callback may or may not push a value which will cause this to be
      # called again and then we'll deal with any pending ends further down
      # the pending end stack.
      callback(total_count, open_result, values, data)

  def _schedule_end(self, total_count, callback, open_result, data):
    """
    Schedule the given callback to be called after total_count values have
    become available. The count has to be > 0 because that simplifies things
    and also, if the number of remaining values is 0 the caller can just
    create the result immediately.
    """
    assert total_count > 0
    assert callback
    self._pending_ends.append([total_count, 0, callback, open_result, data])


class SharedStructureDetected(Exception):
  """
  Indicated that while traversing an object graph we assumed was tree-shaped we
  came across some shared structure.
  """
  pass


class DefaultClassifier(object):
  """
  The standard, vanilla, classifier that divides values based on the plain,
  boring, python types.
  """

  @staticmethod
  def is_array(value):
    """Is the given value one we'll consider an array?"""
    return isinstance(value, (list, tuple))

  @staticmethod
  def is_map(value):
    """Is the given value one we'll consider a map?"""
    return isinstance(value, dict)

  @staticmethod
  def is_struct(value):
    """Is the given value one we'll consider a struct?"""
    return isinstance(value, _types.Struct)

  @staticmethod
  def is_seed(value):
    """Is the given value one we'll consider a seed?"""
    return isinstance(value, _types.Seed)

  @staticmethod
  def is_id(value):
    return isinstance(value, uuid.UUID)

  @staticmethod
  def is_blob(value):
    return isinstance(value, bytearray)


class AbstractObjectDecoder(object):
  """
  An object decoder that leaves the handling of object references up to
  subclasses but otherwise handles everything else.
  """
  __metaclass__ = ABCMeta

  def __init__(self, visitor, classifier=None):
    self._visitor = visitor
    self._classifier = classifier or DefaultClassifier()
    self._next_ref_offset = 0

  def _get_ref_offset(self):
    result = self._next_ref_offset
    self._next_ref_offset += 1
    return result

  @abstractmethod
  def _preprocess(self, value):
    """Called before this encoder starts serializing the given value."""
    pass

  @abstractmethod
  def _should_add_ref(self, value):
    """
    Returns true iff the subclass determines that we should create a ref to the
    given value at this point in the stream.
    """
    pass

  @abstractmethod
  def _get_backref(self, value):
    """
    If at this point we should make a reference back to a previous occurrence of
    the given value, return of absolute offset of that occurrence.
    """

  def decode(self, value):
    self._preprocess(value)
    return self._decode(value)

  def _decode(self, value):
    if (value is None) or isinstance(value, bool):
      return self._visitor.on_singleton(value)
    elif isinstance(value, _INT_TYPES):
      return self._visitor.on_int(value)
    elif isinstance(value, _BASESTRING_TYPE):
      return self._visitor.on_string(value.encode("utf-8"), None)
    elif self._classifier.is_array(value):
      return self._decode_array(value)
    elif self._classifier.is_map(value):
      return self._decode_map(value)
    elif self._classifier.is_id(value):
      return self._visitor.on_id(value.bytes)
    elif self._classifier.is_blob(value):
      return self._visitor.on_blob(value)
    elif self._classifier.is_seed(value):
      return self._decode_seed(value)
    elif self._classifier.is_struct(value):
      return self._decode_struct(value)
    else:
      return self._visitor.on_invalid_value(value)

  def _decode_array(self, array):
    ref_key = None
    if self._should_add_ref(array):
      ref_key = self._get_ref_offset()
    else:
      ref = self._get_backref(array)
      if not ref is None:
        return self._visitor.on_get_ref(ref)
    self._visitor.on_begin_array(len(array), ref_key)
    for value in self._traverse_composite(array):
      self._decode(value)

  def _decode_map(self, map):
    ref_key = None
    if self._should_add_ref(map):
      ref_key = self._get_ref_offset()
    else:
      ref = self._get_backref(map)
      if not ref is None:
        return self._visitor.on_get_ref(ref)
    self._visitor.on_begin_map(len(map), ref_key)
    for value in self._traverse_composite(map):
      self._decode(value)

  def _decode_seed(self, seed):
    ref_key = None
    if self._should_add_ref(seed):
      ref_key = self._get_ref_offset()
    else:
      ref = self._get_backref(seed)
      if not ref is None:
        return self._visitor.on_get_ref(ref)
    self._visitor.on_begin_seed(len(seed.fields), ref_key)
    self._decode(seed.header)
    for value in self._traverse_composite(seed):
      self._decode(value)

  def _decode_struct(self, struct):
    ref_key = None
    if self._should_add_ref(struct):
      ref_key = self._get_ref_offset()
    else:
      ref = self._get_backref(struct)
      if not ref is None:
        return self._visitor.on_get_ref(ref)
    self._visitor.on_begin_struct([t for (t, v) in struct.fields], ref_key)
    for value in self._traverse_composite(struct):
      self._decode(value)

  def _is_composite(self, value):
    return (self._classifier.is_array(value)
        or self._classifier.is_map(value)
        or self._classifier.is_struct(value)
        or self._classifier.is_seed(value))

  def _traverse_composite(self, value):
    """
    For a given composite value generates all the sub-values one at at time.
    Note that for the composites where values come in pairs (like key-value for
    maps) this generates them alternatingly the same way they'll appear in the
    encoded format.
    """
    if self._classifier.is_array(value):
      for elm in value:
        yield elm
    elif self._classifier.is_map(value):
      for (k, v) in value.items():
        yield k
        yield v
    elif self._classifier.is_struct(value):
      for (t, v) in value.fields:
        yield v
    else:
      assert self._classifier.is_seed(value)
      for (f, v) in value.fields.items():
        yield f
        yield v


class ObjectTreeDecoder(AbstractObjectDecoder):
  """
  An encoder that assumes that the input is strictly tree shaped (that is, there
  are no cycles or shared substructures) and throws an exception if that
  assumption turns out not to hold.
  """

  def __init__(self, visitor, classifier=None):
    super(ObjectTreeDecoder, self).__init__(visitor, classifier)
    self.ids_seen = set()

  def _preprocess(self, value):
    pass

  def _get_backref(self, value):
    """
    We never actually return backrefs but we use this call to track whether
    we've seen a value before.
    """
    value_id = id(value)
    if value_id in self.ids_seen:
      raise SharedStructureDetected()
    self.ids_seen.add(value_id)

  def _should_add_ref(self, value):
    """
    We don't encode shared structure we only detect it so we never add refs.
    """
    return False


class ObjectGraphDecoder(AbstractObjectDecoder):
  """
  A decoder that makes no assumptions about the shape of an object graph, it
  keeps track of shared subexpressions and inserts references appropriately to
  represent them in the output.
  """

  def __init__(self, visitor, classifier=None):
    super(ObjectGraphDecoder, self).__init__(visitor, classifier)
    self.has_seen_once = set()
    self.has_seen_twice = set()
    self.ref_offsets = {}
    self._ref_count = 0

  def _preprocess(self, value):
    self._look_for_shared_structure(value)

  def _look_for_shared_structure(self, value):
    if self._is_composite(value):
      if self._check_if_already_seen(value):
        return
      for elm in self._traverse_composite(value):
        self._look_for_shared_structure(elm)

  def _check_if_already_seen(self, value):
    """
    Checks whether we've previously seen the given value. Returns true iff that
    is the case.
    """
    value_id = id(value)
    if value_id in self.has_seen_once:
      self.has_seen_twice.add(value_id)
      return True
    else:
      self.has_seen_once.add(value_id)
      return False

  def _get_backref(self, value):
    offset = self.ref_offsets.get(id(value))
    if offset is None:
      return None
    else:
      return offset

  def _should_add_ref(self, value):
    value_id = id(value)
    if value_id in self.ref_offsets:
      # We've already made a reference to this value so don't make one again.
      return False
    elif value_id in self.has_seen_twice:
      # Preprocessing says we'll want to make a reference to this and we haven't
      # seen it before so this looks like the time to make the reference.
      self.ref_offsets[value_id] = self._ref_count
      self._ref_count += 1
      return True
    else:
      return False
