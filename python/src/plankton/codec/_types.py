"""
Plankton-related type declarations.
"""


from abc import abstractmethod, ABCMeta


__all__ = ["Seed", "Struct", "Visitor"]


class Seed(object):

  def __init__(self, header, fields):
    self.header = header
    self.fields = fields


class Struct(object):

  def __init__(self, fields):
    self.fields = fields

  def __str__(self):
    return "#<Struct {}>".format(self.fields)


class Visitor(object):
  __metaclass__ = ABCMeta

  @abstractmethod
  def on_invalid_instruction(self, code):
    pass

  @abstractmethod
  def on_int(self, value):
    pass

  @abstractmethod
  def on_singleton(self, value):
    pass

  @abstractmethod
  def on_id(self, data):
    pass

  @abstractmethod
  def on_blob(self, data):
    pass

  @abstractmethod
  def on_string(self, data, encoding):
    pass

  @abstractmethod
  def on_begin_array(self, length, ref_key):
    pass

  @abstractmethod
  def on_begin_map(self, length, ref_key):
    pass

  @abstractmethod
  def on_begin_seed(self, field_count, ref_key):
    pass

  @abstractmethod
  def on_begin_struct(self, tags, ref_key):
    pass

  @abstractmethod
  def on_get_ref(self, offset):
    pass


class StackingBuilder(Visitor):
  """
  A builder that is convenient when you need to go from the prefix-style data
  where you just get a length but no end marker to data where the end marker is
  explicit. The builder does this by keeping track of how many values it's seen
  and notifying when it's seen the expected number which acts like the end
  marker.

  Meant for internal use.
  """
  __metaclass__ = ABCMeta

  def __init__(self):
    # The stack of values we've seen so far but haven't packed into a composite
    # value of some sort.
    self._value_stack = []
    # A stack of info about how to pack values into composites when we've
    # collected enough values.
    self._pending_ends = []
    # The final result of building.
    self._result = None

    # Schedule an end that doesn't do anything but that ensures that we don't
    # have to explicitly check for the bottom of the pending ends.
    self._schedule_end(2, 1, None, None)

    # Schedule an end that stores the result in the _result field.
    self._schedule_end(1, self._store_result, None, None)

  def _store_result(self, total_count, open_result, values, data):
    """Store the value currently on the stack in the result field."""
    [self._result] = values
    self._push(None)

  @property
  def has_result(self):
    """Has this builder completed building the object graph?"""
    return len(self._pending_ends) == 1

  @property
  def result(self):
    """If this builder has a completed value, yields it."""
    assert self.has_result
    assert [None] == self._value_stack
    return self._result

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
