import hashlib

SHA_224 = 0x1d


class IdType(object):
  """Base type for the different types of ids."""

  @staticmethod
  def default():
    return Sha224


class Sha224(IdType):

  @staticmethod
  def code():
    return SHA_224

  @staticmethod
  def new_hasher():
    return hashlib.sha224()


class Sha512(IdType):

  @staticmethod
  def code():
    return SHA_512

  @staticmethod
  def new_hasher():
    return hashlib.sha512()


class Id(object):
  """A schema id."""

  def __init__(self, type, sum):
    self.type = type
    self.sum = sum
