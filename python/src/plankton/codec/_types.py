"""
Plankton-related type declarations.
"""


__all__ = ["Seed", "Struct"]


class Seed(object):

  def __init__(self, header, fields):
    self.header = header
    self.fields = fields


class Struct(object):

  def __init__(self, fields):
    self.fields = fields

  def __str__(self):
    return "#<Struct {}>".format(self.fields)
