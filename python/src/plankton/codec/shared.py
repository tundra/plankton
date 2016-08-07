__all__ = ["Seed", "Struct"]


class Codec(object):

  INT_0_TAG = 0x00
  INT_1_TAG = 0x01
  INT_2_TAG = 0x02
  INT_P_TAG = 0x08
  INT_M_TAG = 0x09
  INT_M1_TAG = 0x0f

  SINGLETON_NULL_TAG = 0x10
  SINGLETON_TRUE_TAG = 0x11
  SINGLETON_FALSE_TAG = 0x12

  ID_16_TAG = 0x14
  ID_32_TAG = 0x15
  ID_64_TAG = 0x16
  ID_128_TAG = 0x17

  ARRAY_0_TAG = 0x20
  ARRAY_1_TAG = 0x21
  ARRAY_2_TAG = 0x22
  ARRAY_3_TAG = 0x23
  ARRAY_N_TAG = 0x28

  MAP_0_TAG = 0x30
  MAP_1_TAG = 0x31
  MAP_2_TAG = 0x32
  MAP_3_TAG = 0x33
  MAP_N_TAG = 0x38

  BLOB_N_TAG = 0x48

  DEFAULT_STRING_0_TAG = 0x50
  DEFAULT_STRING_1_TAG = 0x51
  DEFAULT_STRING_2_TAG = 0x52
  DEFAULT_STRING_3_TAG = 0x53
  DEFAULT_STRING_4_TAG = 0x54
  DEFAULT_STRING_5_TAG = 0x55
  DEFAULT_STRING_6_TAG = 0x56
  DEFAULT_STRING_7_TAG = 0x57
  DEFAULT_STRING_N_TAG = 0x58

  SEED_0_TAG = 0x60
  SEED_1_TAG = 0x61
  SEED_2_TAG = 0x62
  SEED_3_TAG = 0x63
  SEED_N_TAG = 0x68

  ADD_REF_TAG = 0xa0
  GET_REF_TAG = 0xa1

  STRUCT_LINEAR_0_TAG = 0x80
  STRUCT_LINEAR_1_TAG = 0x81
  STRUCT_LINEAR_2_TAG = 0x82
  STRUCT_LINEAR_3_TAG = 0x83
  STRUCT_LINEAR_4_TAG = 0x84
  STRUCT_LINEAR_5_TAG = 0x85
  STRUCT_LINEAR_6_TAG = 0x86
  STRUCT_LINEAR_7_TAG = 0x87
  STRUCT_N_TAG = 0x88


class Seed(object):

  def __init__(self, header, fields):
    self.header = header
    self.fields = fields


class Struct(object):

  def __init__(self, fields):
    self.fields = fields

  def __str__(self):
    return "#<Struct {}>".format(self.fields)