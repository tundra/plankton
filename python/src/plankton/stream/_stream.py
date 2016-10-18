

class StreamWriter(object):

  def __init__(self, encoder):
    self._encoder = encoder

  def define_schema(self, schema):
    self._encoder.on_define_schema(schema.id)
    schema.write(self._encoder)
