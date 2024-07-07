import io
import struct
from typing import cast, Any

objectEnd = object()

def parse(reader: io.BytesIO):
  while reader:
    tag_byte = reader.read(1)
    if tag_byte == b'':
      reader.close()
      return
    match int.from_bytes(tag_byte, byteorder='big'):
      case 0: # Number (8 bytes)
        return cast(float, struct.unpack('>d', reader.read(8))[0])
      case 1: # Boolean (1 bytes)
        return reader.read(1) != b'\x00'
      case 2: # String
        length = int.from_bytes(reader.read(2), byteorder='big')
        return reader.read(length).decode('utf-8')
      case 3: # Object
        result = dict()
        while True:
          length = int.from_bytes(reader.read(2), byteorder='big')
          name = reader.read(length).decode('utf-8')
          value = parse(reader)
          if value is objectEnd:
            return result
          result[name] = value
      case 4: # movie clip (reserved, not supported)
        reader.close()
        return None
      case 5: # null
        return None
      case 6: # undefined
        return None # for python convenience, undefined to None (same as null) conversion
      case 7: # reference
        #FIXME: I didn't see this tag
        reader.close()
        return None
      case 8: # ECMA Array
        result = dict()
        for _ in range(int.from_bytes(reader.read(4), byteorder='big') + 1): # +1: ObjectEnd used in librtmp for terminate ECMA Array
          length = int.from_bytes(reader.read(2), byteorder='big')
          name = reader.read(length).decode('utf-8')
          value = parse(reader)
          if value is objectEnd:
            return result
          result[name] = value
        return result
      case 9: # Object End
        return objectEnd
      case 10: # Strict Array
        length = int.from_bytes(reader.read(4), byteorder='big')
        return [parse(reader) for _ in range(length)]
      case 11: # Date
        timestamp = cast(float, struct.unpack('>d', reader.read(8)))
        timezone = int.from_bytes(reader.read(2), byteorder='big', signed=True) # should be set zero
        return timestamp + timezone
      case 12: # Long String
        length = int.from_bytes(reader.read(4), byteorder='big')
        return reader.read(length).decode('utf-8')
      case 13: # Unsupported
        #FIXME: I didn't see this tag
        reader.close()
        return None
      case 14: # Recordset (reserved, not supported)
        reader.close()
        return None
      case 15: # Xml Document
        #FIXME: I didn't see this tag
        reader.close()
        return None
      case 16: # Typed Object
        #FIXME: I didn't see this tag
        reader.close()
        return None

  reader.close()
  return None

def deserialize(value: bytes | bytearray | memoryview):
  with io.BytesIO(value) as reader:
    result = []
    while reader:
      parsed = parse(reader)
      if reader.closed:
        return result
      result.append(parsed)
    return []

def serialize(values: list[Any] | Any):
  amf = bytearray()
  for value in values if type(values) is list else [values]:
    if value is None:
      amf += b'\x05' # null used, undefined are not serialized
    elif type(value) == bytes or type(value) == bytearray or type(value) == memoryview:
      amf += value  # already byteslike, so insert
    elif type(value) == int or type(value) == float:
      amf += b'\x00'
      amf += struct.pack('>d', float(value))
    elif type(value) == bool:
      amf += b'\x01'
      amf += b'\x01' if value else b'\x00'
    elif type(value) == str: # LongString or String
      if len(value) >= 0xFFFF: # Long String
        amf += b'\x0C'
        amf += len(value).to_bytes(4, byteorder='big')
        amf += value.encode('utf-8')
      else:
        amf += b'\x02' # String
        amf += len(value).to_bytes(2, byteorder='big')
        amf += value.encode('utf-8')
    elif type(value) == list: # Strict Array used
      amf += b'\x0A'
      amf += int.to_bytes(len(value), 4, byteorder='big')
      for _ in range(len(value)): amf += serialize([value])
    elif type(value) == dict: # Object used
      amf += b'\x03'
      for k, v in value.items():
        amf += len(k).to_bytes(2, byteorder='big')
        amf += k.encode('utf-8')
        amf += serialize([v])
      amf += b'\x00\x00\x09' # length = 0, name='', value=ObjectEnd

  return amf

