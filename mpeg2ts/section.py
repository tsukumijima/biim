#!/usr/bin/env python3

class Section:
  BASIC_HEADER_SIZE = 3
  EXTENDED_HEADER_SIZE = 8
  CRC_SIZE = 4

  def __init__(self, payload = b''):
    self.payload = memoryview(payload)

  def __getitem__(self, item):
    return self.payload[item]

  def __len__(self):
    return len(self.payload)

  def table_id(self):
    return self.payload[0]

  def section_length(self):
    return ((self.payload[1] & 0x0F) << 8) | self.payload[2]

  def table_id_extension(self):
    return (self.payload[3] << 8) | self.payload[4]

  def version_number(self):
    return (self.payload[5] & 0x3E) >> 1

  def current_next_indicator(self):
    return (self.payload[5] & 0x01) != 0

  def section_number(self):
    return self.payload[6]

  def last_section_number(self):
    return self.payload[7]

  def CRC32(self):
    crc = 0xFFFFFFFF
    for byte in self.payload:
      for index in range(7, -1, -1):
        bit = (byte & (1 << index)) >> index
        c = 1 if crc & 0x80000000 else 0
        crc <<= 1
        if c ^ bit: crc ^= 0x04c11db7
        crc &= 0xFFFFFFFF
    return crc

