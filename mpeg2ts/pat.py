#!/usr/bin/env python3

from mpeg2ts.section import Section

class PATSection(Section):
  def __init__(self, payload = b''):
    super().__init__(payload)
    self.entry = [
      ((payload[offset + 0] << 8) | payload[offset + 1], ((payload[offset + 2] & 0x1F) << 8) | payload[offset + 3])
      for offset in range(Section.EXTENDED_HEADER_SIZE, 3 + self.section_length() - Section.CRC_SIZE, 4)
    ]
  
  def __iter__(self):
    return iter(self.entry)
