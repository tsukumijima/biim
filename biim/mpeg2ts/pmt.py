#!/usr/bin/env python3

from typing import Iterator

from biim.mpeg2ts.section import Section

class PMTSection(Section):
  def __init__(self, payload: bytes | bytearray | memoryview = b''):
    super().__init__(payload)
    self.PCR_PID: int = ((self.payload[Section.EXTENDED_HEADER_SIZE + 0] & 0x1F) << 8) | self.payload[Section.EXTENDED_HEADER_SIZE + 1]
    self.entry: list[tuple[int, int, list[tuple[int, bytes | bytearray | memoryview]]]] = []

    program_info_length: int = ((self.payload[Section.EXTENDED_HEADER_SIZE + 2] & 0x0F) << 8) | self.payload[Section.EXTENDED_HEADER_SIZE + 3]
    begin: int = Section.EXTENDED_HEADER_SIZE + 4 + program_info_length
    while begin < 3 + self.section_length() - Section.CRC_SIZE:
      stream_type = self.payload[begin + 0]
      elementary_PID = ((self.payload[begin + 1] & 0x1F) << 8) | self.payload[begin + 2]
      ES_info_length = ((self.payload[begin + 3] & 0x0F) << 8) | self.payload[begin + 4]

      descriptors: list[tuple[int, bytes | bytearray | memoryview]] = []
      offset = begin + 5
      while offset  < begin + 5 + ES_info_length:
        descriptor_tag = self.payload[offset + 0]
        descriptor_length = self.payload[offset + 1]
        descriptors.append((descriptor_tag, self.payload[offset + 2: offset + 2 + descriptor_length]))
        offset += 2 + descriptor_length

      self.entry.append((stream_type, elementary_PID, descriptors))
      begin += 5 + ES_info_length

  def __iter__(self) -> Iterator[tuple[int, int, list[tuple[int, bytes | bytearray | memoryview]]]]:
    return iter(self.entry)
