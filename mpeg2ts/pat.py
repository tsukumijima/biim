#!/usr/bin/env python3

from typing import Iterator

from mpeg2ts.section import Section

class PATSection(Section):
  def __init__(self, payload: bytes | bytearray | memoryview = b''):
    super().__init__(payload)
    self.entry: list[tuple[int, int]] = [
      ((payload[offset + 0] << 8) | payload[offset + 1], ((payload[offset + 2] & 0x1F) << 8) | payload[offset + 3])
      for offset in range(Section.EXTENDED_HEADER_SIZE, 3 + self.section_length() - Section.CRC_SIZE, 4)
    ]

  def __iter__(self) -> Iterator[tuple[int, int]]:
    return iter(self.entry)
