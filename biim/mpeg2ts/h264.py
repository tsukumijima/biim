#!/usr/bin/env python3

import re
from typing import Iterator

from biim.mpeg2ts.pes import PES

SPLIT = re.compile('\0\0\0?\1'.encode('ascii'))

class H264PES(PES):
  def __init__(self, payload: bytes | bytearray | memoryview = b''):
    super().__init__(payload)
    PES_packet_data = self.PES_packet_data()
    self.ebsps: list[bytes] = [x for x in re.split(SPLIT, PES_packet_data) if len(x) > 0]

  def __iter__(self) -> Iterator[bytes]:
    return iter(self.ebsps)
