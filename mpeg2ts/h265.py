#!/usr/bin/env python3

from mpeg2ts.pes import PES

import re
SPLIT = re.compile('\0\0\0?\1'.encode('ascii'))

class H265PES(PES):
  def __init__(self, payload=b''):
    super().__init__(payload)
    PES_packet_data = self.PES_packet_data()
    self.ebsps = [x for x in re.split(SPLIT, PES_packet_data) if len(x) > 0]

  def __iter__(self):
    return iter(self.ebsps)
