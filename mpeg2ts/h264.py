#!/usr/bin/env python3

from mpeg2ts.pes import PES

class H264PES(PES):
  def __init__(self, payload=b''):
    super().__init__(payload)
    self.ebsps = []

    PES_packet_data = self.PES_packet_data()
    prev, begin = None, 0
    while begin < len(PES_packet_data):
      if begin + 3 < len(PES_packet_data) and int.from_bytes(PES_packet_data[begin:begin+4], byteorder='big') == 1:
        if prev is not None:
          self.ebsps.append(PES_packet_data[prev:begin])
        begin += 4
        prev = begin
      elif begin + 2 < len(PES_packet_data) and int.from_bytes(PES_packet_data[begin:begin+3], byteorder='big') == 1:
        if prev is not None:
          self.ebsps.append(PES_packet_data[prev:begin])
        begin += 3
        prev = begin
      else:
        begin += 1
    if prev is not None and prev != begin:
      self.ebsps.append(PES_packet_data[prev:])

  def __iter__(self):
    return iter(self.ebsps)
