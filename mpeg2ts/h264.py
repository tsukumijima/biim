#!/usr/bin/env python3

from mpeg2ts.pes import PES

class H264PES(PES):
  def __init__(self, payload=b''):
    super().__init__(payload)
    self.ebsps = []

    PES_packet_data = self.PES_packet_data()
    length = len(PES_packet_data)
    prev, begin = None, 0
    while begin < length:
      if begin + 2 < length and PES_packet_data[begin+0] == 0 and PES_packet_data[begin+1] == 0:
        if begin + 3 < length and PES_packet_data[begin+2] == 0 and PES_packet_data[begin+3] == 1:
          if prev is not None:
            self.ebsps.append(PES_packet_data[prev:begin])
          begin += 4
          prev = begin
        elif begin + 2 < length and PES_packet_data[begin+2] == 1:
          if prev is not None:
            self.ebsps.append(PES_packet_data[prev:begin])
          begin += 3
          prev = begin
        else:
          begin += 2
      elif begin + 2 < length and PES_packet_data[begin+2] != 0:
        begin += 3
      elif begin + 1 < length and PES_packet_data[begin+1] != 0:
        begin += 2
      else:
        begin += 1
    if prev is not None and prev != begin:
      self.ebsps.append(PES_packet_data[prev:])

  def __iter__(self):
    return iter(self.ebsps)
