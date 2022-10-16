#!/usr/bin/env python3

from mpeg2ts.pes import PES

class H264PES(PES):
  def __init__(self, payload=b''):
    super().__init__(payload)
    ebsps = []

    PES_packet_data = self.PES_packet_data()
    prev, begin = None, 0
    while begin < len(PES_packet_data):
      if begin + 3 < len(PES_packet_data) and int.from_bytes(PES_packet_data[begin:begin+4], byteorder='big') == 1:
        if prev is not None:
          ebsps.append(PES_packet_data[prev:begin])
        begin += 4
        prev = begin
      elif begin + 2 < len(PES_packet_data) and int.from_bytes(PES_packet_data[begin:begin+3], byteorder='big') == 1:
        if prev is not None:
          ebsps.append(PES_packet_data[prev:begin])
        begin += 3
        prev = begin
      else:
        begin += 1
    if prev is not None and prev != begin:
      ebsps.append(PES_packet_data[prev:])

    prev, begin = None, 2
    self.rbsps = []
    for ebsp in ebsps:
      rbsp = bytearray(ebsp[0:begin])
      while begin < len(ebsp):
        if begin < (len(ebsp) - 1) and ebsp[begin - 2] == 0 and ebsp[begin - 1] == 0 and ebsp[begin + 0] == 3 and ebsp[begin + 1] in [0, 1, 2, 3]:
          rbsp += ebsp[prev:begin]
          prev = begin + 1
        begin += 1
      if prev is not None and prev != begin:
        rbsp += ebsp[prev:begin]
      self.rbsps.append(bytes(rbsp))
  
  def __iter__(self):
    return iter(self.rbsps)
