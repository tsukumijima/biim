#!/usr/bin/env python3

import argparse
import os
import sys

from pathlib import Path

from mpeg2ts import ts
from mpeg2ts.packetize import packetize_section, packetize_pes
from mpeg2ts.section import Section
from mpeg2ts.pat import PATSection
from mpeg2ts.pmt import PMTSection
from mpeg2ts.pes import PES
from mpeg2ts.h264 import H264PES
from mpeg2ts.h265 import H265PES
from mpeg2ts.parser import SectionParser, PESParser


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description=('seek'))

  parser.add_argument('-i', '--input', type=Path, required=True)
  parser.add_argument('-s', '--start', type=float, nargs='?', default=0)
  parser.add_argument('-n', '--SID', type=int, nargs='?')

  args = parser.parse_args()

  FIRST_DTS = None

  PAT_Parser = SectionParser(PATSection)
  PMT_Parser = SectionParser(PMTSection)

  LATEST_VIDEO_TIMESTAMP = None
  LATEST_VIDEO_MONOTONIC_TIME = None
  LATEST_VIDEO_SLEEP_DIFFERENCE = 0

  PMT_PID = None
  MPEG2_PID = None
  H264_PID = None
  H265_PID = None

  OUTPUT = False

  with open(args.input, 'rb') as reader:
    while True:
      while True:
        sync_byte = reader.read(1)
        if sync_byte == ts.SYNC_BYTE:
          break
        elif sync_byte == b'':
          exit(0)

      packet = ts.SYNC_BYTE + reader.read(ts.PACKET_SIZE - 1)
      if len(packet) != 188:
        exit(0)

      PID = ts.pid(packet)
      if PID == 0x00:
        PAT_Parser.push(packet)
        for PAT in PAT_Parser:
          if PAT.CRC32() != 0: continue

          for program_number, program_map_PID in PAT:
            if program_number == 0: continue

            if program_number == args.SID:
              PMT_PID = program_map_PID
            elif not PMT_PID and not args.SID:
              PMT_PID = program_map_PID

      elif PID == PMT_PID:
        PMT_Parser.push(packet)
        for PMT in PMT_Parser:
          if PMT.CRC32() != 0: continue
          LAST_PMT = PMT

          PCR_PID = PMT.PCR_PID
          for stream_type, elementary_PID in PMT:
            if stream_type == 0x1b:
              MPEG2_PID = elementary_PID
            elif stream_type == 0x1b:
              H264_PID = elementary_PID

      elif PID == MPEG2_PID:
        if ts.payload_unit_start_indicator(packet):
          MPEG2 = PES(ts.payload(packet))
          timestamp = MPEG2.dts() if MPEG2.has_dts() else MPEG2.pts()
          if FIRST_DTS is None: FIRST_DTS = timestamp
          DIFF = ((timestamp - FIRST_DTS + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ
          if args.start <= DIFF: OUTPUT = True

      elif PID == H264_PID:
        if ts.payload_unit_start_indicator(packet):
          H264 = PES(ts.payload(packet))
          timestamp = H264.dts() if H264.has_dts() else H264.pts()
          if FIRST_DTS is None: FIRST_DTS = timestamp
          DIFF = ((timestamp - FIRST_DTS + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ
          if args.start <= DIFF: OUTPUT = True

      if OUTPUT or PID == 0x00 or PID == PMT_PID:
        sys.stdout.buffer.write(packet)
