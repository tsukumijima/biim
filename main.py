#!/usr/bin/env python3

from typing import cast

import asyncio
from aiohttp import web

import argparse
import sys
import os
import time

from biim.mpeg2ts import ts
from biim.mpeg2ts.pat import PATSection
from biim.mpeg2ts.pmt import PMTSection
from biim.mpeg2ts.scte import SpliceInfoSection
from biim.mpeg2ts.pes import PES
from biim.mpeg2ts.h264 import H264PES
from biim.mpeg2ts.parser import SectionParser, PESParser

from biim.variant.mpegts import MpegtsVariantHandler

from biim.util.reader import BufferingAsyncReader

async def main():
  loop = asyncio.get_running_loop()
  parser = argparse.ArgumentParser(description=('biim: LL-HLS origin'))

  parser.add_argument('-i', '--input', type=argparse.FileType('rb'), nargs='?', default=sys.stdin.buffer)
  parser.add_argument('-s', '--SID', type=int, nargs='?')
  parser.add_argument('-w', '--window_size', type=int, nargs='?')
  parser.add_argument('-t', '--target_duration', type=int, nargs='?', default=1)
  parser.add_argument('-p', '--part_duration', type=float, nargs='?', default=0.1)
  parser.add_argument('--port', type=int, nargs='?', default=8080)

  args = parser.parse_args()

  handler = MpegtsVariantHandler(
    target_duration=args.target_duration,
    part_target=args.part_duration,
    window_size=args.window_size,
    has_video=True,
    has_audio=True,
  )

  # setup aiohttp
  app = web.Application()
  app.add_routes([
    web.get('/playlist.m3u8', handler.playlist),
    web.get('/segment', handler.segment),
    web.get('/part', handler.partial),
  ])
  runner = web.AppRunner(app)
  await runner.setup()
  await loop.create_server(cast(web.Server, runner.server), '0.0.0.0', args.port)

  # setup reader
  PAT_Parser: SectionParser[PATSection] = SectionParser(PATSection)
  PMT_Parser: SectionParser[PMTSection] = SectionParser(PMTSection)
  SCTE35_Parser: SectionParser[SpliceInfoSection] = SectionParser(SpliceInfoSection)
  H264_PES_Parser: PESParser[H264PES] = PESParser(H264PES)
  AAC_PES_Parser: PESParser[PES] = PESParser(PES)

  LATEST_VIDEO_TIMESTAMP: int | None = None
  LATEST_VIDEO_MONOTONIC_TIME: float | None = None
  LATEST_VIDEO_SLEEP_DIFFERENCE: float = 0

  PMT_PID: int | None = None
  H264_PID: int | None = None
  AAC_PID: int | None = None
  SCTE35_PID: int | None = None
  PCR_PID: int | None = None

  if args.input is not sys.stdin.buffer or os.name == 'nt':
    reader = BufferingAsyncReader(args.input, ts.PACKET_SIZE * 16)
  else:
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, args.input)

  while True:
    isEOF = False
    while True:
      sync_byte = await reader.read(1)
      if sync_byte == ts.SYNC_BYTE:
        break
      elif sync_byte == b'':
        isEOF = True
        break
    if isEOF:
      break

    packet = None
    try:
      packet = ts.SYNC_BYTE + await reader.readexactly(ts.PACKET_SIZE - 1)
    except asyncio.IncompleteReadError:
      break

    PID = ts.pid(packet)
    if PID == 0x00:
      PAT_Parser.push(packet)
      for PAT in PAT_Parser:
        if PAT.CRC32() != 0: continue
        handler.PAT(PAT)

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
        handler.PMT(PID, PMT)

        PCR_PID = PMT.PCR_PID
        for stream_type, elementary_PID, _ in PMT:
          if stream_type == 0x1b:
            H264_PID = elementary_PID
          elif stream_type == 0x86:
            SCTE35_PID = elementary_PID

    elif PID == H264_PID:
      H264_PES_Parser.push(packet)
      for H264 in H264_PES_Parser:
        handler.h264(PID, H264)

        if (timestamp := H264.dts() or H264.pts()) is None: continue
        if LATEST_VIDEO_TIMESTAMP is not None and LATEST_VIDEO_MONOTONIC_TIME is not None:
          TIMESTAMP_DIFF = ((timestamp - LATEST_VIDEO_TIMESTAMP + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ
          TIME_DIFF = time.monotonic() - LATEST_VIDEO_MONOTONIC_TIME
          if args.input is not sys.stdin.buffer:
            SLEEP_BEGIN = time.monotonic()
            await asyncio.sleep(max(0, TIMESTAMP_DIFF - (TIME_DIFF + LATEST_VIDEO_SLEEP_DIFFERENCE)))
            SLEEP_END = time.monotonic()
            LATEST_VIDEO_SLEEP_DIFFERENCE = (SLEEP_END - SLEEP_BEGIN) - max(0, TIMESTAMP_DIFF - (TIME_DIFF + LATEST_VIDEO_SLEEP_DIFFERENCE))
        LATEST_VIDEO_TIMESTAMP = timestamp
        LATEST_VIDEO_MONOTONIC_TIME = time.monotonic()

    elif PID == AAC_PID:
      AAC_PES_Parser.push(packet)
      for AAC in AAC_PES_Parser:
        handler.aac(PID, AAC)

    elif PID == SCTE35_PID:
      handler.packet(packet)
      SCTE35_Parser.push(packet)
      for SCTE35 in SCTE35_Parser:
        if SCTE35.CRC32() != 0: continue
        handler.scte35(SCTE35)

    else:
      handler.packet(packet)

    if PID == PCR_PID and ts.has_pcr(packet):
      handler.pcr(cast(int, ts.pcr(packet)))

if __name__ == '__main__':
  asyncio.run(main())
