#!/usr/bin/env python3

import asyncio
from aiohttp import web

import argparse
import sys
import os

from pathlib import Path
from datetime import datetime, timedelta

from mpeg2ts import ts
from mpeg2ts.packetize import packetize_section, packetize_pes
from mpeg2ts.section import Section
from mpeg2ts.pat import PATSection
from mpeg2ts.pmt import PMTSection
from mpeg2ts.pes import PES
from mpeg2ts.parser import SectionParser, PESParser

from hls.m3u8 import M3U8

async def main():
  loop = asyncio.get_running_loop()
  parser = argparse.ArgumentParser(description=('biim: LL-HLS origin'))

  parser.add_argument('-i', '--input', type=argparse.FileType('rb'), nargs='?', default=sys.stdin.buffer)
  parser.add_argument('-s', '--SID', type=int, nargs='?')
  parser.add_argument('-l', '--list_size', type=int, nargs='?')
  parser.add_argument('-t', '--target_duration', type=int, nargs='?', default=1)
  parser.add_argument('-p', '--part_duration', type=float, nargs='?', default=0.1)
  parser.add_argument('--port', type=int, nargs='?', default=8080)

  args = parser.parse_args()

  m3u8 = M3U8(args.target_duration, args.part_duration, args.list_size)
  async def playlist(request):
    nonlocal m3u8
    msn = request.query['_HLS_msn'] if '_HLS_msn' in request.query else None
    part = request.query['_HLS_part'] if '_HLS_part' in request.query else None

    if msn is None and part is None:
      return web.Response(headers={'Access-Control-Allow-Origin': '*'}, text=m3u8.manifest(), content_type="application/x-mpegURL")
    else:
      msn = int(msn)
      if part is None: part = 0
      part = int(part)
      future = m3u8.future(msn, part)
      if future is None:
        return web.Response(headers={'Access-Control-Allow-Origin': '*'}, status=400, content_type="application/x-mpegURL")

      result = await future
      return web.Response(headers={'Access-Control-Allow-Origin': '*'}, text=result, content_type="application/x-mpegURL")
  async def segment(request):
    nonlocal m3u8
    msn = request.query['msn'] if 'msn' in request.query else None

    if msn is None: msn = 0
    msn = int(msn)
    future = m3u8.segment(msn)
    if future is None:
      return web.Response(headers={'Access-Control-Allow-Origin': '*'}, status=400, content_type="video/mp2t")

    body = await future
    return web.Response(headers={'Access-Control-Allow-Origin': '*'}, body=body, content_type="video/mp2t")
  async def partial(request):
    nonlocal m3u8
    msn = request.query['msn'] if 'msn' in request.query else None
    part = request.query['part'] if 'part' in request.query else None

    if msn is None: msn = 0
    msn = int(msn)
    if part is None: part = 0
    part = int(part)
    future = m3u8.partial(msn, part) 
    if future is None:
      return web.Response(headers={'Access-Control-Allow-Origin': '*'}, status=400, content_type="video/mp2t")

    body = await future
    return web.Response(headers={'Access-Control-Allow-Origin': '*'}, body=body, content_type="video/mp2t")

  # setup aiohttp
  app = web.Application()
  app.add_routes([
    web.get('/playlist.m3u8', playlist),
    web.get('/segment', segment),
    web.get('/part', partial),
  ])
  runner = web.AppRunner(app)
  await runner.setup()
  await loop.create_server(runner.server, '0.0.0.0', args.port)

  # setup reader
  PAT_Parser = SectionParser(PATSection)
  PMT_Parser = SectionParser(PMTSection)
  H264_PES_Parser = PESParser(PES)

  PMT_PID = None
  PCR_PID = None
  H264_PID = None
  FIRST_IDR_DETECTED = False

  LAST_PAT = None
  LAST_PMT = None
  PAT_CC = 0
  PMT_CC = 0
  H264_CC = 0

  PARTIAL_BEGIN_TIMESTAMP = None

  def push_PAT_PMT(PAT, PMT):
    nonlocal m3u8
    nonlocal PAT_CC, PMT_CC
    nonlocal PMT_PID
    if PAT:
      packets = packetize_section(PAT, False, False, 0x00, 0, PAT_CC)
      PAT_CC = (PAT_CC + len(packets)) & 0x0F
      for p in packets: m3u8.push(p)
    if PMT:
      packets = packetize_section(PMT, False, False, PMT_PID, 0, PMT_CC)
      PMT_CC = (PMT_CC + len(packets)) & 0x0F
      for p in packets: m3u8.push(p)

  reader = asyncio.StreamReader()
  protocol = asyncio.StreamReaderProtocol(reader)
  await loop.connect_read_pipe(lambda: protocol, args.input)

  while True:
    isEOF = False
    while True:
      try:
        sync_byte = await reader.readexactly(1)
        if sync_byte == ts.SYNC_BYTE:
          break
      except asyncio.IncompleteReadError:
        isEOF = True
    if isEOF:
      break

    packet = None
    try:
      packet = ts.SYNC_BYTE + await reader.readexactly(ts.PACKET_SIZE - 1)
    except asyncio.IncompleteReadError:
      break

    if ts.pid(packet) == 0x00:
      PAT_Parser.push(packet)
      for PAT in PAT_Parser:
        if PAT.CRC32() != 0: continue
        LAST_PAT = PAT

        for program_number, program_map_PID in PAT:
          if program_number == 0: continue

          if program_number == args.SID:
            PMT_PID = program_map_PID
          elif not PMT_PID and not args.SID:
            PMT_PID = program_map_PID

        if FIRST_IDR_DETECTED:
          packets = packetize_section(PAT, False, False, 0x00, 0, PAT_CC)
          PAT_CC = (PAT_CC + len(packets)) & 0x0F
          for p in packets: m3u8.push(p)

    elif ts.pid(packet) == PMT_PID:
      PMT_Parser.push(packet)
      for PMT in PMT_Parser:
        if PMT.CRC32() != 0: continue
        LAST_PMT = PMT

        PCR_PID = PMT.PCR_PID
        for stream_type, elementary_PID in PMT:
          if stream_type == 0x1b:
            H264_PID = elementary_PID

        if FIRST_IDR_DETECTED:
          packets = packetize_section(PMT, False, False, PMT_PID, 0, PMT_CC)
          PMT_CC = (PMT_CC + len(packets)) & 0x0F
          for p in packets: m3u8.push(p)

    elif ts.pid(packet) == H264_PID:
      H264_PES_Parser.push(packet)
      for H264 in H264_PES_Parser:
        hasIDR = False
        timestamp = H264.dts() or H264.pts()

        data = H264.PES_packet_data()
        begin = 0
        while begin < len(data):
          if begin + 2 >= len(data): break

          if data[begin + 0] != 0:
            begin += 1
            continue
          elif data[begin + 1] != 0:
            begin += 1
            continue
          elif data[begin + 2] != 1:
            begin += 1
            continue
          elif begin + 3 >= len(data):
            break

          nal_unit_type = data[begin + 3] & 0x1f
          if nal_unit_type == 5:
            hasIDR = True
            break

          begin += 4

        if hasIDR:
          if not FIRST_IDR_DETECTED:
            if LAST_PAT and LAST_PMT:
              FIRST_IDR_DETECTED = True
            if FIRST_IDR_DETECTED:
              PARTIAL_BEGIN_TIMESTAMP = timestamp
              m3u8.newSegment(PARTIAL_BEGIN_TIMESTAMP, True)
              push_PAT_PMT(LAST_PAT, LAST_PMT)
          else:
            PARTIAL_BEGIN_TIMESTAMP = timestamp
            m3u8.completeSegment(PARTIAL_BEGIN_TIMESTAMP)
            m3u8.newSegment(PARTIAL_BEGIN_TIMESTAMP, True)
            push_PAT_PMT(LAST_PAT, LAST_PMT)
        elif PARTIAL_BEGIN_TIMESTAMP is not None:
          PART_DIFF = (timestamp - PARTIAL_BEGIN_TIMESTAMP + ts.PCR_CYCLE) % ts.PCR_CYCLE
          if args.part_duration * ts.HZ < PART_DIFF:
            PARTIAL_BEGIN_TIMESTAMP = timestamp
            m3u8.completePartial(PARTIAL_BEGIN_TIMESTAMP)
            m3u8.newPartial(PARTIAL_BEGIN_TIMESTAMP)

        if FIRST_IDR_DETECTED:
          packets = packetize_pes(H264, False, False, H264_PID, 0, H264_CC)
          H264_CC = (H264_CC + len(packets)) & 0x0F
          for p in packets: m3u8.push(p)
    else:
      m3u8.push(packet)

if __name__ == '__main__':
  asyncio.run(main())
