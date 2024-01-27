#!/usr/bin/env python3

from typing import cast, Callable, Any

import asyncio
from aiohttp import web

import argparse
import sys
import os

from biim.mpeg2ts import ts
from biim.mpeg2ts.pat import PATSection
from biim.mpeg2ts.pmt import PMTSection
from biim.mpeg2ts.scte import SpliceInfoSection
from biim.mpeg2ts.pes import PES
from biim.mpeg2ts.h264 import H264PES
from biim.mpeg2ts.h265 import H265PES
from biim.mpeg2ts.parser import SectionParser, PESParser

from biim.variant.fmp4 import Fmp4VariantHandler

from biim.util.reader import BufferingAsyncReader

async def setup(port: int, prefix: str = '', all_handlers: list[tuple[int, Fmp4VariantHandler]] = [], all_video_handlers: list[tuple[int, Fmp4VariantHandler]] = [], all_audio_handler: list[tuple[int, Fmp4VariantHandler]] = []):
  # setup aiohttp
  loop = asyncio.get_running_loop()
  app = web.Application()

  async def master(request: web.Request):
    m3u8 = '#EXTM3U\n#EXT-X-VERSION:3\n\n'

    has_audio = bool(all_audio_handler)
    for index, (pid, handler) in enumerate(all_audio_handler):
      name = ['Primary', 'Secondary'][index] if index < 2 else f'Other-{index}'
      m3u8 += f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="{name}",DEFAULT={"YES" if index == 0 else "NO"},AUTOSELECT=YES,LANGUAGE="JPN",URI="./{pid}/playlist.m3u8"\n'
    if has_audio: m3u8 += '\n'

    for pid, handler in all_video_handlers:
      m3u8 += f'#EXT-X-STREAM-INF:BANDWIDTH={await handler.bandwidth()},CODECS="{await handler.codec()}"' + (',AUDIO="audio"' if has_audio else '') +'\n'
      m3u8 += f'./{pid}/playlist.m3u8\n'

    return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=36000'}, text=m3u8, content_type="application/x-mpegURL")

  app.add_routes(
    sum(
      [
        [web.get(f'{prefix}/master.m3u8', master)]
      ] + [
        [
          web.get(f'{prefix}/{pid}/playlist.m3u8', handler.playlist),
          web.get(f'{prefix}/{pid}/segment', handler.segment),
          web.get(f'{prefix}/{pid}/part', handler.partial),
          web.get(f'{prefix}/{pid}/init', handler.initialization),
        ] for pid, handler in all_handlers
      ],
      []
    )
  )
  runner = web.AppRunner(app)
  await runner.setup()
  await loop.create_server(cast(web.Server, runner.server), '0.0.0.0', port)

async def main():
  parser = argparse.ArgumentParser(description=('biim: LL-HLS origin'))

  parser.add_argument('-i', '--input', type=argparse.FileType('rb'), nargs='?', default=sys.stdin.buffer)
  parser.add_argument('-s', '--SID', type=int, nargs='?')
  parser.add_argument('-w', '--window_size', type=int, nargs='?')
  parser.add_argument('-t', '--target_duration', type=int, nargs='?', default=1)
  parser.add_argument('-p', '--part_duration', type=float, nargs='?', default=0.1)
  parser.add_argument('--port', type=int, nargs='?', default=8080)

  args = parser.parse_args()
  loop = asyncio.get_running_loop()

  PMT_VERSION = None
  PMT_PID: int | None = None
  PCR_PID: int | None = None

  PAT_Parser: SectionParser[PATSection] = SectionParser(PATSection)
  PMT_Parser: SectionParser[PMTSection] = SectionParser(PMTSection)

  cb: dict[int, tuple[PESParser | SectionParser, Callable[[Any], Any]]] = dict()

  ALL_HANDLER: list[tuple[int, Fmp4VariantHandler]] = []
  ALL_VIDEO_HANDLER: list[tuple[int, Fmp4VariantHandler]] = []
  ALL_AUDIO_HANDLER: list[tuple[int, Fmp4VariantHandler]] = []
  def ID3_CALLBACK(ID3: PES):
    for _, handler in ALL_VIDEO_HANDLER: handler.id3(ID3)
  def SCTE35_CALLBACK(SCTE35: SpliceInfoSection):
    if SCTE35.CRC32() != 0: return
    for _, handler in ALL_HANDLER: handler.scte35(SCTE35)

  # setup reader
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
        if PMT.version_number() == PMT_VERSION: continue
        PMT_VERSION = PMT.version_number()

        cb = dict()
        ALL_HANDLER = []
        ALL_VIDEO_HANDLER = []
        ALL_AUDIO_HANDLER = []

        PCR_PID = PMT.PCR_PID
        for stream_type, elementary_PID, _ in PMT:
          if stream_type == 0x1b:
            handler = Fmp4VariantHandler(target_duration=args.target_duration, part_target=args.part_duration, window_size=args.window_size, has_video=True, has_audio=False)
            ALL_HANDLER.append((elementary_PID, handler))
            ALL_VIDEO_HANDLER.append((elementary_PID, handler))
            cb[elementary_PID] = (PESParser[H264PES](H264PES), handler.h264)
          elif stream_type == 0x24:
            handler = Fmp4VariantHandler(target_duration=args.target_duration, part_target=args.part_duration, window_size=args.window_size, has_video=True, has_audio=False)
            ALL_HANDLER.append((elementary_PID, handler))
            ALL_VIDEO_HANDLER.append((elementary_PID, handler))
            cb[elementary_PID] = (PESParser[H265PES](H265PES), handler.h265)
          elif stream_type == 0x0F:
            handler = Fmp4VariantHandler(target_duration=args.target_duration, part_target=args.part_duration, window_size=args.window_size, has_video=False, has_audio=True)
            ALL_HANDLER.append((elementary_PID, handler))
            ALL_AUDIO_HANDLER.append((elementary_PID, handler))
            cb[elementary_PID] = (PESParser[PES](PES), handler.aac)
          elif stream_type == 0x15:
            cb[elementary_PID] = (PESParser[PES](PES), ID3_CALLBACK)
          elif stream_type == 0x86:
            cb[elementary_PID] = (SectionParser[SpliceInfoSection](SpliceInfoSection), SCTE35_CALLBACK)

        for _, handler in ALL_VIDEO_HANDLER:
          handler.set_renditions([f'../{pid}/playlist.m3u8' for pid, r in ALL_VIDEO_HANDLER if r != handler])
        for _, handler in ALL_AUDIO_HANDLER:
          handler.set_renditions([f'../{pid}/playlist.m3u8' for pid, r in ALL_AUDIO_HANDLER if r != handler])
        await setup(args.port, '', ALL_HANDLER, ALL_VIDEO_HANDLER, ALL_AUDIO_HANDLER)

    elif PID in cb:
      cb[PID][0].push(packet)
      for data in cb[PID][0]: cb[PID][1](data)

    else:
      pass

    if PID == PCR_PID and ts.has_pcr(packet):
      for _, handler in ALL_HANDLER: handler.pcr(cast(int, ts.pcr(packet)))

if __name__ == '__main__':
  asyncio.run(main())
