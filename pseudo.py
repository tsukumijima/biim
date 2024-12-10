#!/usr/bin/env python3

# Linux 環境において、事前に FFmpeg・FFprobe・QSVEncC・NVEncC・tsreadex がパスの通った場所にインストールされている前提で実装されている

from typing import cast

import asyncio
from aiohttp import web
from aiohttp_sse import sse_response

import json
import math
from itertools import accumulate

from biim.mpeg2ts import ts
from biim.mpeg2ts.packetize import packetize_section, packetize_pes
from biim.mpeg2ts.parser import SectionParser, PESParser
from biim.mpeg2ts.pat import PATSection
from biim.mpeg2ts.pmt import PMTSection
from biim.mpeg2ts.pes import PES

import argparse
import os
from datetime import datetime
from pathlib import Path

from pseudo_quality import getEncoderCommand

async def keyframe_info(input: Path, targetduration: float) -> list[tuple[int, float]]:
  options = ['-i', f'{input}', '-select_streams', 'v:0', '-show_packets', '-show_entries', 'packet=pts,dts,flags,pos', '-of', 'json']
  prober = await asyncio.subprocess.create_subprocess_exec('ffprobe', *options, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
  raw_frames = [('K' in data['flags'], int(data['pos']), int(data['dts'])) for data in json.loads((await cast(asyncio.StreamReader, prober.stdout).read()).decode('utf-8'))['packets']]
  filtered_frames = [(pos, dts) for (key, pos, dts) in raw_frames if key] + ([(raw_frames[-1][1], raw_frames[-1][2])] if raw_frames[-1][0] else [])
  segments = [(pos, (end - begin) / ts.HZ) for (pos, begin), (_, end) in zip(filtered_frames[0:], filtered_frames[1:])]
  merged = [segments[0]]
  for segment in segments[1:]:
    if merged[-1][1] >= targetduration:
      merged.append((segment))
    else:
      merged[-1] = (merged[-1][0], merged[-1][1] + segment[1])
  return merged

async def main():
  loop = asyncio.get_running_loop()
  parser = argparse.ArgumentParser(description=('biim: HLS Pseudo VOD In-Memroy Origin'))

  parser.add_argument('-i', '--input', type=Path, required=True)
  parser.add_argument('-t', '--targetduration', type=float, nargs='?', default=2.5)
  parser.add_argument('-p', '--port', type=int, nargs='?', default=8080)
  parser.add_argument('-e', '--encoder', type=str, nargs='?', default='FFmpeg')
  parser.add_argument('-q', '--quality', type=str, nargs='?', default='1080p')

  args = parser.parse_args()
  input_path: Path = args.input

  # setup pseudo playlist/segment
  print('calculating keyframe info...')
  segments = await keyframe_info(input_path, args.targetduration)
  offsets = [0] + list(accumulate(duration for _, duration in segments))
  print('calculating keyframe info... done')
  num_of_segments = len(segments)
  target_duration = math.ceil(max(duration for _, duration in segments))
  virtual_cache: str = f'init-{datetime.now().strftime("%Y%m%d%H%M%S")}'
  virtual_segments: list[asyncio.Future[bytes | bytearray | memoryview | None]] = []
  processing: list[bool] = []
  process_queue: asyncio.Queue[int] = asyncio.Queue()
  buffer_index: tuple[int, int] = (0, 0)
  buffer_notify: asyncio.Future[None] = asyncio.Future()

  async def index(request):
    return web.FileResponse('pseudo.html')

  async def m3u8(cache: str, s: int):
    virutal_playlist_header = ''
    virutal_playlist_header += f'#EXTM3U\n'
    virutal_playlist_header += f'#EXT-X-VERSION:6\n'
    virutal_playlist_header += f'#EXT-X-TARGETDURATION:{target_duration}\n'
    virutal_playlist_header += f'#EXT-X-PLAYLIST-TYPE:VOD\n'
    virtual_playlist_body = ''
    for seq, (_, duration) in enumerate(segments):
      virtual_playlist_body += f"#EXTINF:{duration:.06f}\n"
      virtual_playlist_body += f"segment?seq={seq}&_={cache}\n"
      virtual_playlist_body += "\n"
    virtual_playlist_tail = '#EXT-X-ENDLIST\n'
    return virutal_playlist_header + virtual_playlist_body + virtual_playlist_tail

  async def playlist(request: web.Request) -> web.Response:
    nonlocal virtual_cache
    version = request.query['_'] if '_' in request.query else f'init-{datetime.now().strftime("%Y%m%d%H%M%S")}'
    t = float(request.query['t']) if 't' in request.query else 0
    seq = 0
    for segment in segments:
      if t < segment[1]: break
      t -= segment[1]
      seq += 1

    if not virtual_segments[seq].done() and not processing[seq]:
      virtual_cache = version

    result = await asyncio.shield(m3u8(virtual_cache, seq))
    return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, text=result, content_type="application/x-mpegURL")

  async def segment(request: web.Request) -> web.Response:
    nonlocal buffer_index
    seq = request.query['seq'] if 'seq' in request.query else None

    if seq is None:
      return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=400, content_type="video/mp2t")

    seq = int(seq)
    if seq < 0 or seq >= len(virtual_segments):
      return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=400, content_type="video/mp2t")

    if not virtual_segments[seq].done() and not processing[seq]:
      await process_queue.put(seq)
      await process_queue.join()

    body = await asyncio.shield(virtual_segments[seq])

    if body is None:
      return await playlist(request)

    for prev in range(buffer_index[0], seq - 10):
      virtual_segments[prev] = asyncio.Future()
      buffer_index = (prev + 1, buffer_index[1])
      if not buffer_notify.done(): buffer_notify.set_result(None)

    return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=3600'}, body=body, content_type="video/mp2t")

  async def buffer(request: web.Request) -> web.StreamResponse:
    nonlocal buffer_notify
    async with sse_response(request) as resp:
      while resp.is_connected():
        time_dict = {"begin": offsets[buffer_index[0]], "end": offsets[buffer_index[1]]}
        data = json.dumps(time_dict, indent=2)
        await resp.send(data)
        buffer_notify = asyncio.Future()
        try:
          await asyncio.wait_for(asyncio.shield(buffer_notify), timeout=1.0)
        except asyncio.TimeoutError:
          pass
    return resp

  # setup aiohttp
  app = web.Application()
  app.add_routes([
    web.get('/', index),
    web.get('/playlist.m3u8', playlist),
    web.get('/segment', segment),
    web.get('/buffer', buffer)
  ])
  runner = web.AppRunner(app)
  await runner.setup()
  await loop.create_server(cast(web.Server, runner.server), '0.0.0.0', args.port)

  virtual_segments = [asyncio.Future[bytes | bytearray | memoryview | None]() for _ in range(num_of_segments)]
  processing = [False for _ in range(num_of_segments)]
  encoder: asyncio.subprocess.Process | None = None

  await process_queue.put(0)
  while True:
    seq = await process_queue.get()
    for idx in range(len(processing)): processing[idx] = False
    processing[seq] = True
    for future in virtual_segments:
      if not future.done(): future.set_result(None)
    virtual_segments = [asyncio.Future[bytes | bytearray | memoryview | None]() for _ in range(num_of_segments)]
    pos, _ = segments[seq]
    offset = sum((duration for _, duration in segments[:seq]), 0)
    buffer_index = (seq, seq)
    if not buffer_notify.done(): buffer_notify.set_result(None)
    process_queue.task_done()

    encoder_command = getEncoderCommand(args.encoder, args.quality, int(offset))
    print(encoder_command)
    if encoder:
      if file:
        file.seek(0, os.SEEK_END)
        file.close()
      await encoder.communicate()
      await encoder.wait()
    file = open(input_path, "rb")
    file.seek(pos)
    encoder = await asyncio.subprocess.create_subprocess_shell(" ".join(encoder_command), stdin=file, stdout=asyncio.subprocess.PIPE)
    reader = cast(asyncio.StreamReader, encoder.stdout)

    PAT_Parser: SectionParser[PATSection] = SectionParser(PATSection)
    PMT_Parser: SectionParser[PMTSection] = SectionParser(PMTSection)
    Video_Praser: PESParser[PES] = PESParser(PES)
    Audio_Praser: PESParser[PES] = PESParser(PES)
    LATEST_PAT: PATSection | None = None
    LATEST_PMT: PMTSection | None = None
    PAT_CC: int = 0
    PMT_PID: int | None = None
    PMT_CC: int = 0
    VIDEO_PID: int | None = None
    VIDEO_CC: int = 0
    AUDIO_PID: int | None = None
    AUDIO_CC: int = 0
    candidate = bytearray()

    while process_queue.empty():
      if seq >= len(segments): break

      isEOF = False
      while True:
        try:
          sync_byte = await reader.readexactly(1)
          if sync_byte == ts.SYNC_BYTE:
            break
          elif sync_byte == b'':
            isEOF = True
            break
        except asyncio.IncompleteReadError:
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
          LATEST_PAT = PAT

          for program_number, program_map_PID in PAT:
            if program_number == 0: continue
            PMT_PID = program_map_PID

          for packet in packetize_section(PAT, False, False, 0, 0, PAT_CC):
            candidate += packet
            PAT_CC = (PAT_CC + 1) & 0x0F

      elif PID == PMT_PID:
        PMT_Parser.push(packet)
        for PMT in PMT_Parser:
          if PMT.CRC32() != 0: continue
          LATEST_PMT = PMT

          for stream_type, elementary_PID, _ in PMT:
            if stream_type == 0x1b: # H.264
              VIDEO_PID = elementary_PID
            elif stream_type == 0x24: # H.265
              VIDEO_PID = elementary_PID
            elif stream_type == 0x0F: # AAC
              AUDIO_PID = elementary_PID

          for packet in packetize_section(PMT, False, False, cast(int, PMT_PID), 0, PMT_CC):
            candidate += packet
            PMT_CC = (PMT_CC + 1) & 0x0F

      elif PID == VIDEO_PID:
        Video_Praser.push(packet)
        for VIDEO in Video_Praser:
          timestamp = cast(int, VIDEO.dts() or VIDEO.pts()) / ts.HZ

          if timestamp >= offset + segments[seq][1]:
            virtual_segments[seq].set_result(candidate)
            processing[seq] = False
            buffer_index = (buffer_index[0], seq + 1)
            if not buffer_notify.done(): buffer_notify.set_result(None)
            offset += segments[seq][1]
            seq += 1
            candidate = bytearray()
            if seq >= len(segments):
              break
            processing[seq] = True

            for packet in packetize_section(cast(PATSection, LATEST_PAT), False, False, 0, 0, PAT_CC):
              candidate += packet
              PAT_CC = (PAT_CC + 1) & 0x0F
            for packet in packetize_section(cast(PMTSection, LATEST_PMT), False, False, cast(int, PMT_PID), 0, PMT_CC):
              candidate += packet
              PMT_CC = (PMT_CC + 1) & 0x0F

          for packet in packetize_pes(VIDEO, False, False, cast(int, VIDEO_PID), 0, VIDEO_CC):
            candidate += packet
            VIDEO_CC = (VIDEO_CC + 1) & 0x0F

      elif PID == AUDIO_PID:
        Audio_Praser.push(packet)
        for AUDIO in Audio_Praser:
          for packet in packetize_pes(AUDIO, False, False, cast(int, AUDIO_PID), 0, AUDIO_CC):
            candidate += packet
            AUDIO_CC = (AUDIO_CC + 1) & 0x0F

      else:
        candidate += packet

if __name__ == '__main__':
  asyncio.run(main())
