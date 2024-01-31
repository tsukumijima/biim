#!/usr/bin/env python3

from typing import cast

import asyncio
from aiohttp import web

import argparse

import math
from pathlib import Path
import math

from biim.mp4.mp4a import mp4aTrack
from biim.mp4.box import ftyp, moov, mvhd, mvex, trex, moof, mdat
from biim.variant.codec import aac_codec_parameter_string
from biim.variant.fmp4 import Fmp4VariantHandler

async def estimate_duration(input: Path) -> float:
  options = ['-i', f'{input}', '-show_entries', 'format=duration']
  probe = await asyncio.subprocess.create_subprocess_exec('ffprobe', *options, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
  duration = float((await cast(asyncio.StreamReader, probe.stdout).read()).decode('utf-8').split('\n')[1].split('=')[1])
  return duration

async def audio(input: Path, ffmpeg: Path, target_duration: int, selector = '0:a:0'):
  sampling_frequency_table = {
    0x00: 96000,
    0x01: 88200,
    0x02: 64000,
    0x03: 48000,
    0x04: 44100,
    0x05: 32000,
    0x06: 24000,
    0x07: 22050,
    0x08: 16000,
    0x09: 12000,
    0x0a: 11025,
    0x0b: 8000,
    0x0c: 7350,
  }
  total_duration = await estimate_duration(input)

  options = [
    '-i', input, '-vn', '-map', selector,
    '-c:a', 'aac', '-ac', '2',
    '-f', 'adts', '-'
  ]
  exec = await asyncio.subprocess.create_subprocess_exec(ffmpeg, *options, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
  reader = cast(asyncio.StreamReader, exec.stdout)

  codec: asyncio.Future[str] = asyncio.Future[str]()
  init: asyncio.Future[bytes] = asyncio.Future[bytes]()

  timestamp: int = 0
  segment_count = 0
  segment_mp4: bytearray = bytearray()
  frame_count: int = 0
  frame_per_segment: int | None = None
  detected: asyncio.Future[bool] = asyncio.Future[bool]()
  segments: list[tuple[float, int, asyncio.Future[memoryview]]] | None = None

  async def playlist(request: web.Request) -> web.Response:
    await detected
    m3u8 = ''
    m3u8 += f'#EXTM3U\n'
    m3u8 += f'#EXT-X-VERSION:6\n'
    m3u8 += f'#EXT-X-TARGETDURATION:{target_duration}\n'
    m3u8 += f'#EXT-X-PLAYLIST-TYPE:"VOD"\n'
    m3u8 += f'#EXT-X-MEDIA-SEQUENCE:{0}\n'
    m3u8 += f'\n'
    m3u8 += f'#EXT-X-MAP:URI="init"\n'
    m3u8 += f'\n'
    for msn, (extinf, _, _) in enumerate(cast(list[tuple[float, int, asyncio.Future[memoryview]]], segments)):
      m3u8 += f'#EXTINF:{extinf:.06f}\n'
      m3u8 += f'segment?msn={msn}\n'
    m3u8 += f'\n'
    m3u8 += f'#EXT-X-ENDLIST\n'
    return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=200, body=m3u8, content_type="application/x-mpegURL")
  async def segment(request: web.Request) -> web.Response:
    await detected
    msn = request.query['msn'] if 'msn' in request.query else None

    if msn is None:
      return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=400, content_type="video/mp4")
    msn = int(msn)
    segment = await (cast(list[tuple[float, int, asyncio.Future[memoryview]]], segments)[msn][2])
    return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=200, body=segment, content_type="video/mp4")
  async def initalization(request: web.Request) -> web.Response:
    return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=36000'}, body=(await init), content_type="video/mp4")

  async def process():
    nonlocal timestamp
    nonlocal segment_mp4, segment_count, frame_count, frame_per_segment, segments

    try:
      while True:
        header = bytearray(await reader.readexactly(7))
        protection = (header[1] & 0b00000001) == 0
        profile = ((header[2] & 0b11000000) >> 6)
        samplingFrequencyIndex = ((header[2] & 0b00111100) >> 2)
        channelConfiguration = ((header[2] & 0b00000001) << 2) | ((header[3] & 0b11000000) >> 6)
        frameLength = ((header[3] & 0x03) << 11) | (header[4] << 3) | ((header[5] & 0xE0) >> 5)
        duration = 1024
        header += await reader.readexactly(2) if protection else b''
        body = await reader.readexactly(frameLength - (9 if protection else 7))

        if not codec.done(): codec.set_result(aac_codec_parameter_string(profile + 1))
        if not init.done(): init.set_result(b''.join([
          ftyp(),
          moov(
            mvhd(sampling_frequency_table[samplingFrequencyIndex]),
            mvex([
              trex(1)
            ]),
            b''.join([
              mp4aTrack(
                1,
                sampling_frequency_table[samplingFrequencyIndex],
                bytes([
                  ((profile + 1) << 3) | ((samplingFrequencyIndex & 0x0E) >> 1),
                  ((samplingFrequencyIndex & 0x01) << 7) | (channelConfiguration << 3)
                ]),
                channelConfiguration,
                sampling_frequency_table[samplingFrequencyIndex]
              )
            ])
          )
        ]))

        if frame_per_segment is None:
          frame_per_segment = (target_duration * sampling_frequency_table[samplingFrequencyIndex] + (duration - 1)) // duration
        if segments is None:
          extinf = frame_per_segment * 1024 / sampling_frequency_table[samplingFrequencyIndex]
          segments = [(min(extinf, total_duration - extinf * i), frame_per_segment, asyncio.Future[memoryview]()) for i in range(math.ceil(total_duration / extinf))]
        if not detected.done(): detected.set_result(True)

        segment_mp4 += b''.join(
          [
            moof(0,
            [
              (1, duration, timestamp, 0, [(frameLength - (9 if protection else 7), duration, False, 0)])
            ]
          ),
          mdat(bytes(body))
        ])
        frame_count += 1
        if frame_count >= segments[segment_count][1]:
          segments[segment_count][2].set_result(memoryview(segment_mp4))
          segment_count += 1
          frame_count = 0
          segment_mp4 = bytearray()

        timestamp += duration
    except asyncio.IncompleteReadError as e:
      pass

  asyncio.create_task(process())
  return codec, playlist, segment, initalization

async def video(input: Path, ffmpeg: Path, target_duration: int, selector = '0:v:0'):
  total_duration = await estimate_duration(input)
  num_of_segments = int((total_duration + target_duration - 1) // target_duration)

  segments: list[tuple[float, asyncio.Future[memoryview]]] = [(min(target_duration, total_duration - target_duration * i), asyncio.Future[memoryview]()) for i in range(num_of_segments)]
  init: asyncio.Future[bytes] = asyncio.Future[bytes]()

  processing: list[bool] = [False for _ in range(num_of_segments)]
  process_caindidate: int | None = None
  process_queue: asyncio.Queue[int] = asyncio.Queue()

  process_queue.put_nowait(0)
  process_caindidate = 1

  async def playlist(request: web.Request) -> web.Response:
    m3u8 = ''
    m3u8 += f'#EXTM3U\n'
    m3u8 += f'#EXT-X-VERSION:6\n'
    m3u8 += f'#EXT-X-TARGETDURATION:{target_duration}\n'
    m3u8 += f'#EXT-X-PLAYLIST-TYPE:"VOD"\n'
    m3u8 += f'#EXT-X-MEDIA-SEQUENCE:{0}\n'
    m3u8 += f'\n'
    m3u8 += f'#EXT-X-MAP:URI="init"\n'
    m3u8 += f'\n'
    for msn, (extinf, _) in enumerate(cast(list[tuple[float, asyncio.Future[memoryview]]], segments)):
      m3u8 += f'#EXTINF:{extinf:.06f}\n'
      m3u8 += f'segment?msn={msn}\n'
    m3u8 += f'\n'
    m3u8 += f'#EXT-X-ENDLIST\n'
    return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=200, body=m3u8, content_type="application/x-mpegURL")
  async def segment(request):
    nonlocal process_caindidate
    msn = request.query['msn'] if 'msn' in request.query else None

    if msn is None:
      return web.Response(headers={'Access-Control-Allow-Origin': '*'}, status=400, content_type="video/mp4")
    msn = int(msn)

    if msn < 0 or msn >= len(segments):
      return web.Response(headers={'Access-Control-Allow-Origin': '*'}, status=400, content_type="video/mp4")

    if not segments[msn][1].done() and not processing[msn]:
      process_queue.put_nowait(msn)

    body = await segments[msn][1]
    if msn + 1 < len(segments) and not segments[msn][1].done() and not processing[msn]:
      process_caindidate = msn + 1

    response = web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-cache,no-store'}, body=body, content_type="video/mp4")
    return response
  async def initalization(request: web.Request) -> web.Response:
    return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=36000'}, body=(await init), content_type="video/mp4")

  async def process():
    nonlocal process_caindidate, process_queue
    while True:
      msn = await process_queue.get()
      process_queue.task_done()
      if segments[msn][1].done(): continue
      processing[msn] = True

      ss = max(0, msn * target_duration)
      t =  target_duration
      options = [
        '-ss', str(max(0, ss - 10)), '-i', str(input), '-ss', str(ss - max(0, ss - 10)), '-t', str(t),
        '-an', '-map', selector,
        '-c:v', 'libx264', '-tune', 'zerolatency', '-preset', 'ultrafast',
        '-movflags', 'frag_keyframe+empty_moov+default_base_moof',
        '-f', 'mp4', '-'
      ]

      encoder = await asyncio.subprocess.create_subprocess_exec(ffmpeg, *options, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
      output = bytearray(await cast(asyncio.StreamReader, encoder.stdout).read())

      begin, end = 0, len(output)
      header = bytearray()
      body = bytearray()
      timescale = 1
      while begin < end:
        size = int.from_bytes(output[begin + 0: begin + 4], byteorder='big')
        fourcc = output[begin + 4: begin + 8].decode('ascii')

        if fourcc == 'ftyp':
          header += output[begin: begin + size]
        elif fourcc == 'moov':
          moov_begin, moov_end = begin + 8, begin + size
          while moov_begin < moov_end:
            moov_size = int.from_bytes(output[moov_begin + 0: moov_begin + 4], byteorder='big')
            fourcc = output[moov_begin + 4: moov_begin + 8].decode('ascii')
            if fourcc == 'trak': # Assume One Variant
              trak_begin, trak_end = moov_begin + 8, moov_begin + moov_size
              while trak_begin < trak_end:
                trak_size = int.from_bytes(output[trak_begin + 0: trak_begin + 4], byteorder='big')
                fourcc = output[trak_begin + 4: trak_begin + 8].decode('ascii')
                if fourcc == 'mdia':
                  mdia_begin, mdia_end = trak_begin + 8, trak_begin + moov_size
                  while mdia_begin < mdia_end:
                    mdia_size = int.from_bytes(output[mdia_begin + 0: mdia_begin + 4], byteorder='big')
                    fourcc = output[mdia_begin + 4: mdia_begin + 8].decode('ascii')
                    if fourcc == 'mdhd':
                      timescale = int.from_bytes(output[mdia_begin + 20: mdia_begin + 24], byteorder='big')
                    mdia_begin += mdia_size
                trak_begin += trak_size
            moov_begin += moov_size
          header += output[begin: begin + size]
        elif fourcc == 'moof':
          moof_begin, moof_end = begin + 8, begin + size
          while moof_begin < moof_end:
            moof_size = int.from_bytes(output[moof_begin + 0: moof_begin + 4], byteorder='big')
            fourcc = output[moof_begin + 4: moof_begin + 8].decode('ascii')
            if fourcc == 'traf': # Assume One Variant
              traf_begin, traf_end = moof_begin + 8, moof_begin + moof_size
              while traf_begin < traf_end:
                traf_size = int.from_bytes(output[traf_begin + 0: traf_begin + 4], byteorder='big')
                fourcc = output[traf_begin + 4: traf_begin + 8].decode('ascii')
                if fourcc == 'tfdt':
                  version = output[traf_begin + 8]
                  if version == 0:
                    output[traf_begin + 12: traf_begin + 16] = int.to_bytes((msn * target_duration) * timescale + int.from_bytes(output[traf_begin + 12: traf_begin + 16], byteorder='big'), 4, byteorder='big')
                  elif version == 1:
                    output[traf_begin + 12: traf_begin + 20] = int.to_bytes((msn * target_duration) * timescale + int.from_bytes(output[traf_begin + 12: traf_begin + 20], byteorder='big'), 8, byteorder='big')
                traf_begin += traf_size
            moof_begin += moof_size
          body += output[begin: begin + size]
        elif fourcc == 'mdat':
          body += output[begin: begin + size]

        begin += size

      processing[msn] = False
      if not init.done(): init.set_result(header)
      segments[msn][1].set_result(memoryview(body))
      if process_caindidate is not None and not segments[process_caindidate][1].done():
        process_queue.put_nowait(process_caindidate)
        if process_caindidate + 1 < len(segments) and not segments[process_caindidate + 1][1].done():
          process_caindidate += 1

  asyncio.create_task(process())
  return '', playlist, segment, initalization

async def main():
  loop = asyncio.get_running_loop()
  parser = argparse.ArgumentParser(description=('biim: HLS VOD In-Memroy Origin'))

  parser.add_argument('-i', '--input', type=Path, required=True)
  parser.add_argument('-t', '--target_duration', type=int, nargs='?', default=5)
  parser.add_argument('-p', '--port', type=int, nargs='?', default=8080)

  args = parser.parse_args()

  audio_codec, audio_playlist, audio_segment, audio_init = await audio(args.input, Path('ffmpeg'), args.target_duration)
  video_codec, video_playlist, video_segment, video_init = await video(args.input, Path('ffmpeg'), args.target_duration)
  async def master_playlist(request: web.Request):
    m3u8 = ''
    m3u8 += f'#EXTM3U\n'
    m3u8 += f'#EXT-X-VERSION:3\n'
    m3u8 += f'\n'
    m3u8 += f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="主音声",DEFAULT="YES",AUTOSELECT=YES,LANGUAGE="JPN",URI="./audio/playlist.m3u8"\n'
    m3u8 += f'#EXT-X-STREAM-INF:BANDWIDTH=1,AUDIO="audio"\n'
    m3u8 += f'./video/playlist.m3u8'
    return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=36000'}, text=m3u8, content_type="application/x-mpegURL")

  # setup aiohttp
  app = web.Application()
  loop = asyncio.get_running_loop()
  app.add_routes([
    web.get('/master.m3u8', master_playlist),
    web.get('/audio/playlist.m3u8', audio_playlist),
    web.get('/audio/segment', audio_segment),
    web.get('/audio/init', audio_init),
    web.get('/video/playlist.m3u8', video_playlist),
    web.get('/video/segment', video_segment),
    web.get('/video/init', video_init),
  ])
  runner = web.AppRunner(app)
  await runner.setup()
  await loop.create_server(cast(web.Server, runner.server), '0.0.0.0', args.port)

  await asyncio.sleep(math.inf)

if __name__ == '__main__':
  asyncio.run(main())
