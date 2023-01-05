#!/usr/bin/env python3

import asyncio
from aiohttp import web

import argparse
import sys

from pathlib import Path

async def estimate_duration(path):
  options = ['-i', path, '-show_entries', 'format=duration']

  probe = await asyncio.subprocess.create_subprocess_exec('ffprobe', *options, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
  duration = float((await probe.stdout.read()).decode('utf-8').split('\n')[1].split('=')[1])
  return duration

async def main():
  loop = asyncio.get_running_loop()
  parser = argparse.ArgumentParser(description=('biim: LL-HLS origin'))

  parser.add_argument('-i', '--input', type=Path, required=True)
  parser.add_argument('-t', '--target_duration', type=int, nargs='?', default=5)
  parser.add_argument('-p', '--port', type=int, nargs='?', default=8080)

  args = parser.parse_args()

  duration = await estimate_duration(args.input)
  num_of_segments = int((duration + args.target_duration - 1) // args.target_duration)

  virutal_playlist = asyncio.Future()
  virtual_segments = []
  prosessing = []
  process_caindidate = None
  process_queue = asyncio.Queue()

  async def playlist(request):
    result = await asyncio.shield(virutal_playlist)
    return web.Response(headers={'Access-Control-Allow-Origin': '*'}, text=result, content_type="application/x-mpegURL")
  async def segment(request):
    nonlocal process_caindidate
    seq = request.query['seq'] if 'seq' in request.query else None

    if seq is None:
      return web.Response(headers={'Access-Control-Allow-Origin': '*'}, status=400, content_type="video/mp4")
    seq = int(seq)

    if seq < 0 or seq >= len(virtual_segments):
      return web.Response(headers={'Access-Control-Allow-Origin': '*'}, status=400, content_type="video/mp4")

    if not virtual_segments[seq].done() and not prosessing[seq]:
      process_queue.put_nowait(seq)

    body = await asyncio.shield(virtual_segments[seq])
    if seq + 1 < len(virtual_segments) and not virtual_segments[seq].done() and not prosessing[seq]:
      process_caindidate = seq + 1

    response = web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-cache,no-store'}, body=body, content_type="video/mp4")
    return response

  # setup aiohttp
  app = web.Application()
  app.add_routes([
    web.get('/playlist.m3u8', playlist),
    web.get('/segment', segment),
  ])
  runner = web.AppRunner(app)
  await runner.setup()
  await loop.create_server(runner.server, '0.0.0.0', args.port)

  virutal_playlist_header = ''
  virutal_playlist_header += f'#EXTM3U\n'
  virutal_playlist_header += f'#EXT-X-VERSION:6\n'
  virutal_playlist_header += f'#EXT-X-TARGETDURATION:{args.target_duration}\n'
  virutal_playlist_header += f'#EXT-X-PLAYLIST-TYPE:VOD\n'
  virtual_playlist_body = '\n'.join([
    f'#EXTINF:{min(duration, (seq + 1) * args.target_duration) - seq * args.target_duration:.06f}\nsegment?seq={seq}\n'
    for seq in range(num_of_segments)
  ])
  virtual_playlist_tail = '#EXT-X-ENDLIST\n'
  virtual_inits = [asyncio.Future() for _ in range(num_of_segments)]
  virtual_segments = [asyncio.Future() for _ in range(num_of_segments)]
  prosessing = [False for _ in range(num_of_segments)]

  virutal_playlist.set_result(virutal_playlist_header + virtual_playlist_body + virtual_playlist_tail)
  while True:
    seq = await process_queue.get()
    process_queue.task_done()
    if virtual_segments[seq].done(): continue
    prosessing[seq] = True

    ss = max(0, seq * args.target_duration)
    t = args.target_duration
    options = [
      '-ss', str(max(0, ss - 10)), '-i', str(args.input), '-ss', str(ss - max(0, ss - 10)), '-t', str(t),
      '-map', '0:v', '-map', '0:a',
      '-c:v', 'libx264', '-tune', 'zerolatency', '-preset', 'ultrafast',
      '-c:a', 'copy',
      '-fflags', 'nobuffer', '-flags', 'low_delay', '-max_delay', '0',
      '-output_ts_offset', str(ss),
      '-f', 'mpegts', '-'
    ]

    encoder = await asyncio.subprocess.create_subprocess_exec('ffmpeg', *options, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    output = await encoder.stdout.read()

    prosessing[seq] = False
    virtual_segments[seq].set_result(output)
    if process_caindidate is not None and not virtual_segments[process_caindidate].done():
      process_queue.put_nowait(process_caindidate)
      if process_caindidate + 1 < len(virtual_segments) and not virtual_segments[process_caindidate + 1].done():
        process_caindidate += 1

if __name__ == '__main__':
  asyncio.run(main())
