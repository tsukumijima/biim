#!/usr/bin/env python3

from typing import cast

import asyncio
import io
from aiohttp import web

import argparse
from threading import Semaphore

from biim.rtmp.rtmp import recieve, STREAM_TYPE_ID_AUDIO, STREAM_TYPE_ID_VIDEO, STREAM_TYPE_ID_DATA
from biim.rtmp.amf0 import deserialize
from biim.rtmp.remuxer import FLVfMP4Remuxer

from biim.hls.m3u8 import M3U8

async def serve(args):
  appName: str = args.app_name
  streamKey: str = args.stream_key
  connections: int = args.connections

  # Setup Concurrency
  semaphore = Semaphore(connections) # Never Blocking, only Limiting

  # Setup RTMP/FLV Reciever
  async def connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    if not semaphore.acquire(blocking=False): # Exceeded Connectin Capacity
      writer.close()
      await writer.wait_closed()
      return

    # FLV Demuxer definision
    class Remuxer(FLVfMP4Remuxer):
      pass
    remuxer = Remuxer()

    async for message in recieve(reader, writer, appName, streamKey):
      remuxer.parseRTMP(message)

    writer.close()
    await writer.wait_closed()
    semaphore.release()
  return connection

async def main():
  parser = argparse.ArgumentParser(description=('biim: LL-HLS origin'))

  parser.add_argument('--window_size', type=int, nargs='?')
  parser.add_argument('--target_duration', type=int, nargs='?', default=1)
  parser.add_argument('--part_duration', type=float, nargs='?', default=0.25)
  parser.add_argument('--hls_port', type=int, nargs='?', default=8080)
  parser.add_argument('--rtmp_port', type=int, nargs='?', default=1935)
  parser.add_argument('--app_name', type=str, required=True)
  parser.add_argument('--stream_key', type=str, required=True)
  parser.add_argument('--connections', type=int, default=1)

  args = parser.parse_args()

  server = await asyncio.start_server(await serve(args), 'localhost', args.rtmp_port)
  async with server: await server.serve_forever()

if __name__ == '__main__':
  asyncio.run(main())
