#!/usr/bin/env python3

import asyncio
from aiohttp import web

import argparse
import sys
import os

from collections import deque

from pathlib import Path
from datetime import datetime, timedelta

from mpeg2ts import ts
from mpeg2ts.packetize import packetize_section, packetize_pes
from mpeg2ts.section import Section
from mpeg2ts.pat import PATSection
from mpeg2ts.pmt import PMTSection
from mpeg2ts.pes import PES
from mpeg2ts.h264 import H264PES
from mpeg2ts.h265 import H265PES
from mpeg2ts.parser import SectionParser, PESParser

from hls.m3u8 import M3U8

from mp4.box import ftyp, moov, mvhd, mvex, trex, moof, mdat, emsg
from mp4.avc import avcTrack
from mp4.hevc import hevcTrack
from mp4.mp4a import mp4aTrack

SAMPLING_FREQUENCY = {
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

  total = await estimate_duration(args.input)
  num_of_segments = int((total + args.target_duration - 1) // args.target_duration)

  virutal_playlist = asyncio.Future()
  virtual_init = asyncio.Future()
  virtual_segments = []
  processing = []
  process_caindidate = None
  process_queue = asyncio.Queue()

  async def playlist(request):
    result = await asyncio.shield(virutal_playlist)
    return web.Response(headers={'Access-Control-Allow-Origin': '*'}, text=result, content_type="application/x-mpegURL")
  async def initalization(request):
    if not virtual_segments[0].done() and not processing[0]:
      process_queue.put_nowait(0)
    body = await asyncio.shield(virtual_init)
    return web.Response(headers={'Access-Control-Allow-Origin': '*'}, body=body, content_type="video/mp4")
  async def segment(request):
    nonlocal process_caindidate
    seq = request.query['seq'] if 'seq' in request.query else None

    if seq is None:
      return web.Response(headers={'Access-Control-Allow-Origin': '*'}, status=400, content_type="video/mp4")
    seq = int(seq)

    if seq < 0 or seq >= len(virtual_segments):
      return web.Response(headers={'Access-Control-Allow-Origin': '*'}, status=400, content_type="video/mp4")

    if not virtual_segments[seq].done() and not processing[seq]:
      process_queue.put_nowait(seq)

    body = await asyncio.shield(virtual_segments[seq])

    response = web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-cache,no-store'}, body=body, content_type="video/mp4")
    return response

  # setup aiohttp
  app = web.Application()
  app.add_routes([
    web.get('/playlist.m3u8', playlist),
    web.get('/init', initalization),
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
  virutal_playlist_header += f'#EXT-X-MAP:URI="init"\n'
  virtual_playlist_body = '\n'.join([
    f'#EXTINF:{args.target_duration}\nsegment?seq={seq}\n'
    for seq in range(num_of_segments)
  ])
  virtual_playlist_tail = '#EXT-X-ENDLIST\n'
  virtual_segments = [asyncio.Future() for _ in range(num_of_segments)]
  processing = [False for _ in range(num_of_segments)]

  virutal_playlist.set_result(virutal_playlist_header + virtual_playlist_body + virtual_playlist_tail)
  process_queue.put_nowait(0)
  wait_read = True
  while True:
    if wait_read: seq = await process_queue.get()
    wait_read = False
    ss = max(0, seq * args.target_duration)

    options = [
      '-ss', str(max(0, ss - 10)), '-i', str(args.input), '-ss', str(ss - max(0, ss - 10)),
      '-map', '0:v:0', '-map', '0:a:0',
      '-c:v', 'libx264', '-tune', 'zerolatency', '-preset', 'ultrafast',
      '-c:a', 'copy',
      '-fflags', 'nobuffer', '-flags', 'low_delay', '-max_delay', '0',
      '-output_ts_offset', str(ss),
      '-f', 'mpegts', '-'
    ]
    encoder = await asyncio.subprocess.create_subprocess_exec('ffmpeg', *options, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)

    processing[seq] = True
    fmp4 = bytearray()
    async def remux():
      nonlocal seq
      nonlocal total
      nonlocal virtual_init
      nonlocal virtual_segments
      nonlocal fmp4
      nonlocal wait_read

      # setup reader
      PAT_Parser = SectionParser(PATSection)
      PMT_Parser = SectionParser(PMTSection)
      AAC_PES_Parser = PESParser(PES)
      H264_PES_Parser = PESParser(H264PES)
      H265_PES_Parser = PESParser(H265PES)
      ID3_PES_Parser = PESParser(PES)

      PCR_PID = None
      PMT_PID = None
      AAC_PID = None
      H264_PID = None
      H265_PID = None
      ID3_PID = None

      AAC_CONFIG = None
      AAC_DATA = None

      CURR_H264 = None
      NEXT_H264 = None
      CURR_H265 = None
      NEXT_H265 = None

      VPS = None
      SPS = None
      PPS = None

      while True:
        try:
          old_seq = seq
          seq = process_queue.get_nowait()
          wait_read = False
          processing[old_seq] = False
          return
        except asyncio.QueueEmpty:
          pass

        packet = None
        try:
          packet = await encoder.stdout.readexactly(188)
        except:
          return

        endDTS = min(total, (seq + 1) * args.target_duration) * ts.HZ
        PID = ts.pid(packet)
        if PID == H264_PID:
          H264_PES_Parser.push(packet)
          for H264 in H264_PES_Parser:
            timestamp = H264.dts() if H264.has_dts() else H264.pts()
            cts = (H264.pts() - (H264.dts() if H264.has_dts() else H264.pts()) + ts.PCR_CYCLE) % ts.PCR_CYCLE
            keyInSamples = False
            samples = deque()

            for ebsp in H264:
              nal_unit_type = ebsp[0] & 0x1f

              if nal_unit_type == 0x07: # SPS
                SPS = ebsp
              elif nal_unit_type == 0x08: # PPS
                PPS = ebsp
              elif nal_unit_type == 0x09 or nal_unit_type == 0x06: # AUD or SEI
                pass
              elif nal_unit_type == 0x05:
                keyInSamples = True
                samples.append(ebsp)
              else:
                samples.append(ebsp)
            NEXT_H264 = (keyInSamples, samples, timestamp, cts) if samples else None

            hasIDR = False
            if CURR_H264:
              isKeyframe, samples, dts, cts = CURR_H264
              hasIDR = isKeyframe
              duration = timestamp - dts
              content = bytearray()
              while samples:
                ebsp = samples.popleft()
                content += len(ebsp).to_bytes(4, byteorder='big') + ebsp

              fmp4 += b''.join([
                moof(0,
                  [
                    (1, duration, dts, 0, [(len(content), duration, isKeyframe, cts)])
                  ]
                ),
                mdat(content)
              ])
            NEXT_H264, CURR_H264 = CURR_H264, NEXT_H264

            if SPS and PPS and AAC_CONFIG and not virtual_init.done():
              virtual_init.set_result(b''.join([
                ftyp(),
                moov(
                  mvhd(ts.HZ),
                  mvex([
                    trex(1),
                    trex(2)
                  ]),
                  [
                    avcTrack(1, ts.HZ, SPS, PPS),
                    mp4aTrack(2, ts.HZ, *AAC_CONFIG),
                  ]
                )
              ]))

            if timestamp >= endDTS:
              if not virtual_segments[seq].done():
                virtual_segments[seq].set_result(fmp4)
              fmp4 = bytearray()
              processing[seq] = False
              if seq + 1 < len(virtual_segments) and not virtual_segments[seq + 1].done():
                seq += 1
                processing[seq] = True
              else:
                break

        elif PID == H265_PID:
          H265_PES_Parser.push(packet)
          for H265 in H265_PES_Parser:
            timestamp = H265.dts() if H265.has_dts() else H265.pts()
            cts = (H265.pts() - (H265.dts() if H265.has_dts() else  H265.pts()) + ts.PCR_CYCLE) % ts.PCR_CYCLE
            keyInSamples = False
            samples = deque()

            for ebsp in H265:
              nal_unit_type = (ebsp[0] >> 1) & 0x3f

              if nal_unit_type == 0x20: # VPS
                VPS = ebsp
              elif nal_unit_type == 0x21: # SPS
                SPS = ebsp
              elif nal_unit_type == 0x22: # PPS
                PPS = ebsp
              elif nal_unit_type == 0x23 or nal_unit_type == 0x27: # AUD or SEI
                pass
              elif nal_unit_type == 19 or nal_unit_type == 20 or nal_unit_type == 21: # IDR_W_RADL, IDR_W_LP, CRA_NUT
                keyInSamples = True
                samples.append(ebsp)
              else:
                samples.append(ebsp)
            NEXT_H265 = (keyInSamples, samples, timestamp, cts) if samples else None

            hasIDR = False
            if CURR_H265:
              isKeyframe, samples, dts, cts = CURR_H265
              hasIDR = isKeyframe
              duration = timestamp - dts
              content = bytearray()
              while samples:
                ebsp = samples.popleft()
                content += len(ebsp).to_bytes(4, byteorder='big') + ebsp

              fmp4 += b''.join([
                moof(0,
                  [
                    (1, duration, dts, 0, [(len(content), duration, isKeyframe, cts)])
                  ]
                ),
                mdat(content)
              ])
            NEXT_H265, CURR_H265 = CURR_H265, NEXT_H265

            if VPS and SPS and PPS and AAC_CONFIG and not virtual_init.done():
              virtual_init.set_result(b''.join([
                ftyp(),
                moov(
                  mvhd(ts.HZ),
                  mvex([
                    trex(1),
                    trex(2)
                  ]),
                  [
                    hevcTrack(1, ts.HZ, VPS, SPS, PPS),
                    mp4aTrack(2, ts.HZ, *AAC_CONFIG),
                  ]
                )
              ]))

            if timestamp >= endDTS:
              virtual_segments[seq].set_result(fmp4)
              fmp4 = bytearray()
              processing[seq] = False
              if seq + 1 < len(virtual_segments) and not virtual_segments[seq + 1].done():
                seq += 1
                processing[seq] = True
              else:
                break

        elif PID == AAC_PID:
          AAC_PES_Parser.push(packet)
          for AAC_PES in AAC_PES_Parser:
            timestamp = AAC_PES.pts()
            begin, ADTS_AAC = 0, AAC_PES.PES_packet_data()
            length = len(ADTS_AAC)
            while begin + 1 < length:
              if ((ADTS_AAC[begin + 0] << 4) | ((ADTS_AAC[begin + 1] & 0xF0) >> 4)) != 0xFFF:
                if AAC_DATA is not None:
                  AAC_DATA[3].extend(ADTS_AAC[begin:begin+1])
                  if AAC_DATA[2] == len(AAC_DATA[3]):
                    fmp4 += b''.join([
                      moof(0,
                        [
                          (2, AAC_DATA[1], AAC_DATA[0], 0, [(AAC_DATA[2], AAC_DATA[1], True, 0)])
                        ]
                      ),
                      mdat(AAC_DATA[3])
                    ])
                    AAC_DATA = None
                begin += 1
                continue

              protection = (ADTS_AAC[begin + 1] & 0b00000001) == 0
              profile = ((ADTS_AAC[begin + 2] & 0b11000000) >> 6)
              samplingFrequencyIndex = ((ADTS_AAC[begin + 2] & 0b00111100) >> 2)
              channelConfiguration = ((ADTS_AAC[begin + 2] & 0b00000001) << 2) | ((ADTS_AAC[begin + 3] & 0b11000000) >> 6)
              frameLength = ((ADTS_AAC[begin + 3] & 0x03) << 11) | (ADTS_AAC[begin + 4] << 3) | ((ADTS_AAC[begin + 5] & 0xE0) >> 5)
              if not AAC_CONFIG:
                AAC_CONFIG = (bytes([
                  ((profile + 1) << 3) | ((samplingFrequencyIndex & 0x0E) >> 1),
                  ((samplingFrequencyIndex & 0x01) << 7) | (channelConfiguration << 3)
                ]), channelConfiguration, SAMPLING_FREQUENCY[samplingFrequencyIndex])
              duration = 1024 * ts.HZ // SAMPLING_FREQUENCY[samplingFrequencyIndex]
              AAC_DATA = (timestamp, duration, frameLength - (9 if protection else 7), bytearray(ADTS_AAC[begin + (9 if protection else 7): begin + frameLength]))
              if AAC_DATA[2] == len(AAC_DATA[3]):
                fmp4 += b''.join([
                  moof(0,
                    [
                      (2, AAC_DATA[1], AAC_DATA[0], 0, [(AAC_DATA[2], AAC_DATA[1], True, 0)])
                    ]
                  ),
                  mdat(AAC_DATA[3])
                ])
                AAC_DATA = None
              timestamp += duration
              begin += frameLength

        elif PID == 0x00:
          PAT_Parser.push(packet)
          for PAT in PAT_Parser:
            if PAT.CRC32() != 0: continue
            LAST_PAT = PAT

            for program_number, program_map_PID in PAT:
              if program_number == 0: continue

              if not PMT_PID:
                PMT_PID = program_map_PID

        elif PID == PMT_PID:
          PMT_Parser.push(packet)
          for PMT in PMT_Parser:
            if PMT.CRC32() != 0: continue
            LAST_PMT = PMT

            PCR_PID = PMT.PCR_PID
            for stream_type, elementary_PID in PMT:
              if stream_type == 0x1b:
                H264_PID = elementary_PID
              elif stream_type == 0x24:
                H265_PID = elementary_PID
              elif stream_type == 0x0F:
                AAC_PID = elementary_PID
              elif stream_type == 0x15:
                ID3_PID = elementary_PID

        elif PID == ID3_PID:
          ID3_PES_Parser.push(packet)
          for ID3_PES in ID3_PES_Parser:
            if LATEST_PCR_VALUE is None: continue
            timestamp = ID3_PES.pts()
            ID3 = ID3_PES.PES_packet_data()
            fmp4 += emsg(ts.HZ, timestamp, None, 'https://aomedia.org/emsg/ID3', ID3)

        else:
          pass

        """
        if CURR_H264:
          isKeyframe, samples, dts, cts = CURR_H264
          hasIDR = isKeyframe
          duration = max(0, int(end - dts))
          content = bytearray()
          while samples:
            ebsp = samples.popleft()
            content += len(ebsp).to_bytes(4, byteorder='big') + ebsp

          fmp4 += b''.join([
            moof(0,
              [
                (1, duration, dts, 0, [(len(content), duration, isKeyframe, cts)])
              ]
            ),
            mdat(content)
          ])
        if CURR_H265:
          isKeyframe, samples, dts, cts = CURR_H265
          hasIDR = isKeyframe
          duration = max(0, int(end - dts))
          content = bytearray()
          while samples:
            ebsp = samples.popleft()
            content += len(ebsp).to_bytes(4, byteorder='big') + ebsp

          fmp4 += b''.join([
            moof(0,
              [
                (1, duration, dts, 0, [(len(content), duration, isKeyframe, cts)])
              ]
            ),
            mdat(content)
          ])
        """
    await remux()

if __name__ == '__main__':
  asyncio.run(main())
