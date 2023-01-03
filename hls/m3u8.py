#!/usr/bin/env python3

import asyncio
import math
from collections import deque

from hls.segment import Segment

class M3U8:
  def __init__(self, target_duration, part_target, list_size, hasInit = False):
    self.media_sequence = 0
    self.target_duration = target_duration
    self.part_target = part_target
    self.list_size = list_size
    self.hasInit = hasInit
    self.segments = deque()
    self.outdated = deque()
    self.published = False
    self.futures = []

  def in_range(self, msn):
    return self.media_sequence <= msn and msn < self.media_sequence + len(self.segments)

  def in_outdated(self, msn):
    return self.media_sequence > msn and msn >= self.media_sequence - len(self.outdated)

  def plain(self):
    f = asyncio.Future()
    if self.published:
      f.set_result(self.manifest())
    else:
      self.futures.append(f)
    return f

  def blocking(self, msn, part):
    if not self.in_range(msn): return None

    f = asyncio.Future()
    index = msn - self.media_sequence

    if part is None:
      if self.segments[index].isCompleted():
        f.set_result(self.manifest())
      else:
        self.segments[index].m3u8s.append(f)
    else:
      if part > len(self.segments[index].partials): return None

      if self.segments[index].partials[part].isCompleted():
        f.set_result(self.manifest())
      else:
        self.segments[index].partials[part].m3u8s.append(f)
    return f

  def push(self, packet):
    if not self.segments: return
    self.segments[-1].push(packet)

  def newSegment(self, beginPTS, isIFrame = False):
    self.segments.append(Segment(beginPTS, isIFrame))
    while self.list_size is not None and self.list_size < len(self.segments):
      self.outdated.appendleft(self.segments.popleft())
      self.media_sequence += 1
    while self.list_size is not None and self.list_size < len(self.outdated):
      self.outdated.pop()

  def newPartial(self, beginPTS, isIFrame = False):
    if not self.segments: return
    self.segments[-1].newPartial(beginPTS, isIFrame)

  def completeSegment(self, endPTS):
    self.published = True

    if not self.segments: return
    self.segments[-1].complete(endPTS)
    for m in self.segments[-1].partials[-1].m3u8s:
      if not m.done(): m.set_result(self.manifest())
    self.segments[-1].partials[-1].m3u8s = []
    for m in self.segments[-1].m3u8s:
      if not m.done(): m.set_result(self.manifest())
    self.segments[-1].m3u8s = []
    for f in self.futures:
      if not f.done(): f.set_result(self.manifest())
    self.futures = []

  def completePartial(self, endPTS):
    if not self.segments: return
    self.segments[-1].completePartial(endPTS)
    for m in self.segments[-1].partials[-1].m3u8s:
      if not m.done(): m.set_result(self.manifest())
    self.segments[-1].partials[-1].m3u8s

  def continuousSegment(self, endPTS, isIFrame = False):
    lastSegment = self.segments[-1] if self.segments else None
    self.newSegment(endPTS, isIFrame)

    if not lastSegment: return
    self.published = True
    lastSegment.complete(endPTS)
    for m in lastSegment.partials[-1].m3u8s:
      if not m.done(): m.set_result(self.manifest())
    lastSegment.partials[-1].m3u8s = []
    for m in lastSegment.m3u8s:
      if not m.done(): m.set_result(self.manifest())
    lastSegment.m3u8s = []
    for f in self.futures:
      if not f.done(): f.set_result(self.manifest())
    self.futures = []

  def continuousPartial(self, endPTS, isIFrame = False):
    lastSegment = self.segments[-1] if self.segments else None
    lastPartial = lastSegment.partials[-1] if lastSegment else None
    self.newPartial(endPTS, isIFrame)

    if not lastPartial: return
    lastPartial.complete(endPTS)
    for m in lastPartial.m3u8s:
      if not m.done(): m.set_result(self.manifest())
    lastPartial.m3u8s = []

  async def segment(self, msn):
    if not self.in_range(msn):
      if not self.in_outdated(msn): return None
      index = (self.media_sequence - msn) - 1
      return await self.outdated[index].response()
    index = msn - self.media_sequence
    return await self.segments[index].response()

  async def partial(self, msn, part):
    if not self.in_range(msn):
      if not self.in_outdated(msn): return None
      index = (self.media_sequence - msn) - 1
      if part > len(self.outdated[index].partials): return None
      return await self.outdated[index].partials[part].response()
    index = msn - self.media_sequence
    if part > len(self.segments[index].partials): return None
    return await self.segments[index].partials[part].response()

  def manifest(self):
    m3u8 = ''
    m3u8 += f'#EXTM3U\n'
    m3u8 += f'#EXT-X-VERSION:6\n'
    m3u8 += f'#EXT-X-TARGETDURATION:{self.target_duration}\n'
    m3u8 += f'#EXT-X-PART-INF:PART-TARGET={self.part_target:.06f}\n'
    m3u8 += f'#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={(self.part_target * 3.001):.06f}\n'
    m3u8 += f'#EXT-X-MEDIA-SEQUENCE:{self.media_sequence}\n'

    if self.hasInit:
      m3u8 += f'#EXT-X-MAP:URI="init"\n'

    for seg_index, segment in enumerate(self.segments):
      msn = self.media_sequence + seg_index
      m3u8 += f'\n'
      m3u8 += f'#EXT-X-PROGRAM-DATE-TIME:{segment.program_date_time.isoformat()}\n'
      for part_index, partial in enumerate(segment):
        hasIFrame = ',INDEPENDENT=YES' if partial.hasIFrame else ''
        if not partial.isCompleted():
          m3u8 += f'#EXT-X-PRELOAD-HINT:TYPE=PART,URI="part?msn={msn}&part={part_index}"{hasIFrame}\n'
        else:
          m3u8 += f'#EXT-X-PART:DURATION={partial.extinf().total_seconds():.06f},URI="part?msn={msn}&part={part_index}"{hasIFrame}\n'

      if segment.isCompleted():
        m3u8 += f'#EXTINF:{segment.extinf().total_seconds():.06f}\n'
        m3u8 += f'segment?msn={msn}\n'

    return m3u8
