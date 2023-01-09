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

  def blocking(self, msn, part, skip=False):
    if not self.in_range(msn): return None

    index = msn - self.media_sequence

    f = None
    if part is None:
      f = self.segments[index].m3u8(skip)
      if self.segments[index].isCompleted():
        f.set_result(self.manifest(skip))
    else:
      if part > len(self.segments[index].partials): return None

      f = self.segments[index].partials[part].m3u8(skip)
      if self.segments[index].partials[part].isCompleted():
        f.set_result(self.manifest(skip))
    return f

  def push(self, packet):
    if not self.segments: return
    self.segments[-1].push(packet)

  def newSegment(self, beginPTS, isIFrame = False, programDateTime = None):
    self.segments.append(Segment(beginPTS, isIFrame, programDateTime))
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
    self.segments[-1].notify(self.manifest(True), self.manifest(False))
    for f in self.futures:
      if not f.done(): f.set_result(self.manifest())
    self.futures = []

  def completePartial(self, endPTS):
    if not self.segments: return
    self.segments[-1].completePartial(endPTS)
    self.segments[-1].notify(self.manifest(True), self.manifest(False))

  def continuousSegment(self, endPTS, isIFrame = False, programDateTime = None):
    lastSegment = self.segments[-1] if self.segments else None
    self.newSegment(endPTS, isIFrame, programDateTime)

    if not lastSegment: return
    self.published = True
    lastSegment.complete(endPTS)
    lastSegment.notify(self.manifest(True), self.manifest(False))
    for f in self.futures:
      if not f.done(): f.set_result(self.manifest())
    self.futures = []

  def continuousPartial(self, endPTS, isIFrame = False):
    lastSegment = self.segments[-1] if self.segments else None
    lastPartial = lastSegment.partials[-1] if lastSegment else None
    self.newPartial(endPTS, isIFrame)

    if not lastPartial: return
    lastPartial.complete(endPTS)
    lastPartial.notify(self.manifest(True), self.manifest(False))

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

  def estimated_tartget_duration(self):
    target_duration = self.target_duration
    for segment in self.segments:
      if segment.isCompleted(): target_duration = max(target_duration, math.ceil(segment.extinf().total_seconds()))
    return target_duration

  def manifest(self, skip=False):
    m3u8 = ''
    m3u8 += f'#EXTM3U\n'
    m3u8 += f'#EXT-X-VERSION:{9 if self.list_size is None else 6}\n'
    m3u8 += f'#EXT-X-TARGETDURATION:{self.estimated_tartget_duration()}\n'
    m3u8 += f'#EXT-X-PART-INF:PART-TARGET={self.part_target:.06f}\n'
    if self.list_size is None:
      m3u8 += f'#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={(self.part_target * 3.001):.06f},CAN-SKIP-UNTIL={self.estimated_tartget_duration() * 6}\n'
      m3u8 += f'#EXT-X-PLAYLIST-TYPE:EVENT\n'
    else:
      m3u8 += f'#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={(self.part_target * 3.001):.06f}\n'
    m3u8 += f'#EXT-X-MEDIA-SEQUENCE:{self.media_sequence}\n'

    if self.hasInit:
      m3u8 += f'#EXT-X-MAP:URI="init"\n'

    skip_end_index = 0
    if skip:
      elapsed = 0
      for seg_index, segment in enumerate(reversed(self.segments)):
        seg_index = (len(self.segments) - 1) - seg_index
        if not segment.isCompleted(): continue
        elapsed += segment.extinf().total_seconds()
        if elapsed >= self.estimated_tartget_duration() * 6:
          skip_end_index = seg_index
          break
    if skip_end_index > 0:
      m3u8 += f'\n'
      m3u8 += f'#EXT-X-SKIP:SKIPPED-SEGMENTS={skip_end_index}\n'

    for seg_index, segment in enumerate(self.segments):
      if seg_index < skip_end_index: continue # SKIP
      msn = self.media_sequence + seg_index
      m3u8 += f'\n'
      m3u8 += f'#EXT-X-PROGRAM-DATE-TIME:{segment.program_date_time.isoformat()}\n'
      if seg_index >= len(self.segments) - 4:
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
