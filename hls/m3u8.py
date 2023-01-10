#!/usr/bin/env python3

import asyncio
import math
from collections import deque
from datetime import datetime

from typing import Any

from hls.segment import Segment

class Daterange:
  def __init__(self, id: str, start_date: datetime, end_date: datetime | None = None, **kwargs):
    self.id: str = id
    self.start_date: datetime = start_date
    self.end_date: datetime | None = end_date
    self.attributes: dict[str, Any] = kwargs

  def close(self, end_date: datetime) -> None:
    if self.end_date is not None: return
    self.end_date = end_date

  def __str__(self) -> str:
    duration = (self.end_date - self.start_date) if self.end_date else None
    attributes = ','.join(([f'DURATION={duration.total_seconds()}'] if duration is not None else []) + [f'{attr}={value}' for attr, value in self.attributes.items() if duration is None or attr != "PLANNED-DURATION"])
    attributes = ',' + attributes if attributes else ''

    return f'#EXT-X-DATERANGE:ID="{self.id}",START-DATE="{self.start_date.isoformat()}"{attributes}\n' + (f'#EXT-X-DATERANGE:ID="{self.id}",END-DATE="{self.end_date.isoformat()}"\n' if self.end_date is not None else '')

class M3U8:
  def __init__(self, target_duration: int, part_target: float, list_size: int, hasInit: bool = False):
    self.media_sequence: int = 0
    self.target_duration: int = target_duration
    self.part_target: float = part_target
    self.list_size: int = list_size
    self.hasInit: bool = hasInit
    self.dateranges: dict[str, Daterange] = dict()
    self.segments: deque[Segment] = deque()
    self.outdated: deque[Segment] = deque()
    self.published: bool = False
    self.futures: list[asyncio.Future[str]] = []

  def in_range(self, msn: int) -> bool:
    return self.media_sequence <= msn and msn < self.media_sequence + len(self.segments)

  def in_outdated(self, msn: int) -> bool:
    return self.media_sequence > msn and msn >= self.media_sequence - len(self.outdated)

  def plain(self) -> asyncio.Future[str] | None:
    f: asyncio.Future[str] = asyncio.Future()
    if self.published:
      f.set_result(self.manifest())
    else:
      self.futures.append(f)
    return f

  def blocking(self, msn: int, part: int | None, skip: bool = False) -> asyncio.Future[str] | None:
    if not self.in_range(msn): return None

    index = msn - self.media_sequence

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

  def push(self, packet: bytes | bytearray | memoryview) -> None:
    if not self.segments: return
    self.segments[-1].push(packet)

  def newSegment(self, beginPTS: int, isIFrame: bool = False, programDateTime: datetime | None = None) -> None:
    self.segments.append(Segment(beginPTS, isIFrame, programDateTime))
    while self.list_size is not None and self.list_size < len(self.segments):
      self.outdated.appendleft(self.segments.popleft())
      self.media_sequence += 1
    while self.list_size is not None and self.list_size < len(self.outdated):
      self.outdated.pop()

  def newPartial(self, beginPTS: int, isIFrame: bool = False) -> None:
    if not self.segments: return
    self.segments[-1].newPartial(beginPTS, isIFrame)

  def completeSegment(self, endPTS: int) -> None:
    self.published = True

    if not self.segments: return
    self.segments[-1].complete(endPTS)
    self.segments[-1].notify(self.manifest(True), self.manifest(False))
    for f in self.futures:
      if not f.done(): f.set_result(self.manifest())
    self.futures = []

  def completePartial(self, endPTS: int) -> None:
    if not self.segments: return
    self.segments[-1].completePartial(endPTS)
    self.segments[-1].notify(self.manifest(True), self.manifest(False))

  def continuousSegment(self, endPTS: int, isIFrame: bool = False, programDateTime: datetime | None = None) -> None:
    lastSegment = self.segments[-1] if self.segments else None
    self.newSegment(endPTS, isIFrame, programDateTime)

    if not lastSegment: return
    self.published = True
    lastSegment.complete(endPTS)
    lastSegment.notify(self.manifest(True), self.manifest(False))
    for f in self.futures:
      if not f.done(): f.set_result(self.manifest())
    self.futures = []

  def continuousPartial(self, endPTS: int, isIFrame: bool = False) -> None:
    lastSegment = self.segments[-1] if self.segments else None
    lastPartial = lastSegment.partials[-1] if lastSegment else None
    self.newPartial(endPTS, isIFrame)

    if not lastPartial: return
    lastPartial.complete(endPTS)
    lastPartial.notify(self.manifest(True), self.manifest(False))

  async def segment(self, msn: int) -> asyncio.Queue[bytes | bytearray | memoryview | None] | None:
    if not self.in_range(msn):
      if not self.in_outdated(msn): return None
      index = (self.media_sequence - msn) - 1
      return await self.outdated[index].response()
    index = msn - self.media_sequence
    return await self.segments[index].response()

  async def partial(self, msn: int, part: int) -> asyncio.Queue[bytes | bytearray | memoryview | None] | None:
    if not self.in_range(msn):
      if not self.in_outdated(msn): return None
      index = (self.media_sequence - msn) - 1
      if part > len(self.outdated[index].partials): return None
      return await self.outdated[index].partials[part].response()
    index = msn - self.media_sequence
    if part > len(self.segments[index].partials): return None
    return await self.segments[index].partials[part].response()

  def open(self, id: str, start_date: datetime, end_date: datetime | None = None,  **kwargs):
    if id in self.dateranges: return
    self.dateranges[id] = Daterange(id, start_date, end_date, **kwargs)

  def close(self, id: str, end_date: datetime):
    if id not in self.dateranges: return
    self.dateranges[id].close(end_date)

  def estimated_tartget_duration(self) -> int:
    target_duration = self.target_duration
    for segment in self.segments:
      if segment.isCompleted(): target_duration = max(target_duration, math.ceil(segment.extinf().total_seconds()))
    return target_duration

  def manifest(self, skip: bool = False) -> str:
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
      m3u8 += f'\n'
      m3u8 += f'#EXT-X-MAP:URI="init"\n'

    if len(self.segments) > 0:
      for id in list(self.dateranges.keys()):
        if self.dateranges[id].end_date is None: continue
        if self.dateranges[id].end_date >= self.segments[0].program_date_time: continue
        del self.dateranges[id]

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

    for daterange in self.dateranges.values():
      m3u8 += f'\n'
      m3u8 += f'{daterange}'

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
