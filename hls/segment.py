#!/usr/bin/env python3

from typing import Iterator
import asyncio
from datetime import datetime, timedelta, timezone

from mpeg2ts import ts

class PartialSegment:
  def __init__(self, beginPTS: int, isIFrame: bool = False):
    self.beginPTS: int = beginPTS
    self.endPTS: int | None = None
    self.hasIFrame: bool = isIFrame
    self.buffer: bytearray = bytearray()
    self.queues: list[asyncio.Queue[bytes | bytearray | memoryview | None]] = []
    self.m3u8s_with_skip: list[asyncio.Future[str]]= []
    self.m3u8s_without_skip: list[asyncio.Future[str]] = []

  def push(self, packet: bytes | bytearray | memoryview):
    self.buffer += packet
    for q in self.queues: q.put_nowait(packet)

  async def response(self) -> asyncio.Queue[bytes | bytearray | memoryview | None]:
    queue: asyncio.Queue[bytes | bytearray | memoryview | None] = asyncio.Queue()

    queue.put_nowait(self.buffer)
    if (self.isCompleted()):
      queue.put_nowait(None)
    else:
      self.queues.append(queue)
    return queue

  def m3u8(self, skip = False) -> asyncio.Future[str]:
    f: asyncio.Future[str] = asyncio.Future()
    if not self.isCompleted():
      if skip: self.m3u8s_with_skip.append(f)
      else: self.m3u8s_without_skip.append(f)
    return f

  def complete(self, endPTS: int) -> None:
    self.endPTS = endPTS
    for q in self.queues: q.put_nowait(None)
    self.queues = []

  def notify(self, skipped_manifest: str, all_manifest: str) -> None:
    for f in self.m3u8s_with_skip:
      if not f.done(): f.set_result(skipped_manifest)
    self.m3u8s_with_skip = []
    for f in self.m3u8s_without_skip:
      if not f.done(): f.set_result(all_manifest)
    self.m3u8s_without_skip = []

  def isCompleted(self) -> bool:
    return self.endPTS is not None

  def extinf(self) -> timedelta | None:
    if not self.endPTS:
      return None
    else:
      return timedelta(seconds = (((self.endPTS - self.beginPTS + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ))

  def estimate(self, endPTS: int) -> timedelta:
    return timedelta(seconds = (((endPTS - self.beginPTS + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ))

class Segment(PartialSegment):
  def __init__(self, beginPTS, isIFrame = False, programDateTime = None):
    super().__init__(beginPTS, isIFrame = False)
    self.partials: list[PartialSegment] = [PartialSegment(beginPTS, isIFrame)]
    self.program_date_time: datetime = programDateTime or datetime.now(timezone.utc)

  def __iter__(self) -> Iterator[PartialSegment]:
    return iter(self.partials)

  def __len__(self) -> int:
    return len(self.partials)

  def push(self, packet: bytes | bytearray | memoryview) -> None:
    super().push(packet)
    if not self.partials: return
    self.partials[-1].push(packet)

  def completePartial(self, endPTS: int) -> None:
    if not self.partials: return
    self.partials[-1].complete(endPTS)

  def notifyPartial(self, skipped_manifest: str, all_manifest: str) -> None:
    if not self.partials: return
    self.partials[-1].notify(skipped_manifest, all_manifest)

  def newPartial(self, beginPTS: int, isIFrame: bool = False) -> None:
    self.partials.append(PartialSegment(beginPTS, isIFrame))

  def complete(self, endPTS: int) -> None:
    super().complete(endPTS)
    self.completePartial(endPTS)

  def notify(self, skipped_manifest: str, all_manifest: str) -> None:
    super().notify(skipped_manifest, all_manifest)
    self.notifyPartial(skipped_manifest, all_manifest)

