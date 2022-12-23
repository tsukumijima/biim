#!/usr/bin/env python3

import math
import os
import asyncio
from datetime import datetime, timedelta

from mpeg2ts import ts

class PartialSegment:
  def __init__(self, beginPTS, isIFrame = False):
    self.beginPTS = beginPTS
    self.endPTS = None
    self.hasIFrame = isIFrame
    self.buffer = bytearray()
    self.writers = []
    self.m3u8s = []

  def push(self, packet):
    self.buffer += packet
    for w in self.writers: w.write(packet)

  async def pipe(self):
    rpipe, wpipe = os.pipe()
    r = open(rpipe, 'rb')
    w = open(wpipe, 'wb')

    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(loop=loop)
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader, loop=loop), r)

    writer_transport, writer_protocol = await loop.connect_write_pipe(lambda: asyncio.streams.FlowControlMixin(loop=loop), w)
    writer = asyncio.streams.StreamWriter(writer_transport, writer_protocol, None, loop)

    writer.write(self.buffer)
    if (self.isCompleted()):
      writer.write_eof()
    else:
      self.writers.append(writer)
    return reader

  def m3u8(self):
    f = asyncio.Future()
    if not self.isCompleted():
      self.m3u8s.append(f)
    return f

  def complete(self, endPTS):
    self.endPTS = endPTS
    for w in self.writers: w.write_eof()

  def isCompleted(self):
    return self.endPTS is not None

  def extinf(self):
    if not self.endPTS:
      return None
    else:
      return timedelta(seconds = (((self.endPTS - self.beginPTS + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ))

  def estimate(self, endPTS):
    return timedelta(seconds = (((endPTS - self.beginPTS + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ))

class Segment(PartialSegment):
  def __init__(self, beginPTS, isIFrame = False):
    super().__init__(beginPTS, isIFrame = False)
    self.partials = [PartialSegment(beginPTS, isIFrame)]
    self.program_date_time = datetime.now()

  def __iter__(self):
    return iter(self.partials)

  def __len__(self):
    return len(self.parital)

  def push(self, packet):
    super().push(packet)
    if not self.partials: return
    self.partials[-1].push(packet)

  def completePartial(self, endPTS):
    if not self.partials: return
    self.partials[-1].complete(endPTS)

  def newPartial(self, beginPTS, isIFrame = False):
    self.partials.append(PartialSegment(beginPTS, isIFrame))

  def complete(self, endPTS):
    super().complete(endPTS)
    self.completePartial(endPTS)

