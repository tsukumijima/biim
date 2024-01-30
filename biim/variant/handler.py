import asyncio
from aiohttp import web

from abc import ABC
from typing import cast
from collections import deque
from datetime import datetime, timezone, timedelta

from biim.hls.m3u8 import M3U8
from biim.mpeg2ts import ts
from biim.mpeg2ts.scte import SpliceInfoSection, SpliceInsert, TimeSignal, SegmentationDescriptor

class VariantHandler(ABC):

  def __init__(self, target_duration: int, part_target: float, content_type: str, window_size: int | None = None, has_init: bool = False, has_video: bool = True, has_audio: bool = True):
    self.target_duration = target_duration
    self.part_target = part_target
    self.segment_timestamp: int | None = None
    self.part_timestamp: int | None = None

    # M3U8
    self.m3u8 = M3U8(target_duration=target_duration, part_target=part_target, window_size=window_size, has_init=has_init)
    self.init = asyncio.Future[bytes | bytearray | memoryview]() if has_init else None
    self.content_type = content_type
    self.has_video = has_video
    self.has_audio = has_audio
    self.video_codec = asyncio.Future[str]()
    self.audio_codec = asyncio.Future[str]()
    # PCR
    self.latest_pcr_value: int | None = None
    self.latest_pcr_datetime: datetime | None = None
    self.latest_pcr_monotonic_timestamp_90khz: int = 0
    # SCTE35
    self.scte35_out_queue: deque[tuple[str, datetime, datetime | None, dict]] = deque()
    self.scte35_in_queue: deque[tuple[str, datetime]] = deque()
    # Bitrate
    self.bitrate = asyncio.Future[int]()

  async def playlist(self, request: web.Request) -> web.Response:
    msn = request.query['_HLS_msn'] if '_HLS_msn' in request.query else None
    part = request.query['_HLS_part'] if '_HLS_part' in request.query else None
    skip = request.query['_HLS_skip'] == 'YES' if '_HLS_skip' in request.query else False

    if msn is None and part is None:
      future = self.m3u8.plain()
      if future is None:
        return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=400, content_type="application/x-mpegURL")

      result = await future
      return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, text=result, content_type="application/x-mpegURL")
    else:
      if msn is None:
        return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=400, content_type="application/x-mpegURL")
      msn = int(msn)
      if part is None: part = 0
      part = int(part)
      future = self.m3u8.blocking(msn, part, skip)
      if future is None:
        return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=400, content_type="application/x-mpegURL")

      result = await future
      return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=36000'}, text=result, content_type="application/x-mpegURL")

  async def segment(self, request: web.Request) -> web.Response | web.StreamResponse:
    msn = request.query['msn'] if 'msn' in request.query else None

    if msn is None:
      return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=400, content_type=self.content_type)
    msn = int(msn)
    queue = await self.m3u8.segment(msn)
    if queue is None:
      return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=400, content_type=self.content_type)

    response = web.StreamResponse(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=36000', 'Content-Type': self.content_type}, status=200)
    await response.prepare(request)

    while True:
      stream = await queue.get()
      if stream == None : break
      await response.write(stream)

    await response.write_eof()
    return response

  async def partial(self, request: web.Request) -> web.Response | web.StreamResponse:
    msn = request.query['msn'] if 'msn' in request.query else None
    part = request.query['part'] if 'part' in request.query else None

    if msn is None:
      return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=400, content_type=self.content_type)
    msn = int(msn)
    if part is None:
      return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=400, content_type=self.content_type)
    part = int(part)
    queue = await self.m3u8.partial(msn, part)
    if queue is None:
      return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=400, content_type=self.content_type)

    response = web.StreamResponse(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=36000', 'Content-Type': self.content_type}, status=200)
    await response.prepare(request)

    while True:
      stream = await queue.get()
      if stream == None : break
      await response.write(stream)

    await response.write_eof()
    return response

  async def initialization(self, _: web.Request) -> web.Response:
    if self.init is None:
      return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=0'}, status=400, content_type=self.content_type)

    body = await asyncio.shield(self.init)
    return web.Response(headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=36000'}, body=body, content_type=self.content_type)

  async def bandwidth(self) -> int:
    return await self.m3u8.bandwidth()

  async def codec(self) -> str:
    if self.has_video and self.has_audio:
      return f'{await self.video_codec},{await self.audio_codec}'
    elif self.has_video:
      return f'{await self.video_codec}'
    elif self.has_audio:
      return f'{await self.audio_codec}'
    else:
      return ''

  def set_renditions(self, renditions: list[str]):
    self.m3u8.set_renditions(renditions)

  def program_date_time(self, pts: int | None) -> datetime | None:
    if self.latest_pcr_value is None or self.latest_pcr_datetime is None or pts is None: return None
    return self.latest_pcr_datetime + timedelta(seconds=(((pts - self.latest_pcr_value + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ))

  def timestamp(self, pts: int | None) -> int | None:
    if self.latest_pcr_value is None or pts is None: return None
    return ((pts - self.latest_pcr_value + ts.PCR_CYCLE) % ts.PCR_CYCLE) + self.latest_pcr_monotonic_timestamp_90khz

  def update(self, new_segment: bool | None, timestamp: int, program_date_time: datetime) -> bool:
    # SCTE35
    if new_segment:
      while self.scte35_out_queue:
        if self.scte35_out_queue[0][1] <= program_date_time:
          id, _, end_date, attributes = self.scte35_out_queue.popleft()
          self.m3u8.open(id, program_date_time, end_date, **attributes) # SCTE-35 の OUT を セグメント にそろえてる
        else: break
      while self.scte35_in_queue:
        if self.scte35_in_queue[0][1] <= program_date_time:
          id, _ = self.scte35_in_queue.popleft()
          self.m3u8.close(id, program_date_time)  # SCTE-35 の IN を セグメント にそろえてる
        else: break
    # M3U8
    if new_segment or (new_segment is None and (self.segment_timestamp is None or (timestamp - self.segment_timestamp) >= self.target_duration * ts.HZ)):
      if self.part_timestamp is not None:
        part_diff = timestamp - self.part_timestamp
        if self.part_target * ts.HZ < part_diff:
          self.part_timestamp = int(timestamp - max(0, part_diff - self.part_target * ts.HZ))
          self.m3u8.continuousPartial(self.part_timestamp, False)
      self.part_timestamp = timestamp
      self.segment_timestamp = timestamp
      self.m3u8.continuousSegment(self.part_timestamp, True, program_date_time)
      return True
    elif self.part_timestamp is not None:
      part_diff = timestamp - self.part_timestamp
      if self.part_target * ts.HZ <= part_diff:
        self.part_timestamp = int(timestamp - max(0, part_diff - self.part_target * ts.HZ))
        self.m3u8.continuousPartial(self.part_timestamp)
    return False

  def pcr(self, pcr: int):
    pcr = (pcr - ts.HZ + ts.PCR_CYCLE) % ts.PCR_CYCLE
    diff = ((pcr - self.latest_pcr_value + ts.PCR_CYCLE) % ts.PCR_CYCLE) if self.latest_pcr_value is not None else 0
    self.latest_pcr_monotonic_timestamp_90khz += diff
    if self.latest_pcr_datetime is None: self.latest_pcr_datetime = datetime.now(timezone.utc) - timedelta(seconds=(1))
    self.latest_pcr_datetime += timedelta(seconds=(diff / ts.HZ))
    self.latest_pcr_value = pcr

  def scte35(self, scte35: SpliceInfoSection):
    if scte35.CRC32() != 0: return

    if scte35.splice_command_type == SpliceInfoSection.SPLICE_INSERT:
      splice_insert: SpliceInsert = cast(SpliceInsert, scte35.splice_command)
      id = str(splice_insert.splice_event_id)
      if splice_insert.splice_event_cancel_indicator: raise NotImplementedError()
      if not splice_insert.program_splice_flag: raise NotImplementedError()
      if splice_insert.out_of_network_indicator:
        attributes = { 'SCTE35-OUT': '0x' + ''.join([f'{b:02X}' for b in scte35[:]]) }
        if splice_insert.splice_immediate_flag or not splice_insert.splice_time.time_specified_flag:
          if self.latest_pcr_datetime is None: return
          start_date = self.latest_pcr_datetime

          if splice_insert.duration_flag:
            attributes['PLANNED-DURATION'] = str(splice_insert.break_duration.duration / ts.HZ)
            if splice_insert.break_duration.auto_return:
              self.scte35_in_queue.append((id, start_date + timedelta(seconds=(splice_insert.break_duration.duration / ts.HZ))))
          self.scte35_out_queue.append((id, start_date, None, attributes))
        else:
          if self.latest_pcr_value is None: return
          if self.latest_pcr_datetime is None: return
          start_date = timedelta(seconds=(((cast(int, splice_insert.splice_time.pts_time) + scte35.pts_adjustment - self.latest_pcr_value + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ)) + self.latest_pcr_datetime

          if splice_insert.duration_flag:
            attributes['PLANNED-DURATION'] = str(splice_insert.break_duration.duration / ts.HZ)
            if splice_insert.break_duration.auto_return:
              self.scte35_in_queue.append((id, start_date + timedelta(seconds=(splice_insert.break_duration.duration / ts.HZ))))
          self.scte35_out_queue.append((id, start_date, None, attributes))
      else:
        if splice_insert.splice_immediate_flag or not splice_insert.splice_time.time_specified_flag:
          if self.latest_pcr_datetime is None: return
          end_date = self.latest_pcr_datetime
          self.scte35_in_queue.append((id, end_date))
        else:
          if self.latest_pcr_value is None: return
          if self.latest_pcr_datetime is None: return
          end_date = timedelta(seconds=(((cast(int, splice_insert.splice_time.pts_time) + scte35.pts_adjustment - self.latest_pcr_value + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ)) + self.latest_pcr_datetime
          self.scte35_in_queue.append((id, end_date))

    elif scte35.splice_command_type == SpliceInfoSection.TIME_SIGNAL:
      time_signal: TimeSignal = cast(TimeSignal, scte35.splice_command)
      if self.latest_pcr_value is None: return
      if self.latest_pcr_datetime is None: return
      specified_time = self.latest_pcr_datetime
      if time_signal.splice_time.time_specified_flag:
        specified_time = timedelta(seconds=(((cast(int, time_signal.splice_time.pts_time) + scte35.pts_adjustment - self.latest_pcr_value + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ)) + self.latest_pcr_datetime
      for descriptor in scte35.descriptors:
        if descriptor.descriptor_tag != 0x02: return
        segmentation_descriptor: SegmentationDescriptor = cast(SegmentationDescriptor, descriptor)
        id = str(segmentation_descriptor.segmentation_event_id)
        if segmentation_descriptor.segmentation_event_cancel_indicator: raise NotImplementedError()
        if not segmentation_descriptor.program_segmentation_flag: raise NotImplementedError()

        if segmentation_descriptor.segmentation_event_id in SegmentationDescriptor.ADVERTISEMENT_BEGIN:
          attributes = { 'SCTE35-OUT': '0x' + ''.join([f'{b:02X}' for b in scte35[:]]) }
          if segmentation_descriptor.segmentation_duration_flag:
            attributes['PLANNED-DURATION'] = str(segmentation_descriptor.segmentation_duration / ts.HZ)
          self.scte35_out_queue.append((id, specified_time, None, attributes))
        elif segmentation_descriptor.segmentation_type_id in SegmentationDescriptor.ADVERTISEMENT_END:
          self.scte35_in_queue.append((id, specified_time))
