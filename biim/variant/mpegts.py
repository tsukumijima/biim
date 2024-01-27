from abc import ABC, abstractmethod
from typing import cast
from datetime import datetime, timezone, timedelta

from biim.variant.handler import VariantHandler
from biim.mpeg2ts import ts
from biim.mpeg2ts.packetize import packetize_section, packetize_pes
from biim.mpeg2ts.section import Section
from biim.mpeg2ts.pes import PES
from biim.mpeg2ts.h264 import H264PES
from biim.mpeg2ts.h265 import H265PES

class MpegtsVariantHandler(VariantHandler):

  def __init__(self, target_duration: int, part_target: float, window_size: int | None = None, has_video: bool = True, has_audio: bool = True):
    super().__init__(target_duration, part_target, 'video/mp2t', window_size, False, has_video, has_audio)
    # PAT/PMT
    self.last_pat: Section | None = None
    self.last_pmt: Section | None = None
    self.pmt_pid: int | None = None
    self.pat_cc = 0
    self.pmt_cc = 0
    # Video Codec Specific
    self.h264_idr_detected = False
    self.h265_idr_detected = False
    self.h264_cc = 0
    self.h265_cc = 0
    # Audio Codec Specific
    self.aac_cc = 0

  def program_date_time(self, pts: int | None) -> datetime | None:
    if self.latest_pcr_value is None or self.latest_pcr_datetime is None or pts is None: return None
    return self.latest_pcr_datetime + timedelta(seconds=(((pts - self.latest_pcr_value + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ))

  def timestamp(self, pts: int | None) -> int | None:
    if self.latest_pcr_value is None or pts is None: return None
    return ((pts - self.latest_pcr_value + ts.PCR_CYCLE) % ts.PCR_CYCLE) + self.latest_pcr_monotonic_timestamp_90khz

  def PAT(self, PAT: Section):
    if PAT.CRC32() != 0: return
    self.last_pat = PAT

  def PMT(self, pid: int, PMT: Section):
    if PMT.CRC32() != 0: return
    self.last_pmt = PMT
    self.pmt_pid = pid

  def update(self, new_segment: bool, timestamp: int, program_date_time: datetime):
    super().update(new_segment, timestamp, program_date_time)
    if not new_segment: return
    if self.last_pat is None or self.last_pmt is None or self.pmt_pid is None: return

    packets = packetize_section(self.last_pat, False, False, 0x00, 0, self.pat_cc)
    self.pat_cc = (self.pat_cc + len(packets)) & 0x0F
    for p in packets: self.m3u8.push(p)
    packets = packetize_section(self.last_pmt, False, False, self.pmt_pid, 0, self.pmt_cc)
    self.pmt_cc = (self.pmt_cc + len(packets)) & 0x0F
    for p in packets: self.m3u8.push(p)

  def h265(self, pid: int, h265: H265PES):
    if (timestamp := self.timestamp(h265.dts() or h265.pts())) is None: return
    if (program_date_time := self.program_date_time(h265.dts() or h265.pts())) is None: return

    hasIDR = False
    for ebsp in h265:
      nal_unit_type = (ebsp[0] >> 1) & 0x3f

      if nal_unit_type == 19 or nal_unit_type == 20 or nal_unit_type == 21: # IDR_W_RADL, IDR_W_LP, CRA_NUT
        hasIDR = True

    self.h265_idr_detected |= hasIDR
    if not self.h265_idr_detected: return

    self.update(hasIDR, timestamp, program_date_time)

    packets = packetize_pes(h265, False, False, pid, 0, self.h265_cc)
    self.h265_cc = (self.h265_cc + len(packets)) & 0x0F
    for p in packets: self.m3u8.push(p)

  def h264(self, pid: int, h264: H264PES):
    if (timestamp := self.timestamp(h264.dts() or h264.pts())) is None: return
    if (program_date_time := self.program_date_time(h264.dts() or h264.pts())) is None: return

    hasIDR = False
    for ebsp in h264:
      nal_unit_type = ebsp[0] & 0x1f

      if nal_unit_type == 0x05:
        hasIDR = True

    self.h264_idr_detected |= hasIDR
    if not self.h264_idr_detected: return

    self.update(hasIDR, timestamp, program_date_time)

    packets = packetize_pes(h264, False, False, pid, 0, self.h264_cc)
    self.h264_cc = (self.h264_cc + len(packets)) & 0x0F
    for p in packets: self.m3u8.push(p)

  def aac(self, pid: int, aac: PES):
    packets = packetize_pes(aac, False, False, pid, 0, self.aac_cc)
    self.aac_cc = (self.aac_cc + len(packets)) & 0x0F
    for p in packets: self.m3u8.push(p)


  def packet(self, packet: bytes | bytearray | memoryview):
    self.m3u8.push(packet)
