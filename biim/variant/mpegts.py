from datetime import datetime, timedelta

from biim.variant.handler import VariantHandler
from biim.variant.codec import aac_codec_parameter_string
from biim.variant.codec import avc_codec_parameter_string
from biim.variant.codec import hevc_codec_parameter_string

from biim.mpeg2ts import ts
from biim.mpeg2ts.packetize import packetize_section, packetize_pes
from biim.mpeg2ts.section import Section
from biim.mpeg2ts.pes import PES
from biim.mpeg2ts.h264 import H264PES
from biim.mpeg2ts.h265 import H265PES

AAC_SAMPLING_FREQUENCY = {
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

  def PAT(self, PAT: Section):
    if PAT.CRC32() != 0: return
    self.last_pat = PAT

  def PMT(self, pid: int, PMT: Section):
    if PMT.CRC32() != 0: return
    self.last_pmt = PMT
    self.pmt_pid = pid

  def update(self, new_segment: bool | None, timestamp: int, program_date_time: datetime) -> bool:
    if self.last_pat is None or self.last_pmt is None or self.pmt_pid is None: return False
    if not super().update(new_segment, timestamp, program_date_time): return False

    packets = packetize_section(self.last_pat, False, False, 0x00, 0, self.pat_cc)
    self.pat_cc = (self.pat_cc + len(packets)) & 0x0F
    for p in packets: self.m3u8.push(p)
    packets = packetize_section(self.last_pmt, False, False, self.pmt_pid, 0, self.pmt_cc)
    self.pmt_cc = (self.pmt_cc + len(packets)) & 0x0F
    for p in packets: self.m3u8.push(p)
    return True

  def h265(self, pid: int, h265: H265PES):
    if (timestamp := self.timestamp(h265.dts() or h265.pts())) is None: return
    if (program_date_time := self.program_date_time(h265.dts() or h265.pts())) is None: return

    hasIDR = False
    sps = None
    for ebsp in h265:
      nal_unit_type = (ebsp[0] >> 1) & 0x3f

      if nal_unit_type == 0x21: # SPS
        sps = ebsp
      elif nal_unit_type == 19 or nal_unit_type == 20 or nal_unit_type == 21: # IDR_W_RADL, IDR_W_LP, CRA_NUT
        hasIDR = True

    if sps and not self.video_codec.done():
      self.video_codec.set_result(hevc_codec_parameter_string(sps))

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
    sps = None
    for ebsp in h264:
      nal_unit_type = ebsp[0] & 0x1f

      if nal_unit_type == 0x07: # SPS
        sps = ebsp
      elif nal_unit_type == 0x05:
        hasIDR = True

    if sps and not self.video_codec.done():
      self.video_codec.set_result(avc_codec_parameter_string(sps))
    self.h264_idr_detected |= hasIDR
    if not self.h264_idr_detected: return

    self.update(hasIDR, timestamp, program_date_time)

    packets = packetize_pes(h264, False, False, pid, 0, self.h264_cc)
    self.h264_cc = (self.h264_cc + len(packets)) & 0x0F
    for p in packets: self.m3u8.push(p)

  def aac(self, pid: int, aac: PES):
    if (timestamp := self.timestamp(aac.pts())) is None: return
    if (program_date_time := self.program_date_time(aac.pts())) is None: return

    if not self.has_video:
      self.update(None, timestamp, program_date_time)

    packets = packetize_pes(aac, False, False, pid, 0, self.aac_cc)
    self.aac_cc = (self.aac_cc + len(packets)) & 0x0F
    for p in packets: self.m3u8.push(p)

    begin, ADTS_AAC = 0, aac.PES_packet_data()
    length = len(ADTS_AAC)
    while begin < length:
      protection = (ADTS_AAC[begin + 1] & 0b00000001) == 0
      profile = ((ADTS_AAC[begin + 2] & 0b11000000) >> 6)
      samplingFrequencyIndex = ((ADTS_AAC[begin + 2] & 0b00111100) >> 2)
      channelConfiguration = ((ADTS_AAC[begin + 2] & 0b00000001) << 2) | ((ADTS_AAC[begin + 3] & 0b11000000) >> 6)
      frameLength = ((ADTS_AAC[begin + 3] & 0x03) << 11) | (ADTS_AAC[begin + 4] << 3) | ((ADTS_AAC[begin + 5] & 0xE0) >> 5)
      duration = 1024 * ts.HZ // AAC_SAMPLING_FREQUENCY[samplingFrequencyIndex]

      if not self.audio_codec.done():
        self.audio_codec.set_result(aac_codec_parameter_string(profile + 1))

      timestamp += duration
      program_date_time += timedelta(seconds=duration/ts.HZ)
      begin += frameLength

  def packet(self, packet: bytes | bytearray | memoryview):
    self.m3u8.push(packet)
