from datetime import datetime, timedelta

from biim.variant.handler import VariantHandler
from biim.variant.codec import aac_codec_parameter_string
from biim.variant.codec import avc_codec_parameter_string
from biim.variant.codec import hevc_codec_parameter_string

from biim.mpeg2ts import ts
from biim.mpeg2ts.pes import PES
from biim.mpeg2ts.h264 import H264PES
from biim.mpeg2ts.h265 import H265PES
from biim.mp4.box import ftyp, moov, mvhd, mvex, trex, moof, mdat, emsg
from biim.mp4.avc import avcTrack
from biim.mp4.hevc import hevcTrack
from biim.mp4.mp4a import mp4aTrack

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

class Fmp4VariantHandler(VariantHandler):

  def __init__(self, target_duration: int, part_target: float, window_size: int | None = None, has_video: bool = True, has_audio: bool = True):
    super().__init__(target_duration, part_target, 'video/mp4', window_size, True, has_video, has_audio)
    # M3U8 Tracks
    self.audio_track: bytes | None = None
    self.video_track: bytes | None = None
    # Video Codec Specific
    self.h264_idr_detected = False
    self.h265_idr_detected = False
    self.curr_h264: tuple[bool, bytearray, int, int, datetime] | None = None # hasIDR, mdat, timestamp, cts, program_date_time
    self.curr_h265: tuple[bool, bytearray, int, int, datetime] | None = None # hasIDR, mdat, timestamp, cts, program_date_time
    # Audio Codec Specific
    self.last_aac_timestamp = None

  def h265(self, h265: H265PES):
    if (dts := h265.dts() or h265.pts()) is None: return
    if (pts := h265.pts()) is None: return
    cto = (pts - dts + ts.PCR_CYCLE) % ts.PCR_CYCLE
    if (timestamp := self.timestamp(dts)) is None: return
    if (program_date_time := self.program_date_time(dts)) is None: return

    hasIDR = False
    content = bytearray()
    vps, sps, pps = None, None, None
    for ebsp in h265:
      nal_unit_type = (ebsp[0] >> 1) & 0x3f

      if nal_unit_type == 0x20: # VPS
        vps = ebsp
      elif nal_unit_type == 0x21: # SPS
        sps = ebsp
      elif nal_unit_type == 0x22: # PPS
        pps = ebsp
      elif nal_unit_type == 0x23 or nal_unit_type == 0x27: # AUD or SEI
        pass
      elif nal_unit_type == 19 or nal_unit_type == 20 or nal_unit_type == 21: # IDR_W_RADL, IDR_W_LP, CRA_NUT
        hasIDR = True
        content += len(ebsp).to_bytes(4, byteorder='big') + ebsp
      else:
        content += len(ebsp).to_bytes(4, byteorder='big') + ebsp
    if sps and not self.video_codec.done():
      self.video_codec.set_result(hevc_codec_parameter_string(sps))
    if vps and sps and pps:
      self.video_track = hevcTrack(1, ts.HZ, vps, sps, pps)

    if self.init and not self.init.done() and self.video_track:
      if not self.has_audio:
        self.init.set_result(b''.join([
          ftyp(),
          moov(
            mvhd(ts.HZ),
            mvex([
              trex(1),
            ]),
            b''.join([
              self.video_track
            ])
          )
        ]))
      elif self.audio_track:
        self.init.set_result(b''.join([
          ftyp(),
          moov(
            mvhd(ts.HZ),
            mvex([
              trex(1),
              trex(2),
            ]),
            b''.join([
              self.video_track,
              self.audio_track
            ])
          )
        ]))

    next_h265 = (hasIDR, content, timestamp, cto, program_date_time)

    if not self.curr_h265:
      self.curr_h265 = next_h265
      return

    next_timestamp = timestamp
    hasIDR, content, timestamp, cto, program_date_time = self.curr_h265
    duration = next_timestamp - timestamp
    self.curr_h265 = next_h265

    self.h265_idr_detected|= hasIDR
    if not self.h265_idr_detected: return

    self.update(hasIDR, timestamp, program_date_time)
    self.m3u8.push(
      b''.join([
        moof(0,
          [
            (1, duration, timestamp, 0, [(len(content), duration, hasIDR, cto)])
          ]
        ),
        mdat(content)
      ])
    )

  def h264(self, h264: H264PES):
    if (dts := h264.dts() or h264.pts()) is None: return
    if (pts := h264.pts()) is None: return
    cto = (pts - dts + ts.PCR_CYCLE) % ts.PCR_CYCLE
    if (timestamp := self.timestamp(dts)) is None: return
    if (program_date_time := self.program_date_time(dts)) is None: return

    hasIDR = False
    content = bytearray()
    sps, pps = None, None
    for ebsp in h264:
      nal_unit_type = ebsp[0] & 0x1f

      if nal_unit_type == 0x07: # SPS
        sps = ebsp
      elif nal_unit_type == 0x08: # PPS
        pps = ebsp
      elif nal_unit_type == 0x09 or nal_unit_type == 0x06: # AUD or SEI
        pass
      elif nal_unit_type == 0x05:
        hasIDR = True
        content += len(ebsp).to_bytes(4, byteorder='big') + ebsp
      else:
        content += len(ebsp).to_bytes(4, byteorder='big') + ebsp

    if sps and not self.video_codec.done():
      self.video_codec.set_result(avc_codec_parameter_string(sps))
    if sps and pps:
      self.video_track = avcTrack(1, ts.HZ, sps, pps)

    if self.init and not self.init.done() and self.video_track:
      if not self.has_audio:
        self.init.set_result(b''.join([
          ftyp(),
          moov(
            mvhd(ts.HZ),
            mvex([
              trex(1),
            ]),
            b''.join([
              self.video_track
            ])
          )
        ]))
      elif self.audio_track:
        self.init.set_result(b''.join([
          ftyp(),
          moov(
            mvhd(ts.HZ),
            mvex([
              trex(1),
              trex(2),
            ]),
            b''.join([
              self.video_track,
              self.audio_track
            ])
          )
        ]))

    next_h264 = (hasIDR, content, timestamp, cto, program_date_time)

    if not self.curr_h264:
      self.curr_h264 = next_h264
      return

    next_timestamp = timestamp
    hasIDR, content, timestamp, cto, program_date_time = self.curr_h264
    duration = next_timestamp - timestamp
    self.curr_h264 = next_h264

    self.h264_idr_detected|= hasIDR
    if not self.h264_idr_detected: return

    self.update(hasIDR, timestamp, program_date_time)
    self.m3u8.push(
      b''.join([
        moof(0,
          [
            (1, duration, timestamp, 0, [(len(content), duration, hasIDR, cto)])
          ]
        ),
        mdat(content)
      ])
    )

  def aac(self, aac: PES):
    if (timestamp := self.timestamp(aac.pts())) is None: return
    if (program_date_time := self.program_date_time(aac.pts())) is None: return

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

      if not self.audio_track:
        config = bytes([
          ((profile + 1) << 3) | ((samplingFrequencyIndex & 0x0E) >> 1),
          ((samplingFrequencyIndex & 0x01) << 7) | (channelConfiguration << 3)
        ])
        self.audio_track = mp4aTrack(2, ts.HZ, config, channelConfiguration, AAC_SAMPLING_FREQUENCY[samplingFrequencyIndex])

      if self.init and not self.init.done() and self.audio_track:
        if not self.has_video:
          self.init.set_result(b''.join([
            ftyp(),
            moov(
              mvhd(ts.HZ),
              mvex([
                trex(2)
              ]),
              b''.join([
                self.audio_track
              ])
            )
          ]))

      if not self.has_video:
        self.update(None, timestamp, program_date_time)

      self.m3u8.push(
        b''.join([
          moof(0,
            [
              (2, duration, timestamp, 0, [(frameLength - (9 if protection else 7), duration, False, 0)])
            ]
          ),
          mdat(bytes(ADTS_AAC[begin + (9 if protection else 7): begin + frameLength]))
        ])
      )

      timestamp += duration
      program_date_time += timedelta(seconds=duration/ts.HZ)
      begin += frameLength

  def id3(self, id3: PES):
    if (timestamp := self.timestamp(id3.pts())) is None: return
    self.m3u8.push(emsg(ts.HZ, timestamp, None, 'https://aomedia.org/emsg/ID3', id3.PES_packet_data()))

