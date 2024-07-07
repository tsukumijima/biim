from abc import ABC, abstractmethod

from biim.rtmp.rtmp import Message
from biim.util.bytestream import ByteStream

# TODO!
class FLVDemuxer(ABC):
  def __init__(self):
    pass

  def parseRTMP(self, message: Message):
    try:
      match message.message_type_id:
        case 0x08: pass # Audio
        case 0x09: self.parseVideoData(message.timestamp, ByteStream(message.chunk)) # Video
        case 0x12: pass # Data (AMF0)
    except EOFError:
      return

  def parseVideoData(self, timestamp: int, stream: ByteStream):
    spec = stream.readU8()
    is_exheader = (spec & 0b10000000) != 0
    if is_exheader:
      pass
    else:
      frame_type = (spec & 0b11110000) >> 4
      codec_id = spec & 0b00001111
      match codec_id:
        case 7: self.parseLegacyAVCVideoPacket(timestamp, frame_type, stream)

  def parseLegacyAVCVideoPacket(self, timestamp: int, frame_type: int, stream: ByteStream):
    packet_type = stream.readU8()
    cto = stream.readS24()
    match packet_type:
      case 0: self.onAVCDecoderConfigurationRecord(timestamp, None, stream.readAll()) # AVCDecoderConfigurationRecord
      case 1: self.onAVCVideoData(timestamp, None, frame_type, cto, stream.readAll()) # AVCVideoData
      case 2: self.onAVCEndOfSequence(timestamp, None) # End of Sequence
    pass

  @abstractmethod
  def onAVCDecoderConfigurationRecord(self, timestamp: int, track_id: int | None, data: memoryview):
    pass

  @abstractmethod
  def onAVCVideoData(self, timestamp: int, track_id: int | None, frame_type: int, cto: int, data: memoryview):
    pass

  @abstractmethod
  def onAVCEndOfSequence(self, timestamp: int, track_id: int | None):
    pass

