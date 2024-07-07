from abc import ABC, abstractmethod

from biim.rtmp.demuxer import FLVDemuxer

class FLVRemuxer(FLVDemuxer):
  def __init__(self, initial_track: int):
    self.next_track = initial_track
    self.video_tracks = dict[int, tuple[int, bytes]]() # FLV TrackId -> Track, Configuration
    self.audio_tracks = dict[int, tuple[int, bytes]]() # FLV TrackID -> Track, Configuration

  def onAVCDecoderConfigurationRecord(self, timestamp: int, track_id: int | None, data: memoryview):
    track = track_id + 1 if track_id is not None else 0
    if track in self.video_tracks:
      id, avcC = self.video_tracks[track]
      if avcC == data: return

      self.video_tracks[track] = (id, data)
      self.onTrackConfigurationChanged(timestamp, track, 'avc1', self.remuxAVCDecoderConfigurationRecord(track, data))
      return

    self.video_tracks[track] = (self.next_track, data)
    id = self.next_track
    self.next_track += 1
    self.onTrackAdded(timestamp, id, 'avc1', self.remuxAVCDecoderConfigurationRecord(track, data))

  def onAVCVideoData(self, timestamp: int, track_id: int | None, frame_type: int, cto: int, data: memoryview):
    track = track_id + 1 if track_id is not None else 0
    if track not in self.video_tracks: return
    id, _ = self.video_tracks[track]
    self.onMediaData(timestamp, id, 'avc1', self.remuxAVCVideoData(track, frame_type, cto, data))

  def onAVCEndOfSequence(self, timestamp: int, track_id: int | None):
    track = track_id + 1 if track_id is not None else 0
    if track not in self.video_tracks: return
    id, _ = self.video_tracks[track]
    self.onTrackRemoved(timestamp, id, 'avc1')

  @abstractmethod
  def remuxAVCDecoderConfigurationRecord(self, track: int, avcC: bytes | bytearray | memoryview) -> bytes:
    pass

  @abstractmethod
  def remuxAVCVideoData(self, track: int, frame_type: int, cto: int, data: bytes | bytearray | memoryview) -> bytes:
    pass

  @abstractmethod
  def onTrackAdded(self, timestamp: int, track: int, codec: str, remuxed: bytes):
    pass

  @abstractmethod
  def onTrackConfigurationChanged(self, timestamp: int, track: int, codec: str, remuxed: bytes):
    pass

  @abstractmethod
  def onTrackRemoved(self, timestamp: int, track: int, codec: str):
    pass

  @abstractmethod
  def onMediaData(self, timestamp: int, track: int, codec: str, remuxed: bytes):
    pass

# TODO!!
class FLVfMP4Remuxer(FLVRemuxer):
  def __init__(self):
    super().__init__(initial_track=1) # Track is track_id (fMP4)

  def remuxAVCDecoderConfigurationRecord(self, track: int, avcC: bytes | bytearray | memoryview) -> bytes:
    return b''

  def remuxAVCVideoData(self, track: int, frame_type: int, cto: int, data: bytes | bytearray | memoryview) -> bytes:
    return b''

  def onTrackAdded(self, timestamp: int, track: int, codec: str, remuxed: bytes):
    print('added', timestamp, track, codec, remuxed)
    pass
  def onTrackConfigurationChanged(self, timestamp: int, track: int, codec: str, remuxed: bytes):
    print('changed', timestamp, track, codec, remuxed)
    pass
  def onTrackRemoved(self, timestamp: int, codec: str, track: int):
    pass
  def onMediaData(self, timestamp: int, track: int, codec: str, remuxed: bytes):
    print('media', timestamp, track, codec, remuxed)
    pass
