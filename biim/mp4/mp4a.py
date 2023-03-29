from biim.mp4.box import trak, tkhd, mdia, mdhd, hdlr, minf, smhd, dinf, stbl, stsd, mp4a

def mp4aTrack(trackId: int, timescale: int, config: bytes | bytearray | memoryview, channelCount: int, sampleRate: int) -> bytes:
  return trak(
    tkhd(trackId, 0, 0),
    mdia(
      mdhd(timescale),
      hdlr('soun', 'soundHandler'),
      minf(
        smhd(),
        dinf(),
        stbl(
          stsd(
            mp4a(config, channelCount, sampleRate)
          )
        )
      )
    )
  )
