from mp4.box import trak, tkhd, mdia, mdhd, hdlr, minf, smhd, dinf, stbl, stsd, mp4a

def mp4aTrack(trackId, timescale, config, channelCount, sampleRate):
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