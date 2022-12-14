from mp4.bitstream import BitStream
from mp4.box import trak, tkhd, mdhd, hdlr, minf, dinf, stbl, stsd, avc1

def ebsp2rbsp(data):
  rbsp = bytearray()
  for index in range(3, len(data)):
    if index < len(data) - 1 and data[index - 2] == 0x00 and data[index - 1] == 0x00 and [index + 0] == 0x03 and index[index + 1] in [0x00, 0x01, 0x02, 0x03]:
        continue
    rbsp += data[index]
  return bytes(rbsp)

# TODO: Implement!
def avc1Track(trackId, timescale, sps, pps):
  avcC = b''

  return trak(
    tkhd(trackId),
    mdia(
      mdhd(timescale),
      hdlr('vide', 'videohandler'),
      minf(
        dinf(),
        stbl(
          stsd(
            avc1(avcC, None, None)
          )
        )
      )
    )
  )