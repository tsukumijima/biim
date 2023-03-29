def TXXX(description: str, text: str) -> bytes:
  txxx_payload = b''.join([
    b'\x03', # utf-8
    description.encode('utf-8'),
    b'\x00',
    text.encode('utf-8'),
    b'\x00'
  ])

  txxx_paylaod_size = bytes([
    ((len(txxx_payload) & 0xFE00000) >> 21),
    ((len(txxx_payload) & 0x01FC000) >> 14),
    ((len(txxx_payload) & 0x0003F80) >>  7),
    ((len(txxx_payload) & 0x000007F) >>  0),
  ])
  txxx_frame = b''.join([
    ('TXXX').encode('utf-8'),
    txxx_paylaod_size,
    (0).to_bytes(2, byteorder='big'),
    txxx_payload,
  ])
  txxx_frame_size = bytes([
    ((len(txxx_frame) & 0xFE00000) >> 21),
    ((len(txxx_frame) & 0x01FC000) >> 14),
    ((len(txxx_frame) & 0x0003F80) >>  7),
    ((len(txxx_frame) & 0x000007F) >>  0),
  ])
  return b''.join([
    bytes([0x49, 0x44, 0x33, 0x04, 0x00, 0x00]),
    txxx_frame_size,
    txxx_frame,
  ])