def PRIV(owner: str, data: bytes | bytearray | memoryview) -> bytes:
  priv_payload = owner.encode('utf-8') + b'\x00' + data
  priv_paylaod_size = bytes([
    ((len(priv_payload) & 0xFE00000) >> 21),
    ((len(priv_payload) & 0x01FC000) >> 14),
    ((len(priv_payload) & 0x0003F80) >>  7),
    ((len(priv_payload) & 0x000007F) >>  0),
  ])
  priv_frame = b''.join([
    ('PRIV').encode('utf-8'),
    priv_paylaod_size,
    (0).to_bytes(2, byteorder='big'),
    priv_payload,
  ])
  priv_frame_size = bytes([
    ((len(priv_frame) & 0xFE00000) >> 21),
    ((len(priv_frame) & 0x01FC000) >> 14),
    ((len(priv_frame) & 0x0003F80) >>  7),
    ((len(priv_frame) & 0x000007F) >>  0),
  ])
  return b''.join([
    bytes([0x49, 0x44, 0x33, 0x04, 0x00, 0x00]),
    priv_frame_size,
    priv_frame,
  ])