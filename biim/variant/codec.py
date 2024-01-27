import re

from biim.util.bitstream import BitStream

escapes = set([0x00, 0x01, 0x02, 0x03])
def ebsp2rbsp(data: bytes | bytearray | memoryview) -> bytes:
  rbsp = bytearray(data[:2])
  length = len(data)
  for index in range(2, length):
    if index < length - 1 and data[index - 2] == 0x00 and data[index - 1] == 0x00 and data[index + 0] == 0x03 and data[index + 1] in escapes:
      continue
    rbsp.append(data[index])
  return bytes(rbsp)

def aac_codec_parameter_string(audioObjectType: int):
  return f'mp4a.40.{audioObjectType}'

def avc_codec_parameter_string(sps: bytes | bytearray | memoryview):
  profile_idc = sps[1]
  constraint_flags = sps[2]
  level_idc = sps[3]
  return f'avc1.{profile_idc:02x}{constraint_flags:02x}{level_idc:02x}'

def hevc_codec_parameter_string(sps: bytes | bytearray | memoryview):
  stream = BitStream(ebsp2rbsp(sps))
  stream.readByte(2) # remove header
  stream.readByte() # video_paramter_set_id, max_sub_layers_minus1, temporal_id_nesting_flag

  general_profile_space = ['', 'A', 'B', 'C'][stream.readBits(2)]
  general_tier_flag = 'H' if stream.readBool() else 'L'
  general_profile_idc = stream.readBits(5)
  general_profile_compatibility_flags = stream.readByte(4)
  general_profile_compatibility = 0
  for i in range(32): general_profile_compatibility |= ((general_profile_compatibility_flags >> i) & 1) << (31 - i)
  general_constraint_indicator_flags = stream.readByte(6).to_bytes(6, byteorder='big')
  general_level_idc = stream.readByte()

  codec_parameter_string = f'hvc1.{general_profile_space}{general_profile_idc}.{general_profile_compatibility:X}.{general_tier_flag}{general_level_idc}'
  if general_constraint_indicator_flags[5] != 0: codec_parameter_string += f'.{general_constraint_indicator_flags[5]:X}'
  if general_constraint_indicator_flags[4] != 0: codec_parameter_string += f'.{general_constraint_indicator_flags[4]:X}'
  if general_constraint_indicator_flags[3] != 0: codec_parameter_string += f'.{general_constraint_indicator_flags[3]:X}'
  if general_constraint_indicator_flags[2] != 0: codec_parameter_string += f'.{general_constraint_indicator_flags[2]:X}'
  if general_constraint_indicator_flags[1] != 0: codec_parameter_string += f'.{general_constraint_indicator_flags[1]:X}'
  if general_constraint_indicator_flags[0] != 0: codec_parameter_string += f'.{general_constraint_indicator_flags[0]:X}'

  return codec_parameter_string
