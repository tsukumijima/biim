import asyncio
import random
from dataclasses import dataclass
from itertools import batched
from enum import Enum, auto

import biim.rtmp.amf0 as amf0

STREAM_TYPE_ID_AUDIO = 0x08
STREAM_TYPE_ID_VIDEO = 0x09
STREAM_TYPE_ID_DATA = 0x12
STREAM_TYPE_ID_FOR_MEDIA = set([STREAM_TYPE_ID_AUDIO, STREAM_TYPE_ID_VIDEO, STREAM_TYPE_ID_DATA])

@dataclass
class Message:
  message_type_id: int
  message_stream_id: int
  message_length: int
  timestamp: int
  chunk: bytes

async def receive_message(reader: asyncio.StreamReader):
  chunk_length = 128 # Maximum Chunk length (initial value: 128)
  chunk_memory: dict[int, Message] = dict()

  try:
    while not reader.at_eof():
      first = int.from_bytes(await reader.readexactly(1), byteorder='big')
      fmt = (first & 0xC0) >> 6
      cs_id = first & 0x3F
      if cs_id == 0:
        cs_id = 64 + int.from_bytes(await reader.readexactly(1), byteorder='little')
      elif cs_id == 1:
        cs_id = 64 + int.from_bytes(await reader.readexactly(2), byteorder='little')

      # determine timestamp
      extended_timestamp = False
      timestamp = chunk_memory[cs_id].timestamp if cs_id in chunk_memory else None
      if fmt in [0, 1, 2]: # has timestamp
        timestamp = int.from_bytes(await reader.readexactly(3), byteorder='big')
        if timestamp >= 0xFFFFFF: extended_timestamp = True # has extended1 timestamp
        elif fmt in [1, 2]: # has delta timestampe
          timestamp += chunk_memory[cs_id].timestamp
      elif timestamp is None: # when reference previous timestamp is missing, ignore it
        continue

      # determine message_length and message_type_id
      if fmt in [0, 1]:
        message_length = int.from_bytes(await reader.readexactly(3), byteorder='big')
        message_type_id = int.from_bytes(await reader.readexactly(1), byteorder='big')
      else:
        message_length = chunk_memory[cs_id].message_length
        message_type_id = chunk_memory[cs_id].message_type_id

      # determine message_stream_id
      if fmt == 0:
        message_stream_id = int.from_bytes(await reader.readexactly(4), byteorder='little')
      else:
        message_stream_id = chunk_memory[cs_id].message_stream_id

      # build next chunk
      if fmt == 3:
        chunk = chunk_memory[cs_id].chunk if cs_id in chunk_memory else b''
      else:
        chunk = b''

      # determine extended timestamp
      if extended_timestamp:
        timestamp = int.from_bytes(await reader.readexactly(4), byteorder='big')
        if fmt in [1, 2]: # has delta timestampe
          timestamp += chunk_memory[cs_id].timestamp

      # chunk_lenght is maximum value, so terminate early can occured
      chunk_memory[cs_id] = Message(message_type_id, message_stream_id, message_length, timestamp, chunk + (await reader.readexactly(min(message_length - len(chunk), chunk_length))))
      if chunk_memory[cs_id].message_length <= len(chunk_memory[cs_id].chunk):
        if chunk_memory[cs_id].message_type_id == 1: # "Set Chunk Size" message recieved, slightly change chunk_length (librtmp and obs compatible)
          chunk_length = int.from_bytes(chunk_memory[cs_id].chunk, byteorder='big')
        else: # other message are propagate
          yield chunk_memory[cs_id]
        chunk_memory[cs_id].chunk = b'' # diffent message but same length, this case also fmt = 3 used, so clear flushing fmt = 3 for new message

  except asyncio.IncompleteReadError:
    return

async def send_message(writer: asyncio.StreamWriter, message: Message):
  chunk_length = 128 # Maximum Chunk length (initial value: 128)
  for index, splitted in enumerate(batched(message.chunk, chunk_length)):
    splitted = bytes(splitted)
    chunk = bytearray()
    fmt = 0 if index == 0 else 3
    chunk += bytes([(fmt << 6) | 2]) # for convenience, cs_id send always 2

    extended_timestamp = False
    if fmt == 0:
      extended_timestamp = message.timestamp >= 0xFFFFFF
      chunk += int.to_bytes(min(message.timestamp, 0xFFFFFF), 3, byteorder='big') # timestamp
      chunk += int.to_bytes(message.message_length, 3, byteorder='big') # message_length
      chunk += int.to_bytes(message.message_type_id, 1, byteorder='big') # message_type_id
      chunk += int.to_bytes(message.message_stream_id, 4, byteorder='little')
    if extended_timestamp:
      chunk += int.to_bytes(message.timestamp, 4, byteorder='big') # extended timestamp
    # concat chunk content
    chunk += splitted
    # send!
    writer.write(chunk)
  await writer.drain()

class RecieverState(Enum):
  WAITING_CONNECT = auto()
  WAITING_FCPUBLISH = auto()
  WAITING_CREATESTREAM = auto()
  WAITING_PUBLISH = auto()
  RECEIVING = auto()

async def recieve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, appName: str, streamKey: str):
  # Process Handshake
  try:
    while not reader.at_eof():
      # C0/S0
      await reader.readexactly(1) # C0
      writer.write(b'\x03') # S0
      await writer.drain()
      # C1/S1
      s1_random = random.randbytes(1536 - 4 * 2) # random(1528 bytes)
      writer.write(bytes(8) + s1_random)  # S1 = time(4 bytes) + zero(4 bytes) + random(1528 bytes)
      await writer.drain()
      c1 = await reader.readexactly(1536) # C1 = time(4 bytes) + zero(4 bytes) + random(1528 bytes)
      c1_time = int.from_bytes(c1[0:4], byteorder='big')
      c1_random = c1[8:]
      # C2/S2
      writer.write(c1_time.to_bytes(4, byteorder='big') + bytes(4) + c1_random) # S2 = time(4 bytes) + time2(4 bytes) + random echo(1528 bytes)
      await writer.drain()
      c2_echo = (await reader.readexactly(1536))[8:] # C2 = time(4 bytes) + time2(4 bytes) + random echo(1528 bytes)
      if s1_random == c2_echo: break # Success Handshake
      # Failed Handshake, so continue
  except asyncio.IncompleteReadError:
    return

  state = RecieverState.WAITING_CONNECT

  async for recieved in receive_message(reader):
    match state:
      case RecieverState.WAITING_CONNECT:
        if recieved.message_type_id != 20: continue
        amf = amf0.deserialize(recieved.chunk)
        if amf[0] != 'connect': continue
        transaction_id = amf[1]
        if appName != amf[2]['app']: return # Close Connection

        # TODO: need sescription each parameter!
        connect_result = amf0.serialize([
          '_result',
          transaction_id, # The callee reference number
          {
            'fmsVer': 'FMS/3,5,7,7009',
            'capabilities': 31,
            'mode': 1,
          }, {
            'code': 'NetConnection.Connect.Success', # Important
            'description': 'Connection succeeded.', # Any String
            'data': {
              'version': '3,5,7,7009',
            },
            'objectEncoding': 0, # connection AMF Object Type, 0 => AMF0, 3 => AMF3
            'level': 'status',
          }
        ])
        await send_message(writer, Message(20, 0, len(connect_result), 0, connect_result))
        state = RecieverState.WAITING_FCPUBLISH

      case RecieverState.WAITING_FCPUBLISH:
        if recieved.message_type_id != 20: continue
        amf = amf0.deserialize(recieved.chunk)
        if amf[0] != 'FCPublish': continue
        if streamKey != amf[3]: return # Close Connection

        state = RecieverState.WAITING_CREATESTREAM

      case RecieverState.WAITING_CREATESTREAM:
        if recieved.message_type_id != 20: continue
        amf = amf0.deserialize(recieved.chunk)
        if amf[0] != 'createStream': continue
        transaction_id = amf[1]

        create_stream_result = amf0.serialize([
          '_result',
          transaction_id, # The callee reference number
          None,
          1 # stream_id (0 and 2 is reserved, so 1 used)
        ])
        await send_message(writer, Message(20, 0, len(create_stream_result), 0, create_stream_result))
        state = RecieverState.WAITING_PUBLISH

      case RecieverState.WAITING_PUBLISH:
        if recieved.message_type_id != 20: continue
        amf = amf0.deserialize(recieved.chunk)
        if amf[0] != 'publish': continue
        transaction_id = amf[1]

        publish_result = amf0.serialize([
          'onStatus',
          transaction_id, # The callee reference number
          None,
          {
            'code': 'NetStream.Publish.Start', # Important
            'description': 'Publish Accepted', # Any String
            'level': 'status'
          }
        ])
        await send_message(writer, Message(20, 0, len(publish_result), 0, publish_result))
        state = RecieverState.RECEIVING

      case RecieverState.RECEIVING:
        # Propagate Video/Audio/Metadata
        if recieved.message_type_id in STREAM_TYPE_ID_FOR_MEDIA:
          yield recieved

