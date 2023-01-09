import asyncio
import io

class BufferingAsyncReader:
  def __init__(self, reader: io.BinaryIO, size: int):
    self.reader = reader
    self.buffer: bytearray = bytearray()
    self.size: int = size

  async def __fill(self) -> bool:
    data = await asyncio.to_thread(lambda: self.reader.read(self.size))
    if data == b'': return False
    self.buffer += data
    return True

  async def read(self, n: int) -> memoryview:
    while len(self.buffer) < n:
      if not (await self.__fill()): break
    result = self.buffer[:n]
    self.buffer = self.buffer[n:]
    return memoryview(result)

  async def readexactly(self, n: int) -> memoryview:
    while len(self.buffer) < n:
      if not (await self.__fill()): raise asyncio.IncompleteReadError(self.buffer, None)
    result = self.buffer[:n]
    self.buffer = self.buffer[n:]
    return memoryview(result)
