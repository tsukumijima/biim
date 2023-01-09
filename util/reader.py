import asyncio

class BufferingAsyncReader:
  def __init__(self, reader, size):
    self.reader = reader
    self.buffer = bytearray()
    self.size = size

  async def __fill(self):
    data = await asyncio.to_thread(lambda: self.reader.read(self.size))
    if data == b'': return False
    self.buffer += data
    return True

  async def read(self, n):
    while len(self.buffer) < n:
      if not (await self.__fill()): break
    result = self.buffer[:n]
    self.buffer = self.buffer[n:]
    return result

  async def readexactly(self, n):
    while len(self.buffer) < n:
      if not (await self.__fill()): raise asyncio.IncompleteReadError(self.buffer, None)
    result = self.buffer[:n]
    self.buffer = self.buffer[n:]
    return result
