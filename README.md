# biim

An example for Apple Low Lantency HLS Packager and Origin
## Feature
  * MPEG-TS Demuxing and Apple LL-HLS Packaging in pure Python3 (using asyncio)
  * Packaging MPEG-TS stream to HLS media segment (MPEG-TS or fmp4)
    * `main.py`: Packaging MPEG-TS stream to MPEG-TS segment (for H.264/AVC, AAC, ID3)
      * Support TIMED-ID3 Metadata PassThrough
    * `fmp4.py`: Packaging MPEG-TS stream to fmp4 segment (for H.265/HEVC, AAC, ID3)
      * Support TIMED-ID3 Metadata to EMSG-ID3 Conversion
  * Support LL-HLS Feature (1s Latency with HTTP/2, 2s Latency with HTTP/1.1)
    * Support Blocking Request
    * Support EXT-X-PRELOAD-HINT with Chunked Transfer
    * NOTE: HTTP/2 is currently not Supported. If use with HTTP/2, please proxing HTTP/2 to HTTP/1.1.
  * In Memory (On the fly) LL-HLS Serving
    * Not use disk space for LL-HLS Delivery

## Dependency

* aiohttp

## Usege

```bash
ffmpeg xxx -f mpegts - | ./main.py --port 8080
# watch http://localhost:8080/playlist.m3u8
```
