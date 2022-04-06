# biim

mpegts stream to Apple Low Latency HLS

## Feature
 * mpegts demuxing in pure python3 (using asyncio)
 * mpegts stream to fragmented ts
     * use piping from ffmpeg or something to HLS
 * In Memory (On the fly) Apple Low Latency HLS Origin Serving
     * no use disk space for HLS delivery
     * support Blocking Request and Delta Update

## Dependency 

* aiohttp

## Usege

```bash
ffmpeg xxx -f mpegts - | ./main.py --port 8080
# watch http://localhost:8080/playlist.m3u8
```
