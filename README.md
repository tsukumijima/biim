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

Ingest MPEG-TS Stream to biim's STDIN!

```bash
# mpegts (for H.264)
ffmpeg xxx -f mpegts - | ./main.py --port 8080
# fmp4 (for H.264/H.265)
ffmpeg xxx -f mpegts - | ./fmp4.py --port 8080

# watch http://localhost:8080/playlist.m3u8
```

### Options

* `-i`, `--input`,
  * Specify input source.
  * if not Specified, use STDIN.
  * if Specified file, throttled for pseudo live serving.
  * DEFAULT: STDIN
* `-t`, `--target_duration`
  * Specify minmum TARGETDURATION for LL-HLS
  * DEFAULT: 1
* `-p`, `--part_duration`
  * Specify PART-TARGET for LL-HLS
  * DEFAULT: 0.1
* `-w`, `--window_size`
  * Specify Live Window for LL-HLS
  * if Not Specifed, window size is Infinify, for EVENT(DVR).
  * DEFAULT: Infinity (None)
* `--port`
  * Specify Serving PORT for LL-HLS
  * DEFAULT: 8080

### Example (Generate Test Stream H.265(libx265)/AAC With Timestamp)

```bash
ffmpeg -re \
  -f lavfi -i testsrc=700x180:r=30000/1001 \
  -f lavfi -i sine=frequency=1000 \
  -vf "settb=AVTB,setpts='trunc(PTS/1K)*1K+st(1,trunc(RTCTIME/1K))-1K*trunc(ld(1)/1K)',drawtext=fontsize=60:fontcolor=black:text='%{localtime}.%{eif\:1M*t-1K*trunc(t*1K)\:d\:3}'" \
  -c:v libx265 -tune zerolatency -preset ultrafast -r 30 -g 15 -pix_fmt yuv420p \
  -c:a aac -ac 1 -ar 48000 \
  -f mpegts - | ./fmp4.py -t 1 -p 0.15 -w 10 --port 8080
# watch http://localhost:8080/playlist.m3u8
```
