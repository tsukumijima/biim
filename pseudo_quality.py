# 以下は全て KonomiTV から検証のために移植

import sys
from pydantic import BaseModel, PositiveInt
from typing import Literal


# 品質を表す Pydantic モデル
class Quality(BaseModel):
    is_hevc: bool  # 映像コーデックが HEVC かどうか
    is_60fps: bool  # フレームレートが 60fps かどうか
    width: PositiveInt  # 縦解像度
    height: PositiveInt  # 横解像度
    video_bitrate: str  # 映像のビットレート
    video_bitrate_max: str  # 映像の最大ビットレート
    audio_bitrate: str  # 音声のビットレート

# 品質の種類 (型定義)
QUALITY_TYPES = Literal[
    '1080p-60fps',
    '1080p-60fps-hevc',
    '1080p',
    '1080p-hevc',
    '810p',
    '810p-hevc',
    '720p',
    '720p-hevc',
    '540p',
    '540p-hevc',
    '480p',
    '480p-hevc',
    '360p',
    '360p-hevc',
    '240p',
    '240p-hevc',
]

# 映像と音声の品質
QUALITY: dict[QUALITY_TYPES, Quality] = {
    '1080p-60fps': Quality(
        is_hevc = False,
        is_60fps = True,
        width = 1440,
        height = 1080,
        video_bitrate = '9500K',
        video_bitrate_max = '13000K',
        audio_bitrate = '256K',
    ),
    '1080p-60fps-hevc': Quality(
        is_hevc = True,
        is_60fps = True,
        width = 1440,
        height = 1080,
        video_bitrate = '3500K',
        video_bitrate_max = '5200K',
        audio_bitrate = '192K',
    ),
    '1080p': Quality(
        is_hevc = False,
        is_60fps = False,
        width = 1440,
        height = 1080,
        video_bitrate = '9500K',
        video_bitrate_max = '13000K',
        audio_bitrate = '256K',
    ),
    '1080p-hevc': Quality(
        is_hevc = True,
        is_60fps = False,
        width = 1440,
        height = 1080,
        video_bitrate = '3000K',
        video_bitrate_max = '4500K',
        audio_bitrate = '192K',
    ),
    '810p': Quality(
        is_hevc = False,
        is_60fps = False,
        width = 1440,
        height = 810,
        video_bitrate = '5500K',
        video_bitrate_max = '7600K',
        audio_bitrate = '192K',
    ),
    '810p-hevc': Quality(
        is_hevc = True,
        is_60fps = False,
        width = 1440,
        height = 810,
        video_bitrate = '2500K',
        video_bitrate_max = '3700K',
        audio_bitrate = '192K',
    ),
    '720p': Quality(
        is_hevc = False,
        is_60fps = False,
        width = 1280,
        height = 720,
        video_bitrate = '4500K',
        video_bitrate_max = '6200K',
        audio_bitrate = '192K',
    ),
    '720p-hevc': Quality(
        is_hevc = True,
        is_60fps = False,
        width = 1280,
        height = 720,
        video_bitrate = '2000K',
        video_bitrate_max = '3000K',
        audio_bitrate = '192K',
    ),
    '540p': Quality(
        is_hevc = False,
        is_60fps = False,
        width = 960,
        height = 540,
        video_bitrate = '3000K',
        video_bitrate_max = '4100K',
        audio_bitrate = '192K',
    ),
    '540p-hevc': Quality(
        is_hevc = True,
        is_60fps = False,
        width = 960,
        height = 540,
        video_bitrate = '1400K',
        video_bitrate_max = '2100K',
        audio_bitrate = '192K',
    ),
    '480p': Quality(
        is_hevc = False,
        is_60fps = False,
        width = 854,
        height = 480,
        video_bitrate = '2000K',
        video_bitrate_max = '2800K',
        audio_bitrate = '192K',
    ),
    '480p-hevc': Quality(
        is_hevc = True,
        is_60fps = False,
        width = 854,
        height = 480,
        video_bitrate = '1050K',
        video_bitrate_max = '1750K',
        audio_bitrate = '192K',
    ),
    '360p': Quality(
        is_hevc = False,
        is_60fps = False,
        width = 640,
        height = 360,
        video_bitrate = '1100K',
        video_bitrate_max = '1800K',
        audio_bitrate = '128K',
    ),
    '360p-hevc': Quality(
        is_hevc = True,
        is_60fps = False,
        width = 640,
        height = 360,
        video_bitrate = '750K',
        video_bitrate_max = '1250K',
        audio_bitrate = '128K',
    ),
    '240p': Quality(
        is_hevc = False,
        is_60fps = False,
        width = 426,
        height = 240,
        video_bitrate = '550K',
        video_bitrate_max = '650K',
        audio_bitrate = '128K',
    ),
    '240p-hevc': Quality(
        is_hevc = True,
        is_60fps = False,
        width = 426,
        height = 240,
        video_bitrate = '450K',
        video_bitrate_max = '650K',
        audio_bitrate = '128K',
    ),
}

def buildFFmpegOptions(
    quality: QUALITY_TYPES,
    output_ts_offset: int,
) -> list[str]:
    """
    FFmpeg に渡すオプションを組み立てる

    Args:
        quality (QUALITY_TYPES): 映像の品質
        output_ts_offset (int): 出力 TS のタイムスタンプオフセット (秒)

    Returns:
        list[str]: FFmpeg に渡すオプションが連なる配列
    """

    # オプションの入る配列
    options: list[str] = []

    # 入力
    ## -analyzeduration をつけることで、ストリームの分析時間を短縮できる
    ## -copyts で入力のタイムスタンプを出力にコピーする
    options.append('-f mpegts -analyzeduration 500000 -i pipe:0')

    # ストリームのマッピング
    ## 音声切り替えのため、主音声・副音声両方をエンコード後の TS に含む
    options.append('-map 0:v:0 -map 0:a:0 -map 0:a:1 -map 0:d? -ignore_unknown')

    # フラグ
    ## 主に FFmpeg の起動を高速化するための設定
    options.append('-fflags nobuffer -flags low_delay -max_delay 0 -tune zerolatency -max_interleave_delta 500K -threads auto')

    # 映像
    ## コーデック
    if QUALITY[quality].is_hevc is True:
        options.append('-vcodec libx265')  # H.265/HEVC (通信節約モード)
    else:
        options.append('-vcodec libx264')  # H.264

    ## バイトレートと品質
    options.append(f'-flags +cgop+global_header -vb {QUALITY[quality].video_bitrate} -maxrate {QUALITY[quality].video_bitrate_max}')
    options.append('-preset veryfast -aspect 16:9')
    if QUALITY[quality].is_hevc is True:
        options.append('-profile:v main')
    else:
        options.append('-profile:v high')

    ## 指定された品質の解像度が 1440×1080 (1080p) かつ入力ストリームがフル HD (1920×1080) の場合のみ、
    ## 特別に縦解像度を 1920 に変更してフル HD (1920×1080) でエンコードする
    video_width = QUALITY[quality].width
    video_height = QUALITY[quality].height
    # if (video_width == 1440 and video_height == 1080) and \
    #     (self.recorded_video.video_resolution_width == 1920 and self.recorded_video.video_resolution_height == 1080):
    #     video_width = 1920

    ## 最大 GOP 長 (秒)
    ## 30fps なら ×30 、 60fps なら ×60 された値が --gop-len で使われる
    # gop_length_second = self.GOP_LENGTH_SECOND
    gop_length_second = 2.5

    # インターレース映像のみ
    # if self.recorded_video.video_scan_type == 'Interlaced':
    if True:
        ## インターレース解除 (60i → 60p (フレームレート: 60fps))
        if QUALITY[quality].is_60fps is True:
            options.append(f'-vf yadif=mode=1:parity=-1:deint=1,scale={video_width}:{video_height}')
            options.append(f'-r 60000/1001 -g {int(gop_length_second * 60)}')
        ## インターレース解除 (60i → 30p (フレームレート: 30fps))
        else:
            options.append(f'-vf yadif=mode=0:parity=-1:deint=1,scale={video_width}:{video_height}')
            options.append(f'-r 30000/1001 -g {int(gop_length_second * 30)}')
    # プログレッシブ映像
    ## プログレッシブ映像の場合は 60fps 化する方法はないため、無視して元映像と同じフレームレートでエンコードする
    ## GOP は 30fps だと仮定して設定する
    # elif self.recorded_video.video_scan_type == 'Progressive':
    #     options.append(f'-vf scale={video_width}:{video_height}')
    #     options.append(f'-r 30000/1001 -g {int(gop_length_second * 30)}')

    # 音声
    ## 音声が 5.1ch かどうかに関わらず、ステレオにダウンミックスする
    options.append(f'-acodec aac -aac_coder twoloop -ac 2 -ab {QUALITY[quality].audio_bitrate} -ar 48000 -af volume=2.0')

    # 出力 TS のタイムスタンプオフセット
    options.append(f'-output_ts_offset {output_ts_offset}')

    # 出力
    options.append('-y -f mpegts')  # MPEG-TS 出力ということを明示
    options.append('pipe:1')  # 標準入力へ出力

    # オプションをスペースで区切って配列にする
    result: list[str] = []
    for option in options:
        result += option.split(' ')

    return result

def buildHWEncCOptions(
    quality: QUALITY_TYPES,
    encoder_type: Literal['QSVEncC', 'NVEncC', 'VCEEncC', 'rkmppenc'],
    output_ts_offset: int,
) -> list[str]:
    """
    QSVEncC・NVEncC・VCEEncC・rkmppenc (便宜上 HWEncC と総称) に渡すオプションを組み立てる

    Args:
        quality (QUALITY_TYPES): 映像の品質
        encoder_type (Literal['QSVEncC', 'NVEncC', 'VCEEncC', 'rkmppenc']): エンコーダー (QSVEncC or NVEncC or VCEEncC or rkmppenc)
        output_ts_offset (int): 出力 TS のタイムスタンプオフセット (秒)
    Returns:
        list[str]: HWEncC に渡すオプションが連なる配列
    """

    # オプションの入る配列
    options: list[str] = []

    # 入力
    ## --input-probesize, --input-analyze をつけることで、ストリームの分析時間を短縮できる
    ## 両方つけるのが重要で、--input-analyze だけだとエンコーダーがフリーズすることがある
    options.append('--input-format mpegts --input-probesize 1000K --input-analyze 0.7 --input -')
    ## VCEEncC の HW デコーダーはエラー耐性が低く TS を扱う用途では不安定なので、SW デコーダーを利用する
    if encoder_type == 'VCEEncC':
        options.append('--avsw')
    ## QSVEncC・NVEncC・rkmppenc は HW デコーダーを利用する
    else:
        options.append('--avhw')

    # ストリームのマッピング
    ## 音声切り替えのため、主音声・副音声両方をエンコード後の TS に含む
    ## 音声が 5.1ch かどうかに関わらず、ステレオにダウンミックスする
    options.append('--audio-stream 1?:stereo --audio-stream 2?:stereo --data-copy timed_id3')

    # フラグ
    ## 主に HWEncC の起動を高速化するための設定
    options.append('-m avioflags:direct -m fflags:nobuffer+flush_packets -m flush_packets:1 -m max_delay:250000')
    options.append('-m max_interleave_delta:500K --lowlatency')
    ## QSVEncC と rkmppenc では OpenCL を使用しないので、無効化することで初期化フェーズを高速化する
    if encoder_type == 'QSVEncC' or encoder_type == 'rkmppenc':
        options.append('--disable-opencl')
    ## NVEncC では NVML によるモニタリングを無効化することで初期化フェーズを高速化する
    if encoder_type == 'NVEncC':
        options.append('--disable-nvml 1')

    # 映像
    ## コーデック
    if QUALITY[quality].is_hevc is True:
        options.append('--codec hevc')  # H.265/HEVC (通信節約モード)
    else:
        options.append('--codec h264')  # H.264

    ## ビットレート
    ## H.265/HEVC かつ QSVEncC の場合のみ、--qvbr (品質ベース可変ビットレート) モードでエンコードする
    ## それ以外は --vbr (可変ビットレート) モードでエンコードする
    if QUALITY[quality].is_hevc is True and encoder_type == 'QSVEncC':
        options.append(f'--qvbr {QUALITY[quality].video_bitrate} --fallback-rc')
    else:
        options.append(f'--vbr {QUALITY[quality].video_bitrate}')
    options.append(f'--max-bitrate {QUALITY[quality].video_bitrate_max}')

    ## H.265/HEVC の高圧縮化調整
    if QUALITY[quality].is_hevc is True:
        if encoder_type == 'QSVEncC':
            options.append('--qvbr-quality 30')
        elif encoder_type == 'NVEncC':
            options.append('--qp-min 23:26:30 --lookahead 16 --multipass 2pass-full --weightp --bref-mode middle --aq --aq-temporal')

    ## ヘッダ情報制御 (GOP ごとにヘッダを再送する)
    ## VCEEncC ではデフォルトで有効であり、当該オプションは存在しない
    if encoder_type != 'VCEEncC':
        options.append('--repeat-headers')

    ## 品質
    if encoder_type == 'QSVEncC':
        options.append('--quality balanced')
    elif encoder_type == 'NVEncC':
        options.append('--preset default')
    elif encoder_type == 'VCEEncC':
        options.append('--preset balanced')
    elif encoder_type == 'rkmppenc':
        options.append('--preset best')
    if QUALITY[quality].is_hevc is True:
        options.append('--profile main')
    else:
        options.append('--profile high')
    options.append(f'--interlace tff --dar 16:9')

    ## 最大 GOP 長 (秒)
    ## 30fps なら ×30 、 60fps なら ×60 された値が --gop-len で使われる
    # gop_length_second = self.GOP_LENGTH_SECOND
    gop_length_second = 2.5

    # GOP長を固定にする
    if encoder_type == 'QSVEncC':
        options.append('--strict-gop')
    elif encoder_type == 'NVEncC':
        options.append('--no-i-adapt')

    # インターレース映像
    # if self.recorded_video.video_scan_type == 'Interlaced':
    if True:
        ## インターレース解除 (60i → 60p (フレームレート: 60fps))
        ## NVEncC の --vpp-deinterlace bob は品質が悪いので、代わりに --vpp-yadif を使う
        ## NVIDIA GPU は当然ながら Intel の内蔵 GPU よりも性能が高いので、GPU フィルタを使ってもパフォーマンスに問題はないと判断
        ## VCEEncC では --vpp-deinterlace 自体が使えないので、代わりに --vpp-yadif を使う
        if QUALITY[quality].is_60fps is True:
            if encoder_type == 'QSVEncC':
                options.append('--vpp-deinterlace bob')
            elif encoder_type == 'NVEncC' or encoder_type == 'VCEEncC':
                options.append('--vpp-yadif mode=bob')
            elif encoder_type == 'rkmppenc':
                options.append('--vpp-deinterlace bob_i5')
            options.append(f'--avsync vfr --gop-len {int(gop_length_second * 60)}')
        ## インターレース解除 (60i → 30p (フレームレート: 30fps))
        ## NVEncC の --vpp-deinterlace normal は GPU 機種次第では稀に解除漏れのジャギーが入るらしいので、代わりに --vpp-afs を使う
        ## NVIDIA GPU は当然ながら Intel の内蔵 GPU よりも性能が高いので、GPU フィルタを使ってもパフォーマンスに問題はないと判断
        ## VCEEncC では --vpp-deinterlace 自体が使えないので、代わりに --vpp-afs を使う
        else:
            if encoder_type == 'QSVEncC':
                options.append('--vpp-deinterlace normal')
            elif encoder_type == 'NVEncC' or encoder_type == 'VCEEncC':
                options.append('--vpp-afs preset=default')
            elif encoder_type == 'rkmppenc':
                options.append('--vpp-deinterlace normal_i5')
            options.append(f'--avsync vfr --gop-len {int(gop_length_second * 30)}')
    # プログレッシブ映像
    ## プログレッシブ映像の場合は 60fps 化する方法はないため、無視して元映像と同じフレームレートでエンコードする
    ## GOP は 30fps だと仮定して設定する
    # elif self.recorded_video.video_scan_type == 'Progressive':
    #     options.append(f'--avsync vfr --gop-len {int(gop_length_second * 30)}')

    ## 指定された品質の解像度が 1440×1080 (1080p) かつ入力ストリームがフル HD (1920×1080) の場合のみ、
    ## 特別に縦解像度を 1920 に変更してフル HD (1920×1080) でエンコードする
    video_width = QUALITY[quality].width
    video_height = QUALITY[quality].height
    # if (video_width == 1440 and video_height == 1080) and \
    #     (self.recorded_video.video_resolution_width == 1920 and self.recorded_video.video_resolution_height == 1080):
    #     video_width = 1920
    options.append(f'--output-res {video_width}x{video_height}')

    # 音声
    options.append(f'--audio-codec aac:aac_coder=twoloop --audio-bitrate {QUALITY[quality].audio_bitrate}')
    options.append('--audio-samplerate 48000 --audio-filter volume=2.0 --audio-ignore-decode-error 30')

    # 出力 TS のタイムスタンプオフセット
    options.append(f'-m output_ts_offset:{output_ts_offset}')

    # 出力
    options.append('--output-format mpegts')  # MPEG-TS 出力ということを明示
    options.append('--output -')  # 標準入力へ出力

    # オプションをスペースで区切って配列にする
    result: list[str] = []
    for option in options:
        result += option.split(' ')

    return result

def getEncoderCommand(encoder_type: Literal['FFmpeg', 'QSVEncC', 'NVEncC', 'VCEEncC', 'rkmppenc'], quality: QUALITY_TYPES, output_ts_offset: int) -> list[str]:
    # tsreadex のオプション
    ## 放送波の前処理を行い、エンコードを安定させるツール
    ## オプション内容は https://github.com/xtne6f/tsreadex を参照
    tsreadex_options = [
        'tsreadex',
        # 取り除く TS パケットの10進数の PID
        ## EIT の PID を指定
        '-x', '18/38/39',
        # 特定サービスのみを選択して出力するフィルタを有効にする
        ## 有効にすると、特定のストリームのみ PID を固定して出力される
        ## 視聴対象の録画番組が放送されたチャンネルのサービス ID があれば指定する
        # '-n', f'{self.recorded_program.channel.service_id}' if self.recorded_program.channel is not None else '-1',
        '-n', '-1',
        # 主音声ストリームが常に存在する状態にする
        ## ストリームが存在しない場合、無音の AAC ストリームが出力される
        ## 音声がモノラルであればステレオにする
        ## デュアルモノを2つのモノラル音声に分離し、右チャンネルを副音声として扱う
        '-a', '13',
        # 副音声ストリームが常に存在する状態にする
        ## ストリームが存在しない場合、無音の AAC ストリームが出力される
        ## 音声がモノラルであればステレオにする
        '-b', '7',
        # 字幕ストリームが常に存在する状態にする
        ## ストリームが存在しない場合、PMT の項目が補われて出力される
        ## 実際の字幕データが現れない場合に5秒ごとに非表示の適当なデータを挿入する
        '-c', '5',
        # 文字スーパーストリームが常に存在する状態にする
        ## ストリームが存在しない場合、PMT の項目が補われて出力される
        '-u', '1',
        # 字幕と文字スーパーを aribb24.js が解釈できる ID3 timed-metadata に変換する
        ## +4: FFmpeg のバグを打ち消すため、変換後のストリームに規格外の5バイトのデータを追加する
        ## +8: FFmpeg のエラーを防ぐため、変換後のストリームの PTS が単調増加となるように調整する
        ## +4 は FFmpeg 6.1 以降不要になった (付与していると字幕が表示されなくなる) ため、
        ## FFmpeg 4.4 系に依存している Linux 版 HWEncC 利用時のみ付与する
        '-d', '13' if encoder_type != 'FFmpeg' and sys.platform == 'linux' else '9',
        # 標準入力からの入力を受け付ける
        '-',
    ]

    if encoder_type == 'FFmpeg':
        return tsreadex_options + ['|', 'ffmpeg'] + buildFFmpegOptions(quality, output_ts_offset)
    elif encoder_type == 'QSVEncC':
        return tsreadex_options + ['|', 'qsvencc'] + buildHWEncCOptions(quality, encoder_type, output_ts_offset)
    elif encoder_type == 'NVEncC':
        return tsreadex_options + ['|', 'nvencc'] + buildHWEncCOptions(quality, encoder_type, output_ts_offset)
    elif encoder_type == 'VCEEncC':
        return tsreadex_options + ['|', 'vceencc'] + buildHWEncCOptions(quality, encoder_type, output_ts_offset)
    elif encoder_type == 'rkmppenc':
        return tsreadex_options + ['|', 'rkmppenc'] + buildHWEncCOptions(quality, encoder_type, output_ts_offset)
    else:
        raise ValueError(f'Invalid encoder type: {encoder_type}')
