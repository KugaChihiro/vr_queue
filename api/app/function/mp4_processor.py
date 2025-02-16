import os
import tempfile
import subprocess
import imageio_ffmpeg as ffmpeg
from fastapi import HTTPException

async def save_disk_async(file_data: bytes, destination: str):
    """
    バイナリデータをディスクに非同期で保存する関数。
    """
    try:
        with open(destination, "wb") as out_file:
            out_file.write(file_data)  # 直接bytesを書き込む
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

def convert_wav(input_path: str, output_path: str):
    """
    MP4ファイルをWAVフォーマットに同期的に変換する関数。
    """
    try:
        ffmpeg_path = ffmpeg.get_ffmpeg_exe()
        command = [
            ffmpeg_path,
            "-i",
            input_path,
            "-ar",
            "16000",  # サンプリングレート 16kHz
            "-ac",
            "1",  # モノラルに変換
            "-sample_fmt",
            "s16",  # サンプルフォーマット（16-bit PCM）
            output_path,
        ]
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"FFmpeg failed: {e.stderr}")

async def mp4_processor(file_name: str, file_data: bytes) -> dict:
    """
    MP4ファイルを処理し、WAVファイルに変換する関数。
    """
    try:
        sanitized_filename = os.path.basename(file_name)
        file_extension = os.path.splitext(sanitized_filename)[1].lower()
        # WAVファイルならそのまま返す
        if file_extension == ".wav":
            return {"file_wavname": sanitized_filename, "file_data": file_data,"file_mp4name": sanitized_filename}
        # 一時ディレクトリを利用
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = os.path.join(tmpdir, sanitized_filename)
            output_filename = os.path.splitext(sanitized_filename)[0] + ".wav"
            output_filename_mp4 = os.path.splitext(sanitized_filename)[0] + ".mp4"
            output_path = os.path.join(tmpdir, output_filename)
            # bytes` データをディスクに保存
            await save_disk_async(file_data, input_path)
            # MP4をWAVに変換（同期処理）
            convert_wav(input_path, output_path)
            # WAVファイルを読み取る
            with open(output_path, "rb") as f:
                wav_data = f.read()
            return {"file_wavname": output_filename, "wav_data": wav_data,"file_mp4name": output_filename_mp4}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process file: {str(e)}")
