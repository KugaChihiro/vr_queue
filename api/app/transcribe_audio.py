import asyncio
import aiohttp
from fastapi import HTTPException

async def create_headers(az_speech_key: str) -> dict:
    """
    Azure Speech Service用のヘッダーを作成。

    :param az_speech_key: Azure Speech Serviceキー
    :return: ヘッダー辞書
    """
    return {
        "Ocp-Apim-Subscription-Key": az_speech_key,
        "Content-Type": "application/json",
    }

async def create_transcription_job(blob_url: str, headers: dict, az_speech_endpoint: str) -> str:
    """
    ジョブを作成する。
    """
    body = {
        "displayName": "Transcription",
        "locale": "ja-jp",
        "contentUrls": [blob_url],
        "properties": {
            "diarizationEnabled": True,
            "punctuationMode": "DictatedAndAutomatic",
            "wordLevelTimestampsEnabled": True,
        },
    }
    transcription_url = f"{az_speech_endpoint}/speechtotext/v3.2/transcriptions"
    async with aiohttp.ClientSession() as session:
        async with session.post(
            transcription_url, headers=headers, json=body
        ) as response:
            if response.status != 201:
                raise HTTPException(
                    status_code=response.status,
                    detail=f"ジョブの作成に失敗しました: {await response.text()}",
                )
            return (await response.json())["self"]

async def poll_transcription_status(job_url: str, headers: dict, max_attempts=30, interval=10) -> str:
    """
    ジョブの進行状況をチェックする。
    """
    async with aiohttp.ClientSession() as session:
        for _ in range(max_attempts):
            await asyncio.sleep(interval)
            async with session.get(job_url, headers=headers) as response:
                status_data = await response.json()
                if status_data["status"] == "Succeeded":
                    return status_data["links"]["files"]
                elif status_data["status"] in ["Failed", "Cancelled"]:
                    raise HTTPException(
                        status_code=500,
                        detail=f"ジョブの進行に失敗しました: {status_data['status']}",
                    )
        raise HTTPException(status_code=500, detail="ジョブのタイムアウト")

async def get_transcription_result(file_url: str, headers: dict) -> str:
    """
    ジョブの結果から contentUrl を取得する。
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(file_url, headers=headers) as response:
            if response.status != 200:
                raise HTTPException(
                    status_code=response.status,
                    detail=f"結果の取得に失敗しました: {await response.text()}",
                )
            files_data = await response.json()
            return files_data["values"][0]["links"]["contentUrl"]

async def fetch_transcription_display(content_url: str) -> str:
    """
    contentUrl にアクセスして display を取得する。
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(content_url) as response:
            if response.status != 200:
                raise HTTPException(
                    status_code=response.status,
                    detail=f"contentUrl の取得に失敗しました: {await response.text()}",
                )
            content_data = await response.json()
            return content_data["combinedRecognizedPhrases"][0]["display"]

async def transcribe_audio(blob_url: str, az_speech_key: str, az_speech_endpoint: str) -> str:
    """
    音声ファイルを文字起こしするメイン処理。
    """
    headers = await create_headers(az_speech_key)

    # ジョブ作成
    job_url = await create_transcription_job(blob_url, headers, az_speech_endpoint)

    # ジョブ進行状況を確認
    file_url = await poll_transcription_status(job_url, headers)

    # contentUrl を取得
    content_url = await get_transcription_result(file_url, headers)

    # 文字起こし結果を取得
    return await fetch_transcription_display(content_url)
