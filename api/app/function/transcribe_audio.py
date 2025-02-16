import asyncio
import aiohttp
from fastapi import HTTPException

class AzTranscriptionClient:
    def __init__(self, session: aiohttp.ClientSession, az_speech_key: str, az_speech_endpoint: str):
        self.headers = {
            "Ocp-Apim-Subscription-Key": az_speech_key,
            "Content-Type": "application/json",
        }
        self.az_speech_endpoint = az_speech_endpoint
        self.session = session

    async def close(self):
        await self.session.close()

    async def create_transcription_job(self, blob_url: str) -> str:
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
        transcription_url = f"{self.az_speech_endpoint}/speechtotext/v3.2/transcriptions"
        async with self.session.post(
            transcription_url, headers=self.headers, json=body
        ) as response:
            if response.status != 201:
                raise HTTPException(
                    status_code=response.status,
                    detail=f"ジョブの作成に失敗しました: {await response.text()}",
                )
            return (await response.json())["self"]

    async def poll_transcription_status(
        self, job_url: str, max_attempts=30, initial_interval=2
    ) -> str:
        interval = initial_interval
        for attempt in range(max_attempts):
            async with self.session.get(job_url, headers=self.headers) as response:
                status_data = await response.json()
                #print(f"Attempt {attempt + 1}: Response JSON: {status_data}")
                if status_data["status"] == "Succeeded":
                    return status_data["links"]["files"]
                elif status_data["status"] in ["Failed", "Cancelled"]:
                    raise HTTPException(
                        status_code=500,
                        detail=f"ジョブの進行に失敗しました: {status_data['status']}",
                    )
            await asyncio.sleep(interval)
            interval = min(interval * 2, 10)  # 最大10秒まで間隔を増加
        raise HTTPException(status_code=500, detail="ジョブのタイムアウト")

    async def get_transcription_result(self, file_url: str) -> str:
        async with self.session.get(file_url, headers=self.headers) as response:
            if response.status != 200:
                raise HTTPException(
                    status_code=response.status,
                    detail=f"結果の取得に失敗しました: {await response.text()}",
                )
            files_data = await response.json()
            return files_data["values"][0]["links"]["contentUrl"]

    async def fetch_transcription_display(self, content_url: str) -> str:
        async with self.session.get(content_url) as response:
            if response.status != 200:
                raise HTTPException(
                    status_code=response.status,
                    detail=f"contentUrl の取得に失敗しました: {await response.text()}",
                )
            content_data = await response.json()
            return content_data["combinedRecognizedPhrases"][0]["display"]

    async def transcribe_audio(self, blob_url: str) -> str:
        if self.session.closed:
            self.session = aiohttp.ClientSession()
        job_url = await self.create_transcription_job(blob_url)
        file_url = await self.poll_transcription_status(job_url)
        content_url = await self.get_transcription_result(file_url)
        return await self.fetch_transcription_display(content_url)
