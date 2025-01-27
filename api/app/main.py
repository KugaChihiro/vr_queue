from fastapi import FastAPI, File, UploadFile, HTTPException, Form, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
from dotenv import load_dotenv
import traceback
from fastapi.responses import JSONResponse
import aiohttp

from app.transcribe_audio import AzTranscriptionClient
from app.summary_text import AzOpenAIClient
from app.blob_processor import AzBlobClient
from app.mp4_processor import mp4_processor
from app.word_generator import create_word, cleanup_file
from app.sharepoint_processor import SharePointAccessClass

# 環境変数をロード
load_dotenv()
# 環境変数
AZ_SPEECH_KEY = os.getenv("AZ_SPEECH_KEY")
AZ_SPEECH_ENDPOINT = os.getenv("AZ_SPEECH_ENDPOINT")
AZ_OPENAI_KEY = os.getenv("AZ_OPENAI_KEY")
AZ_OPENAI_ENDPOINT = os.getenv("AZ_OPENAI_ENDPOINT")
AZ_BLOB_CONNECTION = os.getenv("AZ_BLOB_CONNECTION")
AZ_CONTAINER_NAME = "container-vr-dev"
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")

# FastAPIアプリケーションの初期化
app = FastAPI()
# CORS設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# クラスの依存性を定義する関数
def get_az_blob_client():
    return AzBlobClient(AZ_BLOB_CONNECTION, AZ_CONTAINER_NAME)
async def get_az_speech_client():
    async with aiohttp.ClientSession() as session:
        client = AzTranscriptionClient(session, AZ_SPEECH_KEY, AZ_SPEECH_ENDPOINT)
        yield client
def get_az_openai_client():
    return AzOpenAIClient(AZ_OPENAI_KEY, AZ_OPENAI_ENDPOINT)
def get_sp_access():
    return SharePointAccessClass(CLIENT_ID, CLIENT_SECRET, TENANT_ID)
# FastAPI側でのモデルを定義
class Transcribe(BaseModel):
    project: str
    project_directory: str
# クライアントから送信されたフォームデータをjson形式にパースする
def parse_form(
    project: str = Form(...),
    project_directory: str = Form(...)
) -> Transcribe:
    return Transcribe(project=project, project_directory=project_directory)

@app.post("/transcribe")
async def main(
    project_data: Transcribe = Depends(parse_form), 
    file: UploadFile = File(...),
    az_blob_client: AzBlobClient = Depends(get_az_blob_client),
    az_speech_client: AzTranscriptionClient = Depends(get_az_speech_client),
    az_openai_client: AzOpenAIClient = Depends(get_az_openai_client),
    sp_access: SharePointAccessClass = Depends(get_sp_access)
    ) -> str:
    """
    音声ファイルを文字起こしし、要約を返すエンドポイント。
    """
    project_data_dict = project_data.model_dump()
    try:
        # MP4ファイル処理
        response = await mp4_processor(file)
        file_name = response["file_name"]
        file_data = response["file_data"]
        # Azure Blob Storage にアップロード
        blob_url = await az_blob_client.upload_blob(file_name, file_data)
        try:
            # 音声を文字起こし
            transcribed_text = await az_speech_client.transcribe_audio(blob_url)
            # 要約処理
            summarized_text = await az_openai_client.summarize_text(transcribed_text)
            # SharePointにWordファイルをアップロード
            word_file_path = await create_word(summarized_text)
            sp_access.upload_file(project_data_dict["project"], project_data_dict["project_directory"], word_file_path)

            return summarized_text

        finally:
            await az_blob_client.delete_blob(file_name)
            await cleanup_file(word_file_path)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "error": str(e),
                "traceback": traceback.format_exc(),
            },
        )
    finally:
        # 音声処理が終わったらクライアントのリソースを解放
        await az_speech_client.close()

@app.get("/sites")
async def get_sites(sp_access: SharePointAccessClass = Depends(get_sp_access)):
    """
    サイト一覧を取得するエンドポイント
    """
    try:
        return sp_access.get_sites()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"サイト取得中にエラーが発生しました: {str(e)}")

@app.get("/directories/{site_id}")
async def get_directories(site_id: str, sp_access: SharePointAccessClass = Depends(get_sp_access)):
    """
    指定されたサイトIDのディレクトリ一覧を取得するエンドポイント
    """
    try:
        return sp_access.get_folders(site_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ディレクトリ取得中にエラーが発生しました: {str(e)}")
