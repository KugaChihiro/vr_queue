from fastapi import FastAPI, File, UploadFile, HTTPException, Form, Depends, Request, BackgroundTasks, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import json
from dotenv import load_dotenv
import traceback
from fastapi.responses import JSONResponse
import aiohttp
from starlette.websockets import WebSocketDisconnect
from contextlib import asynccontextmanager
from azure.storage.queue import QueueClient
from function.transcribe_audio import AzTranscriptionClient
from function.summary_text import AzOpenAIClient
from function.blob_processor import AzBlobClient
from function.mp4_processor import mp4_processor
from function.word_generator import create_word, cleanup_file
from function.sharepoint_processor import SharePointAccessClass
from urllib.parse import urlparse

# 環境変数をロード
load_dotenv()
# 環境変数
AZ_SPEECH_KEY = os.getenv("AZ_SPEECH_KEY")
AZ_SPEECH_ENDPOINT = os.getenv("AZ_SPEECH_ENDPOINT")
AZ_OPENAI_KEY = os.getenv("AZ_OPENAI_KEY")
AZ_OPENAI_ENDPOINT = os.getenv("AZ_OPENAI_ENDPOINT")
AZ_BLOB_CONNECTION = os.getenv("AZ_BLOB_CONNECTION")
AZ_CONTAINER_NAME =  os.getenv("AZ_CONTAINER_NAME")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
CONNECTION_STRING = os.getenv("CONNECTION_STRING")
QUEUE_NAME = os.getenv("QUEUE_NAME")

# FastAPI側でのモデルを定義
class Transcribe(BaseModel):
    project: str
    project_directory: str

# キューからメッセージを非同期的に取得
async def process_queue_message(queue_client: QueueClient):
    try:
        messages = queue_client.receive_messages(messages_per_page=1)  # メッセージを1件取得
        for msg in messages:
            message_data = msg.content
            if not message_data:
                raise HTTPException(status_code=400, detail="Message data is empty")
            try:
                # メッセージから必要なデータを抽出（例：JSON形式でproject_data、file、client_idを取得）
                data = json.loads(message_data)  
                project= data.get('project')
                project_directory = data.get('project_Directory') 
                project_data = Transcribe(project=project, project_directory=project_directory)
                file_data = data.get('file_path')  # ファイル情報（URL）
                client_id = data.get('client_id')
               
                # メッセージを削除（処理完了後）
                queue_client.delete_message(msg)

                return {
                    "status":"success",
                    "project_data":project_data,
                    "client_id":client_id,
                    "file_url":file_data
                }     

            except json.JSONDecodeError as e:
                # JSONが不正な場合のエラーハンドリング
                raise HTTPException(status_code=400, detail=f"Invalid JSON data: {e}")
            except KeyError as e:
                # 必要なキーが欠けている場合のエラーハンドリング
                raise HTTPException(status_code=400, detail=f"Missing required field: {e}")

    except Exception as e:
        # キューの受信やその他のエラー処理
        raise HTTPException(status_code=500, detail=f"Error processing queue message: {str(e)}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    session = aiohttp.ClientSession()
    app.state.session = session
    app.state.connections = {}
    yield
    await session.close()
    
# FastAPIアプリケーションの初期化
app = FastAPI(lifespan=lifespan)
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
def get_az_speech_client(request: Request):
    session = request.app.state.session
    return AzTranscriptionClient(session, AZ_SPEECH_KEY, AZ_SPEECH_ENDPOINT)
def get_az_openai_client():
    return AzOpenAIClient(AZ_OPENAI_KEY, AZ_OPENAI_ENDPOINT)
def get_sp_access():
    return SharePointAccessClass(CLIENT_ID, CLIENT_SECRET, TENANT_ID)


# クライアントから送信されたフォームデータをjson形式にパースする
def parse_form(
    project: str = Form(...),
    project_directory: str = Form(...)
) -> Transcribe:
    return Transcribe(project=project, project_directory=project_directory)

@app.websocket
async def websocket_endpoint(self,websocket: WebSocket):
    queue = await process_queue_message(self.queue_client)
    client_id = queue["client_id"]
    await websocket.accept()
    app.state.connections[client_id] = websocket  # クライアントIDを登録
    try:
        while True:
            received_text = (await websocket.receive_text())  # クライアントからのメッセージを受信
            await websocket.send_text(received_text)  # 受け取ったメッセージをそのまま返す
    except WebSocketDisconnect:
        pass  # クライアントが切断した場合は何もしない
    finally:
        app.state.connections.pop(client_id, None)

    

async def download_blob_from_url(file_url,az_blob_client: AzBlobClient) -> tuple[str, bytes]:
    """
    Azure Blob StorageのURLをもとにMP4/wavファイルをダウンロードし、ファイル名とデータを返す。
    """
    try:
        # URLからblob名を取得
        parsed_url = urlparse(file_url)
        blob_name = parsed_url.path.split('/')[-1]  # 最後の部分がファイル名

        # Azure Blob からデータをダウンロード
        file_data = await az_blob_client.download_blob(blob_name,AZ_CONTAINER_NAME,AZ_BLOB_CONNECTION)
        print("get_download_stream")
        # ファイル名とデータをタプルで返す
        return blob_name, file_data

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to download blob from URL: {str(e)}"
        )


async def process_audio_task(
    client_id: str,
    file_url:str,
    project_data_dict: dict,
    az_blob_client: AzBlobClient,
    az_speech_client: AzTranscriptionClient,
    az_openai_client: AzOpenAIClient,
    sp_access: SharePointAccessClass,
):
    """音声処理をバックグラウンドで行い、WebSocketで通知"""
    try:
        # MP4ファイル処理
        file_name, file_content = await download_blob_from_url(file_url,az_blob_client)
        wav_sound_data = await mp4_processor(file_name, file_content)
        file_wavname = wav_sound_data["file_wavname"]
        wav_data = wav_sound_data["wav_data"]
        file_mp4name = wav_sound_data["file_mp4name"]
        await az_blob_client.delete_blob(file_mp4name)
        blob_url  = await az_blob_client.upload_blob(file_wavname, wav_data)
        # 文字起こし
        transcribed_text = await az_speech_client.transcribe_audio(blob_url)
        # 要約処理
        summarized_text = await az_openai_client.summarize_text(transcribed_text)
        # SharePointにWordファイルをアップロード
        word_file_path = await create_word(summarized_text)
        print(f"finish_create_word:{word_file_path}")
        #sp_access.upload_file(
        #    project_data_dict["project"],
        #    project_data_dict["project_directory"],
        #    word_file_path,
        #)
        # WebSocket通知（接続がまだあるか確認）
        #if client_id in app.state.connections:
        #    await app.state.connections[client_id].send_text(summarized_text)
        # Blobストレージから削除
        if file_wavname:
            await az_blob_client.delete_blob(file_wavname)
            print("finish_delete_blob")
    except Exception as e:
        print(f"Error processing file for client {client_id}: {str(e)}")

@app.post("/record")
async def main(
    background_tasks: BackgroundTasks,
    az_blob_client: AzBlobClient = Depends(get_az_blob_client),
    az_speech_client: AzTranscriptionClient = Depends(get_az_speech_client),
    az_openai_client: AzOpenAIClient = Depends(get_az_openai_client),
    sp_access: SharePointAccessClass = Depends(get_sp_access)
    ) -> dict:
    """
    音声ファイルを文字起こしし、要約を返すエンドポイント。
    """
    queue_client = QueueClient.from_connection_string(CONNECTION_STRING,QUEUE_NAME) 
    queue = await process_queue_message(queue_client)
    project_data = queue["project_data"]
    client_id = queue["client_id"]
    file_url = queue["file_url"]



    #if client_id not in app.state.connections:
    #    return JSONResponse(
    #        status_code=400,
    #        content={
    #            "error": "WebSocket が開かれていません。先に /ws/{client_id} を開いてください。"
    #        },
    #    )

    project_data_dict = project_data.model_dump()
    try:
        background_tasks.add_task(
            process_audio_task,
            client_id,
            file_url,
            project_data_dict,
            az_blob_client,
            az_speech_client,
            az_openai_client,
            sp_access,
        )
        return {"message": "処理を開始しました"}

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "error": str(e),
                "traceback": traceback.format_exc(),
            },
        )
    

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
