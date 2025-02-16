from fastapi import FastAPI, File, UploadFile, HTTPException,Form
from fastapi import Body
from fastapi.middleware.cors import CORSMiddleware
import logging
import os
from dotenv import load_dotenv
from my_function.blob_processor import upload_blob, delete_blob
from my_function.send_message import send_message_to_queue
from pydantic import BaseModel

# 環境変数をロード
load_dotenv('./my_function/.env')

# 環境変数
AZ_SPEECH_KEY = os.getenv("AZ_SPEECH_KEY").strip()
AZ_SPEECH_ENDPOINT = os.getenv("AZ_SPEECH_ENDPOINT").strip()
AZ_BLOB_CONNECTION = os.getenv("AZ_BLOB_CONNECTION").strip()
CONTAINER_NAME = os.getenv("CONTAINER_NAME").strip()

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FastAPIApp")

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

class FileData(BaseModel):
    blob_url_mp4: str
    file_name: str

@app.post("/transcribe")
async def main(file: UploadFile = File(...),client_id: str = Form(...),project: str = Form(...),project_directory: str = Form(...)):
    """
    BlobへMP4ファイルをアップロードし、Queueへメッセージを送信するエンドポイント
    """
    try:
        logger.info("Processing request...")

        file_name = file.filename
        file_data = await file.read()

        # Azure Blob Storage にアップロード
        blob_url = await upload_blob(file_name, file_data, CONTAINER_NAME, AZ_BLOB_CONNECTION)
        logger.info(f"Blob uploaded: {blob_url}")

        sanitized_filename = os.path.basename(file.filename)
        file_extension = os.path.splitext(sanitized_filename)[1].lower()
        print("get_ready")
        print(blob_url)

        if file_extension == ".mp4":
            send_message_to_queue(project,project_directory,blob_url,client_id)
            print("finished_sending")
            logger.info(f"Waiting for HTTP request to process MP4 file") 
        else:
            print("A video has been uploaded in the wrong file format.")
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)})
    

 