from azure.storage.blob import BlobServiceClient
from fastapi import HTTPException
import asyncio

class AzBlobClient:
    def __init__(self, az_blob_connection: str, az_container_name: str):
        """
        Azure Blob Storageクラスの初期化。
        """
        self.blob_service_client = BlobServiceClient.from_connection_string(az_blob_connection)
        self.container_client = self.blob_service_client.get_container_client(az_container_name)
        self.az_container_name = az_container_name

    async def upload_blob(self, file_name: str, file_data: bytes) -> str:
        """
        Azure Blob Storageにファイルをアップロードする。
        """
        try:
            blob_client = self.container_client.get_blob_client(blob=file_name)
            # ファイルをアップロード
            blob_client.upload_blob(file_data, overwrite=True)
            # アップロードしたBlobのURLを返す
            return blob_client.url
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to upload blob: {str(e)}"
            )
        
    async def download_blob(self,blob_name: str, container_name: str, connection_string: str) -> bytes:
        try:
            # BlobServiceClientを作成して、BlobClientを取得
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

            # 非同期でBlobをダウンロード
            download_stream =blob_client.download_blob()
            file_data = await asyncio.to_thread(download_stream.readall)
            return bytes(file_data)

        except Exception as e:
            print(f"Error downloading blob {blob_name}: {str(e)}")
            raise
            
    async def delete_blob(self, blob_name: str):
        """
        Azure Blob Storageからファイルを削除する。
        """
        try:
            # Blobを削除
            self.container_client.delete_blob(blob_name)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to delete blob: {str(e)}"
            )
