from azure.storage.blob import BlobServiceClient
from fastapi import HTTPException


async def upload_blob(file_name: str, file_data: bytes, container_name: str, blob_connection: str) -> str:
    """
    Azure Blob Storageにファイルをアップロードする関数。

    :param container_name: アップロード先のコンテナ名
    :param blob_name: アップロードするBlobの名前
    :param blob_connection: Azure Blob Storageの接続文字列
    :param file_data: アップロードするファイルのバイナリデータ
    :return: アップロードしたBlobのURL
    """
    try:
        # BlobServiceClientの初期化
        blob_service_client = BlobServiceClient.from_connection_string(blob_connection)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

        # ファイルをアップロード
        blob_client.upload_blob(file_data, overwrite=True)

        # アップロードしたBlobのURLを返す
        return blob_client.url
    except Exception as e:
        # エラー発生時はFastAPI用のHTTPExceptionをスロー
        raise HTTPException(status_code=500, detail=f"Failed to upload blob: {str(e)}")


async def delete_blob(blob_name: str, container_name: str, connection_string: str):
    """
    Azure Blob Storageからファイルを削除する関数。

    :param container_name: 削除対象のコンテナ名
    :param blob_name: 削除するBlobの名前
    :param connection_string: Azure Blob Storageの接続文字列
    """
    try:
        # BlobServiceClientの初期化
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        # Blobを削除
        container_client.delete_blob(blob_name)
    except Exception as e:
        # エラー発生時はFastAPI用のHTTPExceptionをスロー
        raise HTTPException(status_code=500, detail=f"Failed to delete blob: {str(e)}")
