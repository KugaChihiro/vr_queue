def send_message_to_queue(project: str,project_Directory: str,file_path: str,client_id: str):
    from azure.storage.queue import QueueClient
    import json
    import os
    from dotenv import load_dotenv

    # .env を読み込む
    load_dotenv()

    # 環境変数から connection_string を取得
    connection_string = os.getenv("CONNECTION_STRING")
    queue_name = os.getenv("QUEUE_NAME")
    
    # QueueClientのインスタンスを作成
    queue_client = QueueClient.from_connection_string(connection_string, queue_name)
    message = "start_vm_task"

    # メッセージとして送信するデータを作成
    message_data = {
        "project":project,
        "project_Directory":project_Directory,
        "file_path": file_path,
        "client_id":client_id,
        "message": message
    }
    
    # JSON形式にシリアライズしてメッセージを送信
    queue_client.send_message(json.dumps(message_data))
    print(f"Sent message with file path and task id: {file_path}, {message}")