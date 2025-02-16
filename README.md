# 議事録作成ツール
本ツールは、キャプチャーされた会議録画を音声解析し、効率的な議事録作成をサポートするツールです。<br>
mp4あるいはwav形式の音声データをアップロードするだけで、自動的に文字起こしを行い、要約やフォーマット整形された議事録を生成します。

# バックエンドの改定版
こちらのコードはバックエンドの改訂版になります。<br>
フロントエンド　→　HTTPリクエストを送信　→　AzureAppService<br>
AzureAppService →　queueにメッセージを追加　→　AzureFunctions（queueトリガ-／Container環境にデプロイ<br>
AzureAppService = ルートディレクトリ・queueTriger <br>
AzureFunctions  = ルートディレクトリ・api/app

## 主な機能
- **音声文字起こし**: Azure Speech Serviceで音声ファイルを文字起こしをします。
- **要約機能**: 文字起こしされたデータをAzure OpenAIに渡し、プロンプトを投げ要約の生成を行います。
- **要約された議事録の保存**: 要約された議事録をワードファイルとして保存することが可能です。

## 使用技術
- **フロントエンド**: React(Material UI)
- **バックエンド**: FastAPI
- **クラウドサービス**: Azure Speech Service, Azure OpenAI
- **ストレージ**: Azure Blob Storage

## インストール方法
以下の手順で環境構築を行ってください。

1. **リポジトリをクローン**
   ```bash
   git clone https://github.com/sakuya10969/voice_recognition.git
   cd voice_recognition
   ```

2. **依存関係のインストール**
   ```
   cd client
   npm install
   cd ..
   cd api
   pip install -r requirements.txt
   ```
