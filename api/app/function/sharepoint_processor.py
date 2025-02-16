import msal
import requests
from functools import cache

class SharePointAccessClass:
    # 初期化
    def __init__(self, client_id, client_secret, tenant_id):
        """
        Initialize the SharePointAccessClass
        """
        self.client_id = client_id # アプリケーション(クライアント)ID
        self.client_secret = client_secret # シークレット(値)
        self.tenant_id = tenant_id # ディレクトリ(テナント)ID
        self.authority = f"https://login.microsoftonline.com/{tenant_id}"
        self.scope = ["https://graph.microsoft.com/.default"]
        self.access_token: None | str = None
        self.get_access_token()

    # Access Tokenを取得する
    def get_access_token(self):
        """
        Get the access token using the client_id, client_secret, and tenant_id
        """
        # Create a confidential client application using msal library
        """msalを使用してアクセストークンを取得します"""
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=self.authority,
            client_credential=self.client_secret
        )
        result = app.acquire_token_for_client(scopes=self.scope)
        if "access_token" in result:
            # Save the access token
            self.access_token = result["access_token"]
        else:
            raise Exception("No access token available")

    # Graph APIを使用してデータを取得する汎用GETメソッド
    @cache
    def graph_api_get(self, endpoint: str) -> requests.models.Response | None:
        """
        Get data from Graph API using the endpoint
        """
        if self.access_token is not None:
            graph_data = requests.get(
                endpoint,
                headers={'Authorization': 'Bearer ' + self.access_token})
            return graph_data
        else:
            raise Exception("No access token available")

        # Graph APIを使用してデータを送信する汎用PUTメソッド
    def graph_api_put(self, endpoint: str, data) -> requests.models.Response | None:
        """
        Post data to Graph API using the endpoint
        """
        if self.access_token is not None:
            graph_data = requests.put(
                url=endpoint,
                headers={'Authorization': 'Bearer ' + self.access_token},
                data=data)
            return graph_data
        else:
            raise Exception("No access token available")

    # サイト一覧を取得する
    def get_sites(self):
        """
        Get Sites in SharePoint
        """
        endpoints = self.graph_api_get("https://graph.microsoft.com/v1.0/sites")
        return endpoints.json()

    # サイト名からサイトIDを取得する
    def get_site_id(self, site_name):
        """
        Get Site_id  using the site_name
        """
        sites = self.get_sites()
        for site in sites['value']:
            if site['name'] == site_name:
                print(f"site: {site}")
                return site['id']
        return None

    # サイトIDからサイトのフォルダを全て取得する
    def get_folders(self, site_id, folder_id='root'):
        folders = self.graph_api_get(
            f'https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{folder_id}/children')
        if folders is not None:
            return folders.json()
        else:
            return None

    # サイトIDからサイトのフォルダIdを取得する
    def get_folder_id(self, site_id, folder_name, folder_id='root'):
        folders = self.get_folders(site_id, folder_id)
        for folder in folders['value']:
            if folder_name == folder["name"]:
                return folder['id']
        return None

    # サイトIDからサイトのフォルダを取得する
    def get_folder(self, site_id, folder_name, folder_id='root'):
        subfolders = self.get_folders(site_id, folder_id)
        for folder in subfolders['value']:
            if folder_name == folder["name"]:
                return folder
        return None

    # 指定されたサイトIDのサイトから、指定されたディレクトリツリーの最下層のフォルダIDを取得する
    def get_folder_id_from_tree(self, site_id, sharepoint_directory, folder_id='root'):
        # 各ディレクトリを上から順に表示
        folder_id = self.get_folder_id(site_id, sharepoint_directory, folder_id)
        return folder_id

    # ファイルのアップロード
    def upload_file(self, target_site_name, sharepoint_directory, object_file_path):
        """
        Upload a file to SharePoint using the target_site_name, sharepoint_directory, and object_file_path
        """
        # ターゲットサイトのIDを取得
        target_site_id = self.get_site_id(target_site_name)
        # フォルダIDを取得
        folder_id = self.get_folder_id_from_tree(target_site_id, sharepoint_directory, 'root')
        if folder_id:
            # アップロードURLを作成
            url = f'https://graph.microsoft.com/v1.0/sites/{target_site_id}/drive/items/{folder_id}:/{object_file_path.name}:/content'
            # ファイルをアップロード
            with open(object_file_path, 'rb') as f:
                graph_data = self.graph_api_put(url, f)
            # アップロード結果を返す
            return graph_data.json()
        else:
            return "Folder not found"
