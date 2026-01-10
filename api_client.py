import requests
import json
import time
import random
import string
from config import Config
from utils import cal_sign

class ApiClient:
    def __init__(self, auth_token):
        self.auth = auth_token if auth_token.startswith("Basic ") else f"Basic {auth_token}"
        self.base_url = Config.BASE_URL
        self.dev_info = Config.DEV_INFO
        # 引入 Session 保持长连接
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=50)
        self.session.mount('https://', adapter)

    def personal_post(self, pathname, payload):
        url = f"{self.base_url}{pathname}"
        body_str = json.dumps(payload, separators=(',', ':'), ensure_ascii=False)
        
        ts = str(int(time.time() * 1000))
        rand_str = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        sign = cal_sign(body_str, ts, rand_str)
        
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Authorization": self.auth,
            "Content-Type": "application/json;charset=UTF-8",
            "Caller": "web",
            "Cms-Device": "default",
            "Mcloud-Channel": "1000101",
            "Mcloud-Client": "10701",
            "Mcloud-Sign": f"{ts},{rand_str},{sign}",
            "Mcloud-Version": "7.14.0",
            "x-DeviceInfo": self.dev_info,
            "X-Yun-Api-Version": "v1",
            "X-Yun-App-Channel": "10000034",
            "X-Yun-Client-Info": self.dev_info + "dW5kZWZpbmVk||",
            "Origin": "https://yun.139.com",
            "Referer": "https://yun.139.com/",
            "X-Requested-With": "XMLHttpRequest"
        }
        
        resp = self.session.post(url, headers=headers, data=body_str.encode('utf-8'))
        return resp.json()

    def get_personal_files(self, file_id):
        files = []
        next_page_cursor = ""
        while True:
            data = {
                "imageThumbnailStyleList": ["Small", "Large"],
                "orderBy": "updated_at",
                "orderDirection": "DESC",
                "pageInfo": {"pageCursor": next_page_cursor, "pageSize": 100},
                "parentFileId": file_id,
            }
            res = self.personal_post("/file/list", data)
            if res.get("code") != "0000": break
            data_res = res.get("data", {})
            next_page_cursor = data_res.get("nextPageCursor", "")
            for item in data_res.get("items", []):
                files.append({"id": item.get("fileId"), "name": item.get("name"), "type": item.get("type")})
            if not next_page_cursor: break
        return files

    def create_folder(self, parent_id, folder_name):
        data = {
            "parentFileId": parent_id,
            "name": folder_name,
            "description": "",
            "type": "folder",
            "fileRenameMode": "force_rename",
        }
        res = self.personal_post("/file/create", data)
        return res.get("data", {}).get("fileId") if res.get("code") == "0000" else None

    def find_folder_by_name(self, parent_id, folder_name):
        files = self.get_personal_files(parent_id)
        for file_info in files:
            if file_info["name"] == folder_name and file_info["type"] == "folder":
                return file_info["id"]
        return None

    def get_folder_id_by_path(self, folder_path):
        if not folder_path or folder_path == "/": return Config.DEFAULT_PARENT_ID
        parts = [p for p in folder_path.strip("/").split("/") if p]
        current_id = Config.DEFAULT_PARENT_ID
        for part in parts:
            found_id = self.find_folder_by_name(current_id, part)
            if not found_id: return None
            current_id = found_id
        return current_id