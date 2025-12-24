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

    def personal_post(self, pathname, payload):
        url = f"{self.base_url}{pathname}"
        body_str = json.dumps(payload, separators=(',', ':'), ensure_ascii=False)
        
        # 毫秒时间戳格式
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
        
        resp = requests.post(url, headers=headers, data=body_str.encode('utf-8'), timeout=30 )
        return resp.json()

    def get_personal_files(self, file_id):
        # 获取个人云盘文件列表
        files = []
        next_page_cursor = ""
        
        while True:
            data = {
                "imageThumbnailStyleList": ["Small", "Large"],
                "orderBy": "updated_at",
                "orderDirection": "DESC",
                "pageInfo": {
                    "pageCursor": next_page_cursor,
                    "pageSize": 100,
                },
                "parentFileId": file_id,
            }
            
            res = self.personal_post("/file/list", data)
            
            if res.get("code") != "0000":
                print(f"[!] 获取文件列表失败: {res}")
                break
                
            data = res.get("data", {})
            next_page_cursor = data.get("nextPageCursor", "")
            
            for item in data.get("items", []):
                files.append({
                    "id": item.get("fileId"),
                    "name": item.get("name"),
                    "type": item.get("type"),
                    "size": item.get("size", 0),
                    "updated_at": item.get("updatedAt"),
                    "created_at": item.get("createdAt"),
                })
            
            if not next_page_cursor:
                break
                
        return files

    def create_folder(self, parent_id, folder_name):
        # 在网盘中创建文件夹
        data = {
            "parentFileId": parent_id,
            "name": folder_name,
            "description": "",
            "type": "folder",
            "fileRenameMode": "force_rename",
        }
        pathname = "/file/create"
        res = self.personal_post(pathname, data)
        
        if res.get("code") == "0000":
            return res.get("data", {}).get("fileId")
        else:
            print(f"[!] 创建文件夹 {folder_name} 失败: {res}")
            return None

    def find_folder_by_name(self, parent_id, folder_name):
        # 查找是否存在同名文件夹
        files = self.get_personal_files(parent_id)
        for file_info in files:
            if file_info["name"] == folder_name and file_info["type"] == "folder":
                return file_info["id"]
        return None

    def get_folder_id_by_path(self, folder_path):
        # 根据路径获取文件夹ID"""
        if not folder_path or folder_path == "/":
            return Config.DEFAULT_PARENT_ID
            
        # 移除开头的斜杠
        if folder_path.startswith("/"):
            folder_path = folder_path[1:]
        if folder_path.endswith("/"):
            folder_path = folder_path[:-1]
            
        # 分割路径
        path_parts = folder_path.split("/")
        
        current_id = Config.DEFAULT_PARENT_ID
        
        for part in path_parts:
            # 获取当前目录下的文件列表
            files = self.get_personal_files(current_id)
            
            # 查找对应的文件夹
            folder_found = False
            for file_info in files:
                if file_info["name"] == part and file_info["type"] == "folder":
                    current_id = file_info["id"]
                    folder_found = True
                    break
            
            if not folder_found:
                print(f"[!] 路径 '{folder_path}' 中的文件夹 '{part}' 不存在")
                return None
        
        return current_id

