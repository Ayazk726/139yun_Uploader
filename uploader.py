import os
import hashlib
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from api_client import ApiClient
from utils import get_part_size
import threading
import sys
import time
import json
import traceback

class _139Uploader:
    def __init__(self, auth_token):
        self.api_client = ApiClient(auth_token)
        self.progress_file = "upload_progress.json"
        self.progress_lock = threading.Lock()
        self.all_progress = self._load_all_progress()
        
        self.pbar_position_lock = threading.Lock()
        self.active_positions = set()
        self.log_lock = threading.Lock()
        self.dir_map_lock = threading.Lock()

    def _log(self, message):
        with self.log_lock:
            t_name = threading.current_thread().name
            print(f"[{t_name}] {message}")

    def _safe_api_call(self, endpoint, payload):
        try:
            res = self.api_client.personal_post(endpoint, payload)
            if res is None:
                return {"code": "EMPTY_RESPONSE", "message": "Server returned None"}
            return res
        except Exception as e:
            return {"code": "EXCEPTION", "message": str(e)}

    def _load_all_progress(self):
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except: return {}
        return {}

    def _save_to_disk(self):
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(self.all_progress, f, ensure_ascii=False, indent=4)

    def get_file_progress(self, local_path):
        abs_path = os.path.abspath(local_path)
        with self.progress_lock:
            return self.all_progress.get(abs_path)

    def save_file_progress(self, local_path, progress_data):
        abs_path = os.path.abspath(local_path)
        with self.progress_lock:
            self.all_progress[abs_path] = progress_data
            self._save_to_disk()

    def clear_file_progress(self, local_path):
        abs_path = os.path.abspath(local_path)
        with self.progress_lock:
            if abs_path in self.all_progress:
                del self.all_progress[abs_path]
                self._save_to_disk()

    def _get_available_position(self):
        with self.pbar_position_lock:
            for pos in range(1, 100):
                if pos not in self.active_positions:
                    self.active_positions.add(pos)
                    return pos
            return 1

    def _release_position(self, pos):
        with self.pbar_position_lock:
            if pos in self.active_positions:
                self.active_positions.remove(pos)

    def _update_file_pbar(self, file_pbar):
        if file_pbar:
            with file_pbar.get_lock():
                file_pbar.update(1)

    def _upload_chunk_with_retry(self, upload_url, chunk_data, part_number, pbar, interrupted_check_func, filename):
        class ProgressBytesIO:
            def __init__(self, data, pbar, interrupted_check_func):
                self.data = data
                self.pbar = pbar
                self.interrupted_check_func = interrupted_check_func
                self.pos = 0
                self.lock = threading.Lock()
            def read(self, size=-1):
                if self.interrupted_check_func and self.interrupted_check_func(): return b''
                chunk = self.data[self.pos:] if size == -1 else self.data[self.pos:self.pos+size]
                self.pos += len(chunk)
                if chunk and self.pbar: # 如果 pbar 为 None 则不更新
                    with self.lock: self.pbar.update(len(chunk))
                return chunk

        for retry_count in range(5):
            if interrupted_check_func and interrupted_check_func(): return False
            stream = ProgressBytesIO(chunk_data, pbar, interrupted_check_func)
            try:
                resp = requests.put(upload_url, data=stream, headers={"Content-Length": str(len(chunk_data))}, timeout=45)
                if resp.status_code in [200, 201, 204]: return True
            except: pass
            time.sleep(2 ** retry_count)
        return False

    def _complete_upload_with_retry(self, payload, filename, interrupted_check_func):
        for i in range(5):
            if interrupted_check_func and interrupted_check_func(): return False
            res = self._safe_api_call("/file/complete", payload)
            if res.get("code") == "0000" or "exist" in str(res).lower() or "success" in str(res).lower(): 
                return True
            time.sleep(2 ** i)
        return False

    def upload_single_file(self, local_path, parent_id, interrupted_check_func=None, file_pbar=None):
        name = os.path.basename(local_path)
        is_task_finished = False 
        try:
            if interrupted_check_func and interrupted_check_func(): return False, name
            progress_data = self.get_file_progress(local_path)
            if progress_data:
                success, _ = self.resume_upload(local_path, parent_id, progress_data, interrupted_check_func, None)
                is_task_finished = True
                return success, name

            size = os.path.getsize(local_path)
            sha256 = hashlib.sha256()
            with open(local_path, "rb") as f:
                while chunk := f.read(1024*1024):
                    if interrupted_check_func and interrupted_check_func(): return False, name
                    sha256.update(chunk)
            full_hash = sha256.hexdigest().upper()

            part_size = get_part_size(size)
            part_count = (size + part_size - 1) // part_size if size > 0 else 1
            part_infos = [{"partNumber": i+1, "partSize": min(size - i*part_size, part_size), 
                           "parallelHashCtx": {"partOffset": i*part_size}} for i in range(part_count)]

            res = self._safe_api_call("/file/create", {
                "contentHash": full_hash, "contentHashAlgorithm": "SHA256", "size": size,
                "parentFileId": parent_id, "name": name, "type": "file", "fileRenameMode": "auto_rename",
                "partInfos": part_infos[:100]
            })
            
            data = res.get("data", {})
            if res.get("code") != "0000" or data.get("exist"):
                is_task_finished = True
                return (True, name) if data.get("exist") else (False, name)

            file_id, upload_id = data.get("fileId"), data.get("uploadId")
            
            # 小于 10MB 不创建子进度条
            pbar = None
            my_pos = None
            if size >= 10 * 1024 * 1024:
                my_pos = self._get_available_position()
                pbar = tqdm(total=size, unit='B', unit_scale=True, unit_divisor=1024, desc=f"  ↳ {name[:15]}", leave=False, position=my_pos)
            
            upload_info = {"file_id": file_id, "upload_id": upload_id, "part_infos": part_infos, "completed_parts": []}
            pure_auth = self.api_client.auth.replace("Basic ", "")

            with open(local_path, "rb") as f:
                for i in range(0, len(part_infos), 100):
                    batch_parts = part_infos[i:i+100]
                    urls_res = self._safe_api_call("/file/getUploadUrl", {
                               "fileId": file_id, "uploadId": upload_id, "partInfos": batch_parts,
                               "commonAccountInfo": {"account": pure_auth, "accountType": 1}
                           })
                    urls = urls_res.get("data", {}).get("partInfos", []) if urls_res.get("code") == "0000" else []
                    if not urls:
                        if pbar: pbar.close(); self._release_position(my_pos)
                        is_task_finished = True; return False, name

                    for u_info in urls:
                        p_num = u_info['partNumber']
                        f.seek(part_infos[p_num-1]['parallelHashCtx']['partOffset'])
                        if self._upload_chunk_with_retry(u_info['uploadUrl'], f.read(part_infos[p_num-1]['partSize']), p_num, pbar, interrupted_check_func, name):
                            upload_info["completed_parts"].append(p_num)
                            self.save_file_progress(local_path, upload_info)
                        else:
                            if pbar: pbar.close(); self._release_position(my_pos)
                            is_task_finished = True; return False, name
            
            if pbar: pbar.close(); self._release_position(my_pos)
            success = self._complete_upload_with_retry({"fileId": file_id, "uploadId": upload_id, "contentHash": full_hash, "contentHashAlgorithm": "SHA256"}, name, interrupted_check_func)
            if success: self.clear_file_progress(local_path)
            is_task_finished = True
            return success, name

        except Exception as e:
            self._log(f"任务 {name} 异常: {str(e)}")
            is_task_finished = True; return False, name
        finally:
            if is_task_finished: self._update_file_pbar(file_pbar)

    def resume_upload(self, local_path, parent_id, progress_data, interrupted_check_func, file_pbar):
        name = os.path.basename(local_path)
        my_pos = None
        pbar = None
        try:
            file_id, upload_id = progress_data["file_id"], progress_data["upload_id"]
            part_infos, completed = progress_data["part_infos"], progress_data["completed_parts"]
            size = os.path.getsize(local_path)
            pure_auth = self.api_client.auth.replace("Basic ", "")
            
            # 二次判断
            if size >= 10 * 1024 * 1024:
                my_pos = self._get_available_position()
                pbar = tqdm(total=size, unit='B', unit_scale=True, unit_divisor=1024, desc=f"  续传 {name[:15]}", leave=False, position=my_pos)
                pbar.update(sum(p['partSize'] for p in part_infos if p['partNumber'] in completed))

            with open(local_path, "rb") as f:
                for p in part_infos:
                    if p['partNumber'] in completed: continue
                    if interrupted_check_func and interrupted_check_func(): break
                    url_res = self._safe_api_call("/file/getUploadUrl", {
                        "fileId": file_id, "uploadId": upload_id, "partInfos": [p],
                        "commonAccountInfo": {"account": pure_auth, "accountType": 1}
                    })
                    parts_data = url_res.get("data", {}).get("partInfos", []) if url_res.get("code") == "0000" else []
                    if not parts_data: return False, name
                        
                    up_url = parts_data[0].get("uploadUrl")
                    f.seek(p['parallelHashCtx']['partOffset'])
                    if self._upload_chunk_with_retry(up_url, f.read(p['partSize']), p['partNumber'], pbar, interrupted_check_func, name):
                        completed.append(p['partNumber']); self.save_file_progress(local_path, progress_data)
                    else: return False, name
            
            if pbar: pbar.close()
            sha = hashlib.sha256()
            with open(local_path, "rb") as f:
                while chunk := f.read(1024*1024): sha.update(chunk)
            success = self._complete_upload_with_retry({"fileId": file_id, "uploadId": upload_id, "contentHash": sha.hexdigest().upper(), "contentHashAlgorithm": "SHA256"}, name, interrupted_check_func)
            if success: self.clear_file_progress(local_path)
            return success, name
        except: return False, name
        finally:
            if my_pos: self._release_position(my_pos)
            if file_pbar: self._update_file_pbar(file_pbar)

    def create_folder_with_name(self, parent_id, folder_name, interrupted_check_func=None):
        if interrupted_check_func and interrupted_check_func(): return None
        existing_id = self.api_client.find_folder_by_name(parent_id, folder_name)
        if existing_id: return existing_id
        return self.api_client.create_folder(parent_id, folder_name)

    def upload_folder(self, local_folder_path, parent_id, max_workers=3, interrupted_check_func=None):
        folder_name = os.path.basename(local_folder_path)
        root_id = self.create_folder_with_name(parent_id, folder_name, interrupted_check_func)
        if not root_id: return False

        dir_cloud_ids = {os.path.abspath(local_folder_path): root_id}
        all_dirs = []
        for root, dirs, _ in os.walk(local_folder_path):
            for d in dirs: all_dirs.append(os.path.abspath(os.path.join(root, d)))
        
        all_dirs.sort(key=lambda x: x.count(os.sep))
        
        if all_dirs:
            depth_groups = {}
            for path in all_dirs:
                d = path.count(os.sep)
                if d not in depth_groups: depth_groups[d] = []
                depth_groups[d].append(path)
            
            dir_pbar = tqdm(total=len(all_dirs), desc="正在创建目录结构", unit="dir", leave=False)
            with ThreadPoolExecutor(max_workers=10) as executor:
                for d in sorted(depth_groups.keys()):
                    paths_at_depth = depth_groups[d]
                    future_to_path = {}
                    for path in paths_at_depth:
                        if interrupted_check_func and interrupted_check_func(): break
                        parent_path = os.path.dirname(path)
                        p_id = dir_cloud_ids.get(parent_path)
                        if p_id:
                            future = executor.submit(self.create_folder_with_name, p_id, os.path.basename(path), interrupted_check_func)
                            future_to_path[future] = path
                    
                    for future in as_completed(future_to_path):
                        path = future_to_path[future]
                        res_id = future.result()
                        if res_id:
                            with self.dir_map_lock:
                                dir_cloud_ids[path] = res_id
                            dir_pbar.update(1)
            dir_pbar.close()

        file_tasks = []
        for root, _, files in os.walk(local_folder_path):
            abs_root = os.path.abspath(root)
            if files and abs_root in dir_cloud_ids:
                file_tasks.append(([os.path.join(root, f) for f in files], dir_cloud_ids[abs_root]))

        if file_tasks:
            total_files = sum(len(t[0]) for t in file_tasks)
            file_pbar = tqdm(total=total_files, desc="总上传进度", unit="file", leave=True, position=0)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                all_futures = []
                for file_paths, p_id in file_tasks:
                    for fp in file_paths:
                        all_futures.append(executor.submit(self.upload_single_file, fp, p_id, interrupted_check_func, file_pbar))
                for f in as_completed(all_futures):
                    if interrupted_check_func and interrupted_check_func(): break
            file_pbar.close()
        return True