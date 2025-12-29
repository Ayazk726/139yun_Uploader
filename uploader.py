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

class FileSliceReader:
    """
    仿文件对象，仅在 requests 读取时从磁盘读取数据，避免内存溢出。
    """
    def __init__(self, filepath, offset, length, pbar=None, interrupt_func=None):
        self.filepath = filepath
        self.offset = offset
        self.length = length
        self.pbar = pbar
        self.interrupt_func = interrupt_func
        self.f = open(filepath, 'rb')
        self.f.seek(offset)
        self.remaining = length
        self.lock = threading.Lock()

    def read(self, size=-1):
        if self.interrupt_func and self.interrupt_func():
            raise InterruptedError("Task interrupted")
        if self.remaining <= 0: return b''
        read_size = self.remaining if (size is None or size < 0) else min(size, self.remaining)
        data = self.f.read(read_size)
        self.remaining -= len(data)
        if self.pbar:
            with self.lock: self.pbar.update(len(data))
        return data

    def close(self):
        if hasattr(self, 'f') and self.f:
            self.f.close()
            self.f = None

    def __del__(self):
        self.close()

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
            print(f"[{threading.current_thread().name}] {message}")

    def _safe_api_call(self, endpoint, payload):
        try:
            res = self.api_client.personal_post(endpoint, payload)
            return res if res is not None else {"code": "EMPTY_RESPONSE"}
        except Exception as e:
            return {"code": "EXCEPTION", "message": str(e)}

    def _load_all_progress(self):
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception: return {}
        return {}

    def _save_to_disk(self):
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(self.all_progress, f, ensure_ascii=False, indent=4)

    def get_file_progress(self, local_path):
        abs_path = os.path.abspath(local_path)
        with self.progress_lock: return self.all_progress.get(abs_path)

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
            # 从位置 1 开始，位置 0 预留给总体进度条
            for pos in range(1, 100):
                if pos not in self.active_positions:
                    self.active_positions.add(pos)
                    return pos
            return 1

    def _release_position(self, pos):
        with self.pbar_position_lock:
            if pos in self.active_positions: self.active_positions.remove(pos)

    def _update_file_pbar(self, file_pbar):
        if file_pbar:
            with file_pbar.get_lock(): file_pbar.update(1)

    def _upload_chunk_with_retry(self, upload_url, local_path, offset, size, pbar, interrupted_check_func):
        for retry_count in range(5):
            if interrupted_check_func and interrupted_check_func(): return False
            stream = FileSliceReader(local_path, offset, size, pbar, interrupted_check_func)
            try:
                resp = requests.put(upload_url, data=stream, headers={"Content-Length": str(size)}, timeout=60)
                stream.close()
                if resp.status_code in [200, 201, 204]: return True
            except Exception:
                stream.close()
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

    def prepare_file_metadata(self, local_path, parent_id, interrupted_check_func=None):
        name = os.path.basename(local_path)
        try:
            if interrupted_check_func and interrupted_check_func(): return None
            size = os.path.getsize(local_path)

            if size > 50 * 1024 * 1024 * 1024:
                tqdm.write(f"[!] 文件过大: {name} ({size/1024**3:.2f}GB)。已跳过。")
                return {"mode": "finished", "name": name, "success": False, "local_path": local_path}

            pd = self.get_file_progress(local_path)
            if pd:
                return {"mode": "resume", "local_path": local_path, "name": name, "size": size, "progress_data": pd, "parent_id": parent_id}

            sha256 = hashlib.sha256()
            
            # 修改 2: 处理校验进度条覆写及 256MB 显示限制
            hpbar = None
            my_pos = None
            if size >= 256 * 1024 * 1024:
                my_pos = self._get_available_position()
                hpbar = tqdm(total=size, unit='B', unit_scale=True, unit_divisor=1024, 
                             desc=f"校验 {name[:10]}", leave=False, position=my_pos)
            
            try:
                with open(local_path, "rb") as f:
                    while chunk := f.read(1024*1024): 
                        if interrupted_check_func and interrupted_check_func(): return None
                        sha256.update(chunk)
                        if hpbar: hpbar.update(len(chunk))
            finally:
                if hpbar:
                    hpbar.close()
                    self._release_position(my_pos)
            
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
                return {"mode": "finished", "name": name, "success": True, "local_path": local_path}

            return {"mode": "new", "local_path": local_path, "name": name, "size": size, "file_id": data.get("fileId"), "upload_id": data.get("uploadId"), "part_infos": part_infos, "full_hash": full_hash}
        except Exception as e:
            self._log(f"准备失败 {name}: {e}")
            return {"mode": "finished", "name": name, "success": False, "local_path": local_path}

    def execute_file_upload(self, task_context, interrupted_check_func, file_pbar):
        local_path, name = task_context["local_path"], task_context["name"]
        if task_context["mode"] == "finished":
            self._update_file_pbar(file_pbar); return task_context["success"], name

        my_pos, pbar, success = None, None, False
        try:
            if task_context["mode"] == "resume":
                pd = task_context["progress_data"]
                file_id, upload_id = pd["file_id"], pd["upload_id"]
                part_infos, completed, full_hash = pd["part_infos"], pd["completed_parts"], None
            else:
                file_id, upload_id, part_infos, completed, full_hash = task_context["file_id"], task_context["upload_id"], task_context["part_infos"], [], task_context["full_hash"]

            size = task_context["size"]
            pure_auth = self.api_client.auth.replace("Basic ", "")
            
            # 始终使用动态 Position 避免覆写
            if size >= 10 * 1024 * 1024:
                my_pos = self._get_available_position()
                desc = "  续传" if task_context["mode"] == "resume" else "  ↳"
                pbar = tqdm(total=size, unit='B', unit_scale=True, unit_divisor=1024, desc=f"{desc} {name[:15]}", leave=False, position=my_pos)
                if completed: pbar.update(sum(p['partSize'] for p in part_infos if p['partNumber'] in completed))

            for i in range(0, len(part_infos), 100):
                batch = [p for p in part_infos[i:i + 100] if p['partNumber'] not in completed]
                if not batch: continue
                urls_res = self._safe_api_call("/file/getUploadUrl", {"fileId": file_id, "uploadId": upload_id, "partInfos": batch, "commonAccountInfo": {"account": pure_auth, "accountType": 1}})
                raw_urls = urls_res.get("data", {}).get("partInfos", []) if urls_res.get("code") == "0000" else []
                if not raw_urls: break
                url_map = {u['partNumber']: u['uploadUrl'] for u in raw_urls}

                for p_item in batch:
                    p_num = p_item['partNumber']
                    if self._upload_chunk_with_retry(url_map.get(p_num), local_path, p_item['parallelHashCtx']['partOffset'], p_item['partSize'], pbar, interrupted_check_func):
                        completed.append(p_num)
                        self.save_file_progress(local_path, {"file_id": file_id, "upload_id": upload_id, "part_infos": part_infos, "completed_parts": completed})
                    else: raise Exception("分片上传失败")

            if pbar: pbar.close(); self._release_position(my_pos); my_pos = None
            if not full_hash:
                sha = hashlib.sha256()
                with open(local_path, "rb") as f:
                    while chunk := f.read(1024 * 1024): sha.update(chunk)
                full_hash = sha.hexdigest().upper()

            success = self._complete_upload_with_retry({"fileId": file_id, "uploadId": upload_id, "contentHash": full_hash, "contentHashAlgorithm": "SHA256"}, name, interrupted_check_func)
            if success: self.clear_file_progress(local_path)
            return success, name
        except Exception: return False, name
        finally:
            if my_pos: self._release_position(my_pos)
            if pbar: pbar.close()
            self._update_file_pbar(file_pbar)

    def create_folder_with_name(self, parent_id, folder_name, interrupted_check_func=None):
        if interrupted_check_func and interrupted_check_func(): return None
        existing_id = self.api_client.find_folder_by_name(parent_id, folder_name)
        return existing_id if existing_id else self.api_client.create_folder(parent_id, folder_name)

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

            dir_pbar = tqdm(total=len(all_dirs), desc="创建目录结构", unit="dir", leave=False)
            with ThreadPoolExecutor(max_workers=10) as executor:
                for d in sorted(depth_groups.keys()):
                    paths = depth_groups[d]
                    futures = {}
                    for path in paths:
                        if interrupted_check_func and interrupted_check_func(): break
                        p_id = dir_cloud_ids.get(os.path.dirname(path))
                        if p_id:
                            f = executor.submit(self.create_folder_with_name, p_id, os.path.basename(path), interrupted_check_func)
                            futures[f] = path
                    for f in as_completed(futures):
                        res_id = f.result()
                        if res_id:
                            with self.dir_map_lock: dir_cloud_ids[futures[f]] = res_id
                            dir_pbar.update(1)
            dir_pbar.close()

        file_tasks = []
        for root, _, files in os.walk(local_folder_path):
            abs_root = os.path.abspath(root)
            if files and abs_root in dir_cloud_ids:
                file_tasks.append(([os.path.join(root, f) for f in files], dir_cloud_ids[abs_root]))
        if not file_tasks: return True

        total_files = sum(len(t[0]) for t in file_tasks)
        # 总进度条固定在 position 0
        file_pbar = tqdm(total=total_files, desc="总文件进度", unit="file", position=0, leave=True)
        
        # 修改 1: Hash 线程数受传入的 max_workers 控制
        h_exec = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="HashWorker")
        u_exec = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="NetWorker")
        h_futures, u_futures = [], []
        try:
            for f_paths, p_id in file_tasks:
                for fp in f_paths:
                    h_futures.append(h_exec.submit(self.prepare_file_metadata, fp, p_id, interrupted_check_func))
            for f in as_completed(h_futures):
                if interrupted_check_func and interrupted_check_func(): break
                ctx = f.result()
                if ctx: u_futures.append(u_exec.submit(self.execute_file_upload, ctx, interrupted_check_func, file_pbar))
            for f in as_completed(u_futures):
                if interrupted_check_func and interrupted_check_func(): break
                f.result()
        finally:
            h_exec.shutdown(wait=False); u_exec.shutdown(wait=True); file_pbar.close()
        return True
