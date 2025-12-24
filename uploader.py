import os
import hashlib
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from api_client import ApiClient
from utils import get_part_size
import sys

class _139Uploader:
    def __init__(self, auth_token):
        self.api_client = ApiClient(auth_token)

    def upload_single_file(self, local_path, parent_id, interrupted_check_func=None):
        if interrupted_check_func and interrupted_check_func():
            print(f"\n[!] 上传文件 {os.path.basename(local_path)} 被中断。")
            return False, os.path.basename(local_path)

        name = os.path.basename(local_path)
        size = os.path.getsize(local_path)

        sha256_hash = hashlib.sha256()
        try:
            with open(local_path, "rb") as f:
                chunk_count = 0
                while chunk := f.read(1024*1024):
                    chunk_count += 1
                    if chunk_count % 1 == 0:
                        if interrupted_check_func and interrupted_check_func():
                            print(f"\n[!] 上传文件 {name} 被中断 (读取中)。")
                            return False, name
                    sha256_hash.update(chunk)
        except IOError as e:
            print(f"\n[!] 读取文件 {local_path} 失败: {e}")
            return False, name
        full_hash = sha256_hash.hexdigest().upper()

        part_size = get_part_size(size)
        part_count = 1
        if size > part_size:
            part_count = (size + part_size - 1) // part_size

        part_infos = []
        for i in range(part_count):
            start = i * part_size
            byte_size = min(size - start, part_size)
            part_number = i + 1
            part_info = {
                "partNumber": part_number,
                "partSize": byte_size,
                "parallelHashCtx": {
                    "partOffset": start
                }
            }
            part_infos.append(part_info)

        first_part_infos = part_infos[:100] if len(part_infos) > 100 else part_infos
        
        create_payload = {
            "contentHash": full_hash,
            "contentHashAlgorithm": "SHA256",
            "contentType": "application/octet-stream",
            "parallelUpload": False,
            "partInfos": first_part_infos,
            "size": size,
            "parentFileId": parent_id,
            "name": name,
            "type": "file",
            "fileRenameMode": "auto_rename"
        }
        
        if interrupted_check_func and interrupted_check_func():
            print(f"\n[!] 上传文件 {name} 被中断 (创建前)。")
            return False, name

        try:
            res = self.api_client.personal_post("/file/create", create_payload)
        except Exception as e:
            print(f"\n[!] 文件 {name} 创建请求失败: {e}")
            return False, name
        
        if res.get("code") != "0000":
            print(f"\n[!] 文件 {name} 创建失败: {res}")
            return False, name

        data = res.get("data", {})
        file_id = data.get("fileId")
        upload_id = data.get("uploadId")
        
        if data.get("exist"):
            return True, name

        if data.get("partInfos") is not None:
            total_size = os.path.getsize(local_path)
            pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc=f"上传 {name}", leave=True)
            
            try:
                with open(local_path, "rb") as f:
                    uploaded_bytes = 0
                    
                    for i, part_info in enumerate(data['partInfos']):
                        if interrupted_check_func and interrupted_check_func():
                            print(f"\n[!] 上传文件 {name} 被中断 (分片 {part_info['partNumber']})。")
                            pbar.close()
                            return False, name
                        
                        part_number = part_info['partNumber']
                        part_size = part_infos[part_number - 1]['partSize']
                        start_pos = part_infos[part_number - 1]['parallelHashCtx']['partOffset']
                        
                        f.seek(start_pos)
                        chunk = f.read(part_size)
                        
                        upload_url = part_info['uploadUrl']
                        
                        class ProgressBytesIO:
                            def __init__(self, data, pbar, interrupted_check_func, desc=""):
                                self.data = data
                                self.pbar = pbar
                                self.interrupted_check_func = interrupted_check_func
                                self.pos = 0
                                self.desc = desc
                            
                            def read(self, size=-1):
                                if self.interrupted_check_func and self.interrupted_check_func():
                                    print(f"\n[!] 上传流 {self.desc} 被中断。")
                                    return b''
                                
                                if size == -1:
                                    chunk = self.data[self.pos:]
                                    self.pos = len(self.data)
                                else:
                                    chunk = self.data[self.pos:self.pos+size]
                                    self.pos += len(chunk)
                                
                                if chunk:
                                    self.pbar.update(len(chunk))
                                
                                return chunk
                        
                        progress_stream = ProgressBytesIO(chunk, pbar, interrupted_check_func, name)
                        try:
                            put_resp = requests.put(upload_url, data=progress_stream, 
                                                  headers={"Content-Length": str(len(chunk))},
                                                  timeout=5)
                        except requests.exceptions.Timeout:
                            print(f"\n[!] 分片 {part_number} 上传超时")
                            pbar.close()
                            return False, name
                        except requests.exceptions.RequestException as e:
                            print(f"\n[!] 分片 {part_number} 上传请求失败: {e}")
                            pbar.close()
                            return False, name
                       
                        if put_resp.status_code not in [200, 201, 204]:
                            print(f"\n[!] 分片 {part_number} 上传失败，状态码: {put_resp.status_code}")
                            pbar.close()
                            return False, name
                        
                        uploaded_bytes += len(chunk)
                        pbar.set_postfix({"分片": f"{part_number}/{part_count}"})

                    for i in range(100, len(part_infos), 100):
                        if interrupted_check_func and interrupted_check_func():
                            print(f"\n[!] 上传文件 {name} 被中断 (处理后续分片)。")
                            pbar.close()
                            return False, name
                        
                        end = min(i + 100, len(part_infos))
                        batch_part_infos = part_infos[i:end]
                        
                        more_data = {
                            "fileId": file_id,
                            "uploadId": upload_id,
                            "partInfos": batch_part_infos,
                            "commonAccountInfo": {
                                "account": self.api_client.auth.split(" ")[1] if self.api_client.auth.startswith("Basic ") else self.api_client.auth,
                                "accountType": 1,
                            }
                        }
                        
                        pathname = "/file/getUploadUrl"
                        try:
                            more_resp = self.api_client.personal_post(pathname, more_data)
                        except Exception as e:
                            print(f"\n[!] 获取后续分片上传地址请求失败: {e}")
                            pbar.close()
                            return False, name
                        
                        if interrupted_check_func and interrupted_check_func():
                            print(f"\n[!] 上传文件 {name} 被中断 (获取后续地址)。")
                            pbar.close()
                            return False, name
                        
                        if more_resp.get("code") != "0000":
                            print(f"\n[!] 获取后续分片上传地址失败: {more_resp}")
                            pbar.close()
                            return False, name
                        
                        more_data_resp = more_resp.get("data", {})
                        
                        for j, part_info in enumerate(more_data_resp['partInfos']):
                            if interrupted_check_func and interrupted_check_func():
                                print(f"\n[!] 上传文件 {name} 被中断 (上传后续分片 {part_info['partNumber']})。")
                                pbar.close()
                                return False, name
                            
                            part_number = part_info['partNumber']
                            part_size = part_infos[part_number - 1]['partSize']
                            start_pos = part_infos[part_number - 1]['parallelHashCtx']['partOffset']
                            
                            f.seek(start_pos)
                            chunk = f.read(part_size)
                            
                            upload_url = part_info['uploadUrl']
                            
                            class ProgressBytesIO:
                                def __init__(self, data, pbar, interrupted_check_func, desc=""):
                                    self.data = data
                                    self.pbar = pbar
                                    self.interrupted_check_func = interrupted_check_func
                                    self.pos = 0
                                    self.desc = desc
                                
                                def read(self, size=-1):
                                    if self.interrupted_check_func and self.interrupted_check_func():
                                        print(f"\n[!] 上传流 {self.desc} 被中断。")
                                        return b''
                                    
                                    if size == -1:
                                        chunk = self.data[self.pos:]
                                        self.pos = len(self.data)
                                    else:
                                        chunk = self.data[self.pos:self.pos+size]
                                        self.pos += len(chunk)
                                    
                                    if chunk:
                                        self.pbar.update(len(chunk))
                                    
                                    return chunk
                            
                            progress_stream = ProgressBytesIO(chunk, pbar, interrupted_check_func, name)
                            try:
                                put_resp = requests.put(upload_url, data=progress_stream, 
                                                      headers={"Content-Length": str(len(chunk))},
                                                      timeout=5)
                            except requests.exceptions.Timeout:
                                print(f"\n[!] 后续分片 {part_number} 上传超时")
                                pbar.close()
                                return False, name
                            except requests.exceptions.RequestException as e:
                                print(f"\n[!] 后续分片 {part_number} 上传请求失败: {e}")
                                pbar.close()
                                return False, name
                            
                            if put_resp.status_code not in [200, 201, 204]:
                                print(f"\n[!] 后续分片 {part_number} 上传失败，状态码: {put_resp.status_code}")
                                pbar.close()
                                return False, name
                            
                            uploaded_bytes += len(chunk)
                            pbar.set_postfix({"分片": f"{part_number}/{part_count}"})

            except IOError as e:
                print(f"\n[!] 读取文件 {local_path} 时发生IO错误: {e}")
                pbar.close()
                return False, name
            finally:
                pbar.close()
                
            complete_payload = {
                "fileId": file_id,
                "uploadId": upload_id,
                "contentHash": full_hash,
                "contentHashAlgorithm": "SHA256"
            }
            
            if interrupted_check_func and interrupted_check_func():
                print(f"\n[!] 上传文件 {name} 被中断 (完成阶段)。")
                return False, name
            
            try:
                final_res = self.api_client.personal_post("/file/complete", complete_payload)
            except Exception as e:
                print(f"\n[!] 文件 {name} 完成上传请求失败: {e}")
                return False, name
            
            if final_res.get("code") == "0000":
                return True, name
            else:
                print(f"\n[!] 文件 {name} 完成上传失败: {final_res}")
                return False, name
        else:
            return True, name

    def parallel_upload_files(self, file_paths, parent_id, max_workers=3, interrupted_check_func=None):
        if not file_paths:
            return True
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {executor.submit(self.upload_single_file, file_path, parent_id, interrupted_check_func): file_path 
                              for file_path in file_paths}
            
            success_count = 0
            failed_files = []
            
            for future in as_completed(future_to_file):
                if interrupted_check_func and interrupted_check_func():
                    print("\n检测到中断信号，正在取消未完成的文件上传任务...")
                    for f, fp in future_to_file.items():
                        if not f.done():
                            print(f"取消文件上传: {fp}")
                            f.cancel()
                    print("文件上传任务已处理。")
                    return False
                
                file_path = future_to_file[future]
                try:
                    result, filename = future.result()
                    if result:
                        success_count += 1
                    else:
                        failed_files.append(file_path)
                except Exception as exc:
                    print(f"\n[!] 上传文件 {file_path} 时发生异常: {exc}")
                    failed_files.append(file_path)
        
        if failed_files:
            print(f"\n[!] 部分文件上传失败: {failed_files}")
        
        return success_count == len(file_paths)

    def create_folder_with_name(self, parent_id, folder_name, interrupted_check_func=None):
        if interrupted_check_func and interrupted_check_func():
            return None, folder_name
        
        existing_id = self.api_client.find_folder_by_name(parent_id, folder_name)
        if existing_id:
            return existing_id, folder_name
        
        folder_id = self.api_client.create_folder(parent_id, folder_name)
        if folder_id:
            return folder_id, folder_name
        else:
            print(f"\n[!] 无法创建文件夹: {folder_name}")
            return None, folder_name

    def upload_folder(self, local_folder_path, parent_id, max_workers=3, interrupted_check_func=None):
        original_folder_name = os.path.basename(local_folder_path)
        
        if interrupted_check_func and interrupted_check_func():
            print(f"\n[!] 上传文件夹 {original_folder_name} 被中断 (开始前)。")
            return False
        
        total_dirs = 0
        for root, dirs, files in os.walk(local_folder_path):
            if root != local_folder_path:
                total_dirs += len(dirs)
        
        print(f"需要创建 {total_dirs} 个子文件夹")
        
        root_folder_id, root_folder_name = self.create_folder_with_name(parent_id, original_folder_name, interrupted_check_func)
        if not root_folder_id:
            print(f"\n[!] 无法在云端创建文件夹: {original_folder_name}")
            return False
        
        if interrupted_check_func and interrupted_check_func():
            print(f"\n[!] 上传文件夹 {original_folder_name} 被中断 (创建云端文件夹后)。")
            return False
            
        dir_cloud_ids = {local_folder_path: root_folder_id}
        
        all_rel_paths = []
        for root, dirs, files in os.walk(local_folder_path):
            if root != local_folder_path:
                rel_path = os.path.relpath(root, local_folder_path)
                all_rel_paths.append(rel_path)

        all_rel_paths.sort(key=lambda x: x.count(os.sep))
        
        created_dirs = 0
        if all_rel_paths:
            print(f"开始创建 {total_dirs} 个子文件夹...")
            dir_pbar = tqdm(total=total_dirs, desc="创建子目录", unit="dir", leave=True)
            
            for level in range(len(all_rel_paths)):
                current_level_paths = []
                for rel_path in all_rel_paths:
                    if rel_path.count(os.sep) == level:
                        current_level_paths.append(rel_path)
                
                if not current_level_paths:
                    continue
                    
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_path = {}
                    
                    for rel_path in current_level_paths:
                        if interrupted_check_func and interrupted_check_func():
                            print(f"\n[!] 上传被中断 (创建目录层级: {level})")
                            dir_pbar.close()
                            return False

                        parts = rel_path.split(os.sep)
                        parent_path_parts = parts[:-1]
                        if parent_path_parts:
                            parent_rel_path = os.path.join(*parent_path_parts)
                            current_parent = dir_cloud_ids[os.path.join(local_folder_path, parent_rel_path)]
                        else:
                            current_parent = root_folder_id
                        
                        folder_name = parts[-1]
                        future = executor.submit(self.create_folder_with_name, current_parent, folder_name, interrupted_check_func)
                        future_to_path[future] = (rel_path, folder_name)
                    
                    for future in as_completed(future_to_path):
                        if interrupted_check_func and interrupted_check_func():
                            print(f"\n[!] 上传被中断 (等待目录创建完成: {level})")
                            dir_pbar.close()
                            for f, (path, name) in future_to_path.items():
                                if not f.done():
                                    print(f"取消创建目录: {path}")
                            return False
                        
                        folder_id, folder_name = future.result()
                        if folder_id:
                            rel_path, _ = future_to_path[future]
                            full_local_path = os.path.join(local_folder_path, rel_path)
                            dir_cloud_ids[full_local_path] = folder_id
                            created_dirs += 1
                            dir_pbar.update(1)
                        else:
                            rel_path, _ = future_to_path[future]
                            dir_pbar.close()
                            print(f"\n[!] 创建目录失败: {rel_path}")
                            return False
            
            dir_pbar.close()
        
        total_files = sum(len(files) for _, _, files in os.walk(local_folder_path))
        print(f"需要上传 {total_files} 个文件")
        
        file_upload_tasks = []
        for root, dirs, files in os.walk(local_folder_path):
            if files:
                file_paths = [os.path.join(root, f) for f in files]
                cloud_parent = dir_cloud_ids[root]
                file_upload_tasks.append((file_paths, cloud_parent))
        
        if total_files > 0:
            print(f"开始上传 {total_files} 个文件...")
            file_pbar = tqdm(total=total_files, desc="尝试秒传", unit="file", leave=True)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_task = {}
                for file_paths, cloud_parent in file_upload_tasks:
                    if interrupted_check_func and interrupted_check_func():
                        print("\n[!] 上传被中断 (文件上传阶段)")
                        file_pbar.close()
                        return False
                    future = executor.submit(self.parallel_upload_files, file_paths, cloud_parent, max_workers, interrupted_check_func)
                    future_to_task[future] = (file_paths, cloud_parent)
                
                success_count = 0
                for future in as_completed(future_to_task):
                    if interrupted_check_func and interrupted_check_func():
                        print("\n[!] 上传被中断 (处理文件上传结果)")
                        file_pbar.close()
                        for f, (paths, parent) in future_to_task.items():
                            if not f.done():
                                print(f"取消文件上传任务: {parent}")
                        return False
                    
                    success = future.result()
                    if success:
                        success_count += 1
                        task_file_paths, _ = future_to_task[future]
                        file_pbar.update(len(task_file_paths))
                    else:
                        file_pbar.close()
                        file_paths, cloud_parent = future_to_task[future]
                        print(f"\n[!] 部分文件上传失败，父目录ID: {cloud_parent}")
                        return False
            
            file_pbar.close()
        
        print(f"\n[+] 文件夹 {original_folder_name} 上传完成！")
        return True

    def parallel_upload(self, file_paths, parent_id, max_workers=3, interrupted_check_func=None):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {executor.submit(self.upload_single_file, file_path, parent_id, interrupted_check_func): file_path 
                              for file_path in file_paths}
            
            success_count = 0
            failed_files = []
            
            for future in as_completed(future_to_file):
                if interrupted_check_func and interrupted_check_func():
                    print("\n检测到中断信号，正在取消未完成的文件上传任务...")
                    for f, fp in future_to_file.items():
                        if not f.done():
                            print(f"取消文件上传: {fp}")
                            f.cancel()
                    print("文件上传任务已处理。")
                    return False
                
                file_path = future_to_file[future]
                try:
                    result, filename = future.result()
                    if result:
                        success_count += 1
                    else:
                        failed_files.append(file_path)
                except Exception as exc:
                    print(f"\n[!] 上传文件 {file_path} 时发生异常: {exc}")
                    failed_files.append(file_path)
        
        if failed_files:
            print(f"\n失败文件: {failed_files}")
        
        return success_count == len(file_paths)

    def parallel_upload_with_interrupt(self, file_paths, parent_id, max_workers=3, interrupted_check_func=None):
        if not file_paths:
            return True
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {}
            for file_path in file_paths:
                if interrupted_check_func and interrupted_check_func():
                    print("\n检测到中断信号，正在取消未完成的文件上传任务...")
                    return False
                future = executor.submit(self.upload_single_file, file_path, parent_id, interrupted_check_func)
                future_to_file[future] = file_path
            
            success_count = 0
            failed_files = []
            
            for future in as_completed(future_to_file):
                if interrupted_check_func and interrupted_check_func():
                    print("\n检测到中断信号，正在取消未完成的文件上传任务...")
                    for f, fp in future_to_file.items():
                        if not f.done():
                            print(f"取消文件上传: {fp}")
                            f.cancel()
                    print("文件上传任务已处理。")
                    return False
                
                file_path = future_to_file[future]
                try:
                    result, filename = future.result()
                    if result:
                        success_count += 1
                    else:
                        failed_files.append(file_path)
                except Exception as exc:
                    print(f"\n[!] 上传文件 {file_path} 时发生异常: {exc}")
                    failed_files.append(file_path)
        
        if failed_files:
            print(f"\n失败文件: {failed_files}")
        
        return success_count == len(file_paths)