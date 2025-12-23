import os
import hashlib
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from api_client import ApiClient
from utils import get_part_size

class _139Uploader:
    def __init__(self, auth_token):
        self.api_client = ApiClient(auth_token)

    def upload_single_file(self, local_path, parent_id):
        #上传单个文件
        name = os.path.basename(local_path)
        size = os.path.getsize(local_path)

        # 计算 SHA256 - 用于初始化上传
        sha256_hash = hashlib.sha256()
        with open(local_path, "rb") as f:
            while chunk := f.read(1024*1024):
                sha256_hash.update(chunk)
        full_hash = sha256_hash.hexdigest().upper()

        # 计算分片大小和数量
        part_size = get_part_size(size)
        part_count = 1
        if size > part_size:
            part_count = (size + part_size - 1) // part_size

        # 生成所有 partInfos
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

        # 筛选出前100个 partInfos
        first_part_infos = part_infos[:100] if len(part_infos) > 100 else part_infos

        # Step 1: Create - 初始化上传
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
        
        res = self.api_client.personal_post("/file/create", create_payload)
        
        if res.get("code") != "0000":
            print(f"[!] 文件 {name} 创建失败: {res}")
            return False, name

        data = res.get("data", {})
        file_id = data.get("fileId")      # 获取移动云盘api网关返回的 fileId
        upload_id = data.get("uploadId")  # 获取移动云盘api网关返回的 uploadId
        
        if data.get("exist"):
            print(f"[+] {name} 秒传成功！")
            return True, name

        # 检查是否返回分片上传地址
        if data.get("partInfos") is not None:
            # 分片上传

            # 创建进度条
            total_size = os.path.getsize(local_path)
            pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc=f"上传 {name}", leave=True)
            
            try:
                with open(local_path, "rb") as f:
                    uploaded_bytes = 0
                    
                    # 上传前100个分片
                    for i, part_info in enumerate(data['partInfos']):
                        part_number = part_info['partNumber']
                        part_size = part_infos[part_number - 1]['partSize']
                        start_pos = part_infos[part_number - 1]['parallelHashCtx']['partOffset']
                        
                        # 定位到分片位置
                        f.seek(start_pos)
                        chunk = f.read(part_size)
                        
                        # 上传分片
                        upload_url = part_info['uploadUrl']
                        
                        # 创建一个带进度的上传流
                        class ProgressBytesIO:
                            def __init__(self, data, pbar, desc=""):
                                self.data = data
                                self.pbar = pbar
                                self.pos = 0
                                self.desc = desc
                            
                            def read(self, size=-1):
                                if size == -1:
                                    chunk = self.data[self.pos:]
                                    self.pos = len(self.data)
                                else:
                                    chunk = self.data[self.pos:self.pos+size]
                                    self.pos += len(chunk)
                                
                                if chunk:
                                    self.pbar.update(len(chunk))
                                
                                return chunk
                        
                        progress_stream = ProgressBytesIO(chunk, pbar)
                        put_resp = requests.put(upload_url, data=progress_stream, 
                                              headers={"Content-Length": str(len(chunk))})
                        
                        if put_resp.status_code not in [200, 201, 204]:
                            print(f"[!] 分片 {part_number} 上传失败，状态码: {put_resp.status_code}")
                            pbar.close()
                            return False, name
                        
                        uploaded_bytes += len(chunk)
                        # 更新进度条描述
                        pbar.set_postfix({"分片": f"{part_number}/{part_count}"})

                    # 如果还有剩余分片，分批获取上传地址并上传
                    for i in range(100, len(part_infos), 100):
                        end = min(i + 100, len(part_infos))
                        batch_part_infos = part_infos[i:end]
                        
                        # 获取后续分片的上传地址
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
                        more_resp = self.api_client.personal_post(pathname, more_data)
                        if more_resp.get("code") != "0000":
                            print(f"[!] 获取后续分片上传地址失败: {more_resp}")
                            pbar.close()
                            return False, name
                        
                        more_data_resp = more_resp.get("data", {})
                        
                        # 上传后续分片
                        for j, part_info in enumerate(more_data_resp['partInfos']):
                            part_number = part_info['partNumber']
                            part_size = part_infos[part_number - 1]['partSize']
                            start_pos = part_infos[part_number - 1]['parallelHashCtx']['partOffset']
                            
                            # 定位到分片位置
                            f.seek(start_pos)
                            chunk = f.read(part_size)
                            
                            # 上传分片
                            upload_url = part_info['uploadUrl']
                            
                            # 创建一个带进度的上传流
                            class ProgressBytesIO:
                                def __init__(self, data, pbar, desc=""):
                                    self.data = data
                                    self.pbar = pbar
                                    self.pos = 0
                                    self.desc = desc
                                
                                def read(self, size=-1):
                                    if size == -1:
                                        chunk = self.data[self.pos:]
                                        self.pos = len(self.data)
                                    else:
                                        chunk = self.data[self.pos:self.pos+size]
                                        self.pos += len(chunk)
                                    
                                    if chunk:
                                        self.pbar.update(len(chunk))
                                    
                                    return chunk
                            
                            progress_stream = ProgressBytesIO(chunk, pbar)
                            put_resp = requests.put(upload_url, data=progress_stream, 
                                                  headers={"Content-Length": str(len(chunk))})
                            
                            if put_resp.status_code not in [200, 201, 204]:
                                print(f"[!] 分片 {part_number} 上传失败，状态码: {put_resp.status_code}")
                                pbar.close()
                                return False, name
                            
                            uploaded_bytes += len(chunk)
                            # 更新进度条描述
                            pbar.set_postfix({"分片": f"{part_number}/{part_count}"})

            finally:
                pbar.close()

            # Step 3: Complete (确认完成)
            complete_payload = {
                "fileId": file_id,
                "uploadId": upload_id,
                "contentHash": full_hash,  # 确保哈希一致
                "contentHashAlgorithm": "SHA256"
            }
            
            final_res = self.api_client.personal_post("/file/complete", complete_payload)
            if final_res.get("code") == "0000":
                print(f"[+] {os.path.basename(local_path)} 上传成功！")
                return True, name
            else:
                print(f"[!] 文件 {name} 完成上传失败: {final_res}")
                return False, name
        else:
            print(f"[+] {name} 秒传成功！")
            return True, name

    def parallel_upload_files(self, file_paths, parent_id, max_workers=3):
        # 并行上传文件列表
        if not file_paths:
            return True
        
        # 使用线程池执行器进行并行上传
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有上传任务
            future_to_file = {executor.submit(self.upload_single_file, file_path, parent_id): file_path 
                              for file_path in file_paths}
            
            success_count = 0
            failed_files = []
            
            # 处理完成的任务
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    result, filename = future.result()
                    if result:
                        success_count += 1
                    else:
                        failed_files.append(file_path)
                except Exception as exc:
                    print(f"[!] 上传文件 {file_path} 时发生异常: {exc}")
                    failed_files.append(file_path)
        
        return success_count == len(file_paths)

    def upload_folder(self, local_folder_path, parent_id, max_workers=3):
        # 上传整个文件夹
        folder_name = os.path.basename(local_folder_path)
        print(f"[*] 开始上传文件夹: {folder_name}")
        
        # 在云端创建对应的文件夹
        cloud_folder_id = self.api_client.create_folder(parent_id, folder_name)
        if not cloud_folder_id:
            print(f"[!] 无法在云端创建文件夹: {folder_name}")
            return False
        
        # 遍历本地文件夹中的所有文件和子文件夹
        # 创建一个字典来存储每个目录对应的云端文件夹ID
        dir_cloud_ids = {}
        dir_cloud_ids[local_folder_path] = cloud_folder_id
        
        # 首先创建所有子目录结构
        for root, dirs, files in os.walk(local_folder_path):
            if root not in dir_cloud_ids:
                # 计算相对路径，用于在云端重建目录结构
                rel_path = os.path.relpath(root, local_folder_path)
                current_cloud_parent = cloud_folder_id
                
                # 如果不是根目录，则需要创建子目录
                if rel_path != ".":
                    # 重建目录结构
                    path_parts = rel_path.split(os.sep)
                    for part in path_parts:
                        # 检查云端是否已存在同名目录
                        existing_folder_id = self.api_client.find_folder_by_name(current_cloud_parent, part)
                        if existing_folder_id:
                            current_cloud_parent = existing_folder_id
                        else:
                            # 创建新目录
                            new_folder_id = self.api_client.create_folder(current_cloud_parent, part)
                            if new_folder_id:
                                current_cloud_parent = new_folder_id
                            else:
                                print(f"[!] 无法在云端创建子目录: {part}")
                                return False
                
                dir_cloud_ids[root] = current_cloud_parent
        
        # 然后上传所有文件，并行处理每个目录内的文件
        for root, dirs, files in os.walk(local_folder_path):
            if files:  # 如果当前目录有文件
                file_paths = [os.path.join(root, file) for file in files]
                current_cloud_parent = dir_cloud_ids[root]
                
                # 并行上传当前目录的所有文件
                success = self.parallel_upload_files(file_paths, current_cloud_parent, max_workers)
                if not success:
                    print(f"[!] 目录 {root} 中的文件上传失败")
                    return False
        
        print(f"[+] 文件夹 {folder_name} 上传完成！")
        return True

    def parallel_upload(self, file_paths, parent_id, max_workers=3):
        # 并行上传文件
        # 使用线程池执行器进行并行上传
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有上传任务
            future_to_file = {executor.submit(self.upload_single_file, file_path, parent_id): file_path 
                              for file_path in file_paths}
            
            success_count = 0
            failed_files = []
            
            # 处理完成的任务
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    result, filename = future.result()
                    if result:
                        success_count += 1
                    else:
                        failed_files.append(file_path)
                except Exception as exc:
                    print(f"[!] 上传文件 {file_path} 时发生异常: {exc}")
                    failed_files.append(file_path)
        
        if failed_files:
            print(f"失败文件: {failed_files}")
        
        return success_count == len(file_paths)
