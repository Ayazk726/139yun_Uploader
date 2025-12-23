# main.py (更新版，处理末尾斜杠、支持并行上传多个文件夹、优化中断处理)
import sys
import os
import re # 导入正则表达式模块
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import Config
from uploader import _139Uploader
from utils import pause_for_user_input

# 定义操作系统不允许的文件/文件夹名字符 (Windows, Unix-like)
INVALID_CHARS = r'[<>:"/\\|?*]'

def is_path_valid(path):
    """检查路径名称（去除末尾斜杠后）是否包含非法字符"""
    # 去除末尾的斜杠
    clean_name = os.path.basename(path.rstrip('/\\'))
    if re.search(INVALID_CHARS, clean_name):
        return False
    return True

def main():
    executor = None # 定义在更广的作用域，便于 finally 块访问
    try:
        if len(sys.argv) < 2:
            print("用法: python main.py <文件路径1> [文件路径2] ... <文件夹路径1> [文件夹路径2] ... [-w 并发数] [-p 云盘路径]")
            print("示例: python main.py file1.zip file2.txt file3.pdf")
            print("示例: python main.py my_folder")
            print("示例: python main.py file1.zip file2.txt -w 5")
            print("示例: python main.py file1.zip -p /我的文档/上传文件夹")
            print("示例: python main.py folder1 folder2 -p /目标路径")
            print("示例: python main.py file1.zip folder1 folder2")
            print("注意: 文件或文件夹名不能包含以下字符: < > : \" / \\ | ? *")
            sys.exit(1)

        # 解析命令行参数
        args = sys.argv[1:]
        file_paths = []
        folder_paths = []
        max_workers = Config.DEFAULT_MAX_WORKERS  # 默认并发数（用于单个文件夹内的文件上传）
        parent_path = None  # 云盘路径

        i = 0
        while i < len(args):
            if args[i] == '-w' and i + 1 < len(args):
                try:
                    max_workers = int(args[i + 1])
                    i += 2
                except ValueError:
                    print("错误: 并发数必须是整数")
                    sys.exit(1)
            elif args[i] == '-p' and i + 1 < len(args):
                parent_path = args[i + 1]
                i += 2
            else:
                path = args[i]
                # 去除路径末尾的斜杠，以便后续正确处理
                clean_path = path.rstrip('/\\')
                
                # 检查路径名称是否合法（去除末尾斜杠后检查basename）
                if not is_path_valid(path):
                     print(f"[!] 路径名称包含非法字符，无法处理: {path}")
                     print("    不能包含以下字符: < > : \" / \\ | ? *")
                     sys.exit(1)
                
                if os.path.isdir(clean_path):
                    folder_paths.append(clean_path) # 使用清理后的路径
                elif os.path.isfile(clean_path):
                    file_paths.append(clean_path) # 使用清理后的路径
                else:
                    print(f"[!] 路径不存在或不是文件/文件夹: {clean_path}")
                    sys.exit(1)
                i += 1

        if not file_paths and not folder_paths:
            print("[!] 没有找到有效的文件或文件夹路径")
            sys.exit(1)

        # 设置认证令牌
        AUTHORIZATION = Config.DEFAULT_AUTHORIZATION

        # 创建上传器实例
        uploader = _139Uploader(AUTHORIZATION)

        # 如果指定了云盘路径，则获取对应的parent_id
        parent_id = Config.DEFAULT_PARENT_ID
        if parent_path:
            parent_id = uploader.api_client.get_folder_id_by_path(parent_path)
            if not parent_id:
                print(f"[!] 无法找到云盘路径: {parent_path}")
                sys.exit(1)

        # 执行上传任务
        all_success = True

        # 1. 上传所有文件到指定的 parent_id
        if file_paths:
            print(f"[*] 开始上传 {len(file_paths)} 个文件...")
            success = uploader.parallel_upload(file_paths, parent_id, max_workers=max_workers)
            all_success = all_success and success

        # 2. 并行上传所有文件夹到指定的 parent_id
        if folder_paths:
            print(f"[*] 开始并行上传 {len(folder_paths)} 个文件夹，每个文件夹内部上传文件的并发数为 {max_workers}...")
            
            # 创建线程池执行器
            executor = ThreadPoolExecutor(max_workers=len(folder_paths))
            try:
                # 提交所有文件夹上传任务
                future_to_folder = {executor.submit(uploader.upload_folder, folder_path, parent_id, max_workers): folder_path 
                                  for folder_path in folder_paths}
                
                success_count = 0
                failed_folders = []
                
                # 处理完成的任务
                for future in as_completed(future_to_folder):
                    folder_path = future_to_folder[future]
                    try:
                        result = future.result()
                        if result:
                            success_count += 1
                        else:
                            failed_folders.append(folder_path)
                    except Exception as exc:
                        print(f"[!] 上传文件夹 {folder_path} 时发生异常: {exc}")
                        failed_folders.append(folder_path)
            
            finally:
                # 确保线程池被关闭
                print(f"[*] 文件夹上传完成: 成功 {success_count}, 失败 {len(failed_folders)}")
                executor.shutdown(wait=False) # 不等待正在运行的任务，立即关闭
                all_success = all_success and (success_count == len(folder_paths))

        if all_success:
            print("\n[+] 所有上传任务完成！")
        else:
            print("\n[-] 部分上传任务失败。")

    except KeyboardInterrupt:
        print("\n检测到中断信号 (Ctrl+C)，请尝试多次中断以终止所有线程")
        try:
            sys.exit()
        except Exception as e:
            print("\n出现未知错误无法终止")

if __name__ == "__main__":
    main()



