import sys
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event
from config import Config
from uploader import _139Uploader
import keyboard
from tqdm import tqdm
import time

INVALID_CHARS = r'[<>:"/\\|?*]'

interrupt_event = Event()

def keyboard_interrupt_handler():
    def on_ctrl_c():
        if not interrupt_event.is_set():
            print("\n检测到中断信号 (Ctrl+C)，正在尝试停止...")
            interrupt_event.set()
    
    keyboard.add_hotkey('ctrl+c', on_ctrl_c)

def is_path_valid(path):
    clean_name = os.path.basename(path.rstrip('/\\'))
    if re.search(INVALID_CHARS, clean_name):
        return False
    return True

def main():
    executor = None
    try:
        keyboard_interrupt_handler()

        if len(sys.argv) < 2:
            print("用法: python main.py <文件路径1> [文件路径2] ... <文件夹路径1> [文件夹路径2] ... [-w 并发数] [-p 云盘路径]")
            sys.exit(1)

        args = sys.argv[1:]
        file_paths = []
        folder_paths = []
        max_workers = Config.DEFAULT_MAX_WORKERS
        parent_path = None

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
                path = path.strip('"\'')
                clean_path = path.rstrip('/\\')
                
                if not is_path_valid(path):
                     print(f"[!] 路径名称包含非法字符，无法处理: {path}")
                     sys.exit(1)
                
                if os.path.isdir(clean_path):
                    folder_paths.append(clean_path)
                elif os.path.isfile(clean_path):
                    file_paths.append(clean_path)
                else:
                    print(f"[!] 路径不存在或不是文件/文件夹: {clean_path}")
                    sys.exit(1)
                i += 1

        if not file_paths and not folder_paths:
            print("[!] 没有找到有效的文件或文件夹路径")
            sys.exit(1)

        AUTHORIZATION = Config.DEFAULT_AUTHORIZATION
        uploader = _139Uploader(AUTHORIZATION)

        parent_id = Config.DEFAULT_PARENT_ID
        if parent_path:
            parent_id = uploader.api_client.get_folder_id_by_path(parent_path)
            if not parent_id:
                print(f"[!] 无法找到云盘路径: {parent_path}")
                sys.exit(1)

        all_success = True

        if file_paths:
            print(f"[*] 开始上传 {len(file_paths)} 个文件...")
            # 使用tqdm显示上传进度
            file_pbar = tqdm(total=len(file_paths), desc="文件上传总进度", unit="file", leave=True)
            success = uploader.parallel_upload_with_interrupt(file_paths, parent_id, max_workers=max_workers, interrupted_check_func=lambda: interrupt_event.is_set(), file_pbar=file_pbar)
            all_success = all_success and success
            file_pbar.close()

        if folder_paths:
            print(f"[*] 开始并行上传 {len(folder_paths)} 个文件夹，每个文件夹内部上传文件的并发数为 {max_workers}...")
            
            executor = ThreadPoolExecutor(max_workers=len(folder_paths))
            try:
                future_to_folder = {executor.submit(uploader.upload_folder, folder_path, parent_id, max_workers, lambda: interrupt_event.is_set()): folder_path 
                                  for folder_path in folder_paths}
                
                success_count = 0
                failed_folders = []
                
                for future in as_completed(future_to_folder):
                    if interrupt_event.is_set():
                        print("\n主程序已中断，正在等待当前任务完成或取消...")
                        for f, fp in future_to_folder.items():
                            if not f.done():
                                print(f"取消任务: {fp}")
                                f.cancel()
                        executor.shutdown(wait=True)
                        print("所有任务已处理，程序退出。")
                        sys.exit(0)
                    
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
                print(f"[*] 文件夹上传完成: 成功 {success_count}, 失败 {len(failed_folders)}")
                executor.shutdown(wait=False)
                all_success = all_success and (success_count == len(folder_paths))

        if all_success:
            print("\n[+] 所有上传任务完成！")
        else:
            print("\n[-] 部分上传任务失败。")

    except KeyboardInterrupt:
        print("\n检测到中断信号 (Ctrl+C)，正在尝试停止...")
        interrupt_event.set()
        if executor and not executor._shutdown:
            executor.shutdown(wait=False)
    except Exception as e:
        print(f"[!] 程序执行出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            keyboard.unhook_all()
        except:
            pass
        if not interrupt_event.is_set():
            try:
                input("\n按回车键退出程序...")
            except KeyboardInterrupt:
                print("\n再次检测到中断信号，程序立即退出。")
                sys.exit(0)

if __name__ == "__main__":
    main()