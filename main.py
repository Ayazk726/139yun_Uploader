import sys
import os
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event
from config import Config
from uploader import _139Uploader
import keyboard
from tqdm import tqdm
import traceback
from utils import SpeedMonitor, safe_pad

INVALID_CHARS = r'[<>:"/\\|?*]'
interrupt_event = Event()
speed_monitor = SpeedMonitor()  # 全局速度监控器

def keyboard_interrupt_handler():
    def on_ctrl_c():
        if not interrupt_event.is_set():
            print("\n[!] 用户中断 (Ctrl+C)...")
            interrupt_event.set()
    try: keyboard.add_hotkey('ctrl+c', on_ctrl_c)
    except: pass

def is_path_valid(path):
    clean_name = os.path.basename(path.rstrip('/\\'))
    return not re.search(INVALID_CHARS, clean_name)

def process_files_pipeline(uploader, file_paths, parent_id, max_workers, speed_monitor):
    if not file_paths: return

    total_size = sum(os.path.getsize(f) for f in file_paths)

    total_pbar = tqdm(
        total=len(file_paths),
        desc="文件总体进度",
        unit="file",
        position=0,
        leave=True,
        dynamic_ncols=False,  # 禁用动态列宽以保持稳定性
    )
    verify_pbar = tqdm(total=total_size, desc="校验总进度", unit="B", unit_scale=True, unit_divisor=1024, position=1, leave=True)

    hash_executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="HashWorker")
    upload_executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="NetWorker")

    hash_futures = []
    upload_futures = []

    # 速度更新线程
    def update_speed_display():
        """独立线程用于更新进度条上的速度显示"""
        last_update = time.time()
        while not interrupt_event.is_set():
            try:
                current_time = time.time()
                if current_time - last_update >= 0.5:  # 每0.5秒更新一次显示
                    _, speed_str = speed_monitor.get_speed_and_formatted()
                    # 使用 set_postfix 在进度条右侧显示速度
                    total_pbar.set_postfix(speed=speed_str)
                    last_update = current_time
                time.sleep(0.1)
            except Exception:
                break

    speed_update_thread = None

    try:
        # 启动速度显示更新线程
        speed_update_thread = threading.Thread(target=update_speed_display, daemon=True)
        speed_update_thread.start()

        for fp in file_paths:
            if interrupt_event.is_set(): break
            f = hash_executor.submit(
                uploader.prepare_file_metadata,
                fp,
                parent_id,
                lambda: interrupt_event.is_set(),
                verify_pbar
            )
            hash_futures.append(f)

        for future in as_completed(hash_futures):
            if interrupt_event.is_set(): break
            try:
                task_context = future.result()
                if task_context:
                    # 传入速度监控器和文件大小以追踪上传速度
                    file_size = os.path.getsize(task_context.get("local_path", "")) if task_context.get("mode") == "new" else 0
                    up_f = upload_executor.submit(
                        uploader.execute_file_upload,
                        task_context,
                        lambda: interrupt_event.is_set(),
                        total_pbar,
                        speed_monitor,
                        file_size
                    )
                    upload_futures.append(up_f)
            except Exception as e:
                print(f"Hash阶段异常: {e}")

        for future in as_completed(upload_futures):
            if interrupt_event.is_set(): break
            try:
                future.result()
            except Exception:
                pass

        # 最终更新，显示最终速度（应为0或很低）
        _, speed_str = speed_monitor.get_speed_and_formatted()
        total_pbar.set_postfix(speed=speed_str)

    finally:
        hash_executor.shutdown(wait=False)
        upload_executor.shutdown(wait=True)
        total_pbar.close()
        verify_pbar.close()


def process_folder_with_speed(uploader, folder_path, parent_id, max_workers, speed_monitor):
    """处理文件夹上传，带速度显示"""
    # 调用 uploader.upload_folder，速度显示已在 uploader.py 中实现
    result = uploader.upload_folder(
        folder_path,
        parent_id,
        max_workers=max_workers,
        interrupted_check_func=lambda: interrupt_event.is_set(),
        speed_monitor=speed_monitor
    )

    return result

def main():
    try:
        # --- 修复：Auth 判空校验 ---
        auth = Config.DEFAULT_AUTHORIZATION.strip()
        if not auth:
            print("\n[!] 错误: 未在 config.py 中检测到有效的 DEFAULT_AUTHORIZATION。")
            print("[*] 请在 config.py 中填入您的令牌后再运行程序。")
            return

        keyboard_interrupt_handler()
        if len(sys.argv) < 2:
            print("Usage: python main.py <paths...> [-w workers] [-p cloud_path]")
            sys.exit(1)

        args = sys.argv[1:]; file_paths = []; folder_paths = []
        max_workers = Config.DEFAULT_MAX_WORKERS; parent_path = None

        i = 0
        while i < len(args):
            if args[i] == '-w' and i + 1 < len(args):
                max_workers = int(args[i + 1]); i += 2
            elif args[i] == '-p' and i + 1 < len(args):
                parent_path = args[i + 1]; i += 2
            else:
                # --- 修复：处理带斜杠的路径 Bug ---
                raw_input = args[i].strip('"\'').rstrip('/\\')
                if not raw_input:
                    i += 1
                    continue

                if not is_path_valid(raw_input):
                    print(f"[!] 路径包含非法字符: {raw_input}")
                    sys.exit(1)

                if os.path.isdir(raw_input):
                    folder_paths.append(raw_input)
                elif os.path.isfile(raw_input):
                    file_paths.append(raw_input)
                else:
                    print(f"[!] 警告: 找不到该路径: {raw_input}")
                    sys.exit(1)
                i += 1

        uploader = _139Uploader(auth)
        parent_id = Config.DEFAULT_PARENT_ID
        if parent_path:
            parent_id = uploader.api_client.get_folder_id_by_path(parent_path) or parent_id

        # 创建全局速度监控器，供所有上传任务共享
        global speed_monitor
        speed_monitor = SpeedMonitor()

        if file_paths:
            print(f"[*] 正在处理 {len(file_paths)} 个独立文件...")
            process_files_pipeline(uploader, file_paths, parent_id, max_workers, speed_monitor)

        for folder in folder_paths:
            if interrupt_event.is_set(): break
            print(f"[*] 正在处理文件夹: {os.path.basename(folder)}")
            process_folder_with_speed(uploader, folder, parent_id, max_workers, speed_monitor)

        print("\n[+] 任务处理完成。" if not interrupt_event.is_set() else "\n[-] 任务已中断。")

    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
    finally:
        keyboard.unhook_all()
        if not interrupt_event.is_set(): input("\nDone. Press Enter...")

if __name__ == "__main__":
    main()
