import sys
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event
from config import Config
from uploader import _139Uploader
import keyboard
from tqdm import tqdm

INVALID_CHARS = r'[<>:"/\\|?*]'
interrupt_event = Event()

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

def main():
    try:
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
                p = args[i].strip('"\'')
                if not is_path_valid(p): sys.exit(1)
                if os.path.isdir(p): folder_paths.append(p)
                elif os.path.isfile(p): file_paths.append(p)
                i += 1

        uploader = _139Uploader(Config.DEFAULT_AUTHORIZATION)
        parent_id = Config.DEFAULT_PARENT_ID
        if parent_path:
            parent_id = uploader.api_client.get_folder_id_by_path(parent_path) or parent_id

        # 处理独立文件
        if file_paths:
            total_pbar = tqdm(total=len(file_paths), desc="文件总体进度", unit="file", position=0)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(uploader.upload_single_file, fp, parent_id, 
                                         lambda: interrupt_event.is_set(), total_pbar) for fp in file_paths]
                for f in as_completed(futures):
                    if interrupt_event.is_set(): break
            total_pbar.close()

        # 顺序处理文件夹
        for folder in folder_paths:
            if interrupt_event.is_set(): break
            uploader.upload_folder(folder, parent_id, max_workers, lambda: interrupt_event.is_set())

        print("\n[+] 任务处理完成。" if not interrupt_event.is_set() else "\n[-] 任务已中断。")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        keyboard.unhook_all()
        if not interrupt_event.is_set(): input("\nDone. Press Enter...")

if __name__ == "__main__":
    main()