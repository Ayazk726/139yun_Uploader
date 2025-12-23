import sys
import os
from config import Config
from uploader import _139Uploader
from utils import pause_for_user_input

def main():
    try:
        if len(sys.argv) < 2:
            print("用法: python main.py <文件路径1> [文件路径2] [文件路径3] ... [-w 并发数] [-p 云盘路径]")
            print("用法: python main.py <文件夹路径> [-w 并发数] [-p 云盘路径]")
            print("示例: python main.py file1.zip file2.txt file3.pdf")
            print("示例: python main.py my_folder")
            print("示例: python main.py file1.zip file2.txt -w 5")
            print("示例: python main.py file1.zip -p /我的文档/上传文件夹")
            sys.exit(1)
        
        # 解析命令行参数
        args = sys.argv[1:]
        file_paths = []
        folder_path = None
        max_workers = Config.DEFAULT_MAX_WORKERS  # 默认并发数
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
            elif args[i] == '-p':
                parent_path = args[i + 1]
                i += 2
            else:
                path = args[i]
                if os.path.isdir(path):
                    folder_path = path
                elif os.path.isfile(path):
                    file_paths.append(path)
                else:
                    print(f"[!] 路径不存在: {path}")
                    sys.exit(1)
                i += 1
        
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
        
        # 根据参数类型决定上传方式
        if folder_path:
            # 上传整个文件夹
            uploader.upload_folder(folder_path, parent_id, max_workers=max_workers)
        elif file_paths:
            # 并行上传文件
            uploader.parallel_upload(file_paths, parent_id, max_workers=max_workers)
        else:
            print("[!] 没有找到有效的文件或文件夹路径")
            sys.exit(1)
            
    except Exception as e:
        print(f"[!] 程序执行出错: {e}")
    finally:
        # 在程序结束前暂停，防止命令行窗口自动关闭
        pause_for_user_input()

if __name__ == "__main__":
    main()
