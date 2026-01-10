import hashlib
import urllib.parse
import time
import random
import string
import base64
from rich.progress import Progress, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn, FileSizeColumn
from rich.theme import Theme
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.layout import Layout
from rich.table import Table

custom_theme = Theme({
    "bar.back": "#333333",
    "bar.complete": "#00ff00",
    "bar.finished": "#00ff00",
    "progress.percentage": "green",
    "progress.data_speed": "cyan",
    "progress.filesize": "magenta",
    "progress.time": "yellow",
})

console = Console(theme=custom_theme)

class ProgressManager:
    def __init__(self, update_interval=0.2):
        self.progress = None
        self.file_progress = None
        self.main_task_id = None
        self.file_task_id = None
        self.verify_task_id = None
        self.file_tasks = {}
        self.layout = None
        self.last_update_time = {}
        self.update_interval = update_interval
    
    def create_main_progress(self):
        self.progress = Progress(
            TextColumn("[bold blue]{task.description}", justify="left"),
            BarColumn(
                bar_width=None, 
                complete_style="green", 
                finished_style="green",
                pulse_style="green"
            ),
            TextColumn("[green]{task.completed}/{task.total}", justify="left"),
            TextColumn("•", justify="left"),
            TimeRemainingColumn(),
            expand=True,
            refresh_per_second=15,
            transient=False,
            redirect_stdout=False,
            redirect_stderr=False,
        )
        return self.progress
    
    def create_file_progress(self):
        """创建单个文件上传进度条（支持KB/MB/GB单位）"""
        self.file_progress = Progress(
            TextColumn("[bold blue]{task.description}", justify="left"),
            BarColumn(
                bar_width=None, 
                complete_style="green", 
                finished_style="green",
                pulse_style="green"
            ),
            TextColumn("[green]{task.percentage:>6.1f}%", justify="left"),
            TextColumn("•", justify="left"),
            DownloadColumn(binary_units=True),
            TextColumn("•", justify="left"),
            TransferSpeedColumn(),
            TextColumn("•", justify="left"),
            TimeRemainingColumn(),
            expand=True,
            refresh_per_second=15,
            transient=False,
            redirect_stdout=False,
            redirect_stderr=False,
        )
        return self.file_progress
    
    def create_verify_progress(self):
        self.verify_progress = Progress(
            TextColumn("[bold yellow]🔍 {task.description}", justify="left"),
            BarColumn(
                bar_width=40, 
                complete_style="cyan", 
                finished_style="cyan",
                pulse_style="cyan"
            ),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            FileSizeColumn(),
            "•",
            TransferSpeedColumn(),
            refresh_per_second=10,
            transient=False,
            redirect_stdout=False,
            redirect_stderr=False,
        )
        return self.verify_progress
    
    def _should_update(self, task_id):
        current_time = time.time()
        if task_id not in self.last_update_time:
            self.last_update_time[task_id] = 0
            return True
        
        if current_time - self.last_update_time[task_id] >= self.update_interval:
            self.last_update_time[task_id] = current_time
            return True
        return False
    
    def start_main_task(self, description, total):
        if self.progress:
            self.main_task_id = self.progress.add_task(
                description=description,
                total=total
            )
        return self.main_task_id
    
    def start_verify_task(self, description, total):
        if hasattr(self, 'verify_progress') and self.verify_progress:
            self.verify_task_id = self.verify_progress.add_task(
                description=description,
                total=total
            )
        return self.verify_task_id
    
    def add_file_task(self, filename, total, resume=False):
        if self.file_progress:
            self.file_task_id = self.file_progress.add_task(
                description=filename[:25],
                total=total
            )
            self.file_tasks[self.file_task_id] = filename
            return self.file_task_id
        return None
    
    def update_file_task(self, task_id, advance=0, filename=None):
        if self.file_progress and task_id is not None:
            if advance > 0 and not self._should_update(task_id):
                if not hasattr(self, 'pending_advance'):
                    self.pending_advance = {}
                if task_id not in self.pending_advance:
                    self.pending_advance[task_id] = 0
                self.pending_advance[task_id] += advance
                return
            
            if hasattr(self, 'pending_advance') and task_id in self.pending_advance:
                total_pending = self.pending_advance.pop(task_id, 0)
                advance += total_pending
            
            self.file_progress.update(task_id, advance=advance)
            if filename:
                self.file_progress.update(task_id, description=filename[:25])
    
    def update_main_task(self, advance=0):
        if self.progress and self.main_task_id is not None:
            if advance > 0 and not self._should_update(self.main_task_id):
                if not hasattr(self, 'pending_main_advance'):
                    self.pending_main_advance = {}
                self.pending_main_advance[self.main_task_id] = \
                    self.pending_main_advance.get(self.main_task_id, 0) + advance
                return
            
            if hasattr(self, 'pending_main_advance'):
                total_pending = self.pending_main_advance.pop(self.main_task_id, 0)
                advance += total_pending
            
            self.progress.update(self.main_task_id, advance=advance)
    
    def update_verify_task(self, advance=0):
        if hasattr(self, 'verify_progress') and self.verify_progress and self.verify_task_id is not None:
            if advance > 0 and not self._should_update(f"verify_{self.verify_task_id}"):
                if not hasattr(self, 'pending_verify_advance'):
                    self.pending_verify_advance = {}
                key = f"verify_{self.verify_task_id}"
                self.pending_verify_advance[key] = \
                    self.pending_verify_advance.get(key, 0) + advance
                return
            
            if hasattr(self, 'pending_verify_advance'):
                key = f"verify_{self.verify_task_id}"
                total_pending = self.pending_verify_advance.pop(key, 0)
                advance += total_pending
            
            self.verify_progress.update(self.verify_task_id, advance=advance)
    
    def remove_file_task(self, task_id):
        if self.file_progress and task_id is not None:
            self.file_progress.remove_task(task_id)
            if task_id in self.file_tasks:
                del self.file_tasks[task_id]
            if hasattr(self, 'pending_advance') and task_id in self.pending_advance:
                del self.pending_advance[task_id]
    
    def finish_task(self, task_id):
        if self.file_progress and task_id is not None:
            try:
                self.file_progress.update(task_id)
            except:
                pass
    
    def finish_all(self):
        if self.file_progress:
            for task_id in list(self.file_tasks.keys()):
                self.remove_file_task(task_id)
        if self.main_task_id is not None and self.progress:
            try:
                self.progress.update(self.main_task_id)
            except:
                pass
    
    def close(self):
        if self.progress:
            self.progress.stop()
        if self.file_progress:
            self.file_progress.stop()
        if hasattr(self, 'verify_progress') and self.verify_progress:
            self.verify_progress.stop()

def create_rclone_style_layout(progress_manager):
    layout = Layout()
    
    layout.split_column(
        Layout(Panel(progress_manager.progress, title="文件传输", subtitle="实时进度"), size=3),
        Layout(progress_manager.verify_progress, size=3)
    )
    
    return layout

def get_progress_manager():
    return ProgressManager()

def get_md5(s):
    return hashlib.md5(s.encode('utf-8')).hexdigest()

def encode_uri_component(s):
    res = urllib.parse.quote(s, safe="")
    mapping = {"%21": "!", "%27": "'", "%28": "(", "%29": ")", "%2A": "*", "+": "%20"}
    for k, v in mapping.items():
        res = res.replace(k, v)
    return res

def cal_sign(body_str, ts, rand_str):
    body = encode_uri_component(body_str)
    sorted_body = "".join(sorted(body))
    body_b64 = base64.b64encode(sorted_body.encode('utf-8')).decode('utf-8')
    res = get_md5(body_b64) + get_md5(f"{ts}:{rand_str}")
    return get_md5(res).upper()

def get_part_size(size):
    part_size = 128 * 1024 * 1024
    if size > 30 * 1024 * 1024 * 1024:
        part_size = 512 * 1024 * 1024
    return part_size

def pause_for_user_input():
    try:
        input("\n按回车键退出程序...")
    except KeyboardInterrupt:
        print("\n检测到中断信号，程序退出。")
        raise
