import hashlib
import urllib.parse
import time
import random
import string
import base64
import unicodedata
import threading


class SpeedMonitor:
    def __init__(self):
        self._total_bytes = 0
        self._lock = threading.Lock()
        self._last_check_time = time.time()
        self._last_total_bytes = 0
        self._current_speed = 0.0

    def update(self, bytes_sent):
        with self._lock:
            self._total_bytes += bytes_sent

    def get_speed_and_formatted(self):
        now = time.time()
        time_diff = now - self._last_check_time

        # 每0.3秒更新一次显示数值，避免跳动过快
        if time_diff > 0.3:
            with self._lock:
                diff_bytes = self._total_bytes - self._last_total_bytes
                self._current_speed = diff_bytes / time_diff if time_diff > 0 else 0
                self._last_total_bytes = self._total_bytes
                self._last_check_time = now

        return self._current_speed, self._format_speed(self._current_speed)

    def _format_speed(self, speed):
        """格式化速度显示"""
        if speed > 1024 * 1024:
            return f"{speed / (1024 * 1024):.2f} MB/s"
        elif speed > 1024:
            return f"{speed / 1024:.2f} KB/s"
        else:
            return f"{speed:.0f} B/s"


def get_display_width(text):
    width = 0
    for char in text:
        # 'W' (Wide) 和 'F' (Full-width) 为全角，宽度为2
        eaw = unicodedata.east_asian_width(char)
        if eaw in ('W', 'F'):
            width += 2
        elif eaw == 'A':  # Ambiguous (中性字符)
            width += 2  # 按照多数终端默认行为，中性字符按全角处理
        else:
            width += 1
    return width


def safe_pad(text, target_width):
    current_width = get_display_width(text)

    if current_width > target_width:
        # 超出宽度时截断并添加省略号
        # 注意：不能截断全角字符的一半
        while len(text) > 0 and get_display_width(text + "...") > target_width:
            # 如果最后一个字符是全角字符，截断两个字符
            if unicodedata.east_asian_width(text[-1]) in ('W', 'F', 'A'):
                text = text[:-1]
                if len(text) > 0 and unicodedata.east_asian_width(text[-1]) in ('W', 'F', 'A'):
                    text = text[:-1]
            else:
                text = text[:-1]
        remaining_width = target_width - get_display_width(text + "...")
        return text + "..." + " " * remaining_width
    else:
        # 补足空格
        return text + " " * (target_width - current_width)

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
    # 默认分片大小为128MB
    part_size = 128 * 1024 * 1024  # 128MB
    if size > 30 * 1024 * 1024 * 1024:  # 30GB
        part_size = 512 * 1024 * 1024  # 512MB
    return part_size

def pause_for_user_input():
    try:
        input("\n按回车键退出程序...")
    except KeyboardInterrupt:
        print("\n检测到中断信号，程序退出。")
        raise
