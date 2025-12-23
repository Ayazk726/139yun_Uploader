import hashlib
import urllib.parse
import time
import random
import string
import base64

def get_md5(s):
    return hashlib.md5(s.encode('utf-8')).hexdigest()

def encode_uri_component(s):
    res = urllib.parse.quote(s, safe="")
    mapping = {"%21": "!", "%27": "'", "%28": "(", "%29": ")", "%2A": "*", "+": "%20"}
    for k, v in mapping.items():
        res = res.replace(k, v)
    return res

def cal_sign(body_str, ts, rand_str):
    # 修复签名算法
    body = encode_uri_component(body_str)
    # 按照字典序排序
    sorted_body = "".join(sorted(body))
    body_b64 = base64.b64encode(sorted_body.encode('utf-8')).decode('utf-8')
    # 核心算法：GetMD5(body_b64) + GetMD5(ts:randStr)
    res = get_md5(body_b64) + get_md5(f"{ts}:{rand_str}")
    return get_md5(res).upper()

def get_part_size(size):
    """根据文件大小计算分片大小"""
    # 默认分片大小为100MB
    part_size = 100 * 1024 * 1024  # 100MB
    if size > 30 * 1024 * 1024 * 1024:  # 30GB
        part_size = 512 * 1024 * 1024  # 512MB
    return part_size

def pause_for_user_input():
    """暂停程序执行，等待用户输入"""
    try:
        input("\n按回车键退出程序...")
    except KeyboardInterrupt:
        print("\n检测到中断信号，程序退出。")
        # 不需要再次捕获，直接让 KeyboardInterrupt 向上传播
        raise # 重新抛出异常以正常退出