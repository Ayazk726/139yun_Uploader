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
    body = encode_uri_component(body_str)
    strs = sorted(list(body))
    body = "".join(strs)
    body_b64 = base64.b64encode(body.encode('utf-8')).decode('utf-8')
    # MD5加密bodybase64编码拼接随机字符串MD5
    res = get_md5(body_b64) + get_md5(f"{ts}:{rand_str}")
    return get_md5(res).upper()

def get_part_size(size):
    # 默认分片大小为100MB
    part_size = 100 * 1024 * 1024  # 100MB
    if size > 30 * 1024 * 1024 * 1024:  # 30GB
        part_size = 512 * 1024 * 1024  # 512MB
    return part_size

def pause_for_user_input():
    try:
        input("\n按回车键退出程序...")
    except KeyboardInterrupt:
        pass
