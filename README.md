# 139yun_Uploader
本人菜鸟代码水平请轻喷
## 项目简介

本项目是基于 `AList` 和 `OpenList` 关于中国移动云盘驱动代码逻辑移植的python实现，仅支持新个人云api。支持文件和文件夹的上传功能且通过线程池实现并行上传，支持秒传。

## 前期准备

### 1. 获取 `authorization`

通过浏览器抓包获取 `authorization`，具体步骤可参考以下链接关于新个人云api的部分：

[https://doc.oplist.org/guide/drivers/139](https://doc.oplist.org/guide/drivers/139)

### 2. 克隆本项目并安装依赖
```
git clone https://github.com/Ayazk726/139yun_Uploader.git
pip install -r requirement.txt
```
### 3. 配置 `config.py`

将获取到的 `authorization` 填入 `config.py` 文件中的 `DEFAULT_AUTHORIZATION` 字段。

```python
# config.py
DEFAULT_AUTHORIZATION = "your_authorization_token_here"
```
### 4.修改默认父目录路径（可选）
根据需求修改 DEFAULT_PARENT_ID，默认为 "/"。-p 参数指定的路径为相对于父目录的路径，请确定需要再修改一般为"/"即可。
```python
# config.py
DEFAULT_PARENT_ID = "/"  # 默认为根目录
```
## 使用方法
运行脚本时，可以传入多个本地文件路径，并使用以下参数：
- <文件路径1> [文件路径2] ...: 需要上传的本地文件或文件夹路径。
- -w 并发数: 指定上传时的并发数（可选，默认为 3）。
- -p 云盘路径: 指定上传到云盘的路径（可选，默认为 /即你的云盘根路径）。
### 示例
```python
python main.py my_folder
python main.py file1.zip file2.txt -w 5
python main.py file1.zip -p /我的文档/上传文件夹
```
