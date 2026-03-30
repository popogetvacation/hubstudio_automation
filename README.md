# HubStudio Selenium 自动化项目

基于 HubStudio API 的多环境并发 Selenium 自动化框架。

## 项目特性

- 多浏览器环境并发调度
- Selenium 自动化支持
- 通过浏览器环境的网络请求模块
- Access 数据库存储
- 任务队列管理

## 目录结构

```
hubstudio_automation/
├── config/
│   └── settings.yaml          # 配置文件
├── src/
│   ├── __init__.py
│   ├── api/
│   │   ├── __init__.py
│   │   └── hubstudio_client.py  # HubStudio API 客户端
│   ├── browser/
│   │   ├── __init__.py
│   │   ├── environment_manager.py  # 浏览器环境管理
│   │   └── selenium_driver.py      # Selenium 驱动封装
│   ├── scheduler/
│   │   ├── __init__.py
│   │   └── concurrent_scheduler.py # 并发调度器
│   ├── network/
│   │   ├── __init__.py
│   │   └── browser_request.py      # 浏览器网络请求模块
│   ├── database/
│   │   ├── __init__.py
│   │   └── access_db.py            # Access 数据库操作
│   └── utils/
│       ├── __init__.py
│       └── logger.py               # 日志工具
├── main.py                     # 主入口
├── requirements.txt            # 依赖
└── README.md
```

## 安装

```bash
pip install -r requirements.txt
```

## 配置

编辑 `config/settings.yaml`:

```yaml
hubstudio:
  api_url: "http://127.0.0.1:9889"
  api_key: "your_api_key"

database:
  access_path: "./data/automation.accdb"

scheduler:
  max_concurrent: 5  # 最大并发数
  task_timeout: 300  # 任务超时时间(秒)
```

## 快速开始

```python
from src.scheduler.concurrent_scheduler import ConcurrentScheduler

# 创建调度器
scheduler = ConcurrentScheduler(max_workers=5)

# 添加任务
scheduler.add_task(
    env_id="your_env_id",
    task_func=your_automation_function
)

# 启动
scheduler.start()
```

## 依赖说明

- `selenium`: 浏览器自动化
- `pyodbc`: Access 数据库连接
- `requests`: HTTP 请求
- `pyyaml`: 配置文件解析
