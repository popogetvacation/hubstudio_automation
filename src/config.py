"""
配置加载模块
"""
import os
import yaml
from typing import Dict, Any
from dataclasses import dataclass, field


@dataclass
class HubStudioConfig:
    """HubStudio 配置"""
    api_url: str = "http://127.0.0.1:9889"
    api_key: str = ""
    timeout: int = 30


@dataclass
class DatabaseConfig:
    """数据库配置"""
    access_path: str = "./data/automation.accdb"
    pool_size: int = 5


@dataclass
class SchedulerConfig:
    """调度器配置"""
    max_concurrent: int = 5
    task_timeout: int = 300
    env_startup_timeout: int = 60
    max_retries: int = 3
    retry_interval: int = 5


@dataclass
class BrowserConfig:
    """浏览器配置"""
    chromedriver_path: str = ""
    page_load_timeout: int = 30
    script_timeout: int = 30
    implicit_wait: int = 10


@dataclass
class LoggingConfig:
    """日志配置"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file: str = "./logs/automation.log"


@dataclass
class Config:
    """总配置"""
    hubstudio: HubStudioConfig = field(default_factory=HubStudioConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    browser: BrowserConfig = field(default_factory=BrowserConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


def load_config(config_path: str = "config/settings.yaml") -> Config:
    """
    加载配置文件

    Args:
        config_path: 配置文件路径

    Returns:
        Config 对象
    """
    if not os.path.exists(config_path):
        return Config()

    with open(config_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f) or {}

    config = Config()

    if 'hubstudio' in data:
        hs = data['hubstudio']
        config.hubstudio = HubStudioConfig(
            api_url=hs.get('api_url', config.hubstudio.api_url),
            api_key=hs.get('api_key', ''),
            timeout=hs.get('timeout', config.hubstudio.timeout)
        )

    if 'database' in data:
        db = data['database']
        config.database = DatabaseConfig(
            access_path=db.get('access_path', config.database.access_path),
            pool_size=db.get('pool_size', config.database.pool_size)
        )

    if 'scheduler' in data:
        sc = data['scheduler']
        config.scheduler = SchedulerConfig(
            max_concurrent=sc.get('max_concurrent', config.scheduler.max_concurrent),
            task_timeout=sc.get('task_timeout', config.scheduler.task_timeout),
            env_startup_timeout=sc.get('env_startup_timeout', config.scheduler.env_startup_timeout),
            max_retries=sc.get('max_retries', config.scheduler.max_retries),
            retry_interval=sc.get('retry_interval', config.scheduler.retry_interval)
        )

    if 'browser' in data:
        br = data['browser']
        config.browser = BrowserConfig(
            chromedriver_path=br.get('chromedriver_path', config.browser.chromedriver_path),
            page_load_timeout=br.get('page_load_timeout', config.browser.page_load_timeout),
            script_timeout=br.get('script_timeout', config.browser.script_timeout),
            implicit_wait=br.get('implicit_wait', config.browser.implicit_wait)
        )

    if 'logging' in data:
        lg = data['logging']
        config.logging = LoggingConfig(
            level=lg.get('level', config.logging.level),
            format=lg.get('format', config.logging.format),
            file=lg.get('file', config.logging.file)
        )

    return config
