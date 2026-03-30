"""
HubStudio 自动化框架入口
提供初始化和工厂方法
"""
import os
import sys

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config import load_config, Config
from src.utils.logger import setup_logger
from src.api import HubStudioClient
from src.tasks import TaskRunner, TaskFactory, BaseTask

# 导入已注册的任务（确保任务被注册）
import src.tasks.shopee_all_order_task  # noqa: F401


def create_client(config: Config = None) -> HubStudioClient:
    """
    创建 HubStudio API 客户端

    Args:
        config: 配置对象，不传则自动加载

    Returns:
        HubStudioClient 实例
    """
    if config is None:
        config = load_config("config/settings.yaml")

    return HubStudioClient(
        api_url=config.hubstudio.api_url,
        api_key=config.hubstudio.api_key,
        timeout=config.hubstudio.timeout
    )


def create_runner(client: HubStudioClient = None,
                  config: Config = None) -> TaskRunner:
    """
    创建任务运行器

    Args:
        client: API 客户端，不传则自动创建
        config: 配置对象

    Returns:
        TaskRunner 实例
    """
    if config is None:
        config = load_config("config/settings.yaml")

    if client is None:
        client = create_client(config)

    return TaskRunner(
        client=client,
        chromedriver_path=config.browser.chromedriver_path,
        max_workers=config.scheduler.max_concurrent,
        startup_timeout=config.scheduler.env_startup_timeout,
        task_timeout=config.scheduler.task_timeout,
        max_retries=config.scheduler.max_retries
    )


def init_app(config_path: str = "config/settings.yaml") -> tuple:
    """
    初始化应用

    Args:
        config_path: 配置文件路径

    Returns:
        (config, client, runner) 元组
    """
    # 加载配置
    config = load_config(config_path)

    # 设置日志
    logger = setup_logger(
        name="hubstudio_automation",
        level=config.logging.level,
        log_file=config.logging.file
    )

    # 创建客户端
    client = create_client(config)

    # 创建运行器
    runner = create_runner(client, config)

    return config, client, runner


def get_available_tasks() -> list:
    """获取所有可用任务"""
    return TaskFactory.list_tasks()


def create_task(task_name: str, config: dict = None) -> BaseTask:
    """
    创建任务实例

    Args:
        task_name: 任务名称
        config: 任务配置

    Returns:
        任务实例
    """
    return TaskFactory.create(task_name, config)


# 导出的公共接口
__all__ = [
    'init_app',
    'create_client',
    'create_runner',
    'create_task',
    'get_available_tasks',
    'TaskRunner',
    'TaskFactory',
    'BaseTask',
    'Config',
    'load_config'
]


if __name__ == "__main__":
    # 演示入口
    print("HubStudio 自动化框架")
    print("=" * 50)
    print(f"可用任务: {get_available_tasks()}")
    print()
    print("使用方法:")
    print("  from main import init_app, create_task")
    print("  config, client, runner = init_app()")
    print("  task = create_task('shopee', {'search_keyword': 'phone'})")
    print("  result = runner.run_task_by_group(task, 'shopee')")