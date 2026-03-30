"""
任务基类和运行器
支持按环境分组执行任务
"""
from typing import Callable, Optional, Dict, Any, List
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import time
from datetime import datetime

from ..api.hubstudio_client import HubStudioClient
from ..browser.selenium_driver import HubStudioSeleniumDriver
from ..browser.environment_manager import EnvironmentManager
from ..utils.logger import default_logger as logger


class TaskStatus(Enum):
    """任务状态"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class TaskResult:
    """单个任务执行结果"""
    env_id: str
    env_name: str
    success: bool
    result: Any = None
    error: Optional[str] = None
    duration: float = 0
    task_id: str = ""


@dataclass
class GroupTaskResult:
    """分组任务总结果"""
    group_name: str
    task_name: str
    total: int = 0
    success: int = 0
    failed: int = 0
    results: List[TaskResult] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    @property
    def success_rate(self) -> float:
        return (self.success / self.total * 100) if self.total > 0 else 0


class BaseTask(ABC):
    """
    任务基类

    继承此类实现具体任务逻辑
    """

    # 任务名称，子类必须覆盖
    task_name: str = "base_task"

    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化任务

        Args:
            config: 任务配置
        """
        self.config = config or {}

    @abstractmethod
    def execute(self, driver: HubStudioSeleniumDriver, env_info: Dict[str, Any]) -> Any:
        """
        执行任务逻辑（子类必须实现）

        Args:
            driver: Selenium 驱动
            env_info: 环境信息，包含 env_id, env_name, proxy_info 等

        Returns:
            任务结果
        """
        pass

    def setup(self, driver: HubStudioSeleniumDriver, env_info: Dict[str, Any]):
        """
        任务前置操作（可选覆盖）

        Args:
            driver: Selenium 驱动
            env_info: 环境信息
        """
        pass

    def teardown(self, driver: HubStudioSeleniumDriver, env_info: Dict[str, Any]):
        """
        任务后置操作（可选覆盖）

        Args:
            driver: Selenium 驱动
            env_info: 环境信息
        """
        pass

    def on_error(self, driver: HubStudioSeleniumDriver, env_info: Dict[str, Any], error: Exception):
        """
        错误处理（可选覆盖）

        Args:
            driver: Selenium 驱动
            env_info: 环境信息
            error: 异常对象
        """
        logger.error(f"任务 {self.task_name} 执行失败 [{env_info.get('env_name')}]: {error}")


class TaskRunner:
    """
    任务运行器

    负责按环境分组执行任务
    """

    def __init__(self, client: HubStudioClient,
                 chromedriver_path: str = None,
                 max_workers: int = 5,
                 startup_timeout: int = 60,
                 task_timeout: int = 300,
                 max_retries: int = 3):
        """
        初始化任务运行器

        Args:
            client: HubStudio API 客户端
            chromedriver_path: ChromeDriver 路径
            max_workers: 最大并发数
            startup_timeout: 环境启动超时
            task_timeout: 任务超时
            max_retries: 最大重试次数
        """
        self.client = client
        self.chromedriver_path = chromedriver_path
        self.max_workers = max_workers
        self.startup_timeout = startup_timeout
        self.task_timeout = task_timeout
        self.max_retries = max_retries

        # 环境管理器
        self.env_manager = EnvironmentManager(
            client=client,
            startup_timeout=startup_timeout,
            max_retries=max_retries
        )

        # 已加载的环境
        self._loaded = False

    def load_environments(self, group_code: str = None) -> int:
        """
        加载环境列表

        Args:
            group_code: 分组名称，不传则加载所有

        Returns:
            加载的环境数量
        """
        count = self.env_manager.load_environments(group_code=group_code)
        self._loaded = True
        return count

    def get_environments_by_group(self, group_code: str) -> List[Dict[str, Any]]:
        """
        获取指定分组的环境列表

        Args:
            group_code: 分组名称

        Returns:
            环境信息列表
        """
        # 直接从 API 获取指定分组的环境
        env_list = self.client.get_env_list(group_code=group_code)
        return [
            {
                'env_id': env.env_id,
                'env_name': env.env_name,
                'group_code': env.group_code,
                'proxy_info': env.proxy_info
            }
            for env in env_list
        ]

    def run_task(self, task: BaseTask,
                 env_list: List[Dict[str, Any]],
                 concurrency: int = None) -> GroupTaskResult:
        """
        在指定环境列表上执行任务

        Args:
            task: 任务实例
            env_list: 环境列表
            concurrency: 并发数，默认使用 max_workers

        Returns:
            分组任务结果
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading

        concurrency = concurrency or self.max_workers
        result = GroupTaskResult(
            group_name=env_list[0].get('group_code', 'default') if env_list else 'default',
            task_name=task.task_name,
            total=len(env_list),
            start_time=datetime.now()
        )

        if not env_list:
            logger.warning(f"任务 {task.task_name} 没有可执行的环境")
            result.end_time = datetime.now()
            return result

        # 将环境添加到环境池中
        for env_info in env_list:
            self.env_manager.add_environment(
                env_id=env_info['env_id'],
                env_name=env_info.get('env_name', '')
            )

        logger.info(f"开始执行任务 {task.task_name}, 共 {len(env_list)} 个环境, 并发数: {concurrency}")

        def execute_single(env_info: Dict[str, Any]) -> TaskResult:
            """执行单个环境的任务"""
            env_id = env_info['env_id']
            env_name = env_info['env_name']
            start_time = time.time()

            driver = None

            try:
                # 打开浏览器
                logger.info(f"[{task.task_name}] 正在打开环境: {env_name}")

                browser_info = self.env_manager.open_environment(env_id)
                if not browser_info:
                    raise RuntimeError(f"无法打开环境: {env_id}")

                self.env_manager.mark_busy(env_id)

                # 创建 Selenium 驱动
                # 优先使用 HubStudio 自带的 webdriver，其次使用配置的
                webdriver_path = browser_info.webdriver_path or self.chromedriver_path
                driver = HubStudioSeleniumDriver(
                    debug_port=browser_info.debug_port,
                    chromedriver_path=webdriver_path
                )
                driver.connect()

                # 等待页面加载并检查是否有多个标签页
                time.sleep(2)

                # 检查是否有多个窗口/标签页，切换到正确的页面
                window_handles = driver.driver.window_handles
                if len(window_handles) > 1:
                    # 查找非 devtools 的窗口
                    target_handle = None
                    for handle in window_handles:
                        driver.switch_to_window(handle)
                        current_url = driver.get_current_url()
                        if 'devtools://' not in current_url and 'chrome://' not in current_url:
                            target_handle = handle
                            break

                    # 如果没找到，使用第一个窗口
                    if target_handle:
                        driver.switch_to_window(target_handle)
                        logger.info(f"[{task.task_name}] 切换到目标标签页")
                    else:
                        driver.switch_to_window(window_handles[0])

                # 等待页面加载完成
                try:
                    from selenium.webdriver.support.ui import WebDriverWait
                    WebDriverWait(driver.driver, 10).until(
                        lambda d: d.execute_script("return document.readyState") == "complete"
                    )
                except Exception:
                    pass

                # 执行前置操作
                task.setup(driver, env_info)

                # 执行任务
                logger.info(f"[{task.task_name}] 开始执行: {env_name}")
                task_result = task.execute(driver, env_info)

                # 执行后置操作
                task.teardown(driver, env_info)

                duration = time.time() - start_time
                logger.info(f"[{task.task_name}] 执行成功: {env_name}, 耗时: {duration:.2f}s")

                return TaskResult(
                    env_id=env_id,
                    env_name=env_name,
                    success=True,
                    result=task_result,
                    duration=duration
                )

            except Exception as e:
                duration = time.time() - start_time
                error_msg = str(e)
                logger.error(f"[{task.task_name}] 执行失败: {env_name}, 错误: {error_msg}")

                # 错误处理
                if driver:
                    try:
                        task.on_error(driver, env_info, e)
                    except Exception:
                        pass

                return TaskResult(
                    env_id=env_id,
                    env_name=env_name,
                    success=False,
                    error=error_msg,
                    duration=duration
                )

            finally:
                # 断开驱动
                if driver:
                    try:
                        driver.disconnect()
                    except Exception:
                        pass

                # 标记环境空闲
                self.env_manager.mark_idle(env_id)

        # 并发执行
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(execute_single, env): env for env in env_list}

            for future in as_completed(futures):
                task_result = future.result()
                result.results.append(task_result)

                if task_result.success:
                    result.success += 1
                else:
                    result.failed += 1

        result.end_time = datetime.now()

        # 打印统计
        total_duration = (result.end_time - result.start_time).total_seconds()
        logger.info(f"任务 {task.task_name} 执行完成: "
                   f"成功 {result.success}/{result.total}, "
                   f"总耗时 {total_duration:.2f}s")

        return result

    def run_task_by_group(self, task: BaseTask,
                          group_code: str,
                          concurrency: int = None) -> GroupTaskResult:
        """
        在指定分组上执行任务

        Args:
            task: 任务实例
            group_code: 环境分组名称
            concurrency: 并发数

        Returns:
            分组任务结果
        """
        # 获取分组环境
        env_list = self.get_environments_by_group(group_code)

        if not env_list:
            logger.warning(f"分组 {group_code} 没有找到环境")
            return GroupTaskResult(
                group_name=group_code,
                task_name=task.task_name,
                total=0
            )

        return self.run_task(task, env_list, concurrency)

    def close_all_environments(self) -> int:
        """关闭所有打开的环境"""
        return self.env_manager.close_all()


class TaskFactory:
    """
    任务工厂

    用于注册和创建任务实例
    """

    _tasks: Dict[str, type] = {}

    @classmethod
    def register(cls, task_class: type):
        """注册任务类"""
        cls._tasks[task_class.task_name] = task_class
        logger.info(f"已注册任务: {task_class.task_name}")

    @classmethod
    def create(cls, task_name: str, config: Dict[str, Any] = None) -> BaseTask:
        """创建任务实例"""
        if task_name not in cls._tasks:
            raise ValueError(f"未注册的任务: {task_name}")
        return cls._tasks[task_name](config=config)

    @classmethod
    def list_tasks(cls) -> List[str]:
        """列出所有已注册的任务"""
        return list(cls._tasks.keys())