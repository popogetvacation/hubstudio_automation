"""
并发调度器模块
支持多环境并发执行任务
"""
from typing import Callable, Optional, Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, Future
import threading
import queue
import time
from datetime import datetime

from ..api.hubstudio_client import HubStudioClient, BrowserInfo
from ..browser.environment_manager import (
    EnvironmentManager, EnvironmentStatus, ManagedEnvironment
)
from ..browser.selenium_driver import HubStudioSeleniumDriver
from ..utils.logger import default_logger as logger
from ..tasks.task_base import TaskStatus, TaskResult


class Task:
    """任务定义"""
    task_id: str
    env_id: str
    task_func: Callable
    args: tuple = ()
    kwargs: dict = {}
    priority: int = 0
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    retry_count: int = 0
    max_retries: int = 3


class ConcurrentScheduler:
    """
    并发调度器

    管理多个浏览器环境并发执行任务
    """

    def __init__(self, client: HubStudioClient,
                 max_workers: int = 5,
                 task_timeout: int = 300,
                 startup_timeout: int = 60,
                 chromedriver_path: str = None):
        """
        初始化调度器

        Args:
            client: HubStudio API 客户端
            max_workers: 最大并发数
            task_timeout: 任务超时时间(秒)
            startup_timeout: 环境启动超时(秒)
            chromedriver_path: ChromeDriver 路径
        """
        self.client = client
        self.max_workers = max_workers
        self.task_timeout = task_timeout
        self.startup_timeout = startup_timeout
        self.chromedriver_path = chromedriver_path

        # 环境管理器
        self.env_manager = EnvironmentManager(
            client=client,
            startup_timeout=startup_timeout
        )

        # 任务队列
        self._task_queue: queue.PriorityQueue = queue.PriorityQueue()
        self._tasks: Dict[str, Task] = {}
        self._task_counter = 0
        self._lock = threading.RLock()

        # 线程池
        self._executor: Optional[ThreadPoolExecutor] = None
        self._running = False
        self._futures: Dict[str, Future] = {}

    def load_environments(self, group_code: Optional[str] = None) -> int:
        """
        加载环境列表

        Args:
            group_code: 分组代码

        Returns:
            加载的环境数量
        """
        return self.env_manager.load_environments(group_code)

    def add_task(self, env_id: str, task_func: Callable,
                 args: tuple = (), kwargs: dict = None,
                 priority: int = 0, max_retries: int = 3) -> str:
        """
        添加任务

        Args:
            env_id: 环境ID
            task_func: 任务函数
            args: 位置参数
            kwargs: 关键字参数
            priority: 优先级 (数字越小优先级越高)
            max_retries: 最大重试次数

        Returns:
            任务ID
        """
        if kwargs is None:
            kwargs = {}

        with self._lock:
            self._task_counter += 1
            task_id = f"task_{self._task_counter}_{int(time.time())}"

            task = Task(
                task_id=task_id,
                env_id=env_id,
                task_func=task_func,
                args=args,
                kwargs=kwargs,
                priority=priority,
                max_retries=max_retries
            )

            self._tasks[task_id] = task
            # 使用 (priority, counter) 保证同优先级 FIFO
            self._task_queue.put((priority, self._task_counter, task))

        logger.info(f"已添加任务 {task_id}, 环境: {env_id}")
        return task_id

    def add_batch_tasks(self, tasks: List[Dict]) -> List[str]:
        """
        批量添加任务

        Args:
            tasks: 任务列表，每个元素包含 env_id, task_func 等

        Returns:
            任务ID列表
        """
        task_ids = []
        for task_def in tasks:
            task_id = self.add_task(
                env_id=task_def['env_id'],
                task_func=task_def['task_func'],
                args=task_def.get('args', ()),
                kwargs=task_def.get('kwargs', {}),
                priority=task_def.get('priority', 0),
                max_retries=task_def.get('max_retries', 3)
            )
            task_ids.append(task_id)
        return task_ids

    def start(self):
        """启动调度器"""
        if self._running:
            logger.warning("调度器已在运行")
            return

        self._running = True
        self._executor = ThreadPoolExecutor(max_workers=self.max_workers)

        # 启动任务分发线程
        threading.Thread(target=self._dispatch_tasks, daemon=True).start()

        logger.info(f"调度器已启动, 最大并发数: {self.max_workers}")

    def stop(self, wait: bool = True):
        """
        停止调度器

        Args:
            wait: 是否等待任务完成
        """
        self._running = False

        if self._executor:
            self._executor.shutdown(wait=wait)
            self._executor = None

        # 关闭所有环境
        self.env_manager.close_all()

        logger.info("调度器已停止")

    def _dispatch_tasks(self):
        """任务分发线程"""
        while self._running:
            try:
                # 非阻塞获取任务
                try:
                    priority, counter, task = self._task_queue.get(timeout=1)
                except queue.Empty:
                    continue

                # 检查任务是否已取消
                if task.status == TaskStatus.CANCELLED:
                    continue

                # 提交任务到线程池
                future = self._executor.submit(
                    self._execute_task, task
                )
                self._futures[task.task_id] = future

            except Exception as e:
                logger.error(f"任务分发错误: {e}")

    def _execute_task(self, task: Task) -> TaskResult:
        """
        执行单个任务

        Args:
            task: 任务对象

        Returns:
            任务结果
        """
        task.status = TaskStatus.RUNNING
        task.start_time = datetime.now()

        driver = None
        browser_info = None

        try:
            # 打开环境
            browser_info = self.env_manager.open_environment(task.env_id)
            if not browser_info:
                raise RuntimeError(f"无法打开环境: {task.env_id}")

            self.env_manager.mark_busy(task.env_id)

            # 创建 Selenium 驱动
            driver = HubStudioSeleniumDriver(
                debug_port=browser_info.debug_port,
                chromedriver_path=self.chromedriver_path
            )
            driver.connect()

            # 执行任务函数
            # 将 driver 作为第一个参数传递给任务函数
            result = task.task_func(driver, *task.args, **task.kwargs)

            task.status = TaskStatus.COMPLETED
            task.result = result

            return TaskResult(
                task_id=task.task_id,
                env_id=task.env_id,
                success=True,
                result=result
            )

        except Exception as e:
            error_msg = str(e)
            task.error = error_msg

            # 重试逻辑
            if task.retry_count < task.max_retries:
                task.retry_count += 1
                task.status = TaskStatus.PENDING
                logger.warning(f"任务 {task.task_id} 失败，将重试 "
                              f"({task.retry_count}/{task.max_retries})")
                # 重新加入队列
                self._task_queue.put((task.priority, 0, task))
            else:
                task.status = TaskStatus.FAILED
                logger.error(f"任务 {task.task_id} 最终失败: {error_msg}")

            return TaskResult(
                task_id=task.task_id,
                env_id=task.env_id,
                success=False,
                error=error_msg
            )

        finally:
            task.end_time = datetime.now()

            # 断开驱动连接
            if driver:
                try:
                    driver.disconnect()
                except Exception:
                    pass

            # 标记环境空闲
            self.env_manager.mark_idle(task.env_id)

    def get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """获取任务状态"""
        task = self._tasks.get(task_id)
        return task.status if task else None

    def get_task_result(self, task_id: str) -> Optional[TaskResult]:
        """获取任务结果"""
        task = self._tasks.get(task_id)
        if not task:
            return None

        return TaskResult(
            task_id=task.task_id,
            env_id=task.env_id,
            success=task.status == TaskStatus.COMPLETED,
            result=task.result,
            error=task.error
        )

    def cancel_task(self, task_id: str) -> bool:
        """
        取消任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功取消
        """
        task = self._tasks.get(task_id)
        if not task:
            return False

        if task.status == TaskStatus.PENDING:
            task.status = TaskStatus.CANCELLED
            return True

        # 尝试取消正在执行的任务
        future = self._futures.get(task_id)
        if future and not future.done():
            if future.cancel():
                task.status = TaskStatus.CANCELLED
                return True

        return False

    def get_statistics(self) -> Dict[str, int]:
        """
        获取统计信息

        Returns:
            各状态的任务数量
        """
        stats = {status.value: 0 for status in TaskStatus}
        for task in self._tasks.values():
            stats[task.status.value] += 1
        return stats

    def wait_all(self, timeout: Optional[float] = None) -> bool:
        """
        等待所有任务完成

        Args:
            timeout: 超时时间

        Returns:
            是否全部完成
        """
        start_time = time.time()

        while True:
            stats = self.get_statistics()
            pending = stats.get(TaskStatus.PENDING.value, 0)
            running = stats.get(TaskStatus.RUNNING.value, 0)

            if pending == 0 and running == 0:
                return True

            if timeout and (time.time() - start_time) > timeout:
                return False

            time.sleep(0.5)
