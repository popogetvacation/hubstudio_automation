"""
浏览器环境管理模块
负责环境的打开、关闭、状态管理等
"""
from typing import Optional, Dict, List
from dataclasses import dataclass, field
from enum import Enum
import threading
import time

from ..api.hubstudio_client import HubStudioClient, BrowserInfo, EnvironmentInfo
from ..utils.logger import default_logger as logger


class EnvironmentStatus(Enum):
    """环境状态"""
    IDLE = "idle"           # 空闲
    BUSY = "busy"           # 使用中
    OPENING = "opening"     # 正在打开
    ERROR = "error"         # 错误
    CLOSED = "closed"       # 已关闭


@dataclass
class ManagedEnvironment:
    """被管理的环境"""
    env_id: str
    env_name: str
    status: EnvironmentStatus = EnvironmentStatus.IDLE
    browser_info: Optional[BrowserInfo] = None
    last_used: float = 0
    task_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None


class EnvironmentManager:
    """
    浏览器环境管理器

    管理多个浏览器环境的生命周期
    """

    def __init__(self, client: HubStudioClient,
                 startup_timeout: int = 60,
                 max_retries: int = 3):
        """
        初始化环境管理器

        Args:
            client: HubStudio API 客户端
            startup_timeout: 环境启动超时时间
            max_retries: 最大重试次数
        """
        self.client = client
        self.startup_timeout = startup_timeout
        self.max_retries = max_retries

        # 环境池 {env_id: ManagedEnvironment}
        self._env_pool: Dict[str, ManagedEnvironment] = {}
        self._lock = threading.RLock()

    def load_environments(self, group_code: Optional[str] = None) -> int:
        """
        加载环境列表到池中

        Args:
            group_code: 分组代码，不传则加载所有

        Returns:
            加载的环境数量
        """
        env_list = self.client.get_env_list(group_code=group_code)

        with self._lock:
            for env in env_list:
                if env.env_id not in self._env_pool:
                    self._env_pool[env.env_id] = ManagedEnvironment(
                        env_id=env.env_id,
                        env_name=env.env_name,
                        status=EnvironmentStatus.CLOSED if env.status == 0
                               else EnvironmentStatus.IDLE
                    )

        logger.info(f"已加载 {len(env_list)} 个环境")
        return len(env_list)

    def add_environment(self, env_id: str, env_name: str = ""):
        """
        手动添加环境到池中

        Args:
            env_id: 环境ID
            env_name: 环境名称
        """
        with self._lock:
            if env_id not in self._env_pool:
                self._env_pool[env_id] = ManagedEnvironment(
                    env_id=env_id,
                    env_name=env_name
                )

    def get_available_environment(self) -> Optional[ManagedEnvironment]:
        """
        获取一个可用的环境

        Returns:
            可用的环境或 None
        """
        with self._lock:
            for env in self._env_pool.values():
                if env.status == EnvironmentStatus.IDLE:
                    return env
        return None

    def get_all_environments(self) -> List[ManagedEnvironment]:
        """获取所有环境"""
        with self._lock:
            return list(self._env_pool.values())

    def get_environment(self, env_id: str) -> Optional[ManagedEnvironment]:
        """获取指定环境"""
        with self._lock:
            return self._env_pool.get(env_id)

    def open_environment(self, env_id: str) -> Optional[BrowserInfo]:
        """
        打开环境

        Args:
            env_id: 环境ID

        Returns:
            浏览器信息或 None
        """
        with self._lock:
            env = self._env_pool.get(env_id)
            if not env:
                logger.error(f"环境不存在: {env_id}")
                return None

            env.status = EnvironmentStatus.OPENING

        # 重试机制
        for attempt in range(self.max_retries):
            try:
                logger.info(f"正在打开环境 {env.env_name} ({env_id}), "
                           f"尝试 {attempt + 1}/{self.max_retries}")

                browser_info = self.client.open_browser(env_id)

                with self._lock:
                    env.browser_info = browser_info
                    env.status = EnvironmentStatus.IDLE
                    env.last_used = time.time()

                logger.info(f"环境 {env.env_name} 已打开, "
                           f"debug_port={browser_info.debug_port}")

                return browser_info

            except Exception as e:
                logger.error(f"打开环境失败: {env_id}, 错误: {e}")
                with self._lock:
                    env.error_count += 1
                    env.last_error = str(e)

                if attempt < self.max_retries - 1:
                    time.sleep(2)
                else:
                    with self._lock:
                        env.status = EnvironmentStatus.ERROR
                    return None

        return None

    def close_environment(self, env_id: str) -> bool:
        """
        关闭环境

        Args:
            env_id: 环境ID

        Returns:
            是否成功
        """
        with self._lock:
            env = self._env_pool.get(env_id)
            if not env:
                return False

        try:
            self.client.close_browser(env_id)

            with self._lock:
                env.status = EnvironmentStatus.CLOSED
                env.browser_info = None

            logger.info(f"环境 {env.env_name} 已关闭")
            return True

        except Exception as e:
            logger.error(f"关闭环境失败: {env_id}, 错误: {e}")
            return False

    def mark_busy(self, env_id: str):
        """标记环境为忙碌状态"""
        with self._lock:
            env = self._env_pool.get(env_id)
            if env:
                env.status = EnvironmentStatus.BUSY
                env.task_count += 1
                env.last_used = time.time()

    def mark_idle(self, env_id: str):
        """标记环境为空闲状态"""
        with self._lock:
            env = self._env_pool.get(env_id)
            if env:
                env.status = EnvironmentStatus.IDLE

    def mark_error(self, env_id: str, error: str):
        """标记环境为错误状态"""
        with self._lock:
            env = self._env_pool.get(env_id)
            if env:
                env.status = EnvironmentStatus.ERROR
                env.last_error = error
                env.error_count += 1

    def close_all(self) -> int:
        """
        关闭所有打开的环境

        Returns:
            成功关闭的数量
        """
        closed_count = 0

        with self._lock:
            envs_to_close = [
                env for env in self._env_pool.values()
                if env.status in (EnvironmentStatus.IDLE, EnvironmentStatus.BUSY)
                   and env.browser_info
            ]

        for env in envs_to_close:
            if self.close_environment(env.env_id):
                closed_count += 1

        logger.info(f"已关闭 {closed_count} 个环境")
        return closed_count

    def get_status_summary(self) -> Dict[str, int]:
        """
        获取状态统计

        Returns:
            各状态的环境数量
        """
        summary = {status.value: 0 for status in EnvironmentStatus}

        with self._lock:
            for env in self._env_pool.values():
                summary[env.status.value] += 1

        return summary

    def refresh_status(self):
        """刷新环境状态"""
        open_browsers = self.client.get_all_open_browsers()
        open_env_ids = {b.get('envId') for b in open_browsers}

        with self._lock:
            for env in self._env_pool.values():
                if env.env_id in open_env_ids:
                    if env.status == EnvironmentStatus.CLOSED:
                        env.status = EnvironmentStatus.IDLE
                else:
                    if env.status in (EnvironmentStatus.IDLE, EnvironmentStatus.BUSY):
                        env.status = EnvironmentStatus.CLOSED
                        env.browser_info = None
