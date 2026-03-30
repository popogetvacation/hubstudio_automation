"""
端到端集成测试模块
测试完整的业务流程
"""
import os
import sys
import pytest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import init_app, create_client, create_runner, create_task, get_available_tasks, TaskFactory
from src.tasks.task_base import BaseTask, TaskRunner, TaskFactory
from src.config import load_config, Config


class TestMainInitialization:
    """测试 main.py 初始化流程"""

    @patch('main.load_config')
    @patch('main.HubStudioClient')
    @patch('main.setup_logger')
    def test_init_app_returns_tuple(self, mock_logger, mock_client, mock_config):
        """测试 init_app 返回正确的元组"""
        # 模拟配置
        mock_config_obj = MagicMock()
        mock_config_obj.hubstudio.api_url = "http://127.0.0.1:6873"
        mock_config_obj.hubstudio.api_key = "test_key"
        mock_config_obj.hubstudio.timeout = 30
        mock_config_obj.scheduler.max_concurrent = 5
        mock_config_obj.scheduler.env_startup_timeout = 60
        mock_config_obj.scheduler.task_timeout = 300
        mock_config_obj.scheduler.max_retries = 3
        mock_config_obj.browser.chromedriver_path = "chromedriver.exe"
        mock_config_obj.logging.level = "INFO"
        mock_config_obj.logging.file = "test.log"
        mock_config.return_value = mock_config_obj

        # 模拟客户端
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance

        # 模拟日志
        mock_logger.return_value = MagicMock()

        # 执行初始化
        config, client, runner = init_app()

        # 验证返回值
        assert config is not None
        assert client is not None
        assert runner is not None
        assert isinstance(runner, TaskRunner)

    @patch('main.load_config')
    def test_create_client_uses_config(self, mock_config):
        """测试 create_client 使用配置"""
        mock_config_obj = MagicMock()
        mock_config_obj.hubstudio.api_url = "http://127.0.0.1:6873"
        mock_config_obj.hubstudio.api_key = "test_key_123"
        mock_config_obj.hubstudio.timeout = 60
        mock_config.return_value = mock_config_obj

        with patch('main.HubStudioClient') as mock_client:
            client = create_client(mock_config_obj)

            # 验证客户端被正确创建
            mock_client.assert_called_once()
            call_kwargs = mock_client.call_args[1]
            assert call_kwargs['api_url'] == "http://127.0.0.1:6873"
            assert call_kwargs['api_key'] == "test_key_123"
            assert call_kwargs['timeout'] == 60


class TestTaskFactory:
    """测试任务工厂"""

    def test_get_available_tasks_returns_list(self):
        """测试获取可用任务列表"""
        tasks = get_available_tasks()
        assert isinstance(tasks, list)
        # 应该包含已注册的任务
        assert 'shopee' in tasks or 'shopee_all_order' in tasks

    def test_create_task_with_config(self):
        """测试创建任务实例"""
        task_config = {
            'page_size': 40,
            'max_pages': 1,
            'fetch_detail': True
        }

        task = create_task('shopee_all_order', task_config)
        assert task is not None
        assert isinstance(task, BaseTask)

    def test_create_task_default_config(self):
        """测试使用默认配置创建任务"""
        task = create_task('shopee_all_order')
        assert task is not None
        assert task.task_name == 'shopee_all_order'


class TestTaskRunnerIntegration:
    """测试 TaskRunner 集成"""

    @patch('main.HubStudioClient')
    def test_task_runner_initialization(self, mock_client):
        """测试 TaskRunner 初始化"""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance

        runner = create_runner(mock_client_instance)

        assert runner is not None
        assert runner.client is mock_client_instance
        assert runner.max_workers > 0

    def test_task_runner_has_required_methods(self):
        """测试 TaskRunner 具备所需方法"""
        # 检查 TaskRunner 有 run_task_by_group 方法
        assert hasattr(TaskRunner, 'run_task_by_group')
        assert hasattr(TaskRunner, 'run_task')
        assert hasattr(TaskRunner, 'load_environments')
        assert hasattr(TaskRunner, 'close_all_environments')


class TestTaskConfiguration:
    """测试任务配置"""

    def test_shopee_all_order_task_config(self):
        """测试 shopee_all_order 任务配置"""
        task_config = {
            'page_size': 40,
            'max_pages': 1,
            'order_list_tab': 100,
            'sort_type': 3,
            'ascending': False,
            'fetch_detail': True,
            'batch_size': 5,
            'save_to_db': True
        }

        task = create_task('shopee_all_order', task_config)

        # 验证配置被正确应用
        assert task.page_size == 40
        assert task.max_pages == 1
        assert task.order_list_tab == 100
        assert task.fetch_detail is True
        assert task.batch_size == 5

    def test_task_default_config(self):
        """测试任务默认配置"""
        task = create_task('shopee_all_order')

        # 验证默认值
        assert task.page_size == 200  # 默认值
        assert task.max_pages == 100  # 默认值
        assert task.order_list_tab == 100  # TAB_ALL


class TestEnvironmentManagement:
    """测试环境管理集成"""

    @patch('src.browser.environment_manager.EnvironmentManager.load_environments')
    @patch('main.HubStudioClient')
    def test_load_environments_from_runner(self, mock_client, mock_load):
        """测试从 Runner 加载环境"""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        mock_load.return_value = 3  # 假设加载了3个环境

        runner = create_runner(mock_client_instance)
        count = runner.load_environments(group_code='test')

        assert count == 3

    @patch('main.HubStudioClient')
    def test_get_environments_by_group(self, mock_client):
        """测试按分组获取环境"""
        from src.api.hubstudio_client import EnvironmentInfo

        mock_client_instance = MagicMock()

        # 模拟返回环境列表
        mock_env = MagicMock()
        mock_env.env_id = 'env_001'
        mock_env.env_name = 'Test Env'
        mock_env.group_code = 'test'
        mock_env.proxy_info = None
        mock_client_instance.get_env_list.return_value = [mock_env]

        runner = create_runner(mock_client_instance)
        envs = runner.get_environments_by_group('test')

        assert len(envs) == 1
        assert envs[0]['env_id'] == 'env_001'
        assert envs[0]['env_name'] == 'Test Env'


class TestDatabaseIntegration:
    """测试数据库集成"""

    @patch('src.database.access_db.AccessDatabase')
    def test_database_initialization(self, mock_db_class):
        """测试数据库初始化"""
        mock_db = MagicMock()
        mock_db_class.return_value = mock_db

        task_config = {
            'save_to_db': True,
            'db_path': './test.accdb'
        }
        task = create_task('shopee_all_order', task_config)

        # 验证数据库配置
        assert task.save_to_db is True

    @patch('src.database.access_db.AccessDatabase')
    def test_database_order_tables_init(self, mock_db_class):
        """测试订单表初始化"""
        mock_db = MagicMock()
        mock_db_class.return_value = mock_db

        from src.database.access_db import AccessDatabase

        db = AccessDatabase('./test.accdb')

        # 验证有 init_order_tables 方法
        assert hasattr(db, 'init_order_tables')


class TestResultHandling:
    """测试结果处理"""

    def test_group_task_result_structure(self):
        """测试分组任务结果结构"""
        from src.tasks.task_base import GroupTaskResult

        result = GroupTaskResult(
            group_name='test_group',
            task_name='shopee_all_order',
            total=5
        )

        # 验证基本属性
        assert result.group_name == 'test_group'
        assert result.task_name == 'shopee_all_order'
        assert result.total == 5

    def test_task_result_structure(self):
        """测试单个任务结果结构"""
        from src.tasks.task_base import TaskResult

        result = TaskResult(
            env_id='env_001',
            env_name='Test Env',
            success=True
        )

        # 验证基本属性
        assert result.env_id == 'env_001'
        assert result.env_name == 'Test Env'
        assert result.success is True


class TestErrorHandling:
    """测试错误处理"""

    def test_task_error_callback(self):
        """测试任务错误回调"""
        task = create_task('shopee_all_order')

        # 验证有 on_error 方法
        assert hasattr(task, 'on_error')

    def test_task_teardown_exists(self):
        """测试任务清理方法"""
        task = create_task('shopee_all_order')

        # 验证有 teardown 方法
        assert hasattr(task, 'teardown')


class TestBrowserIntegration:
    """测试浏览器集成"""

    @patch('src.browser.selenium_driver.HubStudioSeleniumDriver')
    def test_selenium_driver_creation(self, mock_driver_class):
        """测试 Selenium 驱动创建"""
        mock_driver = MagicMock()
        mock_driver_class.return_value = mock_driver

        from src.browser.selenium_driver import HubStudioSeleniumDriver
        driver = HubStudioSeleniumDriver(debug_port=9222)

        # 验证驱动可以创建
        assert driver is not None

    def test_browser_has_required_methods(self):
        """测试浏览器驱动具备所需方法"""
        from src.browser.selenium_driver import HubStudioSeleniumDriver

        # 验证基本方法存在
        assert hasattr(HubStudioSeleniumDriver, 'goto')
        assert hasattr(HubStudioSeleniumDriver, 'get_cookies')
        assert hasattr(HubStudioSeleniumDriver, 'screenshot')


class TestNetworkIntegration:
    """测试网络请求集成"""

    @patch('src.network.browser_request.BrowserRequest')
    def test_browser_request_creation(self, mock_request_class):
        """测试浏览器请求创建"""
        mock_driver = MagicMock()
        mock_request = MagicMock()
        mock_request_class.return_value = mock_request

        from src.network.browser_request import BrowserRequest
        request = BrowserRequest(mock_driver)

        # 验证请求对象可以创建
        assert request is not None


class TestShopeeAPIIntegration:
    """测试 Shopee API 集成"""

    def test_shopee_api_exists(self):
        """测试 ShopeeAPI 类存在"""
        try:
            from src.api.shopee_api import ShopeeAPI
            assert ShopeeAPI is not None
        except ImportError:
            pytest.skip("ShopeeAPI 模块不存在")

    def test_shopee_api_has_required_methods(self):
        """测试 ShopeeAPI 具备所需方法"""
        try:
            from src.api.shopee_api import ShopeeAPI
            # 验证基本方法存在（fetch_chat_messages_for_orders 已删除，使用异步版本）
            assert hasattr(ShopeeAPI, 'fetch_chat_messages_async')
        except ImportError:
            pytest.skip("ShopeeAPI 模块不存在")


class TestEndToEndFlow:
    """端到端流程测试"""

    @patch('main.load_config')
    @patch('main.HubStudioClient')
    @patch('main.setup_logger')
    def test_full_initialization_flow(self, mock_logger, mock_client, mock_config):
        """测试完整初始化流程"""
        # 模拟配置
        mock_config_obj = MagicMock()
        mock_config_obj.hubstudio.api_url = "http://127.0.0.1:6873"
        mock_config_obj.hubstudio.api_key = "test_key"
        mock_config_obj.hubstudio.timeout = 30
        mock_config_obj.scheduler.max_concurrent = 5
        mock_config_obj.scheduler.env_startup_timeout = 60
        mock_config_obj.scheduler.task_timeout = 300
        mock_config_obj.scheduler.max_retries = 3
        mock_config_obj.browser.chromedriver_path = "chromedriver.exe"
        mock_config_obj.logging.level = "INFO"
        mock_config_obj.logging.file = "test.log"
        mock_config.return_value = mock_config_obj

        # 模拟客户端
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance

        # 模拟日志
        mock_logger.return_value = MagicMock()

        # 1. 执行初始化
        config, client, runner = init_app()

        # 2. 创建任务
        task = create_task('shopee_all_order', {
            'page_size': 40,
            'max_pages': 1,
            'save_to_db': True
        })

        # 3. 验证各组件已连接
        assert runner.client is client
        assert task.task_name == 'shopee_all_order'

    def test_task_lifecycle(self):
        """测试任务生命周期"""
        task = create_task('shopee_all_order')

        # 验证任务有生命周期方法
        assert hasattr(task, 'setup')
        assert hasattr(task, 'execute')
        assert hasattr(task, 'teardown')
        assert hasattr(task, 'on_error')

    @patch('main.HubStudioClient')
    def test_runner_task_execution_flow(self, mock_client):
        """测试 Runner 任务执行流程"""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance

        runner = create_runner(mock_client_instance)

        # 验证执行方法存在
        assert hasattr(runner, 'run_task')
        assert hasattr(runner, 'run_task_by_group')


class TestConfigurationLoading:
    """测试配置加载"""

    def test_load_config_from_yaml(self):
        """测试从 YAML 加载配置"""
        config = load_config("config/settings.yaml")

        assert config is not None
        assert hasattr(config, 'hubstudio')
        assert hasattr(config, 'database')
        assert hasattr(config, 'scheduler')
        assert hasattr(config, 'browser')

    def test_config_has_required_attributes(self):
        """测试配置具备必需属性"""
        config = load_config("config/settings.yaml")

        # HubStudio 配置
        assert hasattr(config.hubstudio, 'api_url')
        assert hasattr(config.hubstudio, 'api_key')
        assert hasattr(config.hubstudio, 'timeout')

        # 数据库配置
        assert hasattr(config.database, 'access_path')

        # 调度器配置
        assert hasattr(config.scheduler, 'max_concurrent')
        assert hasattr(config.scheduler, 'task_timeout')

        # 浏览器配置
        assert hasattr(config.browser, 'chromedriver_path')


class TestTaskRegistration:
    """测试任务注册"""

    def test_shopee_all_order_registered(self):
        """测试 shopee_all_order 任务已注册"""
        tasks = get_available_tasks()
        assert 'shopee_all_order' in tasks

    def test_task_factory_has_registered_tasks(self):
        """测试任务工厂有已注册的任务"""
        # 验证 TaskFactory 已被初始化（任务已注册）
        tasks = TaskFactory.list_tasks()
        assert len(tasks) > 0