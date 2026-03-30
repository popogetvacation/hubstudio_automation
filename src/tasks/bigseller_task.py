"""
BigSeller 任务模块
调用 BigSeller API 上传 Excel 文件进行订单标记批量导入
"""
from typing import Dict, Any, TYPE_CHECKING

from .task_base import BaseTask, TaskFactory
from ..api.bigseller_api import BigSellerAPI
from ..utils.logger import default_logger as logger

if TYPE_CHECKING:
    from ..browser.selenium_driver import HubStudioSeleniumDriver


class BigSellerTask(BaseTask):
    """
    BigSeller 订单标记批量导入任务

    调用 BigSeller API 上传 Excel 文件进行订单标记批量导入
    """

    task_name = "bigseller_import_order_mark"

    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化任务

        Args:
            config: 任务配置
                - excel_file: Excel 文件路径（必填）
                - wait_completion: 是否等待导入完成（默认 False）
                - poll_interval: 轮询间隔秒数（默认 2）
        """
        super().__init__(config)
        self.excel_file = self.config.get('excel_file', '')
        self.wait_completion = self.config.get('wait_completion', False)
        self.poll_interval = self.config.get('poll_interval', 2)

    def execute(self, driver: HubStudioSeleniumDriver, env_info: Dict[str, Any]) -> Any:
        """
        执行 BigSeller 订单标记导入任务

        Args:
            driver: Selenium 驱动
            env_info: 环境信息

        Returns:
            导入结果
        """
        if not self.excel_file:
            raise ValueError("未配置 excel_file 参数")

        env_name = env_info.get('env_name', 'unknown')

        # 步骤1: 跳转到 BigSeller 网站
        logger.info(f"[{env_name}] 步骤1: 跳转到 BigSeller...")
        try:
            driver.driver.set_page_load_timeout(30)
            driver.driver.get("https://www.bigseller.pro/")
            logger.info(f"[{env_name}] 页面加载完成, 当前URL: {driver.driver.current_url}")
        except Exception as e:
            logger.warning(f"[{env_name}] 页面加载超时或出错: {type(e).__name__}: {e}")

        # 步骤2: 获取浏览器 Cookies
        logger.info(f"[{env_name}] 步骤2: 获取浏览器 Cookies...")
        try:
            cookies = driver.driver.get_cookies()
            cookie_names = [c['name'] for c in cookies]
            logger.info(f"[{env_name}] 获取到 {len(cookies)} 个 Cookies: {cookie_names[:5]}...")
        except Exception as e:
            logger.error(f"[{env_name}] 获取Cookies失败: {type(e).__name__}: {e}")
            raise Exception(f"获取Cookies失败: {e}")

        # 步骤3: 创建 BigSeller API 实例并上传文件
        logger.info(f"[{env_name}] 步骤3: 创建 API 实例...")
        bigseller_api = BigSellerAPI(driver.driver)

        logger.info(f"[{env_name}] 步骤4: 上传订单标记文件: {self.excel_file}")
        try:
            result = bigseller_api.import_order_mark(self.excel_file)
            logger.info(f"[{env_name}] 上传响应: {result}")
        except Exception as e:
            logger.error(f"[{env_name}] 上传文件失败: {type(e).__name__}: {e}")
            raise

        # 步骤5: 解析响应获取 key
        logger.info(f"[{env_name}] 步骤5: 解析响应...")
        if not result:
            raise Exception(f"[{env_name}] 导入响应为空")

        code = result.get('code')
        data = result.get('data')

        if code != 0:
            msg = result.get('msg', '未知错误')
            logger.error(f"[{env_name}] 业务错误: code={code}, msg={msg}")
            raise Exception(f"导入失败: code={code}, msg={msg}")

        if not data:
            raise Exception(f"[{env_name}] 响应data为空")

        key = data.get('key')
        if not key:
            logger.warning(f"[{env_name}] 未获取到任务 key")
            return result

        logger.info(f"[{env_name}] 导入任务已提交, key: {key}")

        # 步骤6: 等待导入完成（可选）
        if self.wait_completion:
            logger.info(f"[{env_name}] 步骤6: 等待导入完成...")
            import time
            while True:
                try:
                    progress = bigseller_api.get_import_progress(key)
                    progress_data = progress.get('data', {})
                    closed = progress_data.get('closed', False)

                    if closed:
                        success_num = progress_data.get('successNum', 0)
                        fail_num = progress_data.get('failNum', 0)
                        total_num = progress_data.get('totalNum', 0)
                        fail_reason = progress_data.get('failReason', [])

                        logger.info(f"[{env_name}] 导入完成: 成功 {success_num}, 失败 {fail_num}, 总计 {total_num}")
                        if fail_reason:
                            logger.warning(f"[{env_name}] 失败原因: {fail_reason}")

                        return {
                            'key': key,
                            'success_num': success_num,
                            'fail_num': fail_num,
                            'total_num': total_num,
                            'fail_reason': fail_reason
                        }

                    logger.info(f"[{env_name}] 导入处理中: {progress_data.get('nowNum')}/{progress_data.get('totalNum')}")
                    time.sleep(self.poll_interval)
                except Exception as e:
                    logger.error(f"[{env_name}] 查询进度失败: {e}, 继续重试")
                    time.sleep(self.poll_interval)

        return {
            'key': key,
            'message': '导入任务已提交，请使用 key 查询进度'
        }


# 注册任务到 TaskFactory
TaskFactory.register(BigSellerTask)