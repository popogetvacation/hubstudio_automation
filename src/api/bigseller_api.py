"""
BigSeller API 封装模块
提供订单标记批量导入等 API 调用方法
"""
import os
from typing import Dict, Any, Optional

from ..utils.logger import default_logger as logger


class BigSellerAPI:
    """
    BigSeller API 封装类

    提供订单标记批量导入等 API 调用方法
    """

    # API 基础路径
    BASE_URL = "https://www.bigseller.pro"

    # API 路径
    IMPORT_ORDER_MARK_API = "/api/v1/excel/importOrderMark.json"
    IMPORT_ORDER_MARK_PROGRESS_API = "/api/v1/excel/importOrderMark/progress.json"

    def __init__(self, driver):
        """
        初始化 BigSeller API

        Args:
            driver: Selenium 驱动（用于获取 cookies）
        """
        self._driver = driver

    def _get_cookies(self) -> Dict[str, str]:
        """
        获取浏览器 Cookies

        Returns:
            Cookie 字典
        """
        cookies = self._driver.get_cookies()
        return {c['name']: c['value'] for c in cookies}

    def _build_headers(self) -> Dict[str, str]:
        """
        构建请求头（不含 Content-Type，让 requests 自动生成）

        Returns:
            请求头字典
        """
        cookies = self._get_cookies()
        cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])

        return {
            "accept": "application/json, text/plain, */*",
            "clienttype": "1",
            "cookie": cookie_str,
            "origin": self.BASE_URL,
            "referer": f"{self.BASE_URL}/web/order/index.htm?status=all"
        }

    def import_order_mark(self, file_path: str) -> Dict[str, Any]:
        """
        批量导入订单标记

        通过上传 Excel 文件，批量对订单进行标记（如备注、标签等）。
        该接口为异步处理，成功提交后返回一个任务标识符（key），
        用于后续查询导入进度或结果。

        Args:
            file_path: Excel 文件路径（.xlsx 格式）

        Returns:
            响应数据，包含任务标识符 key 等信息
            {
                "code": 0,
                "data": {
                    "check": True,
                    "key": "65a19d97-3ecd-4f11-8126-fd4386110442",
                    "closed": False
                }
            }

        Raises:
            FileNotFoundError: 文件不存在
            ValueError: 文件格式不正确
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")

        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext not in ['.xlsx', '.xls']:
            raise ValueError(f"文件格式不正确，必须为 .xlsx 或 .xls 格式，当前: {file_ext}")

        # 使用 requests 发送 multipart/form-data 请求
        import requests

        url = f"{self.BASE_URL}{self.IMPORT_ORDER_MARK_API}"
        headers = self._build_headers()

        # 移除 Content-Type，让 requests 自动生成 multipart boundary
        headers.pop('Content-Type', None)

        logger.info(f"上传订单标记文件: {file_path}")

        try:
            with open(file_path, 'rb') as f:
                files = {'file': (os.path.basename(file_path), f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}

                response = requests.post(
                    url,
                    headers=headers,
                    files=files,
                    timeout=60
                )

            if response.status_code == 200:
                try:
                    result = response.json()
                except Exception as e:
                    logger.error(f"JSON 解析失败: {e}, body: {response.text[:500]}")
                    raise Exception(f"JSON 解析失败: {e}")

                if result is None:
                    logger.error(f"响应为空, body: {response.text[:500]}")
                    raise Exception(f"响应为空")

                logger.info(f"订单标记导入响应: code={result.get('code')}, key={result.get('data', {}).get('key')}")

                # 检查业务响应码
                code = result.get('code')
                if code != 0:
                    error_msg = result.get('msg', '未知错误')
                    logger.error(f"业务错误: code={code}, msg={error_msg}")
                    raise Exception(f"业务错误: code={code}, msg={error_msg}")
                return result
            else:
                logger.error(f"订单标记导入失败: HTTP {response.status_code}, body: {response.text}")
                raise Exception(f"请求失败: HTTP {response.status_code}")

        except Exception as e:
            logger.error(f"订单标记导入异常: {e}")
            # 打印更多调试信息
            try:
                if 'response' in locals():
                    logger.error(f"响应状态码: {response.status_code}")
                    logger.error(f"响应头: {response.headers}")
                    logger.error(f"响应内容: {response.text[:1000]}")
            except Exception:
                pass
            raise

    def get_import_progress(self, key: str) -> Dict[str, Any]:
        """
        查询订单标记导入进度

        Args:
            key: 任务标识符（import_order_mark 返回的 data.key）

        Returns:
            进度信息
            {
                "code": 0,
                "data": {
                    "check": True,
                    "successNum": 10,
                    "failNum": 2,
                    "totalNum": 12,
                    "nowNum": 12,
                    "closed": True,
                    "errorMsg": null,
                    "failReason": null
                }
            }
        """
        if not key:
            raise ValueError("key 不能为空")

        import requests

        url = f"{self.BASE_URL}{self.IMPORT_ORDER_MARK_PROGRESS_API}"
        headers = self._build_headers()
        params = {'key': key}

        logger.info(f"查询导入进度: key={key}")

        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                data = result.get('data', {})
                logger.info(f"导入进度: successNum={data.get('successNum')}, failNum={data.get('failNum')}, closed={data.get('closed')}")
                return result
            else:
                logger.error(f"查询导入进度失败: HTTP {response.status_code}")
                raise Exception(f"请求失败: HTTP {response.status_code}")

        except Exception as e:
            logger.error(f"查询导入进度异常: {e}")
            raise