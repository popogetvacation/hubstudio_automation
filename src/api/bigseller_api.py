"""
BigSeller API 封装模块
提供订单标记批量导入等 API 调用方法
"""
import os
from typing import Dict, Any, Optional, List

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
    GET_PENDING_ORDERS_API = "/api/v1/order/new/pageList.json"
    BATCH_EDIT_REMARK_API = "/api/v1/order/sign/batchEdit/remark.json"
    BATCH_MANAGE_LABELS_API = "/api/v1/order/batchAddOrDeleteLabel.json"

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

    def get_pending_orders(self, page_no: int = 1, page_size: int = 300,
                           order_by: str = "expireTime", desc: bool = False,
                           **filters) -> Dict[str, Any]:
        """
        获取待处理订单列表（分页）

        Args:
            page_no: 页码，默认 1
            page_size: 每页数量，默认 300
            order_by: 排序字段，默认 "expireTime"
            desc: 是否降序，默认 False
            **filters: 可选过滤参数（如 shopId, platform 等）

        Returns:
            {
                "code": 0,
                "data": {
                    "page": {
                        "pageNo": 1,
                        "pageSize": 300,
                        "totalPage": 2,
                        "totalSize": 39,
                        "rows": [...]  # 订单列表
                    }
                }
            }

        Raises:
            Exception: API 调用失败时抛出异常
        """
        import requests

        url = f"{self.BASE_URL}{self.GET_PENDING_ORDERS_API}"
        headers = self._build_headers()

        # 构建请求体
        request_body = {
            "status": "new",
            "pageNo": page_no,
            "pageSize": page_size,
            "orderBy": order_by,
            "desc": 0 if not desc else 1,
            "timeType": 1,
            "days": "",
            "beginDate": "",
            "endDate": "",
            "searchType": "orderNo",
            "inquireType": 2,
            "packState": "0",
        }

        # 合并额外的过滤参数
        request_body.update(filters)

        logger.info(f"获取待处理订单: page_no={page_no}, page_size={page_size}")

        try:
            response = requests.post(
                url,
                headers=headers,
                json=request_body,
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()

                if result is None:
                    logger.error(f"响应为空, body: {response.text[:500]}")
                    raise Exception(f"响应为空")

                # 检查业务响应码
                code = result.get('code')
                if code != 0:
                    error_msg = result.get('msg', '未知错误')
                    logger.error(f"业务错误: code={code}, msg={error_msg}")
                    raise Exception(f"业务错误: code={code}, msg={error_msg}")

                data = result.get('data', {})
                page_info = data.get('page', {})
                total_size = page_info.get('totalSize', 0)
                total_page = page_info.get('totalPage', 0)
                rows_count = len(page_info.get('rows', []))

                logger.info(f"获取待处理订单成功: page_no={page_no}, total_size={total_size}, total_page={total_page}, rows={rows_count}")

                return result
            else:
                logger.error(f"获取待处理订单失败: HTTP {response.status_code}, body: {response.text[:500]}")
                raise Exception(f"请求失败: HTTP {response.status_code}")

        except Exception as e:
            logger.error(f"获取待处理订单异常: {e}")
            raise

    def get_all_pending_orders(self, page_size: int = 300,
                              max_pages: int = None, **filters) -> List[Dict]:
        """
        获取所有待处理订单（自动分页）

        Args:
            page_size: 每页数量，默认 300
            max_pages: 最大页数限制，None 表示获取所有
            **filters: 可选过滤参数

        Returns:
            所有订单对象的列表

        Raises:
            Exception: API 调用失败时抛出异常
        """
        import time

        all_orders = []
        page_no = 1

        logger.info(f"开始获取所有待处理订单: page_size={page_size}, max_pages={max_pages}")

        while True:
            # 检查最大页数限制
            if max_pages is not None and page_no > max_pages:
                logger.info(f"达到最大页数限制 {max_pages}，停止获取")
                break

            try:
                result = self.get_pending_orders(
                    page_no=page_no,
                    page_size=page_size,
                    **filters
                )

                if not result:
                    break

                data = result.get('data', {})
                page_info = data.get('page', {})
                rows = page_info.get('rows', [])

                if not rows:
                    logger.info(f"第 {page_no} 页无数据，停止获取")
                    break

                all_orders.extend(rows)

                total_page = page_info.get('totalPage', 0)
                current_page = page_info.get('pageNo', page_no)

                logger.info(f"第 {current_page}/{total_page} 页，已获取 {len(all_orders)} 条订单")

                # 检查是否已到达最后一页
                if current_page >= total_page:
                    logger.info("已获取所有页面，停止获取")
                    break

                page_no += 1

                # 添加小延迟避免触发限流
                time.sleep(0.1)

            except Exception as e:
                logger.error(f"获取第 {page_no} 页订单异常: {e}")
                # 已有部分数据时继续返回
                if all_orders:
                    logger.warning(f"已获取 {len(all_orders)} 条订单，提前返回")
                    break
                raise

        logger.info(f"获取所有待处理订单完成: 共 {len(all_orders)} 条")
        return all_orders

    def batch_edit_order_remarks(self, orders: List[Dict]) -> Dict[str, Any]:
        """
        批量编辑订单备注

        Args:
            orders: 订单对象列表，每个对象包含：
                - orderId (int, required): 订单 ID
                - itemTotalNum (int, required): 商品总数
                - packageNo (str, required): 包裹号
                - orderItemList (list, required): 订单商品列表
                - remarkType (int, required): 备注类型（1=买家，2=卖家，3=系统）
                - content (str, required): 备注内容
                - orderNotApproved (bool, optional): 是否未审核，默认 False

        Returns:
            {"code": 0, "data": "操作成功"}

        Raises:
            ValueError: 参数验证失败
            Exception: API 调用失败时抛出异常
        """
        if not orders:
            raise ValueError("订单列表不能为空")

        import requests

        url = f"{self.BASE_URL}{self.BATCH_EDIT_REMARK_API}"
        headers = self._build_headers()

        # 验证必需字段
        for i, order in enumerate(orders):
            if 'orderId' not in order:
                raise ValueError(f"第 {i+1} 个订单缺少 orderId 字段")
            if 'itemTotalNum' not in order:
                raise ValueError(f"第 {i+1} 个订单缺少 itemTotalNum 字段")
            if 'packageNo' not in order:
                raise ValueError(f"第 {i+1} 个订单缺少 packageNo 字段")
            if 'orderItemList' not in order:
                raise ValueError(f"第 {i+1} 个订单缺少 orderItemList 字段")
            if 'remarkType' not in order:
                raise ValueError(f"第 {i+1} 个订单缺少 remarkType 字段")
            if 'content' not in order:
                raise ValueError(f"第 {i+1} 个订单缺少 content 字段")

            # 设置默认值
            order.setdefault('orderNotApproved', False)

        logger.info(f"批量编辑订单备注: 订单数量={len(orders)}")

        try:
            response = requests.post(
                url,
                headers=headers,
                json=orders,
                timeout=60
            )

            if response.status_code == 200:
                result = response.json()

                if result is None:
                    logger.error(f"响应为空, body: {response.text[:500]}")
                    raise Exception(f"响应为空")

                code = result.get('code')
                if code != 0:
                    error_msg = result.get('msg', '未知错误')
                    logger.error(f"业务错误: code={code}, msg={error_msg}")
                    raise Exception(f"业务错误: code={code}, msg={error_msg}")

                logger.info(f"批量编辑订单备注成功: {len(orders)} 个订单")
                return result
            else:
                logger.error(f"批量编辑订单备注失败: HTTP {response.status_code}, body: {response.text[:500]}")
                raise Exception(f"请求失败: HTTP {response.status_code}")

        except Exception as e:
            logger.error(f"批量编辑订单备注异常: {e}")
            raise

    def build_order_remark(self, order_id: int, item_total_num: int,
                           package_no: str, order_item_list: List[Dict],
                           remark_type: int, content: str,
                           order_not_approved: bool = False) -> Dict:
        """
        构建订单备注对象

        Args:
            order_id: 订单 ID
            item_total_num: 商品总数
            package_no: 包裹号
            order_item_list: 订单商品列表
            remark_type: 备注类型（1=买家，2=卖家，3=系统）
            content: 备注内容
            order_not_approved: 是否未审核

        Returns:
            订单备注对象
        """
        return {
            "orderId": order_id,
            "itemTotalNum": item_total_num,
            "packageNo": package_no,
            "orderItemList": order_item_list,
            "remarkType": remark_type,
            "content": content,
            "orderNotApproved": order_not_approved
        }

    def batch_manage_order_labels(self, order_ids: List[int], label_ids,
                                 operation: str = "add") -> Dict[str, Any]:
        """
        批量添加或删除订单标签

        Args:
            order_ids: 订单 ID 列表
            label_ids: 标签 ID（支持单个字符串或列表，多个标签用逗号拼接）
            operation: 操作类型，"add" 添加或 "delete" 删除

        Returns:
            {"code": 0, "data": {"check": true, "totalNum": 2, "closed": true}}

        Raises:
            ValueError: 参数验证失败
            Exception: API 调用失败时抛出异常
        """
        if not order_ids:
            raise ValueError("订单 ID 列表不能为空")

        if operation not in ("add", "delete"):
            raise ValueError(f"操作类型必须是 'add' 或 'delete'，当前: {operation}")

        # 处理标签ID（支持字符串或列表）
        if isinstance(label_ids, list):
            if not label_ids:
                raise ValueError("标签 ID 列表不能为空")
            lable_id_str = ",".join(str(lid) for lid in label_ids)
        elif label_ids:
            lable_id_str = str(label_ids)
        else:
            raise ValueError("标签 ID 不能为空")

        import requests

        url = f"{self.BASE_URL}{self.BATCH_MANAGE_LABELS_API}"
        headers = self._build_headers()

        # 构建请求体
        request_body = {
            "orderIdsStr": ",".join(map(str, order_ids)),
            "lableIdStr": lable_id_str,
            "type": operation
        }

        logger.info(f"批量{'添加' if operation == 'add' else '删除'}订单标签: 订单数量={len(order_ids)}, 标签ID={lable_id_str}")
        try:
            response = requests.post(
                url,
                headers=headers,
                json=request_body,
                timeout=60
            )

            if response.status_code == 200:
                result = response.json()

                if result is None:
                    logger.error(f"响应为空, body: {response.text[:500]}")
                    raise Exception(f"响应为空")

                code = result.get('code')
                if code != 0:
                    error_msg = result.get('msg', '未知错误')
                    logger.error(f"业务错误: code={code}, msg={error_msg}")
                    raise Exception(f"业务错误: code={code}, msg={error_msg}")

                data = result.get('data', {})
                logger.info(f"批量{'添加' if operation == 'add' else '删除'}订单标签成功: totalNum={data.get('totalNum')}")
                return result
            else:
                logger.error(f"批量管理订单标签失败: HTTP {response.status_code}, body: {response.text[:500]}")
                raise Exception(f"请求失败: HTTP {response.status_code}")

        except Exception as e:
            logger.error(f"批量管理订单标签异常: {e}")
            raise