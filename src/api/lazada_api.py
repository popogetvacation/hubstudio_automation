"""
Lazada Seller Center API 封装模块
提供订单相关的 API 调用方法
"""
import asyncio
import hashlib
import hmac
import json
import os
import time
import urllib.parse
from typing import Dict, Any, Optional, List

from ..network.browser_request import BrowserRequest
from ..utils.logger import default_logger as logger


class LazadaAPIError(Exception):
    """API 调用失败异常"""
    def __init__(self, method: str, code: int, message: str):
        self.method = method
        self.code = code
        self.message = message
        super().__init__(f"[{method}] API返回错误: code={code}, message={message}")


class LazadaAPI:
    """
    Lazada Seller Center API 封装类

    提供订单列表、买家信息、历史订单等 API 调用方法
    """

    # Lazada API 域名
    DOMAIN = "acs-m.lazada.com.ph"
    SELLER_CENTER_DOMAIN = "sellercenter.lazada.com.ph"

    # appKey 映射
    APP_KEY_ORDER = "4272"
    APP_KEY_IM = "24813843"

    # API 路径
    API_ORDER_LIST = "/h5/mtop.lazada.seller.order.query.list/1.0/"
    API_ORDER_COUNT = "/h5/mtop.lazada.seller.order.query.count/1.0/"
    API_ORDER_INIT = "/h5/mtop.lazada.seller.order.query.list.init/1.0/"
    API_ORDER_MAIN_DETAIL = "/h5/mtop.lazada.seller.order.main.detail/1.0/"
    API_ORDER_EXTRA_DETAIL = "/h5/mtop.lazada.seller.order.extra.detail/1.0/"
    API_ORDER_SENSITIVE = "/h5/mtop.lazada.seller.order.query.sensitive/1.0/"
    API_SLA_COUNT = "/h5/mtop.lazada.seller.order.sla.breached.count/1.0/"

    # IM API 路径
    API_IM_OPEN_SESSION = "/h5/mtop.im.use.web.seller.mtopimsessionviewservice.opensession/1.0/"
    API_IM_BUYER_ORDER_LIST = "/h5/mtop.global.im.web.card.order.list.get/1.0/"
    API_IM_CHAT_HISTORY = "/h5/mtop.im.use.seller.messagebox.queryMessageListBySessionId/1.0/"
    API_IM_BUYER_PROFILE = "/h5/mtop.global.im.biz.seller.buyerprofile.get/1.0/"
    API_IM_SESSION_READ = "/h5/mtop.lazada.im.web.seller.session.read/1.0/"

    def __init__(self, driver, browser_request: BrowserRequest = None):
        """
        初始化 Lazada API

        Args:
            driver: Selenium 驱动
            browser_request: 网络请求对象，如果为 None 会自动创建
        """
        self._driver = driver
        self._browser_request = browser_request
        self._auth_info = None
        self._cdp_cookies_cache = None

    @property
    def browser_request(self) -> BrowserRequest:
        """获取或创建浏览器请求对象"""
        if self._browser_request is None:
            self._browser_request = BrowserRequest(self._driver)
        return self._browser_request

    @property
    def auth_info(self) -> Dict[str, Any]:
        """获取认证信息"""
        if self._auth_info is None:
            self._auth_info = self._extract_auth_info()
        return self._auth_info

    def set_auth_info(self, auth_info: Dict[str, Any]):
        """设置认证信息"""
        self._auth_info = auth_info

    def get_base_url(self, env_name: str = None) -> str:
        """
        获取基础 URL

        Args:
            env_name: 环境名称（用于兼容，但 Lazada 不需要）

        Returns:
            基础 URL
        """
        return f"https://{self.DOMAIN}"

    # ==================== 认证相关 ====================

    def _extract_auth_info(self) -> Dict:
        """从 Cookies 中提取认证信息"""
        cookies = self._driver.get_cookies()
        cookie_dict = {c.get('name', ''): c.get('value', '') for c in cookies}

        auth_info = {
            'cookies': cookies,
            'cookie_dict': cookie_dict,
            'site': 'LAZADA_PH'
        }

        # 提取关键认证 Cookie
        m_h5_tk = cookie_dict.get('_m_h5_tk', '')
        if m_h5_tk:
            # 格式: {token}_{timestamp}
            auth_info['m_h5_tk'] = m_h5_tk
            parts = m_h5_tk.split('_')
            if len(parts) >= 2:
                auth_info['token'] = parts[0]
                auth_info['token_timestamp'] = parts[1]
                logger.info(f"[LazadaAPI] _m_h5_tk token: {parts[0]}, timestamp: {parts[1]}")

        # 其他认证 Cookie
        auth_info['m_h5_tk_enc'] = cookie_dict.get('_m_h5_tk_enc', '')
        auth_info['asc_uid'] = cookie_dict.get('asc_uid', '')
        auth_info['asc_uid_enc'] = cookie_dict.get('asc_uid_enc', '')
        auth_info['csrftoken'] = cookie_dict.get('CSRFT', '') or cookie_dict.get('csrftoken', '')
        auth_info['t_sid'] = cookie_dict.get('t_sid', '')

        logger.info(f"[LazadaAPI] 提取认证信息: m_h5_tk={bool(m_h5_tk)}, m_h5_tk_enc={bool(auth_info['m_h5_tk_enc'])}, asc_uid={bool(auth_info['asc_uid'])}")
        logger.info(f"[LazadaAPI] 完整 _m_h5_tk: {m_h5_tk}")
        logger.info(f"[LazadaAPI] _m_h5_tk_enc: {auth_info['m_h5_tk_enc']}")

        return auth_info

    def _get_cdp_cookies(self) -> List[Dict]:
        """
        通过 CDP 获取当前页面的所有 cookies（包含 HttpOnly）
        """
        if self._cdp_cookies_cache is not None:
            return self._cdp_cookies_cache

        try:
            cdp_network = self.browser_request.cdp
            cookies = cdp_network.get_cookies()
            self._cdp_cookies_cache = cookies
            return cookies
        except Exception as e:
            logger.warning(f"[LazadaAPI] CDP 获取 cookies 失败: {e}")
            return []

    def _build_cookies_header(self) -> str:
        """构建 Cookie 请求头"""
        cdp_cookies = self._get_cdp_cookies()
        if cdp_cookies:
            cookie_parts = [f"{c['name']}={c['value']}" for c in cdp_cookies]
            cookie_header = '; '.join(cookie_parts)
            logger.info(f"[LazadaAPI] CDP cookies 数量: {len(cdp_cookies)}, header长度: {len(cookie_header)}")
            # 调试：检查关键 cookie 是否存在
            for c in cdp_cookies:
                if c['name'] in ['_m_h5_tk', '_m_h5_tk_enc', 'asc_uid']:
                    logger.info(f"[LazadaAPI] Cookie {c['name']}: {c.get('domain', 'unknown')}, value={c['value'][:30]}...")
            return cookie_header

        cookies = self._driver.get_cookies()
        cookie_parts = [f"{c['name']}={c['value']}" for c in cookies]
        return '; '.join(cookie_parts)

    def _calculate_sign(self, token: str, timestamp: str, app_key: str, data: str) -> str:
        """
        计算 MD5 签名

        根据 Lazada mtop 签名机制:
        sign = MD5(token&timestamp&appKey&data)

        注意: Lazada 使用普通 MD5，而不是 HMAC-MD5
        """
        message = f"{token}&{timestamp}&{app_key}&{data}"
        sign = hashlib.md5(message.encode('utf-8')).hexdigest()

        logger.info(f"[LazadaAPI] 签名计算 (MD5):")
        logger.info(f"  token: {token}")
        logger.info(f"  timestamp: {timestamp}")
        logger.info(f"  app_key: {app_key}")
        logger.info(f"  data: {data[:100]}...")
        logger.info(f"  计算签名: {sign}")
        return sign.lower()

    def _generate_timestamp(self) -> str:
        """生成 13 位毫秒时间戳"""
        return str(int(time.time() * 1000))

    def _build_common_headers(self, api_name: str, app_key: str) -> Dict[str, str]:
        """构建通用请求头"""
        user_agent = self._driver.execute_script("return navigator.userAgent") or ""

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Referer': f'https://{self.SELLER_CENTER_DOMAIN}/',
            'Origin': f'https://{self.SELLER_CENTER_DOMAIN}',
            'User-Agent': user_agent,
            'Accept-Language': 'en-PH,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
        }

        # 添加 Cookie
        cookie_header = self._build_cookies_header()
        if cookie_header:
            headers['Cookie'] = cookie_header

        return headers

    def _make_mtop_request(self, api_path: str, data: Dict, app_key: str = None) -> Optional[Dict]:
        """
        发送 MTOP 请求

        Args:
            api_path: API 路径
            data: 请求体数据
            app_key: appKey，不传则使用订单 API 的 appKey

        Returns:
            响应数据或 None
        """
        if app_key is None:
            app_key = self.APP_KEY_ORDER

        base_url = f"https://{self.DOMAIN}"
        timestamp = self._generate_timestamp()

        auth = self.auth_info
        token = auth.get('token', '')
        if not token:
            logger.warning(f"[LazadaAPI] 未找到 token，使用空字符串")
            token = ''

        # 使用当前生成的时间戳（浏览器请求也使用当前时间戳，而非 cookie 时间戳）

        # 将 data 转为 JSON 字符串
        data_str = json.dumps(data, separators=(',', ':'))

        # 调试：输出完整的签名计算参数
        logger.info(f"[LazadaAPI] ===== 签名计算参数 =====")
        logger.info(f"[LazadaAPI] token: {token}")
        logger.info(f"[LazadaAPI] timestamp: {timestamp}")
        logger.info(f"[LazadaAPI] app_key: {app_key}")
        logger.info(f"[LazadaAPI] data_str: {data_str}")
        logger.info(f"[LazadaAPI] =============================")

        # 计算签名
        sign = self._calculate_sign(token, timestamp, app_key, data_str)

        # data 参数需要 URL 编码
        encoded_data = urllib.parse.quote(data_str)

        # 构建 URL 参数（GET 请求，data 作为 query 参数）
        params = {
            'jsv': '2.6.1',
            'appKey': app_key,
            't': timestamp,
            'sign': sign,
            'v': '1.0',
            'timeout': '30000',
            'H5Request': 'true',
            'url': api_path.replace('/h5/', '').replace('/1.0/', ''),
            'api': api_path.replace('/h5/', '').replace('/1.0/', ''),
            'type': 'originaljson',
            'dataType': 'json',
            'valueType': 'original',
            'x-i18n-regionID': 'LAZADA_PH',
            'data': encoded_data,  # GET 请求时 data 在 URL 参数中
        }

        # 构建完整 URL
        full_url = f"{base_url}{api_path}"
        if params:
            query_string = '&'.join(f"{k}={v}" for k, v in params.items())
            full_url = f"{full_url}?{query_string}"

        logger.info(f"[LazadaAPI] 完整URL: {full_url[:300]}...")
        logger.info(f"[LazadaAPI] data参数: {params.get('data', '')[:100]}...")

        headers = self._build_common_headers(api_path, app_key)

        try:
            # 使用 GET 请求（根据 Lazada API 文档）
            response = self.browser_request.get(
                url=full_url,
                headers=headers,
                timeout=30
            )

            if response.ok:
                resp_data = response.json()
                logger.info(f"[LazadaAPI] API {api_path} 响应: {str(resp_data)[:200]}")

                # 检查 API 返回码
                if resp_data.get('api'):
                    return resp_data.get('data', {})
                else:
                    code = resp_data.get('code', -1)
                    message = resp_data.get('message', 'unknown')
                    raise LazadaAPIError(api_path, code, message)
            else:
                raise LazadaAPIError(api_path, response.status_code, f"HTTP error: {response.status_code}")

        except LazadaAPIError:
            raise
        except Exception as e:
            logger.error(f"API {api_path} 请求异常: {e}")
            raise LazadaAPIError(api_path, -1, str(e))

    # ==================== 订单列表 API ====================

    def get_order_list(self, page: int = 1, page_size: int = 20,
                       tab: str = "topack", sort: str = "SHIPPING_SLA",
                       sort_order: str = "ASC") -> Optional[Dict]:
        """
        获取订单列表

        Args:
            page: 页码，从 1 开始
            page_size: 每页数量
            tab: 订单状态标签 (toship/topack/unpaid/shipped 等)
            sort: 排序字段
            sort_order: 排序方向 (ASC/DESC)

        Returns:
            订单列表数据
        """
        data = {
            "page": page,
            "pageSize": page_size,
            "filterOrderItems": True,
            "sort": sort,
            "sortOrder": sort_order,
            "tab": tab
        }

        logger.info(f"[LazadaAPI] get_order_list 参数: page={page}, pageSize={page_size}, tab={tab}")

        result = self._make_mtop_request(self.API_ORDER_LIST, data)

        if result:
            return result
        return None

    def get_all_orders(self, tab: str = "toship", max_pages: int = 100) -> List[Dict]:
        """
        获取所有订单（分页）

        Args:
            tab: 订单状态标签
            max_pages: 最大页数

        Returns:
            订单列表
        """
        all_orders = []
        page = 1
        page_size = 20

        logger.info(f"[LazadaAPI] 开始分页获取订单，tab={tab}，max_pages={max_pages}")

        while page <= max_pages:
            logger.info(f"[LazadaAPI] ===== 正在获取第 {page} 页...")

            try:
                result = self.get_order_list(
                    page=page,
                    page_size=page_size,
                    tab=tab
                )

                if result:
                    # 订单列表在 result['data']['dataSource'] 中
                    data_obj = result.get('data', {})
                    data_source = data_obj.get('dataSource', [])
                    orders = data_source if isinstance(data_source, list) else []

                    if not orders:
                        logger.info(f"[LazadaAPI] 第 {page} 页无数据，停止")
                        break

                    all_orders.extend(orders)

                    # 检查分页信息 (也在 result['data']['pageInfo'] 中)
                    page_info = data_obj.get('pageInfo', {})
                    total = page_info.get('total', 0)
                    logger.info(f"[LazadaAPI] 第 {page} 页获取成功，"
                               f"本页 {len(orders)} 条，总计 {total} 条")

                    # 如果当前页数据少于 page_size，说明是最后一页
                    if len(orders) < page_size:
                        logger.info(f"[LazadaAPI] 本页数据不足，停止获取")
                        break
                else:
                    logger.warning(f"[LazadaAPI] API 返回为空")
                    break

            except Exception as e:
                logger.warning(f"[LazadaAPI] 获取订单异常: {e}")
                break

            page += 1
            time.sleep(0.5)

        logger.info(f"[LazadaAPI] 订单列表获取完成: {len(all_orders)} 条")
        return all_orders

    def get_order_count(self) -> Dict:
        """获取各状态订单数量"""
        try:
            result = self._make_mtop_request(self.API_ORDER_COUNT, {})
            return result or {}
        except Exception as e:
            logger.error(f"[LazadaAPI] 获取订单数量异常: {e}")
            return {}

    # ==================== 订单详情 API ====================

    def get_order_main_detail(self, order_number: str) -> Optional[Dict]:
        """
        获取订单主详情

        Args:
            order_number: 订单号

        Returns:
            订单详情数据
        """
        data = {"tradeOrderId": str(order_number)}

        try:
            result = self._make_mtop_request(self.API_ORDER_MAIN_DETAIL, data)
            return result
        except Exception as e:
            logger.error(f"[LazadaAPI] 获取订单详情异常: {e}")
            return None

    def get_order_sensitive_info(self, order_number: str, need_ciphertext: bool = False) -> Optional[Dict]:
        """
        获取订单敏感信息（收货地址、联系电话等）

        Args:
            order_number: 订单号
            need_ciphertext: 是否需要密文

        Returns:
            敏感信息数据
        """
        data = {
            "tradeOrderId": str(order_number),
            "needCiphertext": need_ciphertext
        }

        try:
            result = self._make_mtop_request(self.API_ORDER_SENSITIVE, data)
            return result
        except Exception as e:
            logger.error(f"[LazadaAPI] 获取敏感信息异常: {e}")
            return None

    def get_buyer_address(self, order_number: str) -> Optional[Dict]:
        """
        获取买家收货地址

        Args:
            order_number: 订单号

        Returns:
            地址信息
        """
        sensitive_info = self.get_order_sensitive_info(order_number, need_ciphertext=False)
        if sensitive_info:
            return sensitive_info.get('shippingAddress', {})
        return None

    # ==================== IM/聊天 API ====================

    def _make_im_request(self, api_path: str, data: Dict) -> Optional[Dict]:
        """发送 IM 模块请求（使用不同的 appKey）"""
        return self._make_mtop_request(api_path, data, app_key=self.APP_KEY_IM)

    def im_open_session(self, buyer_id: str, order_id: str,
                        to_account_id: str = None, session_type: int = 103) -> Optional[str]:
        """
        打开 IM 会话，获取 sessionViewId

        Args:
            buyer_id: 买家 ID
            order_id: 订单 ID
            to_account_id: 目标账号 ID（通常等于 buyer_id）
            session_type: 会话类型，默认 103

        Returns:
            sessionViewId 或 None
        """
        if to_account_id is None:
            to_account_id = buyer_id

        auth = self.auth_info
        seller_id = auth.get('asc_uid', '')

        data = {
            "isWindowOpen": "true",
            "buyerId": str(buyer_id),
            "orderId": str(order_id),
            "accessKey": "lazada-pc-h5",
            "accessToken": "lazada-test-secret",
            "toAccountId": str(to_account_id),
            "toAccountType": "1",
            "sessionType": session_type,
            "fromCode": "sc_seller_order"
        }

        try:
            result = self._make_im_request(self.API_IM_OPEN_SESSION, data)
            if result:
                # sessionViewId 在响应中
                session = result.get('session', {})
                body = session.get('body', {})
                type_data = body.get('typeData', {})
                # 优先使用 sessionId（格式如 1#103#buyerId#1#sellerId#2）
                session_view_id = type_data.get('sessionId', '')
                if not session_view_id:
                    session_view_id = type_data.get('entityId', '')
                return session_view_id
            return None
        except Exception as e:
            logger.error(f"[LazadaAPI] 打开 IM 会话异常: {e}")
            return None

    def get_buyer_order_list(self, buyer_id: str, order_id: str,
                             page: int = 1, page_size: int = 20) -> Optional[Dict]:
        """
        获取买家的购买记录列表

        Args:
            buyer_id: 买家 ID
            order_id: 当前订单 ID
            page: 页码
            page_size: 每页数量

        Returns:
            购买记录列表
        """
        data = {
            "isWindowOpen": "true",
            "buyerId": str(buyer_id),
            "orderId": str(order_id),
            "accessKey": "lazada-pc-h5",
            "accessToken": "lazada-test-secret",
            "page": page,
            "pageNo": page,
            "pageSize": page_size,
            "customer": str(buyer_id)
        }

        try:
            result = self._make_im_request(self.API_IM_BUYER_ORDER_LIST, data)
            return result
        except Exception as e:
            logger.error(f"[LazadaAPI] 获取买家订单列表异常: {e}")
            return None

    def get_buyer_all_orders(self, buyer_id: str, order_id: str,
                             max_count: int = 100) -> List[Dict]:
        """
        获取买家的所有历史订单

        Args:
            buyer_id: 买家 ID
            order_id: 当前订单 ID
            max_count: 最大获取数量

        Returns:
            历史订单列表
        """
        all_orders = []
        page = 1
        page_size = 20

        while len(all_orders) < max_count:
            try:
                result = self.get_buyer_order_list(buyer_id, order_id, page, page_size)

                if result and result.get('result'):
                    orders = result.get('result', [])
                    if not orders:
                        break

                    all_orders.extend(orders)

                    total_count = result.get('totalCount', 0)
                    logger.info(f"[LazadaAPI] 买家 {buyer_id} 历史订单第 {page} 页，"
                               f"本页 {len(orders)} 条，总计 {total_count} 条")

                    if len(orders) < page_size:
                        break
                else:
                    break

            except Exception as e:
                logger.warning(f"[LazadaAPI] 获取买家历史订单异常: {e}")
                break

            page += 1
            time.sleep(0.3)

        return all_orders

    def get_chat_history(self, session_view_id: str, buyer_id: str, order_id: str,
                          start_time: int = -1, fetch_count: int = 50) -> List[Dict]:
        """
        获取与买家的聊天记录

        Args:
            session_view_id: 会话视图 ID（从 im_open_session 获取）
            buyer_id: 买家 ID
            order_id: 订单 ID
            start_time: 开始时间戳，-1 表示最新
            fetch_count: 获取消息数量

        Returns:
            聊天消息列表
        """
        auth = self.auth_info
        seller_id = auth.get('asc_uid', '')

        data = {
            "isWindowOpen": "true",
            "buyerId": str(buyer_id),
            "orderId": str(order_id),
            "accessKey": "lazada-pc-h5",
            "accessToken": "lazada-test-secret",
            "accountType": 2,
            "sessionViewId": session_view_id,
            "nodeId": 1,
            "startTime": start_time,
            "fetchCount": fetch_count
        }

        try:
            result = self._make_im_request(self.API_IM_CHAT_HISTORY, data)
            if result:
                return result.get('result', [])
            return []
        except Exception as e:
            logger.error(f"[LazadaAPI] 获取聊天记录异常: {e}")
            return []

    # ==================== 异步批量获取 ====================

    async def get_buyer_address_batch(self, order_numbers: List[str],
                                       max_concurrent: int = 10) -> Dict[str, Dict]:
        """
        异步批量获取买家地址

        Args:
            order_numbers: 订单号列表
            max_concurrent: 最大并发数

        Returns:
            order_number -> 地址信息 的映射
        """
        result = {}
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_single(order_number: str):
            async with semaphore:
                try:
                    await asyncio.sleep(0.1)
                    address = self.get_buyer_address(order_number)
                    return order_number, address
                except Exception as e:
                    logger.warning(f"[Async] 获取订单 {order_number} 地址失败: {e}")
                    return order_number, None

        tasks = [fetch_single(oid) for oid in order_numbers if oid]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, tuple):
                order_number, address = r
                if address:
                    result[order_number] = address

        logger.info(f"[Async] 批量获取买家地址完成: {len(result)}/{len(order_numbers)}")
        return result

    async def get_buyer_history_batch(self, buyer_id: str, order_id: str,
                                       buyer_ids: List[str],
                                       max_concurrent: int = 10) -> Dict[str, List[Dict]]:
        """
        异步批量获取买家历史订单

        Args:
            buyer_id: 当前订单对应的买家 ID（用于获取聊天记录）
            order_id: 当前订单 ID（用于获取聊天记录）
            buyer_ids: 买家 ID 列表
            max_concurrent: 最大并发数

        Returns:
            buyer_id -> 历史订单列表 的映射
        """
        result = {}
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_single(bid: str):
            async with semaphore:
                try:
                    await asyncio.sleep(0.1)
                    orders = self.get_buyer_all_orders(bid, order_id, max_count=50)
                    return bid, orders
                except Exception as e:
                    logger.warning(f"[Async] 获取买家 {bid} 历史订单失败: {e}")
                    return bid, []

        # 构造任务，同时获取当前买家的历史订单
        tasks = [fetch_single(bid) for bid in buyer_ids if bid]

        # 如果当前买家不在列表中，添加它
        if buyer_id and buyer_id not in buyer_ids:
            tasks.append(fetch_single(buyer_id))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, tuple):
                bid, orders = r
                result[bid] = orders

        logger.info(f"[Async] 批量获取买家历史订单完成: {len(result)} 个买家")
        return result

    async def get_chat_history_batch(self, buyer_id: str, order_id: str,
                                     session_view_id: str = None,
                                     max_concurrent: int = 10) -> List[Dict]:
        """
        获取买家的聊天记录

        Args:
            buyer_id: 买家 ID
            order_id: 订单 ID
            session_view_id: 会话视图 ID（如果为 None 会自动获取）
            max_concurrent: 最大并发数

        Returns:
            聊天消息列表
        """
        if session_view_id is None:
            session_view_id = self.im_open_session(buyer_id, order_id)

        if not session_view_id:
            logger.warning(f"[Async] 无法获取 sessionViewId，买家 {buyer_id}")
            return []

        try:
            chats = self.get_chat_history(session_view_id, buyer_id, order_id)
            return chats
        except Exception as e:
            logger.warning(f"[Async] 获取买家 {buyer_id} 聊天记录失败: {e}")
            return []

    # ==================== 工具方法 ====================

    def parse_address(self, address_info: Dict) -> Dict[str, Any]:
        """
        解析地址信息

        Args:
            address_info: 地址信息字典

        Returns:
            解析后的地址
        """
        result = {
            'full_address': '',
            'receiver': '',
            'receiver_phone': '',
            'province': '',
            'city': '',
            'district': ''
        }

        if not address_info:
            return result

        result['receiver'] = address_info.get('receiver', '')
        result['receiver_phone'] = address_info.get('receiverPhone', '')
        result['full_address'] = address_info.get('detailAddress', '')

        # 解析 locationTree (格式: "Province, City, District")
        location_tree = address_info.get('locationTree', '')
        if location_tree:
            parts = location_tree.split(',')
            if len(parts) >= 1:
                result['province'] = parts[0].strip()
            if len(parts) >= 2:
                result['city'] = parts[1].strip()
            if len(parts) >= 3:
                result['district'] = parts[2].strip()

        return result
