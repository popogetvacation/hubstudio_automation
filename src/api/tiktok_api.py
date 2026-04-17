"""
Tokopedia/TikTok Shop API 封装模块
提供订单相关的 API 调用方法
"""
import asyncio
import json
import os
import time
import urllib.parse
import threading
from typing import Dict, Any, Optional, List

from ..network.browser_request import BrowserRequest
from ..network.async_http import AsyncBatchRequest, AsyncHTTPResponse
from ..utils.logger import default_logger as logger


class ApiError(Exception):
    """API 调用失败异常"""
    def __init__(self, method: str, code: int, message: str):
        self.method = method
        self.code = code
        self.message = message
        super().__init__(f"[{method}] API返回错误: code={code}, message={message}")


class TokopediaAPI:
    """
    Tokopedia/TikTok Shop API 封装类

    提供订单列表、买家联系信息、历史订单等 API 调用方法
    """

    # API 路径
    ORDER_LIST_API = "/api/fulfillment/order/list"
    BUYER_CONTACT_API = "/api/fulfillment/orders/buyer_contact_info/get"
    WORKBENCH_DATA_API = "/api/v1/shop_im/shop/workbench/data/list"
    CHAT_BUYER_LINK_API = "/chat/api/seller/mGetContactBuyerLinkByOrder"  # 获取买家联系链接（包含 pigeonUid）
    CREATE_CONVERSATION_API = "/api/v1/shop_im/shop/conversation/create_conversation"  # 创建对话，获取 imcloud_conversation_id

    # 国家域名映射
    DOMAIN_MAP = {
        'id': 'seller-id.tokopedia.com',   # 印尼
        'my': 'seller-my.tiktok.com',     # 马来西亚
        'th': 'seller-th.tiktok.com',     # 泰国
        'ph': 'seller-ph.tiktok.com',     # 菲律宾
    }

    # 国家代码映射 (用于 x-tt-oec-region 头)
    REGION_CODE_MAP = {
        'id': 'ID',
        'my': 'MY',
        'th': 'TH',
        'ph': 'PH',
    }

    def __init__(self, driver, browser_request: BrowserRequest = None):
        """
        初始化 Tokopedia API

        Args:
            driver: Selenium 驱动
            browser_request: 网络请求对象，如果为 None 会自动创建
        """
        self._driver = driver
        self._browser_request = browser_request
        self._auth_info = None
        self._cdp_cookies_cache = None  # 缓存 CDP cookies

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

    def enable_monitoring(self):
        """启用网络监控，捕获 API 请求"""
        try:
            self.browser_request.start_monitoring()
            logger.info("[TokopediaAPI] 网络监控已启动")
        except Exception as e:
            logger.warning(f"[TokopediaAPI] 启动网络监控失败: {e}")

    def disable_monitoring(self):
        """停止网络监控"""
        try:
            self.browser_request.stop_monitoring()
            logger.info("[TokopediaAPI] 网络监控已停止")
        except Exception as e:
            logger.warning(f"[TokopediaAPI] 停止网络监控失败: {e}")

    def get_captured_requests(self) -> List[Dict]:
        """获取捕获的网络请求"""
        try:
            # 从 browser_request 获取监控的请求
            requests = self.browser_request.get_monitored_requests()
            logger.info(f"[TokopediaAPI] 捕获到 {len(requests)} 个网络请求")
            return requests
        except Exception as e:
            logger.warning(f"[TokopediaAPI] 获取捕获请求失败: {e}")
            return []

    def get_cdp_cookies(self) -> List[Dict]:
        """
        通过 CDP 获取当前页面的所有 cookies（包含 HttpOnly）
        使用缓存避免重复获取
        """
        # 返回缓存
        if self._cdp_cookies_cache is not None:
            return self._cdp_cookies_cache

        try:
            cdp_network = self.browser_request.cdp
            # 不限制 URL，获取所有 cookies（包括可能在其他域名下的 gd_random）
            cookies = cdp_network.get_cookies()
            # 缓存结果
            self._cdp_cookies_cache = cookies
            return cookies
        except Exception as e:
            logger.warning(f"[TokopediaAPI] CDP 获取 cookies 失败: {e}")
            return []

    def get_cookies_header(self) -> str:
        """
        获取完整的 cookie 字符串，用于在请求中传递
        """
        # 优先使用 CDP 获取的 cookies
        cdp_cookies = self.get_cdp_cookies()
        if cdp_cookies:
            cookie_parts = [f"{c['name']}={c['value']}" for c in cdp_cookies]
            result = '; '.join(cookie_parts)
            return result

        # 备用：使用 Selenium 获取的 cookies
        cookies = self._driver.get_cookies()
        cookie_parts = [f"{c['name']}={c['value']}" for c in cookies]
        return '; '.join(cookie_parts)

    def get_base_url(self, env_name: str = None) -> str:
        """
        获取基础 URL

        Args:
            env_name: 环境名称，用于确定国家域名

        Returns:
            基础 URL
        """
        if env_name:
            return f"https://{self._get_domain(env_name)}"
        # 尝试从当前 URL 提取
        current_url = self._driver.get_current_url()
        for domain in self.DOMAIN_MAP.values():
            if domain in current_url:
                return f"https://{domain}"
        return f"https://{self.DOMAIN_MAP.get('id', 'seller-id.tokopedia.com')}"

    def get_region_code(self, env_name: str = None) -> str:
        """获取区域代码 (用于 x-tt-oec-region 头)"""
        if env_name:
            country_code = env_name[:2].lower() if env_name else 'id'
            return self.REGION_CODE_MAP.get(country_code, 'ID')
        # 尝试从当前 URL 提取
        current_url = self._driver.get_current_url()
        for code, domain in self.DOMAIN_MAP.items():
            if domain in current_url:
                return self.REGION_CODE_MAP.get(code, 'ID')
        return 'ID'

    # ==================== 认证相关 ====================

    def _extract_auth_info(self) -> Dict:
        """从 Cookies 中提取认证信息"""
        cookies = self._driver.get_cookies()

        auth_info = {
            'cookies': cookies,
            'seller_id': None,
            'oec_seller_id': None,
            'region': 'ID'
        }

        # 从 cookies 提取 seller_id
        for cookie in cookies:
            name = cookie.get('name', '')
            value = cookie.get('value', '')

            if name == 'SELLER_TOKEN':
                auth_info['seller_token'] = value
            elif name == 'UNIFIED_SELLER_TOKEN':
                auth_info['unified_seller_token'] = value
            elif name == 'oec_seller_id_unified_seller_env':
                auth_info['oec_seller_id'] = value
            elif name == 'global_seller_id_unified_seller_env':
                auth_info['seller_id'] = value
            elif name == 'SHOP_ID':
                auth_info['shop_id'] = value

        # 尝试从页面获取 seller_id（备用方案）
        try:
            page_info = self._driver.execute_script("""
                var info = {};
                // 尝试从 __INITIAL_STATE__ 获取
                if (window.__INITIAL_STATE__) {
                    var state = window.__INITIAL_STATE__;
                    if (state.user && state.user.sellerId) {
                        info.seller_id = state.user.sellerId;
                    }
                    if (state.user && state.user.oecSellerId) {
                        info.oec_seller_id = state.user.oecSellerId;
                    }
                    if (state.region) {
                        info.region = state.region;
                    }
                }
                // 尝试从 URL 获取
                var urlParams = new URLSearchParams(window.location.search);
                if (urlParams.has('oec_seller_id')) {
                    info.oec_seller_id = urlParams.get('oec_seller_id');
                }
                if (urlParams.has('seller_id')) {
                    info.seller_id = urlParams.get('seller_id');
                }
                return info;
            """)
            if page_info:
                if page_info.get('seller_id') and not auth_info.get('seller_id'):
                    auth_info['seller_id'] = page_info['seller_id']
                if page_info.get('oec_seller_id') and not auth_info.get('oec_seller_id'):
                    auth_info['oec_seller_id'] = page_info['oec_seller_id']
                if page_info.get('region'):
                    auth_info['region'] = page_info['region']
        except Exception:
            pass

        # 从 env_name 推断 region
        current_url = self._driver.get_current_url()
        for code, domain in self.DOMAIN_MAP.items():
            if domain in current_url:
                auth_info['region'] = self.REGION_CODE_MAP.get(code, 'ID')
                break

        return auth_info

    def _get_domain(self, env_name: str) -> str:
        """获取域名"""
        if env_name and len(env_name) >= 2:
            country_code = env_name[:2].lower()
            return self.DOMAIN_MAP.get(country_code, 'seller-id.tokopedia.com')
        return 'seller-id.tokopedia.com'

    def _build_headers(self, base_url: str, include_auth: bool = True) -> Dict[str, str]:
        """构建请求头"""
        region = self.get_region_code()

        # 获取用户信息
        user_agent = self._driver.execute_script("return navigator.userAgent") or ""
        auth = self.auth_info
        seller_id = auth.get('seller_id') or auth.get('oec_seller_id', '')

        headers = {
            'accept': '*/*',
            'origin': f'https://{self._get_domain_from_url(base_url)}',
            'referer': f'{base_url}',
            'x-tt-oec-region': region,  # 添加区域头
            # 注意：不加 x-requested-with，浏览器成功请求中没有这个头
            'user-agent': user_agent,
            'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
        }

        if include_auth:
            # 注意：之前使用 gd_random，但现在 CDP 无法获取 gd_random
            # 改用 x-tt-oec-region header 来通过验证

            # 添加 Cookie header（CDP 获取的 cookies）
            cookie_header = self.get_cookies_header()
            if cookie_header:
                headers['Cookie'] = cookie_header

            # 添加 CSRF token 如果有
            csrf_token = auth.get('csrf_token')
            if csrf_token:
                headers['x-csrf-token'] = csrf_token

        return headers

    def _get_domain_from_url(self, url: str) -> str:
        """从 URL 提取域名"""
        if '://' in url:
            return url.split('://')[1].split('/')[0]
        return url

    # ==================== 订单列表 API ====================

    def get_order_list(self, base_url: str,
                       order_status: List[str] = None,
                       search_tab: List[str] = None,
                       search_cursor: str = "",
                       offset: int = 0,
                       count: int = 20,
                       sort_info: str = "11") -> Optional[Dict]:
        """
        获取订单列表

        Args:
            base_url: 基础 URL
            order_status: 订单状态列表 ["1"]=待发货
            search_tab: 标签页 ["101"]=待发货
            search_cursor: 分页游标
            offset: 分页偏移量
            count: 每页数量
            sort_info: 排序标识 "11"=按最近更新时间

        Returns:
            订单列表数据或 None
        """
        # 使用待发货状态过滤
        if order_status is None:
            order_status = ["1"]
        if search_tab is None:
            search_tab = ["101"]

        # 获取 seller_id 用于 URL 参数
        auth = self.auth_info
        oec_seller_id = auth.get('oec_seller_id') or auth.get('seller_id', '')
        seller_id = auth.get('seller_id') or oec_seller_id

        # 构建带参数的 URL
        params = {
            'aid': '4068',
            'locale': 'en',
            'oec_seller_id': str(oec_seller_id),
            'seller_id': str(seller_id)
        }
        param_str = '&'.join(f"{k}={v}" for k, v in params.items())
        full_url = f"{base_url}{self.ORDER_LIST_API}?{param_str}"

        request_body = {
            "search_condition": {
                "condition_list": {
                    "order_status": {"value": order_status},
                    "search_tab": {"value": search_tab}
                }
            },
            "offset": offset,
            "count": count,
            "sort_info": sort_info,
            "search_cursor": search_cursor,
            "pagination_type": 0
        }

        try:
            # 构建请求头
            headers = self._build_headers(base_url)

            response = self.browser_request.post(
                url=full_url,
                json_data=request_body,
                headers=headers,
                timeout=30
            )

            if response.ok:
                data = response.json()

                if data and data.get('code') == 0:
                    return data.get('data', {})
                else:
                    code = data.get('code') if data else -1
                    msg = data.get('message', 'unknown') if data else 'empty response'
                    raise ApiError('get_order_list', code, msg)
            else:
                raise ApiError('get_order_list', response.status_code, f"HTTP error: {response.status_code}")

        except ApiError:
            raise
        except Exception as e:
            logger.error(f"获取订单列表异常: {e}")
            raise ApiError('get_order_list', -1, str(e))

    def get_all_orders(self, base_url: str, max_pages: int = 100) -> List[Dict]:
        """
        获取所有订单（分页）

        Args:
            base_url: 基础 URL
            max_pages: 最大页数

        Returns:
            订单列表
        """
        all_orders = []
        offset = 0
        page = 1

        logger.info(f"[TokopediaAPI] 开始分页获取订单，max_pages={max_pages}")

        while page <= max_pages:
            logger.info(f"[TokopediaAPI] ===== 正在获取第 {page} 页... (offset={offset})")

            try:
                data = self.get_order_list(
                    base_url=base_url,
                    offset=offset,
                    count=20
                )

                if data:
                    main_orders = data.get('main_orders', [])
                    all_orders.extend(main_orders)

                    total_count = data.get('total_count', 0)
                    has_more = data.get('has_more', False)

                    logger.info(f"[TokopediaAPI] 第 {page} 页获取成功，"
                               f"本页 {len(main_orders)} 条，总计 {total_count} 条")

                    # 检查是否还有更多
                    if not has_more:
                        logger.info(f"[TokopediaAPI] 无下一页，停止获取")
                        break
                    if len(main_orders) < 20:
                        logger.info(f"[TokopediaAPI] 本页数据不足一页，停止获取")
                        break

                    offset += len(main_orders)
                else:
                    logger.warning(f"[TokopediaAPI] API 返回为空，继续尝试...")

            except ApiError:
                # API 错误（code != 0）应该传播上去，不继续重试
                raise
            except Exception as e:
                logger.warning(f"[TokopediaAPI] 获取订单异常: {e}, 继续尝试...")

            page += 1
            time.sleep(0.5)

        logger.info(f"[TokopediaAPI] 订单列表获取完成: {len(all_orders)} 条")
        return all_orders

    # ==================== 买家联系信息 API ====================

    def get_buyer_contact_info(self, base_url: str, main_order_id: str) -> Optional[Dict]:
        """
        获取买家联系信息（收货地址）

        Args:
            base_url: 基础 URL
            main_order_id: 主订单 ID

        Returns:
            买家联系信息或 None
        """

        # 获取认证信息用于构建请求头
        auth = self.auth_info
        oec_seller_id = auth.get('oec_seller_id') or auth.get('seller_id', '')
        seller_id = auth.get('seller_id') or oec_seller_id

        # 构建 URL 参数
        params = {
            'aid': '4068',
            'locale': 'en',
            'oec_seller_id': str(oec_seller_id),
            'seller_id': str(seller_id)
        }
        param_str = '&'.join(f"{k}={v}" for k, v in params.items())
        full_url = f"{base_url}{self.BUYER_CONTACT_API}?{param_str}"

        request_body = {
            "main_order_id": main_order_id,
            "contact_info_type": 1  # 1 = 收货地址
        }

        try:
            # 构建请求头
            headers = self._build_headers(base_url)

            response = self.browser_request.post(
                url=full_url,
                json_data=request_body,
                headers=headers,
                timeout=30
            )

            if response.ok:
                data = response.json()
                if data and data.get('code') == 0:
                    contact_data = data.get('data', {})
                    return contact_data
                else:
                    code = data.get('code') if data else -1
                    msg = data.get('message', 'unknown') if data else 'empty response'
                    raise ApiError('get_buyer_contact_info', code, msg)
            else:
                raise ApiError('get_buyer_contact_info', response.status_code, f"HTTP error: {response.status_code}")

        except ApiError:
            raise
        except Exception as e:
            logger.error(f"获取买家联系信息异常: {e}")
            raise ApiError('get_buyer_contact_info', -1, str(e))

    def get_buyer_chat_link(self, base_url: str, main_order_id: str) -> Optional[Dict]:
        """
        获取买家聊天链接（包含 pigeonUid / buyer_user_id）

        API: GET /chat/api/seller/mGetContactBuyerLinkByOrder
        返回: pigeonUid (买家的唯一ID，用于获取买家历史订单)

        Args:
            base_url: 基础 URL
            main_order_id: 主订单 ID

        Returns:
            包含 pigeonUid 的字典或 None
        """

        # 获取认证信息
        auth = self.auth_info
        oec_seller_id = auth.get('oec_seller_id') or auth.get('seller_id', '')
        seller_id = auth.get('seller_id') or oec_seller_id

        # 构建 URL 参数
        params = {
            'orderType': '0',  # 0 = 普通订单
            'orderIds': main_order_id,
            'oec_seller_id': str(oec_seller_id),
            'seller_id': str(seller_id),
            'aid': '4068',
            'locale': 'en'
        }

        param_str = urllib.parse.urlencode(params)
        full_url = f"{base_url}{self.CHAT_BUYER_LINK_API}?{param_str}"

        try:
            headers = self._build_headers(base_url)

            response = self.browser_request.get(
                url=full_url,
                headers=headers,
                timeout=30
            )

            if response.ok:
                data = response.json()

                if data and data.get('code') == 0:
                    order_info = data.get('data', {}).get('orderIdToContactLinkInfo', {})
                    contact_link = order_info.get(main_order_id, {})
                    pigeon_uid = contact_link.get('pigeonUid')

                    return {
                        'pigeonUid': pigeon_uid,
                        'urlPc': contact_link.get('urlPc', '')
                    }
                else:
                    code = data.get('code') if data else -1
                    msg = data.get('message', 'unknown') if data else 'empty response'
                    raise ApiError('get_buyer_chat_link', code, msg)
            else:
                raise ApiError('get_buyer_chat_link', response.status_code, f"HTTP error: {response.status_code}")

        except ApiError:
            raise
        except Exception as e:
            logger.error(f"获取买家聊天链接异常: {e}")
            raise ApiError('get_buyer_chat_link', -1, str(e))

    def parse_address(self, contact_info: Dict) -> Dict[str, Any]:
        """
        解析买家地址信息

        Args:
            contact_info: 买家联系信息

        Returns:
            解析后的地址信息
        """
        result = {
            'full_address': '',
            'province': '',
            'city': '',
            'district': '',
            'village': '',
            'street': '',
            'zipcode': ''
        }

        if not contact_info:
            return result

        plain_text = contact_info.get('plain_text_address', {})
        if not plain_text:
            return result

        # 解析 items
        items = plain_text.get('items', [])
        for item in items:
            key = item.get('key', '')
            value = item.get('value', '')
            if key == 'address':
                result['street'] = value
            elif key == 'address_detail':
                result['full_address'] = value
            elif key == 'house_number':
                result['house_number'] = value
            elif key == 'zipcode':
                result['zipcode'] = value

        # 解析 districts
        districts = plain_text.get('districts', [])
        if len(districts) >= 1:
            result['province'] = districts[0].get('name', '')
        if len(districts) >= 2:
            result['city'] = districts[1].get('name', '')
        if len(districts) >= 3:
            result['district'] = districts[2].get('name', '')
        if len(districts) >= 4:
            result['village'] = districts[3].get('name', '')

        # 拼接完整地址
        parts = []
        if result['village']:
            parts.append(result['village'])
        if result['district']:
            parts.append(result['district'])
        if result['city']:
            parts.append(result['city'])
        if result['province']:
            parts.append(result['province'])
        if result['street']:
            parts.append(result['street'])

        result['full_address'] = ', '.join(parts)

        return result

    # ==================== 聊天工作台订单 API ====================

    def get_buyer_orders(self, base_url: str, oec_uid: str,
                         offset: int = 0, count: int = 20) -> Optional[Dict]:
        """
        获取买家的历史订单列表

        Args:
            base_url: 基础 URL
            oec_uid: 买家的 OEC 用户 ID
            offset: 分页偏移量
            count: 每页数量

        Returns:
            买家订单列表或 None
        """
        # 获取认证信息用于构建 URL 参数
        auth = self.auth_info
        oec_seller_id = auth.get('oec_seller_id') or auth.get('seller_id', '')
        seller_id = auth.get('seller_id') or oec_seller_id

        # 构建 URL 参数
        params = {
            'im_version_code': '1523',
            'aid': '4068',
            'PIGEON_BIZ_TYPE': '1',
            'oec_seller_id': str(oec_seller_id),
            }
        param_str = '&'.join(f"{k}={v}" for k, v in params.items())
        full_url = f"{base_url}{self.WORKBENCH_DATA_API}?{param_str}"

        request_body = {
            "order_workbench_query": {
                "seller_order_req": {
                    "without_overview": True,
                    "pagination_type": 0,
                    "sort_info": "6",  # 按时间倒序
                    "count": count,
                    "offset": offset,
                    "search_condition": {
                        "condition_list": {
                            "search_tab": {"value": ["0"]},  # 全部订单
                            "buyer_user_id": {"value": [oec_uid]}
                        }
                    }
                }
            }
        }
        try:
            response = self.browser_request.post(
                url=full_url,
                json_data=request_body,
                headers=self._build_headers(base_url),
                timeout=30
            )

            if response.ok:
                data = response.json()
                if data and data.get('code') == 0:
                    return data.get('data', {})
                else:
                    code = data.get('code') if data else -1
                    msg = data.get('message', 'unknown') if data else 'empty response'
                    raise ApiError('get_buyer_orders', code, msg)
            else:
                raise ApiError('get_buyer_orders', response.status_code, f"HTTP error: {response.status_code}")

        except Exception as e:
            logger.error(f"获取买家历史订单异常: {e}")
            raise

    def get_buyer_all_orders(self, base_url: str, oec_uid: str,
                             max_count: int = 100) -> List[Dict]:
        """
        获取买家的所有历史订单

        Args:
            base_url: 基础 URL
            oec_uid: 买家的 OEC 用户 ID
            max_count: 最大获取数量

        Returns:
            买家订单列表
        """
        all_orders = []
        offset = 0

        while len(all_orders) < max_count:
            try:
                data = self.get_buyer_orders(
                    base_url=base_url,
                    oec_uid=oec_uid,
                    offset=offset,
                    count=20
                )
                if data:
                    workbench_data = data.get('order_workbench_data', {})
                    seller_data = workbench_data.get('seller_order_data', {})
                    main_orders = seller_data.get('main_orders', [])
                    all_orders.extend(main_orders)

                    if len(main_orders) < 20:
                        break

                    offset += len(main_orders)
                else:
                    break

            except Exception as e:
                logger.warning(f"获取买家订单异常: {e}")
                break

            time.sleep(0.3)

        return all_orders

    def create_conversation(self, base_url: str, im_buyer_id: str) -> Optional[str]:
        """
        创建与买家的对话，获取 imcloud_conversation_id

        Args:
            base_url: 基础 URL
            im_buyer_id: 买家的 IM 用户 ID

        Returns:
            imcloud_conversation_id 对话 ID，失败返回 None
        """

        # 获取认证信息用于构建 URL 参数
        auth = self.auth_info
        oec_seller_id = auth.get('oec_seller_id') or auth.get('seller_id', '')
        region = self.get_region_code()

        # 构建 URL 参数
        params = {
            'PIGEON_BIZ_TYPE': '1',
            'oec_region': region,
            'aid': '4068',
            'oec_seller_id': str(oec_seller_id)
        }
        param_str = '&'.join(f"{k}={v}" for k, v in params.items())
        full_url = f"{base_url}{self.CREATE_CONVERSATION_API}?{param_str}"

        request_body = {
            "im_buyer_id": str(im_buyer_id)
        }

        try:
            headers = self._build_headers(base_url)
            response = self.browser_request.post(
                url=full_url,
                json_data=request_body,
                headers=headers,
                timeout=30
            )

            if response.ok:
                data = response.json()

                if data and data.get('code') == 0:
                    conversation_id = data.get('data', {}).get('imcloud_conversation_id')
                    return conversation_id
                else:
                    code = data.get('code') if data else -1
                    msg = data.get('message', 'unknown') if data else 'empty response'
                    raise ApiError('create_conversation', code, msg)
            else:
                raise ApiError('create_conversation', response.status_code, f"HTTP error: {response.status_code}")

        except ApiError:
            raise
        except Exception as e:
            logger.error(f"创建对话异常: {e}")
            raise ApiError('create_conversation', -1, str(e))

    def get_conversation_oec_uid(self, base_url: str, conversation_id: str) -> Optional[str]:
        """
        通过对话 ID 获取 oec_uid（买家的 OEC 用户 ID）

        Args:
            base_url: 基础 URL
            conversation_id: imcloud_conversation_id 对话 ID

        Returns:
            oec_uid 买家 OEC 用户 ID，失败返回 None
        """

        # 获取认证信息用于构建 URL 参数
        auth = self.auth_info
        oec_seller_id = auth.get('oec_seller_id') or auth.get('seller_id', '')
        region = self.get_region_code()

        # 构建 URL 参数
        params = {
            'PIGEON_BIZ_TYPE': '1',
            'oec_region': region,
            'aid': '4068',
            'oec_seller_id': str(oec_seller_id)
        }
        param_str = '&'.join(f"{k}={v}" for k, v in params.items())
        full_url = f"{base_url}/api/v1/shop_im/shop/user/mget_info_v2?{param_str}"

        request_body = {
            "imcloud_conversation_ids": [str(conversation_id)]
        }

        try:
            headers = self._build_headers(base_url)
            response = self.browser_request.post(
                url=full_url,
                json_data=request_body,
                headers=headers,
                timeout=30
            )

            if response.ok:
                data = response.json()

                if data and data.get('code') == 0:
                    user_info = data.get('data', {}).get('user_info_map', {}).get(str(conversation_id), {})
                    oec_uid = user_info.get('oec_uid')
                    return oec_uid
                else:
                    code = data.get('code') if data else -1
                    msg = data.get('message', 'unknown') if data else 'empty response'
                    raise ApiError('get_conversation_oec_uid', code, msg)
            else:
                raise ApiError('get_conversation_oec_uid', response.status_code, f"HTTP error: {response.status_code}")

        except ApiError:
            raise
        except Exception as e:
            logger.error(f"获取对话 oec_uid 异常: {e}")
            raise ApiError('get_conversation_oec_uid', -1, str(e))

    def im_buyer_id_to_oec_uid(self, base_url: str, im_buyer_id: str) -> Optional[str]:
        """
        将 IM 买家 ID 转换为 OEC UID

        Args:
            base_url: 基础 URL
            im_buyer_id: IM 买家 ID

        Returns:
            oec_uid 或 None
        """
        try:
            conversation_id = self.create_conversation(base_url, im_buyer_id)
            if conversation_id:
                return self.get_conversation_oec_uid(base_url, conversation_id)
            return None
        except Exception as e:
            logger.error(f"[TokopediaAPI] 转换 im_buyer_id={im_buyer_id} 到 oec_uid 失败: {e}")
            return None

    async def im_buyer_ids_to_oec_uids_batch(self, base_url: str, im_buyer_ids: List[str], max_concurrent: int = 10) -> Dict[str, str]:
        """
        批量将 IM 买家 ID 转换为 OEC UID

        Args:
            base_url: 基础 URL
            im_buyer_ids: IM 买家 ID 列表
            max_concurrent: 最大并发数

        Returns:
            im_buyer_id -> oec_uid 的映射
        """
        result = {}
        semaphore = asyncio.Semaphore(max_concurrent)

        async def convert_single(im_buyer_id: str):
            async with semaphore:
                try:
                    await asyncio.sleep(0.1)
                    # 在事件循环中调用同步方法
                    loop = asyncio.get_event_loop()
                    oec_uid = await loop.run_in_executor(
                        None,
                        self.im_buyer_id_to_oec_uid,
                        base_url,
                        im_buyer_id
                    )
                    return im_buyer_id, oec_uid
                except Exception as e:
                    logger.warning(f"[Async] 转换 IM 买家 ID {im_buyer_id} 到 OEC UID 失败: {e}")
                    return im_buyer_id, None

        tasks = [convert_single(bid) for bid in im_buyer_ids if bid]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, tuple) and len(r) == 2:
                im_buyer_id, oec_uid = r
                if oec_uid:
                    result[im_buyer_id] = oec_uid

        logger.info(f"[TokopediaAPI] 批量转换完成: {len(result)}/{len(im_buyer_ids)} 个成功")
        return result

    # ==================== 异步请求方法 ====================

    async def get_order_list_async(self, base_url: str,
                                 order_status: List[str] = None,
                                 search_tab: List[str] = None,
                                 search_cursor: str = "",
                                 offset: int = 0,
                                 count: int = 20,
                                 sort_info: str = "11") -> Optional[Dict]:
        """
        异步获取订单列表

        Args:
            base_url: 基础 URL
            order_status: 订单状态列表
            search_tab: 标签页
            search_cursor: 分页游标
            offset: 分页偏移量
            count: 每页数量
            sort_info: 排序标识

        Returns:
            订单列表数据或 None
        """
        if order_status is None:
            order_status = ["1"]
        if search_tab is None:
            search_tab = ["101"]

        auth = self.auth_info
        oec_seller_id = auth.get('oec_seller_id') or auth.get('seller_id', '')
        seller_id = auth.get('seller_id') or oec_seller_id

        params = {
            'aid': '4068',
            'locale': 'en',
            'oec_seller_id': str(oec_seller_id),
            'seller_id': str(seller_id)
        }
        param_str = '&'.join(f"{k}={v}" for k, v in params.items())
        full_url = f"{base_url}{self.ORDER_LIST_API}?{param_str}"

        request_body = {
            "search_condition": {
                "condition_list": {
                    "order_status": {"value": order_status},
                    "search_tab": {"value": search_tab}
                }
            },
            "offset": offset,
            "count": count,
            "sort_info": sort_info,
            "search_cursor": search_cursor,
            "pagination_type": 0
        }

        try:
            headers = self._build_headers(base_url)

            cookies = auth.get('cookies', [])
            async_request = AsyncBatchRequest(cookies=cookies, auth_info=auth, platform='tokopedia')

            response = await async_request.post(
                url=full_url,
                json_data=request_body,
                headers=headers,
                timeout=30
            )

            if response.ok:
                data = response.json()
                if data and data.get('code') == 0:
                    return data.get('data', {})
                else:
                    code = data.get('code') if data else -1
                    msg = data.get('message', 'unknown') if data else 'empty response'
                    raise ApiError('get_order_list_async', code, msg)
            else:
                raise ApiError('get_order_list_async', response.status_code, f"HTTP error: {response.status_code}")

        except ApiError:
            raise
        except Exception as e:
            logger.error(f"异步获取订单列表异常: {e}")
            raise ApiError('get_order_list_async', -1, str(e))

    async def get_all_orders_async(self, base_url: str, max_pages: int = 100) -> List[Dict]:
        """
        异步获取所有订单（分页）

        Args:
            base_url: 基础 URL
            max_pages: 最大页数

        Returns:
            订单列表
        """
        all_orders = []
        offset = 0
        page = 1

        logger.info(f"[TokopediaAPI] 开始异步分页获取订单，max_pages={max_pages}")

        while page <= max_pages:
            logger.info(f"[TokopediaAPI] ===== 正在获取第 {page} 页... (offset={offset})")

            try:
                data = await self.get_order_list_async(
                    base_url=base_url,
                    offset=offset,
                    count=20
                )

                if data:
                    main_orders = data.get('main_orders', [])
                    all_orders.extend(main_orders)

                    total_count = data.get('total_count', 0)
                    has_more = data.get('has_more', False)

                    logger.info(f"[TokopediaAPI] 第 {page} 页获取成功，"
                               f"本页 {len(main_orders)} 条，总计 {total_count} 条")

                    if not has_more:
                        logger.info(f"[TokopediaAPI] 无下一页，停止获取")
                        break
                    if len(main_orders) < 20:
                        logger.info(f"[TokopediaAPI] 本页数据不足一页，停止获取")
                        break

                    offset += len(main_orders)
                else:
                    logger.warning(f"[TokopediaAPI] API 返回为空，继续尝试...")

            except ApiError:
                raise
            except Exception as e:
                logger.warning(f"[TokopediaAPI] 获取订单异常: {e}, 继续尝试...")

            page += 1
            await asyncio.sleep(0.5)

        logger.info(f"[TokopediaAPI] 订单列表获取完成: {len(all_orders)} 条")
        return all_orders

    async def get_buyer_contact_info_async(self, base_url: str, main_order_id: str) -> Optional[Dict]:
        """
        异步获取买家联系信息

        Args:
            base_url: 基础 URL
            main_order_id: 主订单 ID

        Returns:
            买家联系信息或 None
        """
        logger.info(f"[TokopediaAPI] get_buyer_contact_info_async 请求: main_order_id={main_order_id}")

        auth = self.auth_info
        oec_seller_id = auth.get('oec_seller_id') or auth.get('seller_id', '')
        seller_id = auth.get('seller_id') or oec_seller_id

        params = {
            'aid': '4068',
            'locale': 'en',
            'oec_seller_id': str(oec_seller_id),
            'seller_id': str(seller_id)
        }
        param_str = '&'.join(f"{k}={v}" for k, v in params.items())
        full_url = f"{base_url}{self.BUYER_CONTACT_API}?{param_str}"

        request_body = {
            "main_order_id": main_order_id,
            "contact_info_type": 1
        }

        try:
            headers = self._build_headers(base_url)

            cookies = auth.get('cookies', [])
            async_request = AsyncBatchRequest(cookies=cookies, auth_info=auth, platform='tokopedia')

            response = await async_request.post(
                url=full_url,
                json_data=request_body,
                headers=headers,
                timeout=30
            )

            if response.ok:
                data = response.json()

                if data and data.get('code') == 0:
                    return data.get('data', {})
                else:
                    code = data.get('code') if data else -1
                    msg = data.get('message', 'unknown') if data else 'empty response'
                    raise ApiError('get_buyer_contact_info_async', code, msg)
            else:
                raise ApiError('get_buyer_contact_info_async', response.status_code, f"HTTP error: {response.status_code}")

        except ApiError:
            raise
        except Exception as e:
            logger.error(f"异步获取买家联系信息异常: {e}")
            raise ApiError('get_buyer_contact_info_async', -1, str(e))

    async def get_buyer_chat_link_async(self, base_url: str, main_order_id: str) -> Optional[Dict]:
        """
        异步获取买家聊天链接

        Args:
            base_url: 基础 URL
            main_order_id: 主订单 ID

        Returns:
            包含 pigeonUid 的字典或 None
        """

        auth = self.auth_info
        oec_seller_id = auth.get('oec_seller_id') or auth.get('seller_id', '')
        seller_id = auth.get('seller_id') or oec_seller_id

        params = {
            'orderType': '0',
            'orderIds': main_order_id,
            'oec_seller_id': str(oec_seller_id),
            'seller_id': str(seller_id),
            'aid': '4068',
            'locale': 'en'
        }

        param_str = urllib.parse.urlencode(params)
        full_url = f"{base_url}{self.CHAT_BUYER_LINK_API}?{param_str}"

        try:
            headers = self._build_headers(base_url)

            cookies = auth.get('cookies', [])
            async_request = AsyncBatchRequest(cookies=cookies, auth_info=auth, platform='tokopedia')

            response = await async_request.get(
                url=full_url,
                headers=headers,
                timeout=30
            )

            if response.ok:
                data = response.json()

                if data and data.get('code') == 0:
                    order_info = data.get('data', {}).get('orderIdToContactLinkInfo', {})
                    contact_link = order_info.get(main_order_id, {})
                    pigeon_uid = contact_link.get('pigeonUid')

                    logger.info(f"[TokopediaAPI] main_order_id={main_order_id} -> pigeonUid={pigeon_uid}")

                    return {
                        'pigeonUid': pigeon_uid,
                        'urlPc': contact_link.get('urlPc', '')
                    }
                else:
                    code = data.get('code') if data else -1
                    msg = data.get('message', 'unknown') if data else 'empty response'
                    raise ApiError('get_buyer_chat_link_async', code, msg)
            else:
                raise ApiError('get_buyer_chat_link_async', response.status_code, f"HTTP error: {response.status_code}")

        except ApiError:
            raise
        except Exception as e:
            logger.error(f"异步获取买家聊天链接异常: {e}")
            raise ApiError('get_buyer_chat_link_async', -1, str(e))

    async def get_buyer_orders_async(self, base_url: str, oec_uid: str,
                                   offset: int = 0, count: int = 20) -> Optional[Dict]:
        """
        异步获取买家的历史订单列表

        Args:
            base_url: 基础 URL
            oec_uid: 买家的 OEC 用户 ID
            offset: 分页偏移量
            count: 每页数量

        Returns:
            买家订单列表或 None
        """
        auth = self.auth_info
        oec_seller_id = auth.get('oec_seller_id') or auth.get('seller_id', '')
        seller_id = auth.get('seller_id') or oec_seller_id

        # 构建 URL 参数
        params = {
            'aid': '4068',
            'locale': 'en',
            'oec_seller_id': str(oec_seller_id),
            # 'seller_id': str(seller_id)
        }
        param_str = '&'.join(f"{k}={v}" for k, v in params.items())
        full_url = f"{base_url}{self.WORKBENCH_DATA_API}?{param_str}"

        request_body = {
            "order_workbench_query": {
                "seller_order_req": {
                    "without_overview": True,
                    "pagination_type": 0,
                    "sort_info": "6",
                    "count": count,
                    "offset": offset,
                    "search_condition": {
                        "condition_list": {
                            "search_tab": {"value": ["0"]},
                            "oec_uid": {"value": [oec_uid]}
                        }
                    }
                }
            }
        }

        try:
            cookies = auth.get('cookies', [])
            async_request = AsyncBatchRequest(cookies=cookies, auth_info=auth, platform='tokopedia')

            response = await async_request.post(
                url=full_url,
                json_data=request_body,
                headers=self._build_headers(base_url),
                timeout=30
            )

            if response.ok:
                data = response.json()

                if data and data.get('code') == 0:
                    return data.get('data', {})
                else:
                    code = data.get('code') if data else -1
                    msg = data.get('message', 'unknown') if data else 'empty response'
                    raise ApiError('get_buyer_orders_async', code, msg)
            else:
                raise ApiError('get_buyer_orders_async', response.status_code, f"HTTP error: {response.status_code}")

        except Exception as e:
            logger.error(f"异步获取买家历史订单异常: {e}")
            raise ApiError('get_buyer_orders_async', -1, str(e))

    async def get_buyer_all_orders_async(self, base_url: str, oec_uid: str,
                                       max_count: int = 100) -> List[Dict]:
        """
        异步获取买家的所有历史订单

        Args:
            base_url: 基础 URL
            oec_uid: 买家的 OEC 用户 ID
            max_count: 最大获取数量

        Returns:
            买家订单列表
        """
        all_orders = []
        offset = 0

        while len(all_orders) < max_count:
            try:
                data = await self.get_buyer_orders_async(
                    base_url=base_url,
                    oec_uid=oec_uid,
                    offset=offset,
                    count=20
                )

                if data:
                    workbench_data = data.get('order_workbench_data', {})
                    seller_data = workbench_data.get('seller_order_data', {})
                    main_orders = seller_data.get('main_orders', [])
                    all_orders.extend(main_orders)

                    if len(main_orders) < 20:
                        break

                    offset += len(main_orders)
                else:
                    break

            except Exception as e:
                logger.warning(f"获取买家订单异常: {e}")
                break

            await asyncio.sleep(0.3)

        return all_orders

    # ==================== 异步并发版本 ====================

    async def get_buyer_contact_info_batch(self, base_url: str,
                                           order_ids: List[str],
                                           max_concurrent: int = 10) -> Dict[str, Dict]:
        """
        异步批量获取买家联系信息

        Args:
            base_url: 基础 URL
            order_ids: 订单 ID 列表
            max_concurrent: 最大并发数

        Returns:
            order_id -> 联系信息 的映射
        """
        result = {}
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_single(order_id: str):
            async with semaphore:
                try:
                    await asyncio.sleep(0.1)  # 避免请求过快
                    info = self.get_buyer_contact_info(base_url, order_id)
                    return order_id, info
                except Exception as e:
                    logger.warning(f"[Async] 获取订单 {order_id} 联系信息失败: {e}")
                    return order_id, None

        tasks = [fetch_single(oid) for oid in order_ids if oid]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, tuple):
                order_id, info = r
                if info:
                    result[order_id] = info

        logger.info(f"[Async] 批量获取买家联系信息完成: {len(result)}/{len(order_ids)}")
        return result

    async def get_buyer_orders_batch(self, base_url: str,
                                     buyer_ids: List[str],
                                     max_concurrent: int = 10) -> Dict[str, List[Dict]]:
        """
        异步批量获取买家历史订单

        Args:
            base_url: 基础 URL
            buyer_ids: OEC 用户 ID 列表（实际应为 oec_uid）
            max_concurrent: 最大并发数

        Returns:
            oec_uid -> 订单列表 的映射
        """
        result = {}
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_single(buyer_id: str):
            async with semaphore:
                try:
                    await asyncio.sleep(0.1)
                    orders = self.get_buyer_all_orders(base_url, buyer_id, max_count=50)
                    return buyer_id, orders
                except Exception as e:
                    logger.warning(f"[Async] 获取买家 {buyer_id} 历史订单失败: {e}")
                    return buyer_id, []

        tasks = [fetch_single(bid) for bid in buyer_ids if bid]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, tuple):
                buyer_id, orders = r
                result[buyer_id] = orders

        logger.info(f"[Async] 批量获取买家历史订单完成: {len(result)}/{len(buyer_ids)}")
        return result

    async def get_buyer_chat_link_batch(self, base_url: str,
                                         order_ids: List[str],
                                         max_concurrent: int = 10) -> Dict[str, Dict]:
        """
        异步批量获取买家聊天链接（包含 pigeonUid）

        Args:
            base_url: 基础 URL
            order_ids: 订单 ID 列表
            max_concurrent: 最大并发数

        Returns:
            order_id -> {pigeonUid, urlPc} 的映射
        """
        result = {}
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_single(order_id: str):
            async with semaphore:
                try:
                    await asyncio.sleep(0.1)
                    info = self.get_buyer_chat_link(base_url, order_id)
                    return order_id, info
                except Exception as e:
                    logger.warning(f"[Async] 获取订单 {order_id} 聊天链接失败: {e}")
                    return order_id, None

        tasks = [fetch_single(oid) for oid in order_ids if oid]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, tuple):
                order_id, info = r
                if info and info.get('pigeonUid'):
                    result[order_id] = info

        logger.info(f"[Async] 批量获取买家聊天链接完成: {len(result)}/{len(order_ids)}")
        return result