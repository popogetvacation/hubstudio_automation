"""
Shopee API 封装模块
提供订单相关的 API 调用方法
"""
import asyncio
import json
import random
import time
import urllib.parse
from typing import Dict, Any, Optional, List

from ..network.browser_request import BrowserRequest
from ..utils.logger import default_logger as logger


class ShopeeApiError(Exception):
    """Shopee API 调用失败异常"""
    def __init__(self, method: str, code: int, message: str):
        self.method = method
        self.code = code
        self.message = message
        super().__init__(f"[{method}] API返回错误: code={code}, message={message}")


class ShopeeAPI:
    """
    Shopee API 封装类

    提供订单列表、订单详情、买家信息、聊天消息等 API 调用方法
    """

    # API 路径
    ORDER_LIST_API = "/api/v3/order/search_order_list_index"
    ORDER_CARD_LIST_API = "/api/v3/order/get_order_list_card_list"
    USER_INFO_API = "/webchat/api/v1.2/mini/users"
    CHAT_MESSAGES_API = "/webchat/api/v1.2/mini/conversations"
    CONVERSATIONS_API = "/webchat/api/v1.2/mini/conversations"

    # 国家域名映射
    DOMAIN_MAP = {
        'id': 'co.id',
        'th': 'co.th',
        'vn': 'vn',
        'ph': 'ph',
        'sg': 'sg',
        'tw': 'tw',
        'my': 'com.my',
        'br': 'com.br',
        'mx': 'com.mx',
    }

    def __init__(self, driver, browser_request: BrowserRequest = None):
        """
        初始化 Shopee API

        Args:
            driver: Selenium 驱动
            browser_request: 网络请求对象，如果为 None 会自动创建
        """
        self._driver = driver
        self._browser_request = browser_request
        self._auth_info = None

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
            env_name: 环境名称，用于确定国家域名

        Returns:
            基础 URL
        """
        if env_name:
            domain_suffix = self._get_domain_suffix(env_name)
            return f"https://seller.shopee.{domain_suffix}"
        # 尝试从当前 URL 提取
        current_url = self._driver.get_current_url()
        if 'seller.shopee' in current_url:
            # 从当前 URL 提取域名
            parts = current_url.split('seller.shopee')
            if len(parts) > 1:
                return f"https://seller.shopee{parts[1].split('/')[0]}"
        return "https://seller.shopee.com.my"  # 默认

    # ==================== 认证相关 ====================

    def _extract_auth_info(self) -> Dict:
        """从 Cookies 中提取认证信息"""
        cookies = self._driver.get_cookies()

        auth_info = {
            'csrf_token': '',
            'spc_cds_chat': '',
            'spc_ec': '',
            'bearer_token': '',
            'chat_bearer_token': '',
            'shop_id': None,
            'region': 'MY'
        }

        # 从 cookies 提取
        for cookie in cookies:
            name = cookie.get('name', '')
            value = cookie.get('value', '')

            if name == 'csrftoken':
                auth_info['csrf_token'] = value
            elif name == 'SPC_CDS_CHAT':
                auth_info['spc_cds_chat'] = value
            elif name == 'SPC_EC':
                auth_info['spc_ec'] = value

        # 尝试从 localStorage 获取 bearer token
        try:
            bearer_token = self._driver.execute_script("""
                var token = null;
                token = localStorage.getItem('bearerToken');
                if (token) return token;
                token = localStorage.getItem('token');
                if (token) return token;

                if (window.__INITIAL_STATE__) {
                    if (window.__INITIAL_STATE__.user && window.__INITIAL_STATE__.user.token) {
                        return window.__INITIAL_STATE__.user.token;
                    }
                }
                return null;
            """)
            if bearer_token:
                auth_info['bearer_token'] = bearer_token
        except Exception:
            pass

        # 获取聊天专用的 Bearer Token
        auth_info['chat_bearer_token'] = self._get_chat_bearer_token()

        # 尝试从页面获取 region 和 shop_id
        try:
            page_info = self._driver.execute_script("""
                var info = {};
                if (window.__INITIAL_STATE__ && window.__INITIAL_STATE__.user) {
                    info.region = window.__INITIAL_STATE__.user.region;
                    info.shop_id = window.__INITIAL_STATE__.user.shopId;
                }
                return info;
            """)
            if page_info:
                if page_info.get('region'):
                    auth_info['region'] = page_info['region']
                if page_info.get('shop_id'):
                    auth_info['shop_id'] = page_info['shop_id']
        except Exception:
            pass

        return auth_info

    def _get_chat_bearer_token(self) -> str:
        """获取聊天专用的 Bearer Token"""
        try:
            mini_session = self._driver.execute_script("""
                var miniSession = localStorage.getItem('mini-session');
                if (miniSession) {
                    try {
                        var data = JSON.parse(miniSession);
                        return data.token || null;
                    } catch(e) {}
                }
                return null;
            """)

            if mini_session:
                return mini_session

            # 尝试导航到聊天页面
            current_url = self._driver.get_current_url()
            if '/portal/chat' not in current_url:
                base_url = self.get_base_url()
                self._driver.goto(f"{base_url}/portal/chat/")
                time.sleep(3)

            mini_session = self._driver.execute_script("""
                var miniSession = localStorage.getItem('mini-session');
                if (miniSession) {
                    try {
                        var data = JSON.parse(miniSession);
                        return data.token || null;
                    } catch(e) {}
                }
                return null;
            """)

            return mini_session or ''
        except Exception as e:
            logger.warning(f"获取聊天 Token 失败: {e}")
            return ''

    def _get_domain_suffix(self, env_name: str) -> str:
        """获取域名后缀"""
        if env_name and len(env_name) >= 2:
            country_code = env_name[:2].lower()
            return self.DOMAIN_MAP.get(country_code, f'com.{country_code}')
        return 'com.my'

    def _build_headers(self, base_url: str) -> Dict[str, str]:
        """构建请求头"""
        return {
            'accept': 'application/json, text/plain, */*',
            'content-type': 'application/json;charset=UTF-8',
            'origin': base_url,
            'referer': f"{base_url}/portal/sale/order"
        }

    # ==================== 订单 API ====================

    async def get_order_list_async(self, base_url: str,
                                  order_list_tab: int = 300,
                                  page_number: int = 1,
                                  page_sentinel: str = None,
                                  page_size: int = 40,
                                  sort_type: int = 3,
                                  ascending: bool = False) -> Optional[Dict]:
        """
        异步获取订单列表（使用 AsyncBatchRequest）

        Args:
            base_url: 基础 URL
            order_list_tab: 订单标签页类型 (300=待发货, 1000=全部)
            page_number: 页码
            page_sentinel: 分页标记
            page_size: 每页数量
            sort_type: 排序类型
            ascending: 是否升序

        Returns:
            订单列表数据或 None
        """
        from ..network.async_http import AsyncBatchRequest

        request_body = {
            "order_list_tab": order_list_tab,
            "entity_type": 0,
            "pagination": {
                "from_page_number": 1,
                "page_number": page_number,
                "page_size": min(page_size, 200)
            },
            "filter": {
                "fulfillment_type": 0,
                "is_drop_off": 0,
                "fulfillment_source": 0,
                "action_filter": 0,
                "order_to_ship_status": 1,
                "shipping_priority": 0
            },
            "sort": {
                "sort_type": sort_type,
                "ascending": ascending
            }
        }

        if page_sentinel:
            request_body["pagination"]["page_sentinel"] = page_sentinel
            logger.info(f"[get_order_list_async] 使用 page_sentinel 分页，page_number={page_number}")
        else:
            logger.info(f"[get_order_list_async] 无 page_sentinel，使用 page_number={page_number}")

        request_headers = self._build_headers(base_url)

        cookies = self._driver.get_cookies()
        auth_info = self.auth_info

        async_request = AsyncBatchRequest(cookies=cookies, auth_info=auth_info)

        try:
            response = await async_request.post(
                url=f"{base_url}{self.ORDER_LIST_API}",
                json_data=request_body,
                headers=request_headers,
                timeout=30
            )

            if response.ok:
                data = response.json()
                logger.info(f"[get_order_list_async] 响应状态: {response.status_code}")
                if data and data.get('code') == 0:
                    return data.get('data', {})
                else:
                    code = data.get('code') if data else -1
                    msg = data.get('message', 'unknown') if data else 'empty response'
                    raise ShopeeApiError('get_order_list_async', code, msg)
            else:
                raise ShopeeApiError('get_order_list_async', response.status_code, f"HTTP error: {response.status_code}")

        except Exception as e:
            logger.error(f"[get_order_list_async] 获取订单列表异常: {e}")
            raise
        finally:
            await async_request.close()

    def get_order_list(self, base_url: str,
                       order_list_tab: int = 300,
                       page_number: int = 1,
                       page_sentinel: str = None,
                       page_size: int = 40,
                       sort_type: int = 3,
                       ascending: bool = False) -> Optional[Dict]:
        """
        获取订单列表

        Args:
            base_url: 基础 URL
            order_list_tab: 订单标签页类型 (300=待发货, 1000=全部)
            page_number: 页码
            page_sentinel: 分页标记
            page_size: 每页数量
            sort_type: 排序类型
            ascending: 是否升序

        Returns:
            订单列表数据或 None
        """
        request_body = {
            "order_list_tab": order_list_tab,
            "entity_type": 0,
            "pagination": {
                "from_page_number": 1,
                "page_number": page_number,
                "page_size": min(page_size, 200)
            },
            "filter": {
                "fulfillment_type": 0,
                "is_drop_off": 0,
                "fulfillment_source": 0,
                "action_filter": 0,
                "order_to_ship_status": 1,
                "shipping_priority": 0
            },
            "sort": {
                "sort_type": sort_type,
                "ascending": ascending
            }
        }

        if page_sentinel:
            request_body["pagination"]["page_sentinel"] = page_sentinel
            # 有 page_sentinel 时，page_number 保持原值（不是 1）
            logger.info(f"[get_order_list] 使用 page_sentinel 分页，page_number={page_number}")
        else:
            logger.info(f"[get_order_list] 无 page_sentinel，使用 page_number={page_number}")

        # 调试：打印请求内容和请求头
        request_headers = self._build_headers(base_url)
        logger.info(f"[get_order_list] 请求头: {json.dumps(request_headers, ensure_ascii=False)}")
        logger.info(f"[get_order_list] 请求体: {json.dumps(request_body, ensure_ascii=False)}")

        # 额外调试：添加环境信息
        try:
            env_debug = {
                'current_url': self._driver.get_current_url(),
                'user_agent': self._driver.execute_script("return navigator.userAgent"),
                'cookie_count': len(self._driver.get_cookies()),
                'cookie_sample': self._driver.get_cookies()[:3]  # 记录前3个cookie
            }
            logger.info(f"[get_order_list] 环境信息: {json.dumps(env_debug, ensure_ascii=False)}")
        except Exception as e:
            logger.warning(f"[get_order_list] 获取环境信息失败: {e}")

        try:
            # 启动 API 捕获以便在出错时查看详细信息（使用 CDP Fetch 实时拦截）
            self.browser_request.start_api_capture(url_filter="search_order_list_index")

            response = self.browser_request.post(
                url=f"{base_url}{self.ORDER_LIST_API}",
                json_data=request_body,
                headers=request_headers,
                timeout=30
            )

            if response.ok:
                data = response.json()
                # 调试：记录完整响应
                logger.info(f"[get_order_list] 响应状态: {response.status_code}, 响应数据: {json.dumps(data, ensure_ascii=False)[:500]}...")
                if data and data.get('code') == 0:
                    return data.get('data', {})
                else:
                    code = data.get('code') if data else -1
                    msg = data.get('message', 'unknown') if data else 'empty response'

                    # 捕获实际的请求细节
                    captured_apis = self.browser_request.get_captured_apis()
                    logger.error(f"[get_order_list] 错误时捕获的请求数量: {len(captured_apis)}")
                    if captured_apis:
                        for i, api_call in enumerate(captured_apis[:3]):  # 只记录前3个
                            logger.error(f"[get_order_list] 捕获请求 #{i+1}: {json.dumps(api_call, ensure_ascii=False)[:1000]}...")

                    raise ShopeeApiError('get_order_list', code, msg)
            else:
                # 捕获失败的请求
                captured_apis = self.browser_request.get_captured_apis()
                logger.error(f"[get_order_list] HTTP错误，捕获的请求数量: {len(captured_apis)}")
                if captured_apis:
                    logger.error(f"[get_order_list] 捕获请求: {json.dumps(captured_apis[0], ensure_ascii=False)[:1500] if captured_apis else 'none'}")
                raise ShopeeApiError('get_order_list', response.status_code, f"HTTP error: {response.status_code}")

        except Exception as e:
            logger.error(f"获取订单列表异常: {e}")
            raise
        finally:
            # 确保停止 API 捕获
            try:
                captured = self.browser_request.stop_api_capture()
                logger.debug(f"[get_order_list] 停止API捕获，共捕获: {len(captured)} 个调用")
            except Exception:
                pass


    def _get_order_card_list(self, base_url: str,
                             package_params: List[Dict],
                             order_list_tab: int = 300) -> List[Dict]:
        """调用订单卡片列表 API"""
        # 根据 order_list_tab 构建不同的请求体
        if order_list_tab == 300:
            # 待发货：使用 package_param_list
            request_body = {
                "order_list_tab": order_list_tab,
                "need_count_down_desc": False,
                "package_param_list": package_params
            }
        else:
            # 全部订单：使用 order_param_list (order_id, shop_id, region_id)
            order_params = []
            for p in package_params:
                if 'order_id' in p:
                    order_params.append({
                        'order_id': p['order_id'],
                        'shop_id': p.get('shop_id'),
                        'region_id': p.get('region_id', 'MY')
                    })

            if not order_params:
                logger.warning("没有有效的订单参数")
                return []

            request_body = {
                "order_list_tab": order_list_tab,
                "need_count_down_desc": False,
                "order_param_list": order_params
            }

        response = self.browser_request.post(
            url=f"{base_url}{self.ORDER_CARD_LIST_API}",
            json_data=request_body,
            headers=self._build_headers(base_url),
            timeout=60
        )

        if response.ok:
            data = response.json()
            if data and data.get('code') == 0:
                return data.get('data', {}).get('card_list', [])

        return []

    def get_order_card_list(self, base_url: str,
                           package_params: List[Dict],
                           order_list_tab: int = 300) -> List[Dict]:
        """
        获取订单卡片列表（支持全部订单和待发货订单的不同处理逻辑）

        Args:
            base_url: 基础 URL
            package_params: 订单参数列表
            order_list_tab: 订单标签页类型 (300=待发货, 100=全部)

        Returns:
            处理后的订单卡片列表
        """
        card_list = self._get_order_card_list(base_url, package_params, order_list_tab)

        if not card_list:
            return []

        if order_list_tab == 300:
            return card_list
        elif order_list_tab == 100:
            # 全部订单：需要解析 package_level_order_card 或 order_card 结构
            processed_list = []
            skipped_count = 0
            no_package_list_count = 0

            for card in card_list:
                # 尝试获取 order_card 或 package_level_order_card
                order_card = card.get('package_level_order_card')
                if not order_card:
                    order_card = card.get('order_card')

                if not order_card:
                    skipped_count += 1
                    continue

                # 如果没有 package_list，尝试从 package_ext_info_list 构造
                package_list = order_card.get('package_list', [])
                if not package_list:
                    package_ext_info_list = order_card.get('package_ext_info_list', [])
                    if package_ext_info_list:
                        package_list = []
                        for pkg_ext in package_ext_info_list:
                            package_list.append({
                                'item_info_group': order_card.get('item_info_group', {}),
                                'payment_info': order_card.get('payment_info', {}),
                                'status_info': order_card.get('status_info', {}),
                                'fulfilment_info': order_card.get('fulfilment_info', {}),
                                'package_ext_info': pkg_ext
                            })
                        order_card['package_list'] = package_list

                if not package_list:
                    no_package_list_count += 1
                    order_sn = order_card.get('card_header', {}).get('order_sn', 'unknown')
                    logger.warning(f"订单 {order_sn} 没有 package_list")
                    continue

                processed_card = self._convert_order_card_to_package_card(order_card)
                if processed_card:
                    processed_list.append(processed_card)

            if skipped_count > 0:
                logger.warning(f"本批跳过 {skipped_count} 个订单（无 order_card）")
            if no_package_list_count > 0:
                logger.warning(f"本批跳过 {no_package_list_count} 个订单（无 package_list）")

            return processed_list

        return card_list

    def _convert_order_card_to_package_card(self, order_card: Dict) -> Optional[Dict]:
        """将订单级卡片转换为统一格式"""
        card_header = order_card.get('card_header', {})
        order_ext_info = order_card.get('order_ext_info', {})
        package_list = order_card.get('package_list', [])

        if not package_list:
            order_sn = card_header.get('order_sn', 'unknown')
            logger.warning(f"订单 {order_sn} 没有 package_list，跳过")
            return None

        first_package = package_list[0]

        converted = {
            'package_card': {
                'card_header': card_header,
                'item_info_group': first_package.get('item_info_group', {}),
                'payment_info': first_package.get('payment_info', {}),
                'status_info': first_package.get('status_info', {}),
                'fulfilment_info': first_package.get('fulfilment_info', {}),
                'action_info': order_card.get('action_info', {}),
                'order_ext_info': order_ext_info,
                'package_ext_info': first_package.get('package_ext_info', {}),
                'package_list': package_list,
                'order_card_action_info': order_card.get('action_info', {})
            }
        }

        return converted

    # ==================== 异步 API ====================

    async def get_order_card_list_async(self, base_url: str,
                                         package_params: List[Dict],
                                         order_list_tab: int = 100,
                                         batch_size: int = 10,
                                         max_concurrent: int = 20) -> List[Dict]:
        """
        异步批量获取订单卡片列表（并发版本）

        Args:
            base_url: 基础 URL
            package_params: 订单参数列表
            order_list_tab: 订单标签页类型 (300=待发货, 100=全部)
            batch_size: 每批次大小
            max_concurrent: 最大并发数

        Returns:
            处理后的订单卡片列表
        """
        from ..network.async_http import AsyncBatchRequest
        # 准备请求参数
        order_params = []
        for p in package_params:
            if 'order_id' in p:
                order_params.append({
                    'order_id': p['order_id'],
                    'shop_id': p.get('shop_id'),
                    'region_id': p.get('region_id', 'MY')
                })

        if not order_params:
            logger.warning("没有有效的订单参数")
            return []

        # 分批
        batches = [order_params[i:i + batch_size] for i in range(0, len(order_params), batch_size)]
        logger.info(f"[Async] 开始并发获取 {len(order_params)} 条订单详情，分为 {len(batches)} 批")

        # 获取 cookies 和 auth_info
        cookies = self._driver.get_cookies()
        auth_info = self.auth_info

        # 创建异步请求器
        async_request = AsyncBatchRequest(cookies=cookies, auth_info=auth_info)

        try:
            # 并发获取
            card_list = await async_request.post_batch(
                base_url=base_url,
                api_path=self.ORDER_CARD_LIST_API,
                batches=batches,
                request_key='order_param_list',
                max_concurrent=max_concurrent
            )

            # 处理响应数据（与同步版本相同的处理逻辑）
            if not card_list:
                return []

            if order_list_tab == 300:
                return card_list
            elif order_list_tab == 100:
                processed_list = []
                skipped_count = 0
                no_package_list_count = 0

                for card in card_list:
                    # 尝试获取 order_card 或 package_level_order_card（与同步版本一致）
                    order_card = card.get('package_level_order_card')
                    if not order_card:
                        order_card = card.get('order_card')

                    if not order_card:
                        skipped_count += 1
                        continue

                    # 如果没有 package_list，尝试从 package_ext_info_list 构造
                    package_list = order_card.get('package_list', [])
                    if not package_list:
                        package_ext_info_list = order_card.get('package_ext_info_list', [])
                        if package_ext_info_list:
                            package_list = []
                            for pkg_ext in package_ext_info_list:
                                package_list.append({
                                    'item_info_group': order_card.get('item_info_group', {}),
                                    'payment_info': order_card.get('payment_info', {}),
                                    'status_info': order_card.get('status_info', {}),
                                    'fulfilment_info': order_card.get('fulfilment_info', {}),
                                    'package_ext_info': pkg_ext
                                })
                            # 与同步版本一致，设置 package_list
                            order_card['package_list'] = package_list

                    if not package_list:
                        no_package_list_count += 1
                        order_sn = order_card.get('card_header', {}).get('order_sn', 'unknown')
                        logger.warning(f"订单 {order_sn} 没有 package_list")
                        continue

                    # 与同步版本一致，调用转换函数
                    processed_card = self._convert_order_card_to_package_card(order_card)
                    if processed_card:
                        processed_list.append(processed_card)

                if skipped_count > 0:
                    logger.warning(f"本批跳过 {skipped_count} 个订单（无 order_card）")
                if no_package_list_count > 0:
                    logger.warning(f"本批跳过 {no_package_list_count} 个订单（无 package_list）")

                return processed_list

            return []

        finally:
            await async_request.close()

    # ==================== 聊天 API ====================

    def get_conversation_map(self, base_url: str, shop_id: int, region: str = 'MY') -> Dict[int, int]:
        """获取对话列表，建立 user_id -> conversation_id 的映射"""
        user_to_conversation = {}

        auth = self.auth_info
        chat_bearer_token = auth.get('chat_bearer_token', '')
        csrf_token = auth.get('csrf_token', '')
        spc_cds_chat = auth.get('spc_cds_chat', '')

        if not chat_bearer_token:
            return user_to_conversation

        encoded_csrf = urllib.parse.quote(csrf_token, safe='')

        api_url = f"{base_url}/webchat/api/v1.2/mini/conversations"

        params = {
            'limit': 100,
            'offset': 0,
            'shop_id': shop_id,
            '_uid': f'0-{shop_id}',
            '_v': '9.1.7',
            'csrf_token': encoded_csrf,
            'SPC_CDS_CHAT': spc_cds_chat,
            'x-shop-region': region,
            '_api_source': 'sc'
        }

        headers = {
            'accept': 'application/json, text/plain, */*',
            'origin': base_url,
            'referer': f'{base_url}/portal/chat/',
            'x-shop-region': region,
            'Authorization': f'Bearer {chat_bearer_token}'
        }

        try:
            response = self.browser_request.get(
                url=api_url,
                params=params,
                headers=headers,
                timeout=30
            )

            if response.ok:
                data = response.json()
                conversations = []

                if isinstance(data, list):
                    conversations = data
                elif isinstance(data, dict):
                    conversations = data.get('conversations', data.get('data', []))

                for conv in conversations:
                    to_id = conv.get('to_id')
                    conv_id = conv.get('id')
                    if to_id and conv_id:
                        user_to_conversation[to_id] = conv_id

        except Exception as e:
            logger.error(f"获取对话列表异常: {e}")

        return user_to_conversation

    def get_conversation_messages(self, base_url: str,
                                  conversation_id: int,
                                  shop_id: int,
                                  region: str = 'MY') -> Optional[List[Dict]]:
        """获取对话消息列表"""
        auth = self.auth_info
        chat_bearer_token = auth.get('chat_bearer_token', '')
        csrf_token = auth.get('csrf_token', '')
        spc_cds_chat = auth.get('spc_cds_chat', '')

        if not chat_bearer_token:
            return None

        encoded_csrf = urllib.parse.quote(csrf_token, safe='')

        api_url = f"{base_url}{self.CHAT_MESSAGES_API}/{conversation_id}/messages"

        params = {
            'shop_id': shop_id,
            'offset': 0,
            'limit': 50,
            'direction': 'older',
            'biz_id': 0,
            'on_message_received': 'true',
            '_uid': f'0-{shop_id}',
            '_v': '9.1.7',
            'csrf_token': encoded_csrf,
            'SPC_CDS_CHAT': spc_cds_chat,
            'x-shop-region': region,
            '_api_source': 'sc'
        }

        headers = {
            'accept': 'application/json, text/plain, */*',
            'origin': base_url,
            'referer': f'{base_url}/portal/sale/order',
            'x-shop-region': region,
            'Authorization': f'Bearer {chat_bearer_token}'
        }

        try:
            response = self.browser_request.get(
                url=api_url,
                params=params,
                headers=headers,
                timeout=30
            )

            if response.ok:
                data = response.json()
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    messages = data.get('messages', data.get('data', []))
                    return messages

        except Exception as e:
            logger.error(f"获取聊天消息异常: {e}")

        return None

    def filter_user_messages(self, messages: List[Dict], buyer_user_id: int) -> List[Dict]:
        """
        筛选用户（买家）发送的消息

        Args:
            messages: 消息列表
            buyer_user_id: 买家用户 ID

        Returns:
            用户发送的消息列表
        """
        user_messages = []

        # 转换为字符串以便比较
        buyer_user_id_str = str(buyer_user_id)

        for msg in messages:
            if not isinstance(msg, dict):
                continue

            # 尝试多种方式判断发送者
            from_id = msg.get('from_id')
            sender = msg.get('sender')
            source = msg.get('source')
            user_id = msg.get('user_id')

            # 如果 from_id 等于 buyer_user_id，则这条消息是买家发送的
            # 也尝试字符串比较
            if from_id == buyer_user_id or str(from_id) == buyer_user_id_str:
                user_messages.append(msg)
                continue

            # 尝试其他字段
            if sender == buyer_user_id or str(sender) == buyer_user_id_str:
                user_messages.append(msg)
                continue

        return user_messages

    def concatenate_messages(self, messages: List[Dict]) -> str:
        """
        将消息列表拼接成文本

        Args:
            messages: 消息列表

        Returns:
            拼接后的文本
        """
        if not messages:
            return ''

        text_parts = []
        for msg in messages:
            if not isinstance(msg, dict):
                continue

            # 方法1: 获取 content 字段 (Shopee API 返回的消息内容)
            content = msg.get('content', {})
            if isinstance(content, dict):
                text = content.get('text', '')
                if text:
                    text_parts.append(text)

            # 方法2: 获取 message 字段
            message_data = msg.get('message', {})
            if isinstance(message_data, dict):
                # 文本消息
                text = message_data.get('text', '')
                if text:
                    text_parts.append(text)

                # 图片消息
                images = message_data.get('image', [])
                if images:
                    text_parts.append('[图片消息]')

                # 表情
                emoji = message_data.get('emoji')
                if emoji:
                    text_parts.append('[表情]')

                # 贴纸
                sticker = message_data.get('sticker')
                if sticker:
                    text_parts.append('[贴纸]')

                # 语音
                voice = message_data.get('voice')
                if voice:
                    text_parts.append('[语音消息]')

            # 方法3: 获取 custom_preview_text 字段 (系统订单通知等)
            custom_preview = msg.get('custom_preview_text', {})
            if isinstance(custom_preview, dict):
                text = custom_preview.get('text', '')
                if text:
                    text_parts.append(text)

        return '\n'.join(text_parts)

    def get_buyer_user_info(self, base_url: str, buyer_user_ids: List[int],
                           shop_id: int, region: str = 'MY') -> Dict[int, Dict]:
        """
        获取买家用户信息（包括 rating、avatar、country、city 等）

        接口：GET /webchat/api/v1.2/mini/users/{user_id}

        Args:
            base_url: 基础 URL
            buyer_user_ids: 买家用户 ID 列表
            shop_id: 店铺 ID
            region: 地区

        Returns:
            buyer_user_id -> 用户信息 的映射
        """
        user_info_map = {}

        if not buyer_user_ids:
            return user_info_map

        auth = self.auth_info
        chat_bearer_token = auth.get('chat_bearer_token', '')
        csrf_token = auth.get('csrf_token', '')
        spc_cds_chat = auth.get('spc_cds_chat', '')

        if not chat_bearer_token:
            logger.warning("缺少聊天 Bearer Token，无法获取用户信息")
            return user_info_map

        encoded_csrf = urllib.parse.quote(csrf_token, safe='')

        # 构建请求头
        headers = {
            'accept': 'application/json, text/plain, */*',
            'origin': base_url,
            'referer': f'{base_url}/portal/sale/order',
            'x-shop-region': region,
            'Authorization': f'Bearer {chat_bearer_token}'
        }

        # 逐个获取每个买家的信息
        for buyer_user_id in buyer_user_ids:
            try:
                # 构建查询参数
                params = {
                    'shop_id': shop_id,
                    'need_cache': 1,
                    'cache_expires': 1800,
                    '_uid': f'0-{shop_id}',
                    '_v': '9.1.8',
                    'csrf_token': encoded_csrf,
                    'SPC_CDS_CHAT': spc_cds_chat,
                    'x-shop-region': region,
                    '_api_source': 'sc'
                }

                # 构建请求 URL - 注意是 GET 请求，路径中包含 user_id
                api_url = f"{base_url}/webchat/api/v1.2/mini/users/{buyer_user_id}"
                query_string = urllib.parse.urlencode(params)
                full_url = f"{api_url}?{query_string}"

                response = self.browser_request.get(
                    url=full_url,
                    headers=headers,
                    timeout=30
                )

                if response.ok:
                    data = response.json()

                    # 响应是单个用户对象
                    if data and isinstance(data, dict):
                        user_id = data.get('id')
                        if user_id:
                            user_info_map[user_id] = {
                                'user_id': user_id,
                                'username': data.get('username', ''),
                                'portrait': data.get('avatar', ''),  # 头像
                                'rating': data.get('rating'),  # 评分 (1-5)
                                'country': data.get('country'),  # 国家
                                'city': data.get('city'),  # 城市
                                'status': data.get('status'),  # 状态
                                'is_blocked': data.get('is_blocked'),  # 是否被拉黑
                            }
                            logger.info(f"[ShopeeAPI] 获取用户 {user_id} 信息成功: rating={data.get('rating')}, country={data.get('country')}")

                time.sleep(0.1)  # 避免请求过快

            except Exception as e:
                logger.error(f"[ShopeeAPI] 获取用户 {buyer_user_id} 信息失败: {e}")

        return user_info_map

    async def get_buyer_user_info_async(self, base_url: str, buyer_user_ids: List[int],
                                        shop_id: int, region: str = 'MY',
                                        max_concurrent: int = 5) -> Dict[int, Dict]:
        """
        异步并发获取买家用户信息

        Args:
            base_url: 基础 URL
            buyer_user_ids: 买家用户 ID 列表
            shop_id: 店铺 ID
            region: 地区
            max_concurrent: 最大并发数

        Returns:
            buyer_user_id -> 用户信息 的映射
        """
        from ..network.async_http import AsyncBatchRequest

        user_info_map = {}

        if not buyer_user_ids:
            return user_info_map

        auth = self.auth_info
        chat_bearer_token = auth.get('chat_bearer_token', '')
        csrf_token = auth.get('csrf_token', '')
        spc_cds_chat = auth.get('spc_cds_chat', '')

        if not chat_bearer_token:
            logger.warning("缺少聊天 Bearer Token，无法获取用户信息")
            return user_info_map

        # 获取 cookies
        cookies = self._driver.get_cookies()

        # 创建异步请求器
        async_request = AsyncBatchRequest(cookies=cookies, auth_info=auth)

        try:
            # 使用信号量控制并发，降低并发数以避免 429
            semaphore = asyncio.Semaphore(max_concurrent)

            async def fetch_single_buyer(buyer_user_id: int) -> tuple:
                max_retries = 10
                retry_delay = 0.1  # 初始重试延迟

                for attempt in range(max_retries):
                    async with semaphore:
                        try:
                            # 添加随机延迟，避免规律性请求
                            await asyncio.sleep(random.uniform(0.1, 0.3))
                            encoded_csrf = urllib.parse.quote(csrf_token, safe='')

                            params = {
                                'shop_id': shop_id,
                                'need_cache': 1,
                                'cache_expires': 1800,
                                '_uid': f'0-{shop_id}',
                                '_v': '9.1.8',
                                'csrf_token': encoded_csrf,
                                'SPC_CDS_CHAT': spc_cds_chat,
                                'x-shop-region': region,
                                '_api_source': 'sc'
                            }

                            api_url = f"{base_url}/webchat/api/v1.2/mini/users/{buyer_user_id}"
                            query_string = urllib.parse.urlencode(params)
                            full_url = f"{api_url}?{query_string}"

                            headers = {
                                'accept': 'application/json, text/plain, */*',
                                'origin': base_url,
                                'referer': f'{base_url}/portal/sale/order',
                                'x-shop-region': region,
                                'Authorization': f'Bearer {chat_bearer_token}'
                            }

                            # 获取 session 并发送请求
                            session = await async_request._get_session()
                            cookies_header = async_request._build_cookies_header()
                            if cookies_header:
                                headers['Cookie'] = cookies_header

                            async with session.get(url=full_url, headers=headers) as response:
                                if response.ok:
                                    content = await response.text()
                                    data = json.loads(content)

                                    if data and isinstance(data, dict):
                                        user_id = data.get('id')
                                        if user_id:
                                            user_info_map[user_id] = {
                                                'user_id': user_id,
                                                'username': data.get('username', ''),
                                                'avatar': data.get('avatar', ''),
                                                'rating': data.get('rating'),
                                                'country': data.get('country'),
                                                'city': data.get('city'),
                                                'status': data.get('status'),
                                                'is_blocked': data.get('is_blocked'),
                                            }
                                            return user_id, user_info_map[user_id]
                                elif response.status == 429:
                                    # 429 时等待后重试
                                    if attempt < max_retries - 1:
                                        await asyncio.sleep(retry_delay)
                                        retry_delay *= 2  # 指数退避
                                        continue
                                else:
                                    logger.warning(f"[Async] 获取买家 {buyer_user_id} 失败: HTTP {response.status}")

                            return buyer_user_id, {}

                        except Exception as e:
                            if attempt < max_retries - 1:
                                logger.warning(f"[Async] 获取买家 {buyer_user_id} 异常: {e}，等待 {retry_delay}秒后重试...")
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2
                                continue
                            logger.error(f"[Async] 获取买家 {buyer_user_id} 信息失败: {e}")
                            return buyer_user_id, {}

                return buyer_user_id, {}

            # 并发执行
            tasks = [fetch_single_buyer(uid) for uid in buyer_user_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 收集结果
            success_count = 0
            fail_count = 0
            for result in results:
                if isinstance(result, tuple):
                    buyer_user_id, info = result
                    if info:
                        user_info_map[buyer_user_id] = info
                        success_count += 1
                    else:
                        fail_count += 1
                elif isinstance(result, Exception):
                    fail_count += 1

            logger.info(f"[Async] 买家用户信息获取完成: 成功 {success_count}, 失败 {fail_count}")

        finally:
            await async_request.close()

        return user_info_map

    async def get_conversation_map_async(self, base_url: str, shop_id: int,
                                         region: str = 'MY') -> Dict[int, int]:
        """
        异步获取对话列表

        Args:
            base_url: 基础 URL
            shop_id: 店铺 ID
            region: 地区

        Returns:
            user_id -> conversation_id 的映射
        """
        from ..network.async_http import AsyncBatchRequest

        user_to_conversation = {}

        auth = self.auth_info
        chat_bearer_token = auth.get('chat_bearer_token', '')
        csrf_token = auth.get('csrf_token', '')
        spc_cds_chat = auth.get('spc_cds_chat', '')

        if not chat_bearer_token:
            return user_to_conversation

        encoded_csrf = urllib.parse.quote(csrf_token, safe='')

        api_url = f"{base_url}/webchat/api/v1.2/mini/conversations"

        params = {
            'limit': 100,
            'offset': 0,
            'shop_id': shop_id,
            '_uid': f'0-{shop_id}',
            '_v': '9.1.7',
            'csrf_token': encoded_csrf,
            'SPC_CDS_CHAT': spc_cds_chat,
            'x-shop-region': region,
            '_api_source': 'sc'
        }

        headers = {
            'accept': 'application/json, text/plain, */*',
            'origin': base_url,
            'referer': f'{base_url}/portal/chat/',
            'x-shop-region': region,
            'Authorization': f'Bearer {chat_bearer_token}'
        }

        # 获取 cookies
        cookies = self._driver.get_cookies()
        async_request = AsyncBatchRequest(cookies=cookies, auth_info=auth)

        try:
            session = await async_request._get_session()
            cookies_header = async_request._build_cookies_header()
            if cookies_header:
                headers['Cookie'] = cookies_header

            query_string = urllib.parse.urlencode(params)
            full_url = f"{api_url}?{query_string}"

            async with session.get(url=full_url, headers=headers) as response:
                if response.ok:
                    content = await response.text()
                    data = json.loads(content)

                    conversations = []
                    if isinstance(data, list):
                        conversations = data
                    elif isinstance(data, dict):
                        conversations = data.get('conversations', data.get('data', []))

                    for conv in conversations:
                        to_id = conv.get('to_id')
                        conv_id = conv.get('id')
                        if to_id and conv_id:
                            user_to_conversation[to_id] = conv_id

        except Exception as e:
            logger.error(f"[Async] 获取对话列表异常: {e}")

        finally:
            await async_request.close()

        return user_to_conversation

    async def get_conversation_messages_async(self, base_url: str,
                                              conversation_id: int,
                                              shop_id: int,
                                              region: str = 'MY') -> Optional[List[Dict]]:
        """
        异步获取对话消息列表

        Args:
            base_url: 基础 URL
            conversation_id: 对话 ID
            shop_id: 店铺 ID
            region: 地区

        Returns:
            消息列表
        """
        from ..network.async_http import AsyncBatchRequest

        auth = self.auth_info
        chat_bearer_token = auth.get('chat_bearer_token', '')
        csrf_token = auth.get('csrf_token', '')
        spc_cds_chat = auth.get('spc_cds_chat', '')

        if not chat_bearer_token:
            return None

        encoded_csrf = urllib.parse.quote(csrf_token, safe='')

        api_url = f"{base_url}{self.CHAT_MESSAGES_API}/{conversation_id}/messages"

        params = {
            'shop_id': shop_id,
            'offset': 0,
            'limit': 50,
            'direction': 'older',
            'biz_id': 0,
            'on_message_received': 'true',
            '_uid': f'0-{shop_id}',
            '_v': '9.1.7',
            'csrf_token': encoded_csrf,
            'SPC_CDS_CHAT': spc_cds_chat,
            'x-shop-region': region,
            '_api_source': 'sc'
        }

        headers = {
            'accept': 'application/json, text/plain, */*',
            'origin': base_url,
            'referer': f'{base_url}/portal/sale/order',
            'x-shop-region': region,
            'Authorization': f'Bearer {chat_bearer_token}'
        }

        # 获取 cookies
        cookies = self._driver.get_cookies()
        async_request = AsyncBatchRequest(cookies=cookies, auth_info=auth)

        try:
            session = await async_request._get_session()
            cookies_header = async_request._build_cookies_header()
            if cookies_header:
                headers['Cookie'] = cookies_header

            query_string = urllib.parse.urlencode(params)
            full_url = f"{api_url}?{query_string}"

            async with session.get(url=full_url, headers=headers) as response:
                if response.ok:
                    content = await response.text()
                    data = json.loads(content)

                    if isinstance(data, list):
                        return data
                    elif isinstance(data, dict):
                        return data.get('messages', data.get('data', []))

        except Exception as e:
            logger.error(f"[Async] 获取聊天消息异常: {e}")

        finally:
            await async_request.close()

        return None

    async def fetch_chat_messages_async(self, base_url: str, all_orders: List[Dict],
                                        order_details: List[Dict], shop_id: int,
                                        region: str = 'MY',
                                        max_concurrent: int = 20) -> Dict[str, Any]:
        """
        异步并发获取聊天消息和买家信息

        Args:
            base_url: 基础 URL
            all_orders: 订单列表
            order_details: 订单详情列表
            shop_id: 店铺 ID
            region: 地区
            max_concurrent: 最大并发数

        Returns:
            包含 chat_messages 和 buyer_info 的字典
        """
        result = {
            'chat_messages': {},
            'buyer_info': {}
        }

        # 构建 order_buyer_map
        order_buyer_map = {}
        buyer_user_ids = []

        for detail in order_details:
            if not isinstance(detail, dict):
                continue
            card = detail.get('package_card', {})
            if not card or not isinstance(card, dict):
                continue

            header = card.get('card_header', {})
            order_ext = card.get('order_ext_info', {})

            order_id = order_ext.get('order_id')
            order_sn = header.get('order_sn')
            buyer_user_id = order_ext.get('buyer_user_id')

            if order_id and buyer_user_id:
                order_buyer_map[order_id] = {
                    'buyer_user_id': buyer_user_id,
                    'order_sn': order_sn
                }
                if buyer_user_id not in buyer_user_ids:
                    buyer_user_ids.append(buyer_user_id)

        if not order_buyer_map:
            logger.info("[Async] 没有找到买家信息，跳过聊天消息获取")
            return result

        # 1. 异步并发获取买家用户信息
        if buyer_user_ids and shop_id:
            logger.info(f"[Async] 开始并发获取 {len(buyer_user_ids)} 个买家用户信息...")

            buyer_info_map = await self.get_buyer_user_info_async(
                base_url=base_url,
                buyer_user_ids=buyer_user_ids,
                shop_id=shop_id,
                region=region,
                max_concurrent=max_concurrent
            )
            result['buyer_info'] = buyer_info_map

        # 2. 异步获取对话列表
        user_to_conversation = await self.get_conversation_map_async(base_url, shop_id, region)

        if not user_to_conversation:
            logger.warning("[Async] 没有找到任何对话")
            return result

        # 准备需要获取消息的订单
        order_tasks = []
        for order in all_orders:
            order_id = order.get('order_id')
            if not order_id or order_id not in order_buyer_map:
                continue

            buyer_user_id = order_buyer_map[order_id].get('buyer_user_id')
            order_sn = order_buyer_map[order_id].get('order_sn')
            conversation_id = user_to_conversation.get(buyer_user_id)

            if conversation_id:
                order_tasks.append({
                    'order_id': order_id,
                    'order_sn': order_sn,
                    'buyer_user_id': buyer_user_id,
                    'conversation_id': conversation_id
                })

        if not order_tasks:
            logger.info("[Async] 没有需要获取聊天消息的订单")
            return result

        # 3. 异步并发获取聊天消息
        logger.info(f"[Async] 开始并发获取 {len(order_tasks)} 个订单的聊天消息...")

        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_single_chat(task: Dict) -> tuple:
            async with semaphore:
                try:
                    messages = await self.get_conversation_messages_async(
                        base_url, task['conversation_id'], shop_id, region
                    )
                    if messages is None:
                        return task['order_id'], None

                    buyer_user_id = task['buyer_user_id']
                    user_messages = self.filter_user_messages(messages, buyer_user_id)
                    user_message_text = self.concatenate_messages(user_messages) if user_messages else ''

                    return task['order_id'], {
                        'order_id': task['order_id'],
                        'order_sn': task['order_sn'],
                        'conversation_id': task['conversation_id'],
                        'buyer_user_id': buyer_user_id,
                        'total_messages': len(messages),
                        'user_messages_count': len(user_messages),
                        'user_message_text': user_message_text,
                    }
                except Exception as e:
                    logger.error(f"[Async] 获取订单 {task['order_sn']} 聊天消息失败: {e}")
                    return task['order_id'], None

        # 并发执行
        tasks = [fetch_single_chat(task) for task in order_tasks]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 收集结果
        chat_messages = {}
        for result_item in results:
            if isinstance(result_item, tuple):
                order_id, chat_data = result_item
                if chat_data:
                    chat_messages[str(order_id)] = chat_data

        result['chat_messages'] = chat_messages
        logger.info(f"[Async] 聊天消息获取完成: {len(chat_messages)} 条")

        return result