"""
Shopee 全部订单任务
获取所有订单（不限于待发货），不筛选无运单号
"""
from typing import Dict, Any, List
import asyncio
import json
import os
import time
from datetime import datetime

from .task_base import BaseTask, TaskFactory
from ..browser.selenium_driver import HubStudioSeleniumDriver
from selenium.webdriver.support.ui import WebDriverWait
from ..database.access_db import AccessDatabase
from ..api.shopee_api import ShopeeAPI
from ..utils.logger import default_logger as logger
from ..utils.performance_tracker import get_tracker, measure_time
from ..config import load_config


class ShopeeAllOrderTask(BaseTask):
    """
    Shopee 全部订单任务

    功能：
    1. 自动登录（HubStudio 保存的账号密码）
    2. 使用 CDP 发送 API 请求获取所有订单列表
    3. 批量获取订单详情
    4. 支持分页获取
    5. 保存结果到文件
    6. 保存订单到 Access 数据库

    与 ShopeeOrderTask 的区别：
    - 不限于待发货订单（order_list_tab=100）
    - 不筛选无运单号订单
    - 不获取聊天消息
    - 获取所有状态的订单
    """

    task_name = "shopee_all_order"

    # 订单标签页类型
    TAB_TO_SHIP = 300     # 待发货
    TAB_ALL = 100         # 全部订单
    MAX_WORKERS = 100

    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化订单任务

        Args:
            config: 任务配置
                - page_size: 每页订单数 (默认 200, 最大 200)
                - max_pages: 最大获取页数 (默认 100)
                - order_list_tab: 订单标签页类型 (默认 100=全部订单)
                - sort_type: 排序类型 (默认 3=按更新时间)
                - ascending: 是否升序 (默认 False=降序)
                - capture_api: 是否捕获 API 请求 (默认 False)
                - fetch_detail: 是否获取订单详情 (默认 True)
                - batch_size: 批量获取详情数量 (默认 5，API 上限)
                - save_to_file: 是否保存结果到文件 (默认 True)
                - output_dir: 输出目录 (默认 ./output/all_orders)
                - save_to_db: 是否保存到数据库 (默认 False)
                - db_path: 数据库路径 (默认从 config/settings.yaml 中读取)
        """
        super().__init__(config)

        # 加载全局配置
        global_config = load_config()
        default_db_path = global_config.database.access_path

        # 默认配置
        self.page_size = self.config.get('page_size', 200)
        self.max_pages = self.config.get('max_pages', 100)
        self.order_list_tab = self.config.get('order_list_tab', self.TAB_ALL)
        self.sort_type = self.config.get('sort_type', 3)
        self.ascending = self.config.get('ascending', False)
        self.capture_api = self.config.get('capture_api', False)
        self.fetch_detail = self.config.get('fetch_detail', True)
        self.batch_size = self.config.get('batch_size', 5)
        self.save_to_file = self.config.get('save_to_file', True)
        self.output_dir = self.config.get('output_dir', './output/all_orders')
        self.save_to_db = self.config.get('save_to_db', False)
        self.db_path = self.config.get('db_path', default_db_path)

        # 数据库实例
        self._db = None
        self._auth_info = None

    @property
    def database(self) -> Any:
        """获取数据库实例"""
        if self._db is None and self.save_to_db:
            try:
                db_dir = os.path.dirname(self.db_path)
                if db_dir and not os.path.exists(db_dir):
                    os.makedirs(db_dir, exist_ok=True)

                self._db = AccessDatabase.get_instance(self.db_path)
                self._db.init_order_tables()
            except Exception as e:
                logger.warning(f"数据库连接失败: {e}")
                self._db = None
        return self._db

    def setup(self, driver: HubStudioSeleniumDriver, env_info: Dict[str, Any]):
        """前置操作：导航到目标页面，确保登录状态"""
        env_name = env_info.get('env_name', '')
        shopee_api = ShopeeAPI(driver)
        target_url = shopee_api.get_base_url(env_name)

        current_url = driver.get_current_url()

        logger.info(f"[ShopeeAllOrder] 导航到目标页面: {target_url}")
        driver.goto(target_url)
        # 等待页面加载
        time.sleep(3)
        current_url = driver.get_current_url()

        cookies = driver.get_cookies()
        cookie_names = [c.get('name') for c in cookies]
        logger.info(f"[ShopeeAllOrder] 当前URL: {current_url}")

        # 检查是否需要登录：URL包含/login 或者 没有关键的登录Cookie
        login_cookies = ['SPC_CI', 'SPC_U', 'SHOPEE_TOKEN']
        has_login_cookie = any(c in cookie_names for c in login_cookies)

        if '/login' in current_url or not has_login_cookie:
            logger.warning(f"[ShopeeAllOrder] 未登录或登录状态失效，等待登录...")
            self._wait_for_login(driver)
        else:
            logger.info(f"[ShopeeAllOrder] 检测到已登录Cookie，会话已恢复")
            
        logger.info(f"[ShopeeAllOrder] 等待页面加载...")
        try:
            WebDriverWait(driver.driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            logger.info(f"[ShopeeAllOrder] 页面已加载完成")
        except Exception as e:
            logger.warning(f"[ShopeeAllOrder] 等待页面加载超时: {e}")

    def execute(self, driver: HubStudioSeleniumDriver, env_info: Dict[str, Any]) -> Dict[str, Any]:
        """执行订单获取任务"""
        env_name = env_info.get('env_name', '')

        # 初始化性能追踪器
        tracker = get_tracker()
        tracker.reset()

        result = {
            'env_name': env_name,
            'orders': [],
            'order_details': [],
            'total_count': 0,
            'pages_fetched': 0,
            'cookies_count': 0,
            'order_status_summary': {}
        }

        # 使用 ShopeeAPI 进行 API 调用
        shopee_api = ShopeeAPI(driver)
        browser_request = shopee_api.browser_request
        base_url = shopee_api.get_base_url(env_name)

        # 1. 获取当前 Cookies 和认证信息
        cookies = driver.get_cookies()
        result['cookies_count'] = len(cookies)

        # 提取认证信息并设置到 ShopeeAPI
        self._auth_info = shopee_api.auth_info
        shopee_api.set_auth_info(self._auth_info)

        logger.info(f"[ShopeeAllOrder] 开始获取订单列表（使用 AsyncBatchRequest）")

        if self.capture_api:
            browser_request.start_api_capture(url_filter="order")

        # 2. 使用 ShopeeAPI 异步获取订单列表
        tracker.start('获取订单列表', env=env_name)
        all_orders = []
        page_number = 1
        next_page_sentinel = None

        logger.info(f"[ShopeeAllOrder] 开始分页获取订单，max_pages={self.max_pages}, page_size={self.page_size}")

        # 创建事件循环用于异步获取订单列表
        async def fetch_order_list_async():
            nonlocal all_orders, page_number, next_page_sentinel, result
            import asyncio

            while page_number <= self.max_pages:
                logger.info(f"[ShopeeAllOrder] ===== 正在获取第 {page_number} 页... (sentinel={next_page_sentinel[:20] if next_page_sentinel else None})")

                try:
                    order_data = await shopee_api.get_order_list_async(
                        base_url=base_url,
                        order_list_tab=self.order_list_tab,
                        page_number=page_number,
                        page_sentinel=next_page_sentinel,
                        page_size=self.page_size,
                        sort_type=self.sort_type,
                        ascending=self.ascending
                    )

                    if order_data:
                        index_list = order_data.get('index_list', [])
                        pagination = order_data.get('pagination', {})

                        all_orders.extend(index_list)

                        result['total_count'] = pagination.get('total', 0)
                        next_page_sentinel = pagination.get('next_page_sentinel')
                        logger.info(f"[ShopeeAllOrder] 第 {page_number} 页获取成功，"
                                   f"本页 {len(index_list)} 条，总计 {result['total_count']} 条")

                        # 检查是否已获取全部订单
                        # 1. 没有下一页标记
                        # 2. 或者本页数据量小于 page_size
                        # 3. 或者已获取的订单数 >= 总订单数
                        if not next_page_sentinel:
                            logger.info(f"[ShopeeAllOrder] 无下一页，停止获取")
                            break
                        if len(index_list) < self.page_size:
                            logger.info(f"[ShopeeAllOrder] 本页数据不足一页，停止获取")
                            break
                        if result['total_count'] > 0 and len(all_orders) >= result['total_count']:
                            logger.info(f"[ShopeeAllOrder] 已获取全部 {result['total_count']} 条订单，停止获取")
                            break
                    else:
                        logger.warning(f"[ShopeeAllOrder] API 返回为空，继续尝试...")

                except Exception as e:
                    logger.warning(f"[ShopeeAllOrder] 获取订单异常: {e}, 继续尝试...")

                page_number += 1
                await asyncio.sleep(0.5)

            result['orders'] = all_orders
            result['pages_fetched'] = page_number - 1

        # 运行异步获取订单列表
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(fetch_order_list_async())
        finally:
            loop.close()

        logger.info(f"[ShopeeAllOrder] 订单列表获取完成: {len(all_orders)} 条订单，{page_number - 1} 页")
        tracker.end('获取订单列表', {'orders_count': len(all_orders), 'pages': page_number - 1}, env=env_name)

        # 检查订单列表是否有重复 order_id
        order_ids_list = [str(o.get('order_id')) for o in all_orders if o.get('order_id')]
        unique_ids = set(order_ids_list)
        if len(order_ids_list) != len(unique_ids):
            duplicate_count = len(order_ids_list) - len(unique_ids)
            logger.warning(f"[ShopeeAllOrder] 检测到重复 order_id: 总数={len(order_ids_list)}, 唯一={len(unique_ids)}, 重复={duplicate_count}")
            # 找出重复的 order_id
            from collections import Counter
            id_counter = Counter(order_ids_list)
            duplicate_ids = [oid for oid, count in id_counter.items() if count > 1]
            logger.warning(f"[ShopeeAllOrder] 重复的 order_id: {duplicate_ids[:10]}")


        # 3. 批量获取订单详情
        if self.fetch_detail and all_orders:
            logger.info(f"[ShopeeAllOrder] 开始获取订单详情...")
            tracker.start('获取订单详情', env=env_name)
            order_details = self._fetch_order_details(shopee_api, base_url, all_orders)
            result['order_details'] = order_details
            tracker.end('获取订单详情', {'details_count': len(order_details)}, env=env_name)
            logger.info(f"[ShopeeAllOrder] 订单详情获取完成: {len(order_details)} 条")

            # 4. 获取聊天消息和买家信息
            if self.save_to_db:
                logger.info(f"[ShopeeAllOrder] 开始获取聊天消息...")
                tracker.start('获取聊天消息和买家信息', env=env_name)
                chat_result = self._fetch_chat_messages(driver, all_orders, order_details, env_name)
                result['chat_messages'] = chat_result.get('chat_messages', {})
                result['buyer_info'] = chat_result.get('buyer_info', {})
                tracker.end('获取聊天消息和买家信息', {
                    'chat_count': len(result['chat_messages']),
                    'buyer_count': len(result['buyer_info'])
                }, env=env_name)
                logger.info(f"[ShopeeAllOrder] 聊天消息获取完成: {len(result['chat_messages'])} 条")

        if self.capture_api:
            captured_apis = browser_request.stop_api_capture()
            result['captured_apis'] = len(captured_apis)

        # 4. 保存结果到文件
        # if self.save_to_file:
        #     self._save_results(result, env_name)

        # 5. 保存订单到数据库
        if self.save_to_db and self.database and result.get('order_details'):
            result['order_list'] = all_orders
            tracker.start('保存到数据库', env=env_name)
            self._save_orders_to_database(result, env_name)
            tracker.end('保存到数据库', env=env_name)

        # 输出性能统计摘要
        tracker.log_summary("ShopeeAllOrder 任务性能统计")

        return result

    def _count_orders_by_status(self, orders: List[Dict]) -> Dict[str, int]:
        """统计订单状态数量"""
        status_counts = {}
        for order in orders:
            status = order.get('status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
        return status_counts

    def _fetch_order_details(self, shopee_api: ShopeeAPI,
                            base_url: str, orders: List[Dict]) -> List[Dict]:
        """批量获取订单详情（异步并发版本）"""
        all_details = []

        # 准备参数列表
        package_params = []
        for order in orders:
            package_number = order.get('package_number')
            shop_id = order.get('shop_id')
            region_id = order.get('region_id')

            if package_number and shop_id and region_id:
                package_params.append({
                    'package_number': package_number,
                    'shop_id': shop_id,
                    'region_id': region_id
                })

        # 使用订单ID作为备用
        if not package_params:
            for order in orders:
                order_id = order.get('order_id')
                shop_id = order.get('shop_id')
                region_id = order.get('region_id', 'MY')
                if order_id and shop_id:
                    package_params.append({
                        'order_id': order_id,
                        'shop_id': shop_id,
                        'region_id': region_id
                    })

        if not package_params:
            logger.warning("[ShopeeAllOrder] 没有可用的订单参数")
            return all_details

        # 创建新的事件循环来运行异步代码
        loop = asyncio.new_event_loop()
        try:
            all_details = loop.run_until_complete(
                shopee_api.get_order_card_list_async(
                    base_url=base_url,
                    package_params=package_params,
                    order_list_tab=self.order_list_tab,
                    batch_size=self.batch_size,
                    max_concurrent=self.MAX_WORKERS
                )
            )
        finally:
            loop.close()

        return all_details

    def _wait_for_login(self, driver: HubStudioSeleniumDriver, timeout: int = 60):
        """等待用户登录，尝试点击登录按钮"""
        from selenium.webdriver.support import expected_conditions as EC

        # 等待页面完全加载
        logger.info(f"[ShopeeAllOrder] 等待页面加载...")
        try:
            WebDriverWait(driver.driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            logger.info(f"[ShopeeAllOrder] 页面已加载完成")
        except Exception as e:
            logger.warning(f"[ShopeeAllOrder] 等待页面加载超时: {e}")

        # 额外等待确保动态内容加载完成
        time.sleep(2)
        start_time = time.time()
        # 尝试查找并点击 "Log in" 按钮
        login_button_selectors = [
            "button:contains('Log in')",
            "button:contains('Login')",
            "//button[contains(text(), 'Log in')]",
            "//button[contains(text(), 'Login')]",
            "button[type='submit']",
            ".shopee-button-primary",
            "button.btn-primary"
        ]

        # 先尝试点击登录按钮
        for selector in login_button_selectors:
            try:
                # 尝试使用 CSS 选择器
                if ':contains' in selector:
                    continue  # Selenium 不支持 contains CSS 选择器
                if selector.startswith('//'):
                    # XPath
                    elements = driver.driver.find_elements("xpath", selector)
                else:
                    elements = driver.driver.find_elements("css selector", selector)

                for element in elements:
                    try:
                        text = element.text.lower()
                        if 'log in' in text or 'login' in text:
                            logger.info(f"[ShopeeAllOrder] 找到登录按钮，点击: {element.text}")
                            element.click()
                            time.sleep(2)
                            break
                    except:
                        pass
            except Exception as e:
                logger.debug(f"尝试选择器 {selector}: {e}")

        # 等待登录完成
        while time.time() - start_time < timeout:
            current_url = driver.get_current_url()

            # 检查登录Cookie
            cookies = driver.get_cookies()
            cookie_names = [c.get('name') for c in cookies]
            login_cookies = ['SPC_CI', 'SPC_U', 'SHOPEE_TOKEN']
            has_login_cookie = any(c in cookie_names for c in login_cookies)

            if '/login' not in current_url and has_login_cookie:
                logger.info(f"[ShopeeAllOrder] 登录成功")
                return True

            # 如果还是登录页，尝试再次点击登录按钮
            if '/login' in current_url:
                try:
                    buttons = driver.driver.find_elements("tag name", "button")
                    for btn in buttons:
                        try:
                            text = btn.text.lower()
                            if 'log in' in text or 'login' in text:
                                logger.info(f"[ShopeeAllOrder] 再次点击登录按钮: {btn.text}")
                                btn.click()
                                time.sleep(2)
                                break
                        except:
                            pass
                except:
                    pass

            time.sleep(2)

        logger.warning(f"[ShopeeAllOrder] 登录超时")
        return False

    def _fetch_chat_messages(self, driver: HubStudioSeleniumDriver,
                             all_orders: List[Dict],
                             order_details: List[Dict],
                             env_name: str = '') -> Dict[str, Any]:
        """获取订单的聊天消息和买家信息（异步并发版本）"""
        result = {
            'chat_messages': {},
            'buyer_info': {}
        }

        try:
            shopee_api = ShopeeAPI(driver)
            shopee_api.set_auth_info(self._auth_info)

            base_url = shopee_api.get_base_url()
            auth = shopee_api.auth_info
            shop_id = auth.get('shop_id')
            region = auth.get('region', 'MY')

            # 如果没有从 auth_info 获取到 shop_id，尝试从多个途径获取
            if not shop_id:
                shop_id = self._resolve_shop_id(order_details, all_orders, driver)
                if shop_id:
                    self._auth_info['shop_id'] = shop_id
                else:
                    logger.warning("[ShopeeAllOrder] 无法获取 shop_id，跳过聊天消息获取")
                    return result

                shop_id = self._auth_info.get('shop_id')
                region = self._auth_info.get('region', 'MY')

            logger.info(f"[ShopeeAllOrder] 开始异步并发获取聊天消息和买家信息...")

            # 使用异步并发版本
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(
                    shopee_api.fetch_chat_messages_async(
                        base_url=base_url,
                        all_orders=all_orders,
                        order_details=order_details,
                        shop_id=shop_id,
                        region=region,
                        max_concurrent=5,
                        env_name=env_name
                    )
                )
            finally:
                loop.close()

            logger.info(f"[ShopeeAllOrder] 聊天消息获取完成: {len(result.get('chat_messages', {}))} 条")
            logger.info(f"[ShopeeAllOrder] 买家信息获取完成: {len(result.get('buyer_info', {}))} 条")

        except Exception as e:
            logger.error(f"[ShopeeAllOrder] 获取聊天消息失败: {e}")

        return result

    def _resolve_shop_id(self, order_details: List[Dict], all_orders: List[Dict],
                          driver: HubStudioSeleniumDriver) -> int:
        """从多个途径解析 shop_id"""
        import re

        # 1. 从订单详情中获取
        for detail in order_details:
            if isinstance(detail, dict):
                card = detail.get('package_card', {})
                if card and isinstance(card, dict):
                    order_ext = card.get('order_ext_info', {})
                    if order_ext and isinstance(order_ext, dict):
                        shop_id = order_ext.get('shop_id')
                        if shop_id:
                            logger.info(f"[ShopeeAllOrder] 从订单详情中获取到 shop_id: {shop_id}")
                            return shop_id

        # 2. 从订单列表中获取
        for order in all_orders:
            shop_id = order.get('shop_id')
            if shop_id:
                return shop_id

        # 3. 从 cookie 中解析 shop_id
        cookies = driver.get_cookies()
        for cookie in cookies:
            if cookie.get('name') == 'SPC_CI':
                value = cookie.get('value', '')
                if value and '-' in value:
                    try:
                        shop_id = int(value.split('-')[0])
                        if shop_id:
                            logger.info(f"[ShopeeAllOrder] 从 cookie 中获取到 shop_id: {shop_id}")
                            return shop_id
                    except:
                        pass

        # 4. 从页面 URL 中提取
        current_url = driver.get_current_url()
        match = re.search(r'shop/(\d+)', current_url)
        if match:
            shop_id = int(match.group(1))
            logger.info(f"[ShopeeAllOrder] 从 URL 中获取到 shop_id: {shop_id}")
            return shop_id

        return None

    def _save_results(self, result: Dict[str, Any], env_name: str):
        """保存结果到文件"""
        try:
            os.makedirs(self.output_dir, exist_ok=True)

            timestamp = time.strftime('%Y%m%d_%H%M%S')
            safe_env_name = env_name.replace(' ', '_').replace('/', '_')
            filename = f"{safe_env_name}_{timestamp}.json"
            filepath = os.path.join(self.output_dir, filename)

            output_data = {
                'env_name': result.get('env_name'),
                'timestamp': datetime.now().isoformat(),
                'total_count': result.get('total_count'),
                'pages_fetched': result.get('pages_fetched'),
                'order_status_summary': result.get('order_status_summary', {}),
                'orders': []
            }

            # 创建 order_id 到订单列表数据的映射
            order_list = result.get('order_list', [])
            order_id_map = {order.get('order_id'): order for order in order_list if order.get('order_id')}

            # 整理订单数据
            for detail in result.get('order_details', []):
                if not isinstance(detail, dict):
                    continue

                if 'package_card' not in detail:
                    continue

                card = detail.get('package_card', {})
                if not card:
                    continue

                header = card.get('card_header', {})
                order_ext = card.get('order_ext_info', {})
                status_info = card.get('status_info', {})
                payment = card.get('payment_info', {})
                fulfilment = card.get('fulfilment_info', {})
                package_ext = card.get('package_ext_info', {})
                item_group = card.get('item_info_group', {})

                order_id = order_ext.get('order_id')
                buyer_username = header.get('buyer_info', {}).get('username', '')
                if not buyer_username and order_id in order_id_map:
                    buyer_username = order_id_map[order_id].get('buyer_username', '')

                order_data = {
                    'order_id': order_id,
                    'order_sn': header.get('order_sn'),
                    'buyer_user_id': order_ext.get('buyer_user_id'),
                    'buyer_username': buyer_username,
                    'shop_id': order_ext.get('shop_id'),
                    'status': status_info.get('status', ''),
                    'fulfilment_channel': fulfilment.get('fulfilment_channel_name', ''),
                    'total_price': payment.get('total_price', 0) / 100 if payment.get('total_price') else 0,
                    'currency': 'MYR',
                    'items': self._extract_items(item_group),
                    'shipping_name': package_ext.get('shipping_name', ''),
                    'shipping_phone': package_ext.get('shipping_phone', ''),
                    'shipping_address': package_ext.get('shipping_address', ''),
                    'tracking_numbers': self._extract_tracking_numbers(fulfilment),
                }

                output_data['orders'].append(order_data)

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)

            logger.info(f"[ShopeeAllOrder] 结果已保存到: {filepath}")

        except Exception as e:
            logger.error(f"[ShopeeAllOrder] 保存结果失败: {e}")

    def _extract_tracking_numbers(self, fulfilment: Dict) -> List[str]:
        """提取追踪号"""
        tracking_list = fulfilment.get('tracking_number_list', [])
        if not isinstance(tracking_list, list):
            return []

        result = []
        for t in tracking_list:
            if isinstance(t, str):
                result.append(t)
            elif isinstance(t, dict):
                result.append(t.get('tracking_number', ''))
        return result

    def _extract_items(self, item_info_group: Dict) -> List[Dict]:
        """提取商品信息"""
        items = []
        item_list = item_info_group.get('item_info_list', [])

        if not item_list:
            return items

        for item_group in item_list:
            for item in item_group.get('item_list', []):
                inner_info = item.get('inner_item_ext_info', {})
                item_id = inner_info.get('item_id') or item.get('item_id') or item.get('id')
                model_id = inner_info.get('model_id') or item.get('model_id') or item.get('model', {}).get('model_id')

                items.append({
                    'name': item.get('name', ''),
                    'description': item.get('description', ''),
                    'amount': item.get('amount', 1),
                    'item_id': item_id,
                    'model_id': model_id,
                })

        return items

    def _save_orders_to_database(self, result: Dict[str, Any], env_name: str):
        """保存订单到数据库（批量优化版）"""
        logger.info(f"[ShopeeAllOrder] 开始保存订单到数据库, order_details数量: {len(result.get('order_details', []))}")
        order_details = result.get('order_details', [])
        order_list = result.get('order_list', [])
        chat_messages = result.get('chat_messages', {})
        api_buyer_info = result.get('buyer_info', {})

        order_id_map = {str(order.get('order_id')): order for order in order_list if order.get('order_id')}

        # 1. 先解析所有订单数据
        all_orders = []
        all_items = []
        all_buyers = []
        skipped_count = 0
        for detail in order_details:
            try:
                if not isinstance(detail, dict):
                    skipped_count += 1
                    continue
                
                card = detail.get('package_card', {})
                if not card or not isinstance(card, dict):
                    skipped_count += 1
                    continue

                header = card.get('card_header', {})
                order_ext = card.get('order_ext_info', {})
                status_info = card.get('status_info', {})
                payment = card.get('payment_info', {})
                fulfilment = card.get('fulfilment_info', {})
                package_ext = card.get('package_ext_info', {})
                item_group = card.get('item_info_group', {})

                order_id = order_ext.get('order_id')
                order_sn = header.get('order_sn')

                if not order_id or not order_sn:
                    skipped_count += 1
                    continue

                # 获取 shop_id 和 region_id
                order_from_list = order_id_map.get(str(order_id), {})
                shop_id = order_ext.get('shop_id') or order_from_list.get('shop_id')
                region_id = order_ext.get('region_id') or order_from_list.get('region_id')

                # 提取追踪号
                tracking_numbers = self._extract_tracking_numbers(fulfilment)
                tracking_number = ','.join(filter(None, tracking_numbers)) if tracking_numbers else ''

                # 获取买家信息（需要在 order_data 构建前获取 api_info）
                buyer_info_from_detail = header.get('buyer_info', {})
                if not buyer_info_from_detail:
                    buyer_info_from_detail = order_ext.get('buyer_info', {})

                # 获取买家用户 ID - 支持多种字段名，避免日期混入
                buyer_user_id = order_ext.get('buyer_user_id','')
                if not buyer_user_id:
                    buyer_user_id = buyer_info_from_detail.get('user_id')
                # 如果 buyer_user_id 看起来像日期（包含 - 或 /），则丢弃
                if buyer_user_id and isinstance(buyer_user_id, str) and ('/' in buyer_user_id or '-' in buyer_user_id and len(buyer_user_id) > 8):
                    logger.error(f"buyer_user_id error, order_ext:{str(order_ext)}, buyer_user_id:{str(buyer_user_id)}")

                api_info = api_buyer_info.get(buyer_user_id, {}) if api_buyer_info else {}

                # 订单数据
                order_data = {
                    'order_id': str(order_id),
                    'order_sn': order_sn,
                    'shop_id': shop_id,
                    'region_id': region_id,
                    'status': status_info.get('status', ''),
                    'fulfilment_channel': fulfilment.get('fulfilment_channel_name', ''),
                    'total_price': payment.get('total_price', 0) / 100,
                    'currency': 'MYR',
                    'shipping_name': package_ext.get('shipping_name', ''),
                    'shipping_phone': package_ext.get('shipping_phone', ''),
                    'shipping_address': package_ext.get('shipping_address', ''),
                    'tracking_number': tracking_number,
                    'buyer_user_id': order_ext.get('buyer_user_id', ''),
                    'rating': api_info.get('rating'),
                    'order_create_time': order_ext.get('ship_by_date'),
                }
                all_orders.append(order_data)

                # 商品数据
                items = self._extract_items(item_group)
                for item in items:
                    all_items.append({
                        'order_id': str(order_id),
                        'order_sn': order_sn,
                        'item': item
                    })

                # 买家数据（buyer_info_from_detail, buyer_user_id, api_info 已在前面获取）
                avatar = api_info.get('avatar') or api_info.get('portrait', '')
                buyer_username = buyer_info_from_detail.get('username', '')
                if not buyer_username:
                    buyer_username = api_info.get('username', '')

                buyer_data = {
                    'buyer_user_id': buyer_user_id,
                    'buyer_username': buyer_username,
                    'avatar': avatar,
                    'rating': api_info.get('rating'),
                    'country': api_info.get('country'),
                    'city': api_info.get('city'),
                }
                all_buyers.append({
                    'order_id': str(order_id),
                    'order_sn': order_sn,
                    'buyer_data': buyer_data,
                    'chat_data': chat_messages.get(str(order_id), {}) if chat_messages else {}
                })

            except Exception as e:
                logger.error(f"[ShopeeAllOrder] 解析订单数据失败: {e}")

        # 1. 先按 order_sn (主键) 检查已存在的订单
        order_sns = [str(o.get('order_sn')) for o in all_orders if o.get('order_sn')]

        existing_map = {}
        if order_sns:
            try:
                existing_map = self.database.check_orders_exist_batch(order_sns)
            except Exception as e:
                logger.error(f"[ShopeeAllOrder] 检查订单存在失败: {e}")
                existing_map = {}

        # 分类订单：已存在的和新的
        existing_orders = []  # 已存在的订单（需要更新）
        new_orders = []       # 新订单（需要插入）
        existing_items = []   # 已存在订单的商品（不写入）
        new_items = []        # 新订单的商品（需要写入）
        existing_buyers = [] # 已存在订单的买家（需要更新）
        new_buyers = []       # 新订单的买家（需要写入）

        for order in all_orders:
            order_sn = str(order.get('order_sn'))
            is_existing = existing_map.get(order_sn, False)

            if is_existing:
                existing_orders.append(order)
            else:
                new_orders.append(order)

        # 分离商品和买家数据
        for item in all_items:
            order_sn = item.get('order_sn')
            if existing_map.get(order_sn, False):
                existing_items.append(item)
            else:
                new_items.append(item)

        for buyer in all_buyers:
            order_sn = buyer.get('order_sn')
            if existing_map.get(order_sn, False):
                existing_buyers.append(buyer)
            else:
                new_buyers.append(buyer)

        logger.info(f"[ShopeeAllOrder] 总订单数: {len(all_orders)}, 新订单: {len(new_orders)}, 已存在: {len(existing_orders)}")

        saved_count = 0
        error_count = 0

        # 3. 批量保存/更新订单
        # 3.1 插入新订单
        if new_orders:
            try:
                saved_count = self.database.save_orders_batch_transaction(new_orders, env_name)
                logger.info(f"[ShopeeAllOrder] 新订单批量保存完成: {saved_count}")
            except Exception as e:
                error_count += len(new_orders)
                logger.error(f"[ShopeeAllOrder] 新订单批量保存失败: {e}")

        # 3.2 更新已存在的订单（仅更新，不插入）
        if existing_orders:
            try:
                updated_count = self.database.update_orders_batch(existing_orders, env_name)
                logger.info(f"[ShopeeAllOrder] 已存在订单批量更新完成: {updated_count}")
            except Exception as e:
                logger.error(f"[ShopeeAllOrder] 已存在订单批量更新失败: {e}")

        # 4. 批量保存商品（仅新订单）
        logger.info(f"[ShopeeAllOrder] 新订单商品数: {len(new_items)}, 已存在订单商品数: {len(existing_items)}")
        if new_items:
            try:
                items_by_order = {}
                for item in new_items:
                    order_id = item.get('order_id')
                    if order_id not in items_by_order:
                        items_by_order[order_id] = []
                    items_by_order[order_id].append(item.get('item'))
                logger.info(f"[ShopeeAllOrder] 新订单商品分组数: {len(items_by_order)}")

                for order_id, items in items_by_order.items():
                    order_sn = next((o.get('order_sn') for o in new_orders if str(o.get('order_id')) == order_id), '')
                    self.database.save_order_items_batch(order_id, order_sn, items)
                logger.info(f"[ShopeeAllOrder] 新订单商品批量保存完成: {len(new_items)}")
            except Exception as e:
                logger.error(f"[ShopeeAllOrder] 新订单商品批量保存失败: {e}")

        # 5. 批量保存/更新买家信息
        # 5.1 保存新订单买家
        if new_buyers:
            try:
                self.database.save_order_buyers_batch(new_buyers)
                logger.info(f"[ShopeeAllOrder] 新订单买家批量保存完成: {len(new_buyers)}")
            except Exception as e:
                logger.error(f"[ShopeeAllOrder] 新订单买家批量保存失败: {e}")

        # 5.2 更新已存在订单买家
        if existing_buyers:
            try:
                self.database.update_order_buyers_batch(existing_buyers)
                logger.info(f"[ShopeeAllOrder] 已存在订单买家批量更新完成: {len(existing_buyers)}")
            except Exception as e:
                logger.error(f"[ShopeeAllOrder] 已存在订单买家批量更新失败: {e}")

        logger.info(f"[ShopeeAllOrder] 数据库保存完成: 新订单保存 {len(new_orders)}, 已存在订单更新 {len(existing_orders)}, 失败 {error_count}")

    def teardown(self, driver: HubStudioSeleniumDriver, env_info: Dict[str, Any]):
        """后置操作：清理"""
        pass

    def on_error(self, driver: HubStudioSeleniumDriver, env_info: Dict[str, Any], error: Exception):
        """错误处理：截图保存"""
        try:
            error_dir = "./screenshots/errors"
            os.makedirs(error_dir, exist_ok=True)

            env_name = env_info.get('env_name', 'unknown')
            safe_name = env_name.replace(' ', '_').replace('/', '_')
            error_path = f"{error_dir}/{safe_name}_error.png"
            driver.screenshot(error_path)
            logger.info(f"[ShopeeAllOrder] 错误截图已保存: {error_path}")
        except Exception:
            pass


# 注册任务
TaskFactory.register(ShopeeAllOrderTask)
