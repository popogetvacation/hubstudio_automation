"""
浏览器网络请求模块
通过 CDP (Chrome DevTools Protocol) 发送请求，继承浏览器的 Cookies、代理等
支持网络监控、请求拦截和 API 捕获
"""
from typing import Optional, Dict, Any, Union, List
import json

from ..utils.logger import default_logger as logger

# 导入 CDP 模块
from .cdp_network import (
    CDPNetwork,
    CDPRequest,
    NetworkMonitor,
    NetworkInterceptor,
    Response as CDPResponse
)
from .event_listener import (
    NetworkEventListener,
    APICapturer,
    WebSocketMonitor
)


class Response:
    """
    响应数据

    兼容旧版 API，内部使用 CDP Response
    """
    def __init__(self, cdp_response: CDPResponse = None, **kwargs):
        """
        初始化

        Args:
            cdp_response: CDP Response 对象
            **kwargs: 直接设置属性（兼容旧版）
        """
        if cdp_response:
            self._cdp_response = cdp_response
            self.status_code = cdp_response.status
            self.headers = cdp_response.headers
            self.content = cdp_response.body or ""
            self.url = cdp_response.url
            self.elapsed = 0
        else:
            self._cdp_response = None
            self.status_code = kwargs.get('status_code', 0)
            self.headers = kwargs.get('headers', {})
            self.content = kwargs.get('content', '')
            self.url = kwargs.get('url', '')
            self.elapsed = kwargs.get('elapsed', 0)

        self._json_data = None

    def json(self) -> Any:
        """获取 JSON 数据"""
        if self._json_data is not None:
            return self._json_data
        try:
            self._json_data = json.loads(self.content)
            return self._json_data
        except json.JSONDecodeError:
            return None

    @property
    def text(self) -> str:
        """获取文本内容"""
        return self.content

    @property
    def ok(self) -> bool:
        """请求是否成功"""
        return 200 <= self.status_code < 300

    @property
    def cdp_response(self) -> Optional[CDPResponse]:
        """获取底层 CDP Response"""
        return self._cdp_response


class BrowserRequest:
    """
    浏览器网络请求类

    通过 CDP 发送 HTTP 请求，自动继承：
    - Cookies
    - 代理设置
    - User-Agent
    - 其他浏览器指纹

    支持功能：
    - 网络监控
    - 请求拦截
    - API 捕获
    - WebSocket 监控
    """

    def __init__(self, driver, default_timeout: int = 30):
        """
        初始化

        Args:
            driver: Selenium WebDriver 实例（支持 execute_cdp_cmd）
            default_timeout: 默认超时时间
        """
        self.driver = driver
        self.default_timeout = default_timeout

        # CDP 组件
        self._cdp_network = CDPNetwork(driver)
        self._cdp_request = CDPRequest(driver, default_timeout)
        self._monitor: Optional[NetworkEventListener] = None
        self._api_capturer: Optional[APICapturer] = None
        self._ws_monitor: Optional[WebSocketMonitor] = None

    # ==================== 请求方法 ====================

    def request(self, method: str, url: str,
                headers: Optional[Dict] = None,
                params: Optional[Dict] = None,
                data: Optional[Union[Dict, str]] = None,
                json_data: Optional[Any] = None,
                timeout: Optional[int] = None) -> Response:
        """
        发送 HTTP 请求

        Args:
            method: 请求方法 GET/POST/PUT/DELETE
            url: 请求 URL
            headers: 请求头
            params: URL 参数
            data: 请求体数据
            json_data: JSON 数据
            timeout: 超时时间

        Returns:
            Response 对象
        """
        try:
            cdp_response = self._cdp_request.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                data=data,
                json_data=json_data,
                timeout=timeout
            )
            return Response(cdp_response=cdp_response)
        except Exception as e:
            logger.error(f"请求失败: {method} {url}, 错误: {e}")
            raise

    def get(self, url: str, params: Optional[Dict] = None,
            headers: Optional[Dict] = None,
            timeout: Optional[int] = None) -> Response:
        """GET 请求"""
        return self.request('GET', url, params=params,
                           headers=headers, timeout=timeout)

    def post(self, url: str, data: Optional[Union[Dict, str]] = None,
             json_data: Optional[Any] = None,
             headers: Optional[Dict] = None,
             timeout: Optional[int] = None) -> Response:
        """POST 请求"""
        return self.request('POST', url, data=data,
                           json_data=json_data,
                           headers=headers, timeout=timeout)

    def put(self, url: str, data: Optional[Union[Dict, str]] = None,
            json_data: Optional[Any] = None,
            headers: Optional[Dict] = None,
            timeout: Optional[int] = None) -> Response:
        """PUT 请求"""
        return self.request('PUT', url, data=data,
                           json_data=json_data,
                           headers=headers, timeout=timeout)

    def delete(self, url: str, headers: Optional[Dict] = None,
               timeout: Optional[int] = None) -> Response:
        """DELETE 请求"""
        return self.request('DELETE', url, headers=headers, timeout=timeout)

    def patch(self, url: str, data: Optional[Union[Dict, str]] = None,
              json_data: Optional[Any] = None,
              headers: Optional[Dict] = None,
              timeout: Optional[int] = None) -> Response:
        """PATCH 请求"""
        return self.request('PATCH', url, data=data,
                           json_data=json_data,
                           headers=headers, timeout=timeout)

    # ==================== CDP 网络操作 ====================

    @property
    def cdp(self) -> CDPNetwork:
        """获取 CDP 网络操作对象"""
        return self._cdp_network

    def get_response_body(self, request_id: str) -> Optional[str]:
        """
        获取指定请求的响应体

        Args:
            request_id: 请求ID

        Returns:
            响应体内容
        """
        return self._cdp_network.get_response_body(request_id)

    def set_extra_headers(self, headers: Dict[str, str]):
        """
        设置额外的 HTTP 头（对所有后续请求生效）

        Args:
            headers: 请求头字典
        """
        self._cdp_network.set_extra_http_headers(headers)

    def set_user_agent(self, user_agent: str,
                       accept_language: str = None,
                       platform: str = None):
        """
        覆盖 User-Agent

        Args:
            user_agent: User-Agent 字符串
            accept_language: Accept-Language
            platform: Platform
        """
        self._cdp_network.set_user_agent_override(user_agent, accept_language, platform)

    # ==================== 网络监控 ====================

    def start_monitoring(self):
        """
        启动网络监控

        开始捕获所有网络请求和响应
        """
        if self._monitor is None:
            self._monitor = NetworkEventListener(self.driver)

        self._monitor.start()
        logger.info("[BrowserRequest] 网络监控已启动")

    def stop_monitoring(self):
        """停止网络监控"""
        if self._monitor:
            self._monitor.stop()

    def get_monitored_requests(self) -> List[Dict]:
        """获取监控到的所有请求"""
        if self._monitor:
            return self._monitor.get_all_requests()
        return []

    def get_monitored_responses(self) -> List[Dict]:
        """获取监控到的所有响应"""
        if self._monitor:
            return self._monitor.get_all_responses()
        return []

    def get_xhr_requests(self) -> List[Dict]:
        """获取所有 XHR 请求"""
        if self._monitor:
            return self._monitor.get_xhr_requests()
        return []

    # ==================== API 捕获 ====================

    def start_api_capture(self, url_filter: str = None):
        """
        开始捕获 API 请求

        Args:
            url_filter: URL 过滤器，只捕获包含此字符串的请求
        """
        if self._api_capturer is None:
            self._api_capturer = APICapturer(self.driver)

        self._api_capturer.start(url_filter)
        logger.info(f"[BrowserRequest] API 捕获已启动，过滤: {url_filter}")

    def stop_api_capture(self) -> List[Dict]:
        """
        停止捕获 API 请求

        Returns:
            捕获的 API 调用列表
        """
        if self._api_capturer:
            return self._api_capturer.stop()
        return []

    def get_captured_apis(self) -> List[Dict]:
        """获取捕获的 API 调用"""
        if self._api_capturer:
            return self._api_capturer.get_api_calls()
        return []

    def get_captured_json_responses(self) -> List[Dict]:
        """获取捕获的 JSON 响应"""
        if self._api_capturer:
            return self._api_capturer.get_json_responses()
        return []

    # ==================== WebSocket 监控 ====================

    def start_websocket_monitor(self):
        """开始监控 WebSocket"""
        if self._ws_monitor is None:
            self._ws_monitor = WebSocketMonitor(self.driver)

        self._ws_monitor.start()
        logger.info("[BrowserRequest] WebSocket 监控已启动")

    def stop_websocket_monitor(self) -> List[Dict]:
        """
        停止 WebSocket 监控

        Returns:
            WebSocket 消息列表
        """
        if self._ws_monitor:
            return self._ws_monitor.stop()
        return []

    def get_websocket_messages(self) -> List[Dict]:
        """获取 WebSocket 消息"""
        if self._ws_monitor:
            return self._ws_monitor.get_messages()
        return []

    # ==================== Cookie 操作 ====================

    def get_cookies(self, domain: Optional[str] = None) -> List[Dict]:
        """
        获取 Cookies

        Args:
            domain: 过滤域名

        Returns:
            Cookie 列表
        """
        cookies = self.driver.get_cookies()
        if domain:
            cookies = [c for c in cookies if domain in c.get('domain', '')]
        return cookies

    def set_cookies(self, cookies: List[Dict]):
        """
        设置 Cookies

        Args:
            cookies: Cookie 列表
        """
        for cookie in cookies:
            self.driver.add_cookie(cookie)

    def clear_cookies(self):
        """清除所有 Cookies"""
        self._cdp_network.clear_browser_cookies()

    # ==================== 页面操作 ====================

    def get_current_url(self) -> str:
        """获取当前 URL"""
        return self.driver.current_url

    def navigate_and_wait(self, url: str, wait_time: int = 10) -> bool:
        """
        导航并等待页面加载

        Args:
            url: 目标 URL
            wait_time: 等待时间

        Returns:
            是否成功
        """
        try:
            self.driver.get(url)
            from selenium.webdriver.support.ui import WebDriverWait
            WebDriverWait(self.driver, wait_time).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            return True
        except Exception as e:
            logger.error(f"导航失败: {url}, 错误: {e}")
            return False

    def fetch_page(self, url: str) -> str:
        """
        获取页面内容

        Args:
            url: 页面 URL

        Returns:
            页面源码
        """
        self.navigate_and_wait(url)
        return self.driver.page_source

    def fetch_json(self, url: str) -> Optional[Any]:
        """
        获取 JSON 数据

        Args:
            url: API URL

        Returns:
            JSON 数据
        """
        response = self.get(url)
        if response.ok:
            return response.json()
        return None

    # ==================== 文件下载 ====================

    def download_file(self, url: str, save_path: str) -> bool:
        """
        下载文件

        Args:
            url: 文件 URL
            save_path: 保存路径

        Returns:
            是否成功
        """
        try:
            response = self.get(url)
            if response.ok:
                with open(save_path, 'wb') as f:
                    # 处理可能的 base64 或二进制内容
                    content = response.content
                    if isinstance(content, str):
                        # 尝试检测是否为 base64
                        import base64
                        try:
                            decoded = base64.b64decode(content)
                            f.write(decoded)
                        except Exception:
                            f.write(content.encode())
                    else:
                        f.write(content)

                logger.info(f"文件已下载: {save_path}")
                return True
        except Exception as e:
            logger.error(f"下载文件失败: {url}, 错误: {e}")

        return False

    # ==================== 网络模拟 ====================

    def emulate_offline(self, offline: bool = True):
        """
        模拟离线状态

        Args:
            offline: 是否离线
        """
        self._cdp_network.emulate_network_conditions(offline=offline)

    def emulate_slow_network(self, latency_ms: int = 500,
                             download_kbps: int = 500,
                             upload_kbps: int = 500):
        """
        模拟慢速网络

        Args:
            latency_ms: 延迟（毫秒）
            download_kbps: 下载速度（KB/s）
            upload_kbps: 上传速度（KB/s）
        """
        self._cdp_network.emulate_network_conditions(
            latency=latency_ms,
            download_throughput=download_kbps * 1024,
            upload_throughput=upload_kbps * 1024
        )

    def reset_network_conditions(self):
        """重置网络条件"""
        self._cdp_network.emulate_network_conditions()