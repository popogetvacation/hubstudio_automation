"""
Chrome DevTools Protocol (CDP) 网络模块
使用 CDP 进行网络请求拦截、监控和数据捕获
"""
from typing import Optional, Dict, Any, List, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import json
import time
import threading
import queue
from collections import defaultdict

from ..utils.logger import default_logger as logger


class ResourceType(Enum):
    """资源类型"""
    DOCUMENT = "Document"
    STYLESHEET = "Stylesheet"
    IMAGE = "Image"
    MEDIA = "Media"
    FONT = "Font"
    SCRIPT = "Script"
    TEXT_TRACK = "TextTrack"
    XHR = "XHR"
    FETCH = "Fetch"
    EVENT_SOURCE = "EventSource"
    WEB_SOCKET = "WebSocket"
    MANIFEST = "Manifest"
    SIGNED_EXCHANGE = "SignedExchange"
    PING = "Ping"
    CSP_VIOLATION_REPORT = "CSPViolationReport"
    OTHER = "Other"


@dataclass
class Request:
    """请求数据"""
    request_id: str
    url: str
    method: str
    headers: Dict[str, str]
    post_data: Optional[str] = None
    resource_type: str = ""
    frame_id: str = ""
    loader_id: str = ""
    timestamp: float = 0
    wall_time: float = 0

    def to_dict(self) -> Dict:
        return {
            'request_id': self.request_id,
            'url': self.url,
            'method': self.method,
            'headers': self.headers,
            'post_data': self.post_data,
            'resource_type': self.resource_type,
            'timestamp': self.timestamp
        }


@dataclass
class Response:
    """响应数据"""
    request_id: str
    url: str
    status: int
    status_text: str
    headers: Dict[str, str]
    mime_type: str = ""
    body: Optional[str] = None
    body_base64: bool = False
    request: Optional[Request] = None
    timing: Dict = field(default_factory=dict)
    protocol: str = ""
    remote_address: str = ""
    security_state: str = ""

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    def json(self) -> Any:
        if self.body:
            try:
                return json.loads(self.body)
            except json.JSONDecodeError:
                pass
        return None

    @property
    def text(self) -> str:
        return self.body or ""

    def to_dict(self) -> Dict:
        return {
            'request_id': self.request_id,
            'url': self.url,
            'status': self.status,
            'status_text': self.status_text,
            'headers': self.headers,
            'mime_type': self.mime_type,
            'body': self.body[:500] if self.body else None  # 截断显示
        }


@dataclass
class WebSocketFrame:
    """WebSocket 帧"""
    request_id: str
    timestamp: float
    opcode: int
    mask: bool
    data: str
    is_binary: bool = False


class NetworkMonitor:
    """
    网络监控器

    使用 CDP Network 域监控所有网络请求
    """

    def __init__(self, driver):
        """
        初始化

        Args:
            driver: Selenium WebDriver 实例（需支持 execute_cdp_cmd）
        """
        self.driver = driver
        self._enabled = False
        self._lock = threading.Lock()

        # 存储请求和响应
        self._requests: Dict[str, Request] = {}
        self._responses: Dict[str, Response] = {}
        self._web_socket_frames: List[WebSocketFrame] = []

        # 事件回调
        self._on_request_callbacks: List[Callable] = []
        self._on_response_callbacks: List[Callable] = []
        self._on_websocket_callbacks: List[Callable] = []

        # 请求过滤
        self._url_filters: List[str] = []
        self._resource_type_filters: List[str] = []

    def enable(self, capture_headers: bool = True,
               capture_body: bool = True,
               capture_ws: bool = True):
        """
        启用网络监控

        Args:
            capture_headers: 是否捕获请求头
            capture_body: 是否捕获请求体
            capture_ws: 是否捕获 WebSocket
        """
        with self._lock:
            if self._enabled:
                return

            # 启用 Network 域
            self.driver.execute_cdp_cmd('Network.enable', {
                'maxPostDataSize': 65536 if capture_body else 0
            })

            # 启用 WebSocket 监控
            if capture_ws:
                self.driver.execute_cdp_cmd('Network.enableWebSocketMonitoring', {})

            self._enabled = True
            logger.info("[CDP] 网络监控已启用")

    def disable(self):
        """禁用网络监控"""
        with self._lock:
            if not self._enabled:
                return

            try:
                self.driver.execute_cdp_cmd('Network.disable', {})
            except Exception:
                pass

            self._enabled = False
            logger.info("[CDP] 网络监控已禁用")

    def clear(self):
        """清除所有缓存的请求和响应"""
        with self._lock:
            self._requests.clear()
            self._responses.clear()
            self._web_socket_frames.clear()

    def start_capture(self):
        """
        开始捕获网络请求

        通过 CDP 事件监听来捕获
        """
        # 使用 execute_cdp_cmd 监听事件
        # Selenium 需要通过 execute_cdp_cmd 来获取事件
        pass

    def get_request(self, request_id: str) -> Optional[Request]:
        """获取请求"""
        return self._requests.get(request_id)

    def get_response(self, request_id: str) -> Optional[Response]:
        """获取响应"""
        return self._responses.get(request_id)

    def get_all_requests(self, url_filter: str = None,
                         resource_type: str = None) -> List[Request]:
        """
        获取所有请求

        Args:
            url_filter: URL 过滤（包含此字符串）
            resource_type: 资源类型过滤

        Returns:
            请求列表
        """
        requests = list(self._requests.values())

        if url_filter:
            requests = [r for r in requests if url_filter in r.url]

        if resource_type:
            requests = [r for r in requests if r.resource_type == resource_type]

        return requests

    def get_all_responses(self, url_filter: str = None,
                          status_code: int = None) -> List[Response]:
        """
        获取所有响应

        Args:
            url_filter: URL 过滤
            status_code: 状态码过滤

        Returns:
            响应列表
        """
        responses = list(self._responses.values())

        if url_filter:
            responses = [r for r in responses if url_filter in r.url]

        if status_code:
            responses = [r for r in responses if r.status == status_code]

        return responses

    def get_xhr_requests(self) -> List[Request]:
        """获取所有 XHR 请求"""
        return self.get_all_requests(resource_type="XHR")

    def get_api_requests(self) -> List[Request]:
        """获取所有 API 请求（XHR + Fetch）"""
        requests = list(self._requests.values())
        return [r for r in requests if r.resource_type in ("XHR", "Fetch")]

    def add_url_filter(self, url_pattern: str):
        """添加 URL 过滤器"""
        self._url_filters.append(url_pattern)

    def add_resource_type_filter(self, resource_type: str):
        """添加资源类型过滤器"""
        self._resource_type_filters.append(resource_type)

    def on_request(self, callback: Callable[[Request], None]):
        """注册请求回调"""
        self._on_request_callbacks.append(callback)

    def on_response(self, callback: Callable[[Response], None]):
        """注册响应回调"""
        self._on_response_callbacks.append(callback)

    def on_websocket_message(self, callback: Callable[[WebSocketFrame], None]):
        """注册 WebSocket 消息回调"""
        self._on_websocket_callbacks.append(callback)

    # ==================== 内部方法 ====================

    def _handle_request_will_be_sent(self, params: Dict):
        """处理请求发送事件"""
        request_id = params.get('requestId')
        request_data = params.get('request', {})
        timestamp = params.get('timestamp', 0)

        request = Request(
            request_id=request_id,
            url=request_data.get('url', ''),
            method=request_data.get('method', 'GET'),
            headers=request_data.get('headers', {}),
            post_data=request_data.get('postData'),
            resource_type=params.get('type', ''),
            frame_id=params.get('frameId', ''),
            loader_id=params.get('loaderId', ''),
            timestamp=timestamp,
            wall_time=params.get('wallTime', 0)
        )

        self._requests[request_id] = request

        # 触发回调
        for callback in self._on_request_callbacks:
            try:
                callback(request)
            except Exception as e:
                logger.error(f"请求回调执行错误: {e}")

    def _handle_response_received(self, params: Dict):
        """处理响应接收事件"""
        request_id = params.get('requestId')
        response_data = params.get('response', {})

        request = self._requests.get(request_id)

        response = Response(
            request_id=request_id,
            url=response_data.get('url', ''),
            status=response_data.get('status', 0),
            status_text=response_data.get('statusText', ''),
            headers=response_data.get('headers', {}),
            mime_type=response_data.get('mimeType', ''),
            timing=response_data.get('timing', {}),
            protocol=response_data.get('protocol', ''),
            remote_address=f"{response_data.get('remoteIPAddress', '')}:{response_data.get('remotePort', '')}",
            security_state=response_data.get('securityState', ''),
            request=request
        )

        self._responses[request_id] = response

    def _handle_loading_finished(self, params: Dict):
        """处理请求完成事件"""
        pass

    def _handle_websocket_frame_received(self, params: Dict):
        """处理 WebSocket 帧接收事件"""
        request_id = params.get('requestId')
        frame_data = params.get('response', {})

        frame = WebSocketFrame(
            request_id=request_id,
            timestamp=params.get('timestamp', 0),
            opcode=frame_data.get('opcode', 0),
            mask=frame_data.get('mask', False),
            data=frame_data.get('payloadData', '')
        )

        self._web_socket_frames.append(frame)

        for callback in self._on_websocket_callbacks:
            try:
                callback(frame)
            except Exception as e:
                logger.error(f"WebSocket 回调执行错误: {e}")


class NetworkInterceptor:
    """
    网络拦截器

    使用 CDP Fetch 域拦截和修改请求
    """

    def __init__(self, driver):
        """
        初始化

        Args:
            driver: Selenium WebDriver 实例
        """
        self.driver = driver
        self._enabled = False
        self._patterns: List[Dict] = []
        self._request_handlers: Dict[str, Callable] = {}
        self._intercepted_requests: Dict[str, Dict] = {}

    def enable(self):
        """启用请求拦截"""
        if self._enabled:
            return

        if self._patterns:
            self.driver.execute_cdp_cmd('Fetch.enable', {
                'patterns': self._patterns
            })
        else:
            # 默认拦截所有请求
            self.driver.execute_cdp_cmd('Fetch.enable', {
                'patterns': [{'urlPattern': '*'}]
            })

        self._enabled = True
        logger.info("[CDP] 请求拦截已启用")

    def disable(self):
        """禁用请求拦截"""
        if not self._enabled:
            return

        try:
            self.driver.execute_cdp_cmd('Fetch.disable', {})
        except Exception:
            pass

        self._enabled = False
        logger.info("[CDP] 请求拦截已禁用")

    def add_pattern(self, url_pattern: str = "*",
                    request_stage: str = "Request",
                    resource_type: str = None):
        """
        添加拦截模式

        Args:
            url_pattern: URL 匹配模式
            request_stage: 拦截阶段 (Request/Response)
            resource_type: 资源类型
        """
        pattern = {
            'urlPattern': url_pattern,
            'requestStage': request_stage
        }
        if resource_type:
            pattern['resourceType'] = resource_type

        self._patterns.append(pattern)

    def on_request(self, url_pattern: str,
                   handler: Callable[[Dict], Optional[Dict]]):
        """
        注册请求处理函数

        Args:
            url_pattern: URL 匹配模式
            handler: 处理函数，返回修改后的请求配置或 None 继续原始请求
        """
        self._request_handlers[url_pattern] = handler

    def continue_request(self, request_id: str,
                         url: str = None,
                         method: str = None,
                         headers: Dict = None,
                         post_data: str = None):
        """
        继续请求

        Args:
            request_id: 请求ID
            url: 修改后的 URL
            method: 修改后的方法
            headers: 修改后的请求头
            post_data: 修改后的请求体
        """
        params = {'requestId': request_id}

        if url:
            params['url'] = url
        if method:
            params['method'] = method
        if headers:
            params['headers'] = [{'name': k, 'value': v} for k, v in headers.items()]
        if post_data:
            params['postData'] = post_data

        self.driver.execute_cdp_cmd('Fetch.continueRequest', params)

    def fulfill_request(self, request_id: str,
                        status_code: int = 200,
                        headers: Dict = None,
                        body: str = None):
        """
        直接返回响应（不发送实际请求）

        Args:
            request_id: 请求ID
            status_code: 状态码
            headers: 响应头
            body: 响应体
        """
        params = {
            'requestId': request_id,
            'responseCode': status_code
        }

        if headers:
            params['responseHeaders'] = [{'name': k, 'value': v} for k, v in headers.items()]
        if body:
            import base64
            params['body'] = base64.b64encode(body.encode()).decode()

        self.driver.execute_cdp_cmd('Fetch.fulfillRequest', params)

    def fail_request(self, request_id: str, error_reason: str = "Failed"):
        """
        使请求失败

        Args:
            request_id: 请求ID
            error_reason: 错误原因
        """
        self.driver.execute_cdp_cmd('Fetch.failRequest', {
            'requestId': request_id,
            'errorReason': error_reason
        })

    def handle_request_paused(self, params: Dict):
        """
        处理请求暂停事件

        Args:
            params: 事件参数
        """
        request_id = params.get('requestId')
        request = params.get('request', {})
        url = request.get('url', '')

        # 查找匹配的处理器
        for pattern, handler in self._request_handlers.items():
            if pattern in url or pattern == '*':
                try:
                    result = handler(params)
                    if result:
                        # 修改请求
                        self.continue_request(
                            request_id,
                            url=result.get('url'),
                            method=result.get('method'),
                            headers=result.get('headers'),
                            post_data=result.get('post_data')
                        )
                        return
                except Exception as e:
                    logger.error(f"请求处理器执行错误: {e}")

        # 继续原始请求
        self.continue_request(request_id)


class CDPNetwork:
    """
    CDP 网络操作类

    提供统一的 CDP 网络操作接口
    """

    def __init__(self, driver):
        """
        初始化

        Args:
            driver: Selenium WebDriver 实例
        """
        self.driver = driver
        self.monitor = NetworkMonitor(driver)
        self.interceptor = NetworkInterceptor(driver)

    def get_response_body(self, request_id: str) -> Optional[str]:
        """
        获取响应体

        Args:
            request_id: 请求ID

        Returns:
            响应体内容
        """
        try:
            result = self.driver.execute_cdp_cmd('Network.getResponseBody', {
                'requestId': request_id
            })
            return result.get('body')
        except Exception as e:
            logger.error(f"获取响应体失败: {e}")
            return None

    def get_request_post_data(self, request_id: str) -> Optional[str]:
        """
        获取请求体

        Args:
            request_id: 请求ID

        Returns:
            请求体内容
        """
        try:
            result = self.driver.execute_cdp_cmd('Network.getRequestPostData', {
                'requestId': request_id
            })
            return result.get('postData')
        except Exception as e:
            logger.error(f"获取请求体失败: {e}")
            return None

    def set_extra_http_headers(self, headers: Dict[str, str]):
        """
        设置额外的 HTTP 头

        Args:
            headers: 请求头字典
        """
        self.driver.execute_cdp_cmd('Network.setExtraHTTPHeaders', {
            'headers': headers
        })

    def set_user_agent_override(self, user_agent: str,
                                 accept_language: str = None,
                                 platform: str = None):
        """
        覆盖 User-Agent

        Args:
            user_agent: User-Agent 字符串
            accept_language: Accept-Language
            platform: Platform
        """
        params = {'userAgent': user_agent}
        if accept_language:
            params['acceptLanguage'] = accept_language
        if platform:
            params['platform'] = platform

        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', params)

    def set_cookie(self, name: str, value: str,
                   domain: str = None,
                   path: str = "/",
                   secure: bool = False,
                   http_only: bool = False,
                   same_site: str = None,
                   expires: int = None):
        """
        设置 Cookie

        Args:
            name: Cookie 名称
            value: Cookie 值
            domain: 域名
            path: 路径
            secure: 是否安全
            http_only: 是否 HttpOnly
            same_site: SameSite 属性
            expires: 过期时间
        """
        params = {
            'name': name,
            'value': value,
            'path': path,
            'secure': secure,
            'httpOnly': http_only
        }
        if domain:
            params['domain'] = domain
        if same_site:
            params['sameSite'] = same_site
        if expires:
            params['expires'] = expires

        self.driver.execute_cdp_cmd('Network.setCookie', params)

    def get_cookies(self, urls: List[str] = None) -> List[Dict]:
        """
        获取 Cookies

        Args:
            urls: URL 列表

        Returns:
            Cookie 列表
        """
        params = {}
        if urls:
            params['urls'] = urls

        result = self.driver.execute_cdp_cmd('Network.getCookies', params)
        return result.get('cookies', [])

    def clear_browser_cache(self):
        """清除浏览器缓存"""
        self.driver.execute_cdp_cmd('Network.clearBrowserCache', {})

    def clear_browser_cookies(self):
        """清除浏览器 Cookies"""
        self.driver.execute_cdp_cmd('Network.clearBrowserCookies', {})

    def emulate_network_conditions(self, offline: bool = False,
                                    latency: int = 0,
                                    download_throughput: int = -1,
                                    upload_throughput: int = -1):
        """
        模拟网络条件

        Args:
            offline: 是否离线
            latency: 延迟(ms)
            download_throughput: 下载速度(bytes/s), -1 表示不限
            upload_throughput: 上传速度(bytes/s), -1 表示不限
        """
        self.driver.execute_cdp_cmd('Network.emulateNetworkConditions', {
            'offline': offline,
            'latency': latency,
            'downloadThroughput': download_throughput,
            'uploadThroughput': upload_throughput
        })


class CDPRequest:
    """
    CDP 请求发送类

    使用 Fetch API 通过浏览器发送请求，自动继承 Cookies 和代理
    """

    def __init__(self, driver, default_timeout: int = 30):
        """
        初始化

        Args:
            driver: Selenium WebDriver 实例
            default_timeout: 默认超时时间
        """
        self.driver = driver
        self.default_timeout = default_timeout
        self.cdp = CDPNetwork(driver)

    def request(self, method: str, url: str,
                headers: Optional[Dict] = None,
                params: Optional[Dict] = None,
                data: Optional[Union[Dict, str]] = None,
                json_data: Optional[Any] = None,
                timeout: Optional[int] = None) -> Response:
        """
        发送 HTTP 请求

        使用 Fetch API 在浏览器上下文中发送请求

        Args:
            method: 请求方法
            url: 请求 URL
            headers: 请求头
            params: URL 参数
            data: 请求体数据
            json_data: JSON 数据
            timeout: 超时时间

        Returns:
            Response 对象
        """
        timeout = timeout or self.default_timeout

        # 构建 URL
        if params:
            query_string = '&'.join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query_string}" if '?' not in url else f"{url}&{query_string}"

        # 构建请求体
        body = None
        request_headers = headers or {}

        if json_data is not None:
            body = json.dumps(json_data)
            request_headers['Content-Type'] = 'application/json'
        elif data is not None:
            if isinstance(data, dict):
                body = '&'.join(f"{k}={v}" for k, v in data.items())
                request_headers['Content-Type'] = 'application/x-www-form-urlencoded'
            else:
                body = str(data)

        # 使用 Fetch API 发送请求
        fetch_options = {
            'method': method.upper(),
            'url': url,
            'credentials': 'include',  # 包含 cookies
        }

        if request_headers:
            fetch_options['headers'] = request_headers
        if body:
            fetch_options['body'] = body

        # 执行 Fetch 请求
        js_code = f"""
        return (async () => {{
            try {{
                const response = await fetch('{url}', {{
                    method: '{method.upper()}',
                    headers: {json.dumps(request_headers)},
                    body: {json.dumps(body) if body else 'undefined'},
                    credentials: 'include'
                }});

                const headers = {{}};
                response.headers.forEach((value, key) => {{
                    headers[key] = value;
                }});

                let body;
                const contentType = response.headers.get('content-type') || '';
                if (contentType.includes('application/json')) {{
                    body = await response.text();
                }} else {{
                    body = await response.text();
                }}

                return {{
                    status: response.status,
                    statusText: response.statusText,
                    headers: headers,
                    url: response.url,
                    body: body
                }};
            }} catch (error) {{
                return {{ error: error.message }};
            }}
        }})();
        """

        start_time = time.time()

        try:
            result = self.driver.execute_script(js_code)
            elapsed = time.time() - start_time

            if result is None:
                raise Exception("Request returned no result")

            if result.get('error'):
                raise Exception(result.get('error'))

            return Response(
                request_id="",  # 不是通过 CDP Network 发送的
                url=result.get('url', url),
                status=result.get('status', 0),
                status_text=result.get('statusText', ''),
                headers=result.get('headers', {}),
                body=result.get('body', ''),
                mime_type=result.get('headers', {}).get('content-type', '')
            )

        except Exception as e:
            logger.error(f"请求失败: {method} {url}, 错误: {e}")
            raise

    def get(self, url: str, params: Optional[Dict] = None,
            headers: Optional[Dict] = None,
            timeout: Optional[int] = None) -> Response:
        """GET 请求"""
        return self.request('GET', url, params=params, headers=headers, timeout=timeout)

    def post(self, url: str, data: Optional[Union[Dict, str]] = None,
             json_data: Optional[Any] = None,
             headers: Optional[Dict] = None,
             timeout: Optional[int] = None) -> Response:
        """POST 请求"""
        return self.request('POST', url, data=data, json_data=json_data,
                           headers=headers, timeout=timeout)

    def put(self, url: str, data: Optional[Union[Dict, str]] = None,
            json_data: Optional[Any] = None,
            headers: Optional[Dict] = None,
            timeout: Optional[int] = None) -> Response:
        """PUT 请求"""
        return self.request('PUT', url, data=data, json_data=json_data,
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
        return self.request('PATCH', url, data=data, json_data=json_data,
                           headers=headers, timeout=timeout)