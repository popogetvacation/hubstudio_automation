"""
网络事件监听器
通过 Performance Log 捕获 CDP 网络事件
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass
import json
import time
import threading
from collections import defaultdict

from ..utils.logger import default_logger as logger


@dataclass
class NetworkEvent:
    """网络事件"""
    method: str
    params: Dict[str, Any]
    timestamp: float


class NetworkEventListener:
    """
    网络事件监听器

    通过 Chrome Performance Log 捕获 CDP 网络事件
    """

    def __init__(self, driver, poll_interval: float = 0.1):
        """
        初始化

        Args:
            driver: Selenium WebDriver 实例
            poll_interval: 轮询间隔（秒）
        """
        self.driver = driver
        self.poll_interval = poll_interval

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._events: List[NetworkEvent] = []
        self._lock = threading.Lock()

        # 存储请求和响应
        self._requests: Dict[str, Dict] = {}
        self._responses: Dict[str, Dict] = {}

        # 事件回调
        self._callbacks: Dict[str, List[Callable]] = defaultdict(list)

    def start(self):
        """启动事件监听"""
        if self._running:
            return

        # 启用 Performance 日志
        from selenium.webdriver.common.log import Log

        try:
            # 设置 Performance 日志
            self.driver.execute_cdp_cmd('Performance.enable', {})
            self.driver.execute_cdp_cmd('Network.enable', {})
        except Exception as e:
            logger.warning(f"启用 CDP 日志失败: {e}")

        self._running = True
        self._thread = threading.Thread(target=self._poll_events, daemon=True)
        self._thread.start()

        logger.info("[CDP] 网络事件监听已启动")

    def stop(self):
        """停止事件监听"""
        self._running = False

        if self._thread:
            self._thread.join(timeout=2)
            self._thread = None

        try:
            self.driver.execute_cdp_cmd('Network.disable', {})
            self.driver.execute_cdp_cmd('Performance.disable', {})
        except Exception:
            pass

        logger.info("[CDP] 网络事件监听已停止")

    def clear(self):
        """清除缓存的事件"""
        with self._lock:
            self._events.clear()
            self._requests.clear()
            self._responses.clear()

    def on_request(self, callback: Callable[[Dict], None]):
        """
        注册请求事件回调

        Args:
            callback: 回调函数，接收请求参数
        """
        self._callbacks['Network.requestWillBeSent'].append(callback)

    def on_response(self, callback: Callable[[Dict], None]):
        """
        注册响应事件回调

        Args:
            callback: 回调函数，接收响应参数
        """
        self._callbacks['Network.responseReceived'].append(callback)

    def on_loading_finished(self, callback: Callable[[Dict], None]):
        """注册加载完成回调"""
        self._callbacks['Network.loadingFinished'].append(callback)

    def on_websocket_frame(self, callback: Callable[[Dict], None]):
        """注册 WebSocket 帧回调"""
        self._callbacks['Network.webSocketFrameReceived'].append(callback)

    def get_request(self, request_id: str) -> Optional[Dict]:
        """获取请求"""
        return self._requests.get(request_id)

    def get_response(self, request_id: str) -> Optional[Dict]:
        """获取响应"""
        return self._responses.get(request_id)

    def get_all_requests(self) -> List[Dict]:
        """获取所有请求"""
        return list(self._requests.values())

    def get_all_responses(self) -> List[Dict]:
        """获取所有响应"""
        return list(self._responses.values())

    def get_xhr_requests(self) -> List[Dict]:
        """获取 XHR 请求"""
        return [r for r in self._requests.values()
                if r.get('type') in ('XHR', 'Fetch')]

    def get_api_calls(self, url_pattern: str = None) -> List[Dict]:
        """
        获取 API 调用

        Args:
            url_pattern: URL 匹配模式

        Returns:
            API 调用列表
        """
        api_calls = []

        for request_id, request in self._requests.items():
            if request.get('type') in ('XHR', 'Fetch'):
                response = self._responses.get(request_id)
                if url_pattern is None or url_pattern in request.get('url', ''):
                    api_calls.append({
                        'request': request,
                        'response': response
                    })

        return api_calls

    def _poll_events(self):
        """轮询事件"""
        while self._running:
            try:
                # 获取 Performance 日志
                logs = self.driver.get_log('performance')

                for log in logs:
                    try:
                        message = json.loads(log.get('message', '{}'))
                        method = message.get('message', {}).get('method', '')
                        params = message.get('message', {}).get('params', {})

                        if method.startswith('Network.'):
                            self._handle_event(method, params)

                    except json.JSONDecodeError:
                        continue

            except Exception as e:
                if self._running:
                    logger.debug(f"轮询事件错误: {e}")

            time.sleep(self.poll_interval)

    def _handle_event(self, method: str, params: Dict):
        """处理事件"""
        event = NetworkEvent(
            method=method,
            params=params,
            timestamp=time.time()
        )

        with self._lock:
            self._events.append(event)

        # 处理请求发送
        if method == 'Network.requestWillBeSent':
            request_id = params.get('requestId')
            if request_id:
                self._requests[request_id] = params

        # 处理响应接收
        elif method == 'Network.responseReceived':
            request_id = params.get('requestId')
            if request_id:
                self._responses[request_id] = params

        # 触发回调
        for callback in self._callbacks.get(method, []):
            try:
                callback(params)
            except Exception as e:
                logger.error(f"回调执行错误: {e}")


class APICapturer:
    """
    API 捕获器

    使用 CDP Fetch 域拦截 XHR/Fetch API 请求和响应
    """

    def __init__(self, driver):
        """
        初始化

        Args:
            driver: Selenium WebDriver 实例
        """
        self.driver = driver
        self._api_calls: List[Dict] = []
        self._url_filter = None
        self._enabled = False
        self._intercepted_request_ids = set()  # 已拦截的 request_id

    def start(self, url_filter: str = None):
        """
        开始捕获

        Args:
            url_filter: URL 过滤器
        """
        self._url_filter = url_filter
        self._api_calls.clear()
        self._intercepted_request_ids.clear()

        # 使用 JavaScript 注入方式监听网络请求
        try:
            inject_js = """
(function() {
    if (window.__api_capturer__) {
        window.__api_capturer__.requests = [];
        return 'Reset existing capturer';
    }

    window.__api_capturer__ = {
        requests: [],
        originalFetch: window.fetch,
        originalXHR: window.XMLHttpRequest
    };

    // 重写 fetch
    window.fetch = function(input, init) {
        var startTime = Date.now();
        var url = typeof input === 'string' ? input : input.url;
        var method = (init && init.method) || 'GET';

        var requestRecord = {
            id: 'fetch_' + startTime,
            url: url,
            method: method,
            headers: (init && init.headers) || {},
            body: (init && init.body) || null,
            startTime: startTime
        };
        window.__api_capturer__.requests.push(requestRecord);

        return window.__api_capturer__.originalFetch.call(this, input, init).then(function(response) {
            var clonedResponse = response.clone();
            return clonedResponse.text().then(function(bodyText) {
                requestRecord.response = {
                    status: response.status,
                    statusText: response.statusText,
                    headers: {},
                    body: bodyText
                };
                return response;
            });
        });
    };

    // 重写 XMLHttpRequest
    var OriginalXHR = window.XMLHttpRequest;
    var XHRProxy = new Proxy(OriginalXHR, function(target) {
        return new Proxy(target, {
            construct: function(Target, args) {
                var xhr = new Target(...args);
                var url = typeof args[0] === 'string' ? args[0] : args[0].toString();
                var method = 'GET';
                var startTime = Date.now();

                var requestRecord = {
                    id: 'xhr_' + startTime,
                    url: url,
                    method: method,
                    headers: {},
                    body: null,
                    startTime: startTime
                };

                xhr.addEventListener('readystatechange', function() {
                    if (xhr.readyState === 1) {
                        method = xhr._method || method;
                        requestRecord.method = method;
                    }
                    if (xhr.readyState === 2) {
                        requestRecord.response = {
                            status: xhr.status,
                            statusText: xhr.statusText,
                            headers: {},
                            body: null
                        };
                    }
                    if (xhr.readyState === 4) {
                        try {
                            requestRecord.response.body = xhr.responseText;
                        } catch(e) {}
                    }
                });

                window.__api_capturer__.requests.push(requestRecord);
                return xhr;
            }
        });
    });

    window.XMLHttpRequest = XHRProxy;

    return 'API capturer initialized';
})();
"""
            self.driver.execute_script(inject_js)
            self._enabled = True
            logger.info(f"[API Capturer] 开始捕获（JavaScript 注入），过滤: {url_filter}")
        except Exception as e:
            logger.error(f"[API Capturer] 注入网络监听失败: {e}")
            raise

    def stop(self) -> List[Dict]:
        """
        停止捕获

        Returns:
            捕获的 API 调用列表
        """
        if self._enabled:
            # 获取所有捕获的请求
            self._capture_all_requests()

            try:
                # 清理注入的代码
                cleanup_js = """
if (window.__api_capturer__) {
    window.fetch = window.__api_capturer__.originalFetch;
    window.XMLHttpRequest = window.__api_capturer__.originalXHR;
    delete window.__api_capturer__;
    return 'API capturer cleaned';
}
return 'No capturer found';
"""
                self.driver.execute_script(cleanup_js)
                self._enabled = False
                logger.info("[API Capturer] 停止捕获")
            except Exception as e:
                logger.warning(f"[API Capturer] 清理失败: {e}")
        return self._api_calls

    def get_api_calls(self) -> List[Dict]:
        """
        获取捕获的 API 调用

        Returns:
            捕获的 API 调用列表
        """
        self._capture_all_requests()
        return self._api_calls

    def get_json_responses(self) -> List[Dict]:
        """获取 JSON 响应"""
        results = []
        for call in self._api_calls:
            response = call.get('response', {})
            body = call.get('body')
            if body:
                try:
                    json_data = json.loads(body) if isinstance(body, str) else body
                    results.append({
                        'url': call.get('url'),
                        'status': response.get('status'),
                        'data': json_data
                    })
                except json.JSONDecodeError:
                    pass
        return results

    def _capture_all_requests(self):
        """
        从 JavaScript 获取所有捕获的请求
        """
        try:
            requests_data = self.driver.execute_script("""
                if (window.__api_capturer__) {
                    var result = [];
                    for (var i = 0; i < window.__api_capturer__.requests.length; i++) {
                        var req = window.__api_capturer__.requests[i];
                        if (req.response) {
                            result.push({
                                id: req.id,
                                url: req.url,
                                method: req.method,
                                request: {
                                    headers: req.headers,
                                    body: req.body
                                },
                                response: req.response,
                                timestamp: req.startTime / 1000
                            });
                        }
                    }
                    return result;
                }
                return [];
            """)

            # 过滤并添加到结果
            for req in requests_data:
                url = req.get('url', '')
                request_id = req.get('id')

                # 跳过已处理的
                if request_id in self._intercepted_request_ids:
                    continue

                # URL 过滤
                if self._url_filter and self._url_filter not in url:
                    self._intercepted_request_ids.add(request_id)
                    continue

                # 组装 API 调用
                api_call = {
                    'request_id': request_id,
                    'url': url,
                    'method': req.get('method', 'GET'),
                    'request': req.get('request', {}),
                    'response': {
                        'status_code': req.get('response', {}).get('status', 0),
                        'status_text': req.get('response', {}).get('statusText', ''),
                        'headers': req.get('response', {}).get('headers', {}),
                        'mime_type': ''
                    },
                    'body': req.get('response', {}).get('body'),
                    'timestamp': req.get('timestamp', time.time())
                }

                self._api_calls.append(api_call)
                self._intercepted_request_ids.add(request_id)

                logger.debug(f"[API Capturer] 捕获: {api_call['method']} {url} -> {api_call['response']['status_code']}")

        except Exception as e:
            logger.debug(f"[API Capturer] 获取捕获请求失败: {e}")


class WebSocketMonitor:
    """
    WebSocket 监控器

    监控 WebSocket 连接和消息
    """

    def __init__(self, driver):
        """
        初始化

        Args:
            driver: Selenium WebDriver 实例
        """
        self.driver = driver
        self.listener = NetworkEventListener(driver)
        self._messages: List[Dict] = []
        self._connections: Dict[str, Dict] = {}

    def start(self):
        """开始监控"""
        self._messages.clear()
        self._connections.clear()

        # 注册回调
        self.listener.on_request(self._on_request)
        self.listener.on_websocket_frame(self._on_ws_frame)

        # 启动监听
        self.listener.start()

        # 启用 WebSocket 监控
        try:
            self.driver.execute_cdp_cmd('Network.enableWebSocketMonitoring', {})
        except Exception:
            pass

        logger.info("[WebSocket Monitor] 开始监控")

    def stop(self) -> List[Dict]:
        """停止监控"""
        self.listener.stop()
        return self._messages

    def get_messages(self, direction: str = None) -> List[Dict]:
        """
        获取消息

        Args:
            direction: 方向过滤 (sent/received)

        Returns:
            消息列表
        """
        if direction:
            return [m for m in self._messages if m.get('direction') == direction]
        return self._messages

    def get_connections(self) -> Dict[str, Dict]:
        """获取 WebSocket 连接"""
        return self._connections

    def _on_request(self, params: Dict):
        """请求回调"""
        request = params.get('request', {})
        url = request.get('url', '')

        if url.startswith('ws://') or url.startswith('wss://'):
            request_id = params.get('requestId')
            self._connections[request_id] = {
                'url': url,
                'request_id': request_id,
                'connected_at': time.time()
            }
            logger.info(f"[WebSocket Monitor] 新连接: {url}")

    def _on_ws_frame(self, params: Dict):
        """WebSocket 帧回调"""
        request_id = params.get('requestId')
        response = params.get('response', {})

        message = {
            'request_id': request_id,
            'opcode': response.get('opcode'),
            'mask': response.get('mask', False),
            'data': response.get('payloadData', ''),
            'timestamp': params.get('timestamp', time.time()),
            'direction': 'received'
        }

        self._messages.append(message)

        logger.debug(f"[WebSocket Monitor] 收到消息: {message['data'][:100]}")