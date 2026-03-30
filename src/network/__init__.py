"""
网络模块
提供基于 CDP 的网络请求、监控和拦截功能
"""
from .browser_request import BrowserRequest, Response
from .cdp_network import (
    CDPNetwork,
    CDPRequest,
    NetworkMonitor,
    NetworkInterceptor,
    Request,
    Response as CDPResponse,
    ResourceType,
    WebSocketFrame
)
from .event_listener import (
    NetworkEventListener,
    APICapturer,
    WebSocketMonitor,
    NetworkEvent
)

__all__ = [
    # 主要接口
    'BrowserRequest',
    'Response',

    # CDP 网络操作
    'CDPNetwork',
    'CDPRequest',
    'NetworkMonitor',
    'NetworkInterceptor',

    # 数据类
    'Request',
    'CDPResponse',
    'ResourceType',
    'WebSocketFrame',
    'NetworkEvent',

    # 事件监听
    'NetworkEventListener',
    'APICapturer',
    'WebSocketMonitor'
]