"""
HubStudio API 客户端
封装所有 HubStudio API 调用
"""
import requests
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
import time


@dataclass
class BrowserInfo:
    """浏览器环境信息"""
    browser_id: str
    debug_port: int
    ws_endpoint: str
    webdriver_path: str = ""  # HubStudio 自带的 webdriver 路径
    browser_path: str = ""
    download_path: str = ""


@dataclass
class EnvironmentInfo:
    """环境信息"""
    env_id: str
    env_name: str
    group_code: str
    code_seq_id: str
    status: int  # 0-关闭, 1-开启
    proxy_info: Optional[Dict] = None


class HubStudioAPIError(Exception):
    """HubStudio API 错误"""
    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super().__init__(f"API Error [{code}]: {message}")


class HubStudioClient:
    """
    HubStudio API 客户端

    用于与 HubStudio 本地客户端通信，管理浏览器环境
    """

    def __init__(self, api_url: str, api_key: str, timeout: int = 30):
        """
        初始化客户端

        Args:
            api_url: API 基础地址，如 http://127.0.0.1:9889
            api_key: API Key
            timeout: 请求超时时间
        """
        self.api_url = api_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'api-key-hubstudio': api_key
        })

    def _request(self, method: str, endpoint: str,
                 params: Optional[Dict] = None,
                 data: Optional[Dict] = None) -> Dict:
        """
        发送请求

        Args:
            method: 请求方法 GET/POST
            endpoint: API 端点
            params: URL 参数
            data: 请求体数据

        Returns:
            API 响应数据

        Raises:
            HubStudioAPIError: API 错误
        """
        url = f"{self.api_url}{endpoint}"

        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                timeout=self.timeout
            )
            response.raise_for_status()
            result = response.json()

            if result.get('code', 0) != 0:
                raise HubStudioAPIError(
                    result.get('code', -1),
                    result.get('msg', 'Unknown error')
                )

            return result.get('data', {})

        except requests.RequestException as e:
            raise HubStudioAPIError(-1, f"Request failed: {str(e)}")

    # ==================== 环境管理 ====================

    def get_env_list(self, group_code: Optional[str] = None,
                     page: int = 1, page_size: int = 100,
                     container_name: Optional[str] = None) -> List[EnvironmentInfo]:
        """
        获取环境列表

        Args:
            group_code: 分组名称，不传则获取所有
            page: 页码
            page_size: 每页数量
            container_name: 环境名称查询

        Returns:
            环境信息列表
        """
        data_body = {'current': page, 'size': min(page_size, 200)}
        if group_code:
            data_body['tagNames'] = [group_code]
        if container_name:
            data_body['containerName'] = container_name

        data = self._request('POST', '/api/v1/env/list', data=data_body)

        env_list = []
        for item in data.get('list', []):
            env_list.append(EnvironmentInfo(
                env_id=str(item.get('containerCode', '')),
                env_name=item.get('containerName', ''),
                group_code=item.get('tagCode', ''),
                code_seq_id=str(item.get('serialNumber', '')),
                status=0,  # 需要单独查询状态
                proxy_info={
                    'proxyType': item.get('proxyTypeName'),
                    'host': item.get('proxyHost'),
                    'port': item.get('proxyPort'),
                    'username': item.get('proxyAccount'),
                    'password': item.get('proxyPassword')
                } if item.get('proxyTypeName') else None
            ))

        return env_list

    def create_env(self, env_name: str, group_code: Optional[str] = None,
                   proxy_info: Optional[Dict] = None,
                   fingerprint: Optional[Dict] = None,
                   remark: Optional[str] = None) -> str:
        """
        创建环境

        Args:
            env_name: 环境名称
            group_code: 分组代码
            proxy_info: 代理信息
            fingerprint: 指纹配置
            remark: 备注

        Returns:
            新创建的环境ID
        """
        data = {'envName': env_name}
        if group_code:
            data['groupCode'] = group_code
        if proxy_info:
            data['proxyInfo'] = proxy_info
        if fingerprint:
            data['fingerprint'] = fingerprint
        if remark:
            data['remark'] = remark

        result = self._request('POST', '/api/env/add', data=data)
        return result.get('envId', '')

    def update_env(self, env_id: str, remark: Optional[str] = None,
                   group_code: Optional[str] = None) -> bool:
        """
        更新环境

        Args:
            env_id: 环境ID
            remark: 备注
            group_code: 分组代码

        Returns:
            是否成功
        """
        data = {'envId': env_id}
        if remark:
            data['remark'] = remark
        if group_code:
            data['groupCode'] = group_code

        self._request('POST', '/api/env/update', data=data)
        return True

    def update_env_proxy(self, env_id: str, proxy_type: str,
                         host: str, port: int,
                         username: Optional[str] = None,
                         password: Optional[str] = None) -> bool:
        """
        更新环境代理

        Args:
            env_id: 环境ID
            proxy_type: 代理类型 http/socks5
            host: 代理主机
            port: 代理端口
            username: 代理用户名
            password: 代理密码

        Returns:
            是否成功
        """
        data = {
            'envId': env_id,
            'proxyInfo': {
                'proxyType': proxy_type,
                'host': host,
                'port': port
            }
        }
        if username:
            data['proxyInfo']['username'] = username
        if password:
            data['proxyInfo']['password'] = password

        self._request('POST', '/api/env/proxy/update', data=data)
        return True

    def delete_env(self, env_ids: List[str]) -> bool:
        """
        删除环境

        Args:
            env_ids: 环境ID列表，最多1000个

        Returns:
            是否成功
        """
        self._request('POST', '/api/env/delete', data={'envIds': env_ids})
        return True

    # ==================== 浏览器环境 ====================

    def open_browser(self, env_id: str,
                     start_urls: Optional[List[str]] = None,
                     headless: bool = False) -> BrowserInfo:
        """
        打开浏览器环境

        Args:
            env_id: 环境ID (containerCode)
            start_urls: 启动时打开的URL列表
            headless: 是否无头模式

        Returns:
            浏览器信息，包含 debug 端口
        """
        data = {'containerCode': env_id}
        if headless:
            data['isHeadless'] = True

        result = self._request('POST', '/api/v1/browser/start', data=data)

        return BrowserInfo(
            browser_id=str(result.get('browserID', '')),
            debug_port=int(result.get('debuggingPort', 9222)),
            ws_endpoint=f"ws://127.0.0.1:{result.get('debuggingPort', 9222)}",
            webdriver_path=result.get('webdriver', ''),
            browser_path=result.get('browserPath', ''),
            download_path=result.get('downloadPath', '')
        )

    def close_browser(self, env_id: str) -> bool:
        """
        关闭浏览器环境

        Args:
            env_id: 环境ID (containerCode)

        Returns:
            是否成功
        """
        self._request('POST', '/api/v1/browser/stop', data={'containerCode': env_id})
        return True

    def close_all_browsers(self, clear_queue: bool = False) -> bool:
        """
        关闭所有浏览器环境

        Args:
            clear_queue: 是否清空启动队列

        Returns:
            是否成功
        """
        self._request('POST', '/api/browser/closeAll',
                      data={'clearQueue': clear_queue})
        return True

    def get_browser_status(self, env_id: str) -> Dict:
        """
        获取浏览器状态

        Args:
            env_id: 环境ID

        Returns:
            浏览器状态信息
        """
        return self._request('GET', '/api/browser/status',
                             params={'envId': env_id})

    def get_all_open_browsers(self) -> List[Dict]:
        """
        获取所有打开的浏览器环境

        Returns:
            打开的浏览器环境列表
        """
        result = self._request('GET', '/api/browser/list')
        return result.get('list', [])

    # ==================== Cookie 操作 ====================

    def import_cookie(self, env_id: str, cookies: List[Dict]) -> bool:
        """
        导入 Cookie

        Args:
            env_id: 环境ID
            cookies: Cookie 列表

        Returns:
            是否成功
        """
        self._request('POST', '/api/env/cookie/import',
                      data={'envId': env_id, 'cookies': cookies})
        return True

    def export_cookie(self, env_id: str) -> List[Dict]:
        """
        导出 Cookie

        Args:
            env_id: 环境ID

        Returns:
            Cookie 列表
        """
        result = self._request('GET', '/api/env/cookie/export',
                               params={'envId': env_id})
        return result.get('cookies', [])

    # ==================== 其他操作 ====================

    def get_random_ua(self) -> str:
        """
        获取随机 UA

        Returns:
            User-Agent 字符串
        """
        result = self._request('GET', '/api/env/ua/random')
        return result.get('ua', '')

    def clear_cache(self, env_id: str) -> bool:
        """
        清除环境本地缓存

        Args:
            env_id: 环境ID

        Returns:
            是否成功
        """
        self._request('POST', '/api/env/cache/clear',
                      data={'envId': env_id})
        return True

    def refresh_fingerprint(self, env_id: str) -> bool:
        """
        刷新指纹

        Args:
            env_id: 环境ID

        Returns:
            是否成功
        """
        self._request('POST', '/api/env/fingerprint/refresh',
                      data={'envId': env_id})
        return True

    # ==================== 分组管理 ====================

    def get_group_list(self) -> List[Dict]:
        """
        获取环境分组列表

        Returns:
            分组列表
        """
        result = self._request('GET', '/api/group/list')
        return result.get('list', [])

    def create_group(self, group_name: str) -> str:
        """
        创建环境分组

        Args:
            group_name: 分组名称

        Returns:
            分组代码
        """
        result = self._request('POST', '/api/group/add',
                               data={'groupName': group_name})
        return result.get('groupCode', '')

    def delete_group(self, group_code: str) -> bool:
        """
        删除环境分组

        Args:
            group_code: 分组代码

        Returns:
            是否成功
        """
        self._request('POST', '/api/group/delete',
                      data={'groupCode': group_code})
        return True
