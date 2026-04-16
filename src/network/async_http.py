"""
异步 HTTP 请求模块

通过 aiohttp 发送异步 HTTP 请求，继承浏览器的 Cookies 和认证信息
用于实现真正的并发请求
"""
import asyncio
import json
import urllib.parse
from typing import Dict, Any, Optional, List

import aiohttp

from ..utils.logger import default_logger as logger


class AsyncHTTPResponse:
    """
    异步响应封装

    兼容原有 BrowserRequest 的 Response 接口
    """

    def __init__(self, data: Dict):
        """
        初始化

        Args:
            data: 原始响应数据字典
        """
        self._data = data
        self.status_code = data.get('status', 0)
        self.headers = data.get('headers', {})
        self.content = data.get('body', '')
        self.url = data.get('url', '')
        self._json_data = data.get('json')

    @property
    def ok(self) -> bool:
        """是否成功响应"""
        return 200 <= self.status_code < 300

    def json(self) -> Any:
        """获取 JSON 数据"""
        if self._json_data is not None:
            return self._json_data

        if self.content:
            try:
                self._json_data = json.loads(self.content)
                return self._json_data
            except json.JSONDecodeError:
                pass

        return None


class AsyncBatchRequest:
    """
    异步批量请求处理器

    支持并发执行多个请求，支持自定义 headers 模板
    """

    # 默认 Shopee headers 模板（向后兼容）
    DEFAULT_SHOPEE_HEADERS = {
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json;charset=UTF-8',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    }

    def __init__(self, cookies: List[Dict] = None, auth_info: Dict = None,
                 headers_template: Dict[str, str] = None,
                 platform: str = 'shopee'):
        """
        初始化

        Args:
            cookies: 浏览器 Cookies 列表
            auth_info: 认证信息字典
            headers_template: 自定义 headers 模板（可选）
            platform: 平台类型 'shopee' 或 'tokopedia/tiktok'（用于自动选择默认模板）
        """
        self._cookies = cookies or []
        self._auth_info = auth_info or {}
        self._session: Optional[aiohttp.ClientSession] = None

        # 设置 headers 模板
        if headers_template:
            self._headers_template = headers_template.copy()
        elif platform.lower() in ('tokopedia', 'tiktok', 'tt'):
            self._headers_template = self._get_tokopedia_headers_template()
        else:
            self._headers_template = self.DEFAULT_SHOPEE_HEADERS.copy()

    async def _get_session(self) -> aiohttp.ClientSession:
        """获取或创建 session"""
        if self._session is None or self._session.closed:
            # 不使用 CookieJar，而是在每次请求时手动添加 cookie header
            # 这样可以确保 cookies 被正确发送
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=20
            )

            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=60)
            )

        return self._session

    def _get_tokopedia_headers_template(self) -> Dict[str, str]:
        """获取 Tokopedia/TikTok Shop 的默认 headers 模板"""
        return {
            'accept': '*/*',
            'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
        }

    def _build_cookies_header(self) -> str:
        """构建 Cookie 请求头"""
        # 手动构建 Cookie 头，确保 cookies 被包含在请求中
        return "; ".join(
            f"{cookie.get('name', '')}={cookie.get('value', '')}"
            for cookie in self._cookies
            if cookie.get('name')
        )

    def _build_headers(self, base_url: str, additional_headers: Dict = None) -> Dict[str, str]:
        """
        构建请求头

        Args:
            base_url: 基础 URL
            additional_headers: 额外的请求头（会覆盖模板中的同名 header）

        Returns:
            完整的请求头字典
        """
        # 从模板开始
        headers = self._headers_template.copy()

        # 添加 URL 相关 headers
        headers['origin'] = base_url
        headers['referer'] = f"{base_url}/order"

        # 检查平台特定 headers
        if 'x-shopee-region' in self._headers_template:
            # Shopee 平台
            headers['x-csrftoken'] = self._auth_info.get('csrf_token', '')
            headers['SPC_CDS'] = self._auth_info.get('spc_cds_chat', '')
            headers['x-shopee-region'] = self._auth_info.get('region', 'MY').lower()

            bearer_token = self._auth_info.get('bearer_token', '')
            if bearer_token:
                headers['Authorization'] = f'Bearer {bearer_token}'

        # 合并额外的 headers（会覆盖模板中的同名 header）
        if additional_headers:
            headers.update(additional_headers)

        return headers

    async def post(self, url: str, json_data: Dict = None,
                 headers: Dict = None, timeout: int = 30) -> AsyncHTTPResponse:
        """
        发送单个 POST 请求

        Args:
            url: 完整 URL
            json_data: JSON 请求体
            headers: 额外的请求头
            timeout: 超时时间（秒）

        Returns:
            响应对象
        """
        session = await self._get_session()

        # 构建请求头并添加 Cookie
        request_headers = self._build_headers(url.rsplit('/', 1)[0], headers)
        cookies_header = self._build_cookies_header()
        if cookies_header:
            request_headers['Cookie'] = cookies_header

        try:
            async with session.post(
                url=url,
                json=json_data or {},
                headers=request_headers,
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                return AsyncHTTPResponse({
                    'status': response.status,
                    'headers': dict(response.headers),
                    'body': await response.text(),
                    'url': url
                })
        except Exception as e:
            logger.error(f"[AsyncBatch] POST 请求失败: {e}")
            raise

    async def get(self, url: str, params: Dict = None,
                headers: Dict = None, timeout: int = 30) -> AsyncHTTPResponse:
        """
        发送单个 GET 请求

        Args:
            url: 完整 URL
            params: 查询参数
            headers: 额外的请求头
            timeout: 超时时间（秒）

        Returns:
            响应对象
        """
        session = await self._get_session()

        # 构建请求头并添加 Cookie
        request_headers = self._build_headers(url.rsplit('/', 1)[0], headers)
        cookies_header = self._build_cookies_header()
        if cookies_header:
            request_headers['Cookie'] = cookies_header

        try:
            async with session.get(
                url=url,
                params=params or {},
                headers=request_headers,
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                return AsyncHTTPResponse({
                    'status': response.status,
                    'headers': dict(response.headers),
                    'body': await response.text(),
                    'url': str(response.url)
                })
        except Exception as e:
            logger.error(f"[AsyncBatch] GET 请求失败: {e}")
            raise

    async def post_batch(self, base_url: str, api_path: str,
                         batches: List[List[Dict]],
                         request_key: str = 'order_param_list',
                         max_concurrent: int = 20) -> List[Dict]:
        """
        并发批量发送 POST 请求

        Args:
            base_url: 基础 URL
            api_path: API 路径
            batches: 请求数据批次列表
            request_key: 请求体中的参数名
            max_concurrent: 最大并发数

        Returns:
            所有批次的响应结果列表
        """
        session = await self._get_session()

        # 信号量控制并发数
        semaphore = asyncio.Semaphore(max_concurrent)

        # 构建基础请求头并添加 Cookie
        headers = self._build_headers(base_url)
        cookies_header = self._build_cookies_header()
        if cookies_header:
            headers['Cookie'] = cookies_header

        url = f"{base_url}{api_path}"

        async def send_batch(batch_data: List[Dict], batch_idx: int) -> Dict:
            """发送单个批次，带重试机制"""
            async with semaphore:
                for retry_count in range(3):
                    request_body = {
                        "order_list_tab": 100,
                        "need_count_down_desc": False,
                        request_key: batch_data
                    }

                    try:
                        async with session.post(
                            url=url,
                            json=request_body,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=60)
                        ) as response:
                            content = await response.text()

                            if response.ok:
                                try:
                                    data = json.loads(content)
                                    card_list = data.get('data', {}).get('card_list', [])
                                    return {'success': True, 'data': card_list, 'batch': batch_idx + 1}
                                except json.JSONDecodeError as e:
                                    logger.error(f"[AsyncBatch] 批次 {batch_idx + 1} JSON 解析失败: {e}")
                                    return {'success': False, 'error': str(e), 'batch': batch_idx + 1}
                            else:
                                logger.error(f"[AsyncBatch] 批次 {batch_idx + 1} 失败: {response.status}")
                                logger.error(f"[AsyncBatch] 响应内容: {content[:500] if content else '空'}")
                                return {'success': False, 'error': f"HTTP {response.status}", 'batch': batch_idx + 1}

                    except Exception as e:
                        error_msg = str(e)
                        is_retryable = any(x in error_msg.lower() for x in ['server disconnected', 'connection', 'timeout', 'reset'])

                        if is_retryable and retry_count < 2:
                            logger.warning(f"[AsyncBatch] 批次 {batch_idx + 1} 第 {retry_count + 1} 次失败: {e}，{2 - retry_count} 秒后重试...")
                            await asyncio.sleep(2)
                            continue
                        else:
                            logger.error(f"[AsyncBatch] 批次 {batch_idx + 1} 异常: {e}")
                            return {'success': False, 'error': str(e), 'batch': batch_idx + 1}

        # 并发执行所有批次
        tasks = [send_batch(batch, i) for i, batch in enumerate(batches)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 收集成功的结果
        all_details = []
        for result in results:
            if isinstance(result, dict) and result.get('success'):
                all_details.extend(result.get('data', []))

        logger.info(f"[AsyncBatch] 全部完成，成功 {len(all_details)} 条")
        return all_details

    async def close(self):
        """关闭 session"""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def __del__(self):
        """析构时自动关闭 session（避免 Unclosed connector 警告）"""
        # 注意：不能在 __del__ 中调用异步方法，所以只能记录日志
        # 实际关闭需要显式调用 close() 或使用 context manager
        if self._session and not self._session.closed:
            logger.warning("[AsyncBatchRequest] session 未显式关闭，存在资源泄漏风险")

    async def __aenter__(self):
        """支持 async with 语法"""
        return self

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):  # noqa: ARG002
        """退出 async with 块时自动关闭"""
        await self.close()
        return False
