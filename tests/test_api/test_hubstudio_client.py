"""
API 测试模块
"""
import os
import sys
import pytest
from unittest.mock import Mock, patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.hubstudio_client import HubStudioClient, HubStudioAPIError, BrowserInfo, EnvironmentInfo


class TestAPIConnection:
    """测试 API 连通性"""

    def test_client_init_with_valid_url(self):
        """测试使用有效 URL 初始化客户端"""
        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key_123"
        )
        assert client.api_url == "http://127.0.0.1:6873"
        assert client.api_key == "test_key_123"
        assert client.timeout == 30

    def test_client_init_with_custom_timeout(self):
        """测试自定义超时时间"""
        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key",
            timeout=60
        )
        assert client.timeout == 60

    def test_client_init_strips_trailing_slash(self):
        """测试 URL 末尾斜杠会被去除"""
        client = HubStudioClient(
            api_url="http://127.0.0.1:6873/",
            api_key="test_key"
        )
        assert client.api_url == "http://127.0.0.1:6873"

    def test_client_has_session(self):
        """测试客户端有 session 属性"""
        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        assert hasattr(client, 'session')
        assert client.session is not None


class TestAPIRequestMethod:
    """测试 API 请求方法"""

    @patch('requests.Session.request')
    def test_request_get_success(self, mock_request):
        """测试 GET 请求成功"""
        mock_response = Mock()
        mock_response.json.return_value = {'code': 0, 'data': {'test': 'value'}}
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        result = client._request('GET', '/api/test', params={'key': 'value'})

        assert result == {'test': 'value'}
        mock_request.assert_called_once()

    @patch('requests.Session.request')
    def test_request_post_success(self, mock_request):
        """测试 POST 请求成功"""
        mock_response = Mock()
        mock_response.json.return_value = {'code': 0, 'data': {'id': 123}}
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        result = client._request('POST', '/api/create', data={'name': 'test'})

        assert result == {'id': 123}

    @patch('requests.Session.request')
    def test_request_api_error_code(self, mock_request):
        """测试 API 返回错误码"""
        mock_response = Mock()
        mock_response.json.return_value = {'code': 1001, 'msg': 'Invalid API key'}
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="invalid_key"
        )

        with pytest.raises(HubStudioAPIError) as exc_info:
            client._request('GET', '/api/test')
        assert exc_info.value.code == 1001

    @patch('requests.Session.request')
    def test_request_api_error_message(self, mock_request):
        """测试 API 返回错误消息"""
        mock_response = Mock()
        mock_response.json.return_value = {'code': 1002, 'msg': 'Environment not found'}
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )

        with pytest.raises(HubStudioAPIError) as exc_info:
            client._request('GET', '/api/env/status')
        assert 'Environment not found' in str(exc_info.value)

    @patch('requests.Session.request')
    def test_request_http_error(self, mock_request):
        """测试 HTTP 错误（如 404、500）"""
        import requests
        mock_response = Mock()
        # 使用 requests.HTTPError 而不是普通 Exception
        mock_response.raise_for_status.side_effect = requests.HTTPError("HTTP Error: 404 Not Found")
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )

        with pytest.raises(HubStudioAPIError) as exc_info:
            client._request('GET', '/api/notfound')
        assert 'Request failed' in str(exc_info.value)

    @patch('requests.Session.request')
    def test_request_timeout(self, mock_request):
        """测试请求超时"""
        import requests
        mock_request.side_effect = requests.Timeout("Request timed out")

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key",
            timeout=5
        )

        with pytest.raises(HubStudioAPIError) as exc_info:
            client._request('GET', '/api/slow')
        assert 'Request failed' in str(exc_info.value)


class TestAPIEnvironmentManagement:
    """测试环境管理 API"""

    @patch('requests.Session.request')
    def test_get_env_list_success(self, mock_request):
        """测试获取环境列表成功"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 0,
            'data': {
                'list': [
                    {
                        'containerCode': 'env_001',
                        'containerName': 'Test Environment',
                        'tagCode': 'group_001',
                        'serialNumber': '1',
                        'proxyTypeName': 'HTTP',
                        'proxyHost': 'proxy.example.com',
                        'proxyPort': 8080
                    }
                ]
            }
        }
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        env_list = client.get_env_list()

        assert len(env_list) == 1
        assert env_list[0].env_id == 'env_001'
        assert env_list[0].env_name == 'Test Environment'
        assert env_list[0].group_code == 'group_001'

    @patch('requests.Session.request')
    def test_get_env_list_with_group_filter(self, mock_request):
        """测试按分组筛选环境列表"""
        mock_response = Mock()
        mock_response.json.return_value = {'code': 0, 'data': {'list': []}}
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        client.get_env_list(group_code='shopee_my')

        # 验证请求参数
        call_args = mock_request.call_args
        assert call_args[1]['json']['tagNames'] == ['shopee_my']

    @patch('requests.Session.request')
    def test_create_env_success(self, mock_request):
        """测试创建环境成功"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 0,
            'data': {'envId': 'new_env_123'}
        }
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        env_id = client.create_env(env_name='New Environment', group_code='test_group')

        assert env_id == 'new_env_123'

    @patch('requests.Session.request')
    def test_delete_env_success(self, mock_request):
        """测试删除环境成功"""
        mock_response = Mock()
        mock_response.json.return_value = {'code': 0, 'data': {}}
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        result = client.delete_env(['env_001', 'env_002'])

        assert result is True


class TestAPIBrowserOperations:
    """测试浏览器操作 API"""

    @patch('requests.Session.request')
    def test_open_browser_success(self, mock_request):
        """测试打开浏览器成功"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 0,
            'data': {
                'browserID': 'browser_123',
                'debuggingPort': 9222,
                'webdriver': 'C:/webdriver.exe',
                'browserPath': 'C:/chrome.exe',
                'downloadPath': 'C:/downloads'
            }
        }
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        browser_info = client.open_browser(env_id='env_001')

        assert browser_info.browser_id == 'browser_123'
        assert browser_info.debug_port == 9222
        assert browser_info.ws_endpoint == 'ws://127.0.0.1:9222'

    @patch('requests.Session.request')
    def test_open_browser_with_headless(self, mock_request):
        """测试无头模式打开浏览器"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 0,
            'data': {
                'browserID': 'browser_123',
                'debuggingPort': 9222
            }
        }
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        client.open_browser(env_id='env_001', headless=True)

        call_args = mock_request.call_args
        assert call_args[1]['json']['isHeadless'] is True

    @patch('requests.Session.request')
    def test_close_browser_success(self, mock_request):
        """测试关闭浏览器成功"""
        mock_response = Mock()
        mock_response.json.return_value = {'code': 0, 'data': {}}
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        result = client.close_browser(env_id='env_001')

        assert result is True

    @patch('requests.Session.request')
    def test_get_all_open_browsers(self, mock_request):
        """测试获取所有打开的浏览器"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 0,
            'data': {
                'list': [
                    {'containerCode': 'env_001', 'status': 'running'},
                    {'containerCode': 'env_002', 'status': 'running'}
                ]
            }
        }
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        browsers = client.get_all_open_browsers()

        assert len(browsers) == 2


class TestAPICookieOperations:
    """测试 Cookie 操作 API"""

    @patch('requests.Session.request')
    def test_import_cookie_success(self, mock_request):
        """测试导入 Cookie 成功"""
        mock_response = Mock()
        mock_response.json.return_value = {'code': 0, 'data': {}}
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )

        cookies = [
            {'name': 'session', 'value': 'abc123', 'domain': '.shopee.com'},
            {'name': 'token', 'value': 'xyz789', 'domain': '.shopee.com'}
        ]
        result = client.import_cookie(env_id='env_001', cookies=cookies)

        assert result is True
        call_args = mock_request.call_args
        assert call_args[1]['json']['envId'] == 'env_001'
        assert len(call_args[1]['json']['cookies']) == 2

    @patch('requests.Session.request')
    def test_export_cookie_success(self, mock_request):
        """测试导出 Cookie 成功"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 0,
            'data': {
                'cookies': [
                    {'name': 'session', 'value': 'abc123'},
                    {'name': 'token', 'value': 'xyz789'}
                ]
            }
        }
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        cookies = client.export_cookie(env_id='env_001')

        assert len(cookies) == 2
        assert cookies[0]['name'] == 'session'


class TestAPIGroupOperations:
    """测试分组管理 API"""

    @patch('requests.Session.request')
    def test_get_group_list_success(self, mock_request):
        """测试获取分组列表成功"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 0,
            'data': {
                'list': [
                    {'groupCode': 'group_001', 'groupName': 'Shopee MY'},
                    {'groupCode': 'group_002', 'groupName': 'Shopee TH'}
                ]
            }
        }
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        groups = client.get_group_list()

        assert len(groups) == 2
        assert groups[0]['groupName'] == 'Shopee MY'

    @patch('requests.Session.request')
    def test_create_group_success(self, mock_request):
        """测试创建分组成功"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 0,
            'data': {'groupCode': 'new_group_123'}
        }
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        group_code = client.create_group(group_name='New Group')

        assert group_code == 'new_group_123'

    @patch('requests.Session.request')
    def test_delete_group_success(self, mock_request):
        """测试删除分组成功"""
        mock_response = Mock()
        mock_response.json.return_value = {'code': 0, 'data': {}}
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = HubStudioClient(
            api_url="http://127.0.0.1:6873",
            api_key="test_key"
        )
        result = client.delete_group(group_code='group_001')

        assert result is True


class TestAPIErrorHandling:
    """测试 API 错误处理"""

    def test_hubstudio_api_error_code(self):
        """测试 API 错误类包含错误码"""
        error = HubStudioAPIError(1001, "Invalid API key")
        assert error.code == 1001
        assert error.message == "Invalid API key"
        assert "1001" in str(error)
        assert "Invalid API key" in str(error)

    def test_hubstudio_api_error_string(self):
        """测试 API 错误类字符串表示"""
        error = HubStudioAPIError(500, "Internal Server Error")
        error_str = str(error)
        assert "API Error" in error_str
        assert "500" in error_str
        assert "Internal Server Error" in error_str