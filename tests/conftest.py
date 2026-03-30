"""
Pytest 配置和共享 fixtures
"""
import os
import sys
import pytest
import tempfile
import shutil
from datetime import datetime

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def temp_db_path():
    """创建临时数据库文件路径"""
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, 'test.accdb')
    yield db_path
    # 清理临时文件
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_order_data():
    """示例订单数据"""
    return {
        'order_id': 1234567890123456789,
        'order_sn': 'TEST_ORDER_001',
        'shop_id': 12345678,
        'region_id': 'MY',
        'status': 'READY_TO_SHIP',
        'fulfilment_channel': 'SHOPEE',
        'total_price': 99.90,
        'currency': 'MYR',
        'shipping_name': 'Test Buyer',
        'shipping_phone': '+60123456789',
        'shipping_address': '123 Test Street, Kuala Lumpur',
        'tracking_number': '',
        'create_time': datetime(2024, 1, 15, 10, 30, 0),
        'update_time': datetime(2024, 1, 15, 12, 0, 0)
    }


@pytest.fixture
def sample_order_items():
    """示例订单商品数据"""
    return [
        {
            'item_id': 111111111,
            'order_id': 1234567890123456789,
            'order_sn': 'TEST_ORDER_001',
            'item_name': 'Test Product 1',
            'item_description': 'Description for product 1',
            'amount': 2,
            'model_id': 222222222
        },
        {
            'item_id': 111111112,
            'order_id': 1234567890123456789,
            'order_sn': 'TEST_ORDER_001',
            'item_name': 'Test Product 2',
            'item_description': 'Description for product 2',
            'amount': 1,
            'model_id': 222222223
        }
    ]


@pytest.fixture
def sample_buyer_data():
    """示例买家数据"""
    return {
        'buyer_user_id': 9876543210,
        'buyer_username': 'test_buyer_001',
        'avatar': 'https://example.com/avatar.jpg',
        'rating': 4.5,
        'country': 'MY',
        'city': 'Kuala Lumpur'
    }


@pytest.fixture
def sample_chat_data():
    """示例聊天数据"""
    return {
        'buyer_user_id': 9876543210,
        'conversation_id': 5555555555,
        'total_messages': 5,
        'user_messages_count': 3,
        'user_message_text': 'Hello, I have a question about my order'
    }


@pytest.fixture
def mock_api_response_order_list():
    """模拟订单列表 API 响应数据"""
    return {
        'data': {
            'orders': [
                {
                    'order_sn': 'ORDER_001',
                    'order_id': 1234567890123,
                    'shop_id': 12345678,
                    'region_id': 'MY',
                    'status': 'READY_TO_SHIP',
                    'fulfilment_channel': 'SHOPEE',
                    'price': {
                        'currency': 'MYR',
                        'total_price': 199.90
                    },
                    'shipping_address': {
                        'name': 'John Doe',
                        'phone': '+60123456789',
                        'address': {
                            'full_address': '123 Main St, KL'
                        }
                    },
                    'create_time': 1705312800,
                    'update_time': 1705312800
                },
                {
                    'order_sn': 'ORDER_002',
                    'order_id': 1234567890124,
                    'shop_id': 12345678,
                    'region_id': 'MY',
                    'status': 'SHIPPED',
                    'fulfilment_channel': 'SHOPEE',
                    'price': {
                        'currency': 'MYR',
                        'total_price': 299.90
                    },
                    'shipping_address': {
                        'name': 'Jane Smith',
                        'phone': '+60198765432',
                        'address': {
                            'full_address': '456 Second St, PJ'
                        }
                    },
                    'create_time': 1705312800,
                    'update_time': 1705312800
                }
            ]
        }
    }


@pytest.fixture
def mock_api_response_order_detail():
    """模拟订单详情 API 响应数据"""
    return {
        'data': {
            'order': {
                'order_sn': 'ORDER_001',
                'order_id': 1234567890123,
                'status': 'READY_TO_SHIP',
                'fulfilment_channel': 'SHOPEE',
                'price': {
                    'currency': 'MYR',
                    'total_price': 199.90
                },
                'shipping_address': {
                    'name': 'John Doe',
                    'phone': '+60123456789',
                    'address': {
                        'full_address': '123 Main St, KL'
                    }
                },
                'items': [
                    {
                        'item_id': 111111111,
                        'model_id': 222222222,
                        'name': 'Product A',
                        'description': 'Description A',
                        'amount': 2
                    },
                    {
                        'item_id': 111111112,
                        'model_id': 222222223,
                        'name': 'Product B',
                        'description': 'Description B',
                        'amount': 1
                    }
                ],
                'buyer': {
                    'buyer_user_id': 9876543210,
                    'buyer_username': 'buyer_001',
                    'avatar': 'https://cf.shopee.my/file/avatar.jpg',
                    'rating': 4.5,
                    'country': 'MY',
                    'city': 'Kuala Lumpur'
                },
                'create_time': 1705312800,
                'update_time': 1705312800
            }
        }
    }


@pytest.fixture
def mock_api_response_chat_messages():
    """模拟聊天消息 API 响应数据"""
    return {
        'data': {
            'messages': [
                {
                    'message_id': 'msg001',
                    'sender': {'user_id': 9876543210},
                    'content': {'text': 'Hello, I want to ask about my order'},
                    'timestamp': 1705312800
                },
                {
                    'message_id': 'msg002',
                    'sender': {'user_id': 12345678},
                    'content': {'text': 'Sure, what is your question?'},
                    'timestamp': 1705312900
                },
                {
                    'message_id': 'msg003',
                    'sender': {'user_id': 9876543210},
                    'content': {'text': 'When will it be shipped?'},
                    'timestamp': 1705313000
                }
            ],
            'total_messages': 3
        }
    }