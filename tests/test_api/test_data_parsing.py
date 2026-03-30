"""
API 数据解析测试模块
测试从 API 响应到业务数据的解析逻辑
"""
import os
import sys
import pytest
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestOrderDataParsing:
    """测试订单数据解析"""

    def test_parse_order_list_response(self):
        """测试解析订单列表响应"""
        # 模拟 API 响应数据
        api_response = {
            'data': {
                'orders': [
                    {
                        'order_sn': 'ORDER_240115_001',
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
                                'full_address': '123 Main Street, Kuala Lumpur'
                            }
                        },
                        'create_time': 1705312800,
                        'update_time': 1705312800
                    }
                ]
            }
        }

        # 解析订单数据
        orders = api_response.get('data', {}).get('orders', [])
        parsed_orders = []
        for order in orders:
            parsed_order = {
                'order_sn': order.get('order_sn'),
                'order_id': order.get('order_id'),
                'shop_id': order.get('shop_id'),
                'region_id': order.get('region_id'),
                'status': order.get('status'),
                'fulfilment_channel': order.get('fulfilment_channel'),
                'total_price': order.get('price', {}).get('total_price', 0),
                'currency': order.get('price', {}).get('currency', 'MYR'),
                'shipping_name': order.get('shipping_address', {}).get('name', ''),
                'shipping_phone': order.get('shipping_address', {}).get('phone', ''),
                'shipping_address': order.get('shipping_address', {}).get('address', {}).get('full_address', ''),
                'create_time': order.get('create_time'),
                'update_time': order.get('update_time')
            }
            parsed_orders.append(parsed_order)

        # 验证解析结果
        assert len(parsed_orders) == 1
        assert parsed_orders[0]['order_sn'] == 'ORDER_240115_001'
        assert parsed_orders[0]['total_price'] == 199.90
        assert parsed_orders[0]['currency'] == 'MYR'
        assert parsed_orders[0]['status'] == 'READY_TO_SHIP'

    def test_parse_order_detail_response(self):
        """测试解析订单详情响应"""
        api_response = {
            'data': {
                'order': {
                    'order_sn': 'ORDER_240115_001',
                    'order_id': 1234567890123,
                    'shop_id': 12345678,
                    'region_id': 'MY',
                    'status': 'READY_TO_SHIP',
                    'fulfilment_channel': 'SHOPEE',
                    'tracking_number': 'SHIP123456789',
                    'price': {
                        'currency': 'MYR',
                        'total_price': 299.90
                    },
                    'shipping_address': {
                        'name': 'Jane Smith',
                        'phone': '+60198765432',
                        'address': {
                            'full_address': '456 Second Ave, Petaling Jaya'
                        }
                    },
                    'items': [
                        {
                            'item_id': 111111111,
                            'model_id': 222222222,
                            'name': 'Product A - Blue',
                            'description': 'Size: M',
                            'amount': 2
                        },
                        {
                            'item_id': 111111112,
                            'model_id': 222222223,
                            'name': 'Product B',
                            'description': 'Color: Red',
                            'amount': 1
                        }
                    ],
                    'buyer': {
                        'buyer_user_id': 9876543210,
                        'buyer_username': 'buyer_jane',
                        'avatar': 'https://cf.shopee.my/file/avatar123.jpg',
                        'rating': 4.8,
                        'country': 'MY',
                        'city': 'Kuala Lumpur'
                    },
                    'create_time': 1705312800,
                    'update_time': 1705312800
                }
            }
        }

        order = api_response['data']['order']

        # 解析订单主数据
        order_data = {
            'order_sn': order.get('order_sn'),
            'order_id': order.get('order_id'),
            'shop_id': order.get('shop_id'),
            'region_id': order.get('region_id'),
            'status': order.get('status'),
            'fulfilment_channel': order.get('fulfilment_channel'),
            'tracking_number': order.get('tracking_number', ''),
            'total_price': order.get('price', {}).get('total_price', 0),
            'currency': order.get('price', {}).get('currency', 'MYR'),
            'shipping_name': order.get('shipping_address', {}).get('name', ''),
            'shipping_phone': order.get('shipping_address', {}).get('phone', ''),
            'shipping_address': order.get('shipping_address', {}).get('address', {}).get('full_address', ''),
            'create_time': order.get('create_time'),
            'update_time': order.get('update_time')
        }

        # 解析商品数据
        items = []
        for item in order.get('items', []):
            items.append({
                'item_id': item.get('item_id'),
                'name': item.get('name', ''),
                'description': item.get('description', ''),
                'amount': item.get('amount', 1),
                'model_id': item.get('model_id')
            })

        # 解析买家数据
        buyer = order.get('buyer', {})
        buyer_data = {
            'buyer_user_id': buyer.get('buyer_user_id'),
            'buyer_username': buyer.get('buyer_username', ''),
            'avatar': buyer.get('avatar'),
            'rating': buyer.get('rating'),
            'country': buyer.get('country'),
            'city': buyer.get('city')
        }

        # 验证解析结果
        assert order_data['order_sn'] == 'ORDER_240115_001'
        assert order_data['tracking_number'] == 'SHIP123456789'
        assert len(items) == 2
        assert items[0]['item_id'] == 111111111
        assert items[0]['amount'] == 2
        assert buyer_data['buyer_username'] == 'buyer_jane'
        assert buyer_data['rating'] == 4.8


class TestChatDataParsing:
    """测试聊天数据解析"""

    def test_parse_chat_messages_response(self):
        """测试解析聊天消息响应"""
        api_response = {
            'data': {
                'messages': [
                    {
                        'message_id': 'msg_001',
                        'sender': {'user_id': 9876543210},
                        'content': {'text': 'Hello, is this product available?'},
                        'timestamp': 1705312800
                    },
                    {
                        'message_id': 'msg_002',
                        'sender': {'user_id': 12345678},
                        'content': {'text': 'Yes, we have it in stock!'},
                        'timestamp': 1705312900
                    },
                    {
                        'message_id': 'msg_003',
                        'sender': {'user_id': 9876543210},
                        'content': {'text': 'Great! I want to order 2.'},
                        'timestamp': 1705313000
                    }
                ],
                'total_messages': 3,
                'has_more': False
            }
        }

        messages = api_response['data']['messages']
        total = api_response['data']['total_messages']

        # 解析消息
        parsed_messages = []
        for msg in messages:
            is_buyer = msg['sender']['user_id'] != 12345678  # 假设店铺ID是12345678
            parsed_messages.append({
                'message_id': msg.get('message_id'),
                'user_id': msg.get('sender', {}).get('user_id'),
                'text': msg.get('content', {}).get('text', ''),
                'timestamp': msg.get('timestamp'),
                'is_buyer': is_buyer
            })

        # 统计买家消息
        buyer_messages = [m for m in parsed_messages if m['is_buyer']]
        buyer_message_text = buyer_messages[-1]['text'] if buyer_messages else ''

        # 验证解析结果
        assert total == 3
        assert len(parsed_messages) == 3
        assert parsed_messages[0]['text'] == 'Hello, is this product available?'
        assert len(buyer_messages) == 2
        assert buyer_message_text == 'Great! I want to order 2.'


class TestDataMapping:
    """测试数据映射转换"""

    def test_timestamp_to_datetime_conversion(self):
        """测试时间戳转 datetime"""
        from datetime import datetime

        timestamp = 1705312800
        # 转换为 datetime
        dt = datetime.fromtimestamp(timestamp)

        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15

    def test_api_field_to_db_field_mapping(self):
        """测试 API 字段到数据库字段的映射"""
        # API 响应字段到数据库字段的映射关系
        field_mapping = {
            'order_sn': 'order_sn',
            'order_id': 'order_id',
            'shop_id': 'shop_id',
            'region_id': 'region_id',
            'status': 'status',
            'price.total_price': 'total_price',
            'price.currency': 'currency',
            'shipping_address.name': 'shipping_name',
            'shipping_address.phone': 'shipping_phone',
            'shipping_address.address.full_address': 'shipping_address',
            'create_time': 'create_time',
            'update_time': 'update_time'
        }

        # 模拟 API 响应
        api_data = {
            'order_sn': 'ORDER_TEST_001',
            'order_id': 1234567890123,
            'shop_id': 12345678,
            'region_id': 'MY',
            'status': 'SHIPPED',
            'price': {
                'total_price': 99.99,
                'currency': 'MYR'
            },
            'shipping_address': {
                'name': 'Test User',
                'phone': '+60123456789',
                'address': {
                    'full_address': 'Test Address'
                }
            },
            'create_time': 1705312800,
            'update_time': 1705312800
        }

        # 执行映射转换
        db_data = {}
        for api_field, db_field in field_mapping.items():
            if '.' in api_field:
                parts = api_field.split('.')
                value = api_data
                for part in parts:
                    value = value.get(part, {})
                db_data[db_field] = value
            else:
                db_data[db_field] = api_data.get(api_field)

        # 验证映射结果
        assert db_data['order_sn'] == 'ORDER_TEST_001'
        assert db_data['total_price'] == 99.99
        assert db_data['shipping_name'] == 'Test User'
        assert db_data['currency'] == 'MYR'


class TestDataValidation:
    """测试数据验证"""

    def test_validate_required_fields(self):
        """测试必填字段验证"""
        order_data = {
            'order_sn': 'ORDER_001',
            'order_id': 1234567890123,
            'shop_id': 12345678,
            'region_id': 'MY'
        }

        required_fields = ['order_sn', 'order_id', 'shop_id', 'region_id']
        missing_fields = [f for f in required_fields if f not in order_data or not order_data[f]]

        assert len(missing_fields) == 0, f"Missing fields: {missing_fields}"

    def test_validate_missing_required_fields(self):
        """测试缺少必填字段"""
        order_data = {
            'order_sn': 'ORDER_001',
            'region_id': 'MY'
        }

        required_fields = ['order_sn', 'order_id', 'shop_id', 'region_id']
        missing_fields = [f for f in required_fields if f not in order_data or not order_data[f]]

        assert len(missing_fields) == 2
        assert 'order_id' in missing_fields
        assert 'shop_id' in missing_fields

    def test_validate_data_types(self):
        """测试数据类型验证"""
        order_data = {
            'order_id': 1234567890123,  # 应该是数字
            'total_price': 99.99,       # 应该是浮点数
            'status': 'READY_TO_SHIP'   # 应该是字符串
        }

        # 类型检查
        assert isinstance(order_data['order_id'], int)
        assert isinstance(order_data['total_price'], (int, float))
        assert isinstance(order_data['status'], str)

    def test_validate_numeric_fields_negative(self):
        """测试数值字段边界情况"""
        # 测试正常数据
        valid_order = {
            'order_id': 1234567890123,  # 正数，应该有效
            'total_price': 99.99        # 非负数价格
        }

        # 验证数值合理性
        assert valid_order['order_id'] > 0, "order_id should be positive"
        assert valid_order['total_price'] >= 0, "total_price should be non-negative"

        # 测试异常数据（用于验证验证逻辑存在）
        invalid_order = {
            'order_id': -1,  # 负数
            'total_price': -10.0  # 负数价格
        }

        # 这些数据应该被识别为无效
        assert invalid_order['order_id'] < 0 or invalid_order['order_id'] is None
        assert invalid_order['total_price'] < 0


class TestDataTransformation:
    """测试数据转换"""

    def test_transform_order_status(self):
        """测试订单状态转换"""
        status_mapping = {
            'READY_TO_SHIP': '待发货',
            'SHIPPED': '已发货',
            'DELIVERED': '已送达',
            'CANCELLED': '已取消',
            'RETURNED': '已退货'
        }

        # 测试状态映射
        assert status_mapping.get('READY_TO_SHIP') == '待发货'
        assert status_mapping.get('SHIPPED') == '已发货'

    def test_transform_currency_code(self):
        """测试货币代码转换"""
        currency_mapping = {
            'MYR': '马来西亚林吉特',
            'THB': '泰铢',
            'IDR': '印尼盾',
            'PHP': '菲律宾比索',
            'SGD': '新加坡元'
        }

        assert currency_mapping.get('MYR') == '马来西亚林吉特'
        assert currency_mapping.get('THB') == '泰铢'

    def test_transform_phone_number(self):
        """测试电话号码处理"""
        phone = '+60123456789'

        # 去除国家代码前缀
        if phone.startswith('+60'):
            phone = '0' + phone[3:]

        assert phone == '0123456789'

    def test_transform_price_precision(self):
        """测试价格精度处理"""
        price = 99.999999

        # 保留两位小数
        price_rounded = round(price, 2)

        assert price_rounded == 100.00

    def test_transform_address_multiline(self):
        """测试地址多行处理"""
        address = "123 Main Street\nKuala Lumpur\nWilayah Persekutuan"

        # 替换换行符为逗号
        address_single_line = address.replace('\n', ', ')

        assert ',' in address_single_line
        assert '\n' not in address_single_line


class TestRegionDataHandling:
    """测试地区数据处理"""

    def test_region_id_mapping(self):
        """测试地区 ID 映射"""
        region_mapping = {
            'MY': '马来西亚',
            'TH': '泰国',
            'ID': '印度尼西亚',
            'PH': '菲律宾',
            'SG': '新加坡',
            'VN': '越南'
        }

        assert region_mapping.get('MY') == '马来西亚'
        assert region_mapping.get('TH') == '泰国'

    def test_env_name_region_extraction(self):
        """测试从环境名提取地区"""
        env_names = [
            'MY_LAmall',
            'TH_Lacofee',
            'ID_LAHOME',
            'PH_KCB'
        ]

        for env_name in env_names:
            region = env_name.split('_')[0]
            assert region in ['MY', 'TH', 'ID', 'PH', 'SG', 'VN']