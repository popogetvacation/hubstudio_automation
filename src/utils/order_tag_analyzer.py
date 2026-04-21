"""
订单标签分析工具
从 analyze_order_tags.py 解耦标签生成逻辑
"""
from typing import Dict, List, Tuple
from datetime import datetime, timedelta

try:
    import pyodbc
except ImportError:
    raise ImportError("请安装 pyodbc: pip install pyodbc")


# 税务关键词列表
TAX_KEYWORDS = [
    '税', '税务', '发票', 'invoice', 'receipt', 'tax',
    'ภาษี', 'การเก็บภาษี', 'ใบแจ้งหนี้'
]


def get_db_connection(db_path: str):
    """获取数据库连接"""
    import os
    # 使用绝对路径
    abs_path = os.path.abspath(db_path)
    conn_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + abs_path + ';'
    conn = pyodbc.connect(conn_str)
    conn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
    conn.setdecoding(pyodbc.SQL_WCHAR, encoding='latin1')
    return conn


def get_all_orders(conn) -> List[Dict]:
    """获取所有订单数据（筛选 To Ship 且无追踪号）"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT order_sn, order_id, buyer_user_id, rating, shipping_address,
               total_price, currency, order_create_time, status, tracking_number
        FROM shopee_orders
        WHERE status = 'To Ship' AND (tracking_number IS NULL OR tracking_number = '')
    """)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def get_order_items(conn) -> List[Dict]:
    """获取所有订单商品数据"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT order_sn, item_id, model_id, amount, item_name
        FROM shopee_order_items
    """)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def get_order_buyers(conn) -> List[Dict]:
    """获取所有订单买家信息（包括聊天记录）"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT order_sn, buyer_user_id, buyer_username, rating as buyer_rating,
               country, city, conversation_id, user_message_text
        FROM shopee_order_buyer
    """)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def check_low_score(rating) -> bool:
    """检查低分用户：顾客评分 > 0 且 < 3"""
    try:
        r = float(rating)
        return r > 0 and r < 3
    except:
        return False


def check_same_order_multi_items(order_sn: str, items: List[Dict]) -> bool:
    """检查同单多件：同一订单中任意一款产品购买数量 >= 2"""
    order_items = [item for item in items if item.get('order_sn') == order_sn]
    for item in order_items:
        amount = item.get('amount', 0)
        if amount and int(amount) >= 2:
            return True
    return False


def check_high_frequency_repurchase(order_sn: str, order_create_time,
                                     history_orders: List[Tuple],
                                     order_items_map: Dict) -> bool:
    """
    检查高频复购：同一顾客在3小时内有多次下单记录，
    且购买了同款产品（item_id 相同）
    """
    if not order_create_time:
        return False

    # 获取当前订单的商品
    current_items = order_items_map.get(order_sn, [])
    current_item_ids = set(i.get('item_id') for i in current_items if i.get('item_id'))
    if not current_item_ids:
        return False

    # 遍历历史订单
    for row in history_orders:
        hist_order_sn = row[0]
        hist_status = row[1]
        hist_tracking_number = row[2]
        hist_order_create_time = row[3]

        if not hist_order_create_time:
            continue

        # 计算时间差（小时）
        time_diff = abs((order_create_time - hist_order_create_time).total_seconds()) / 3600

        if time_diff <= 3:
            # 检查同款商品
            o_items = order_items_map.get(hist_order_sn, [])
            o_item_ids = set(i.get('item_id') for i in o_items if i.get('item_id'))

            if current_item_ids & o_item_ids:
                # 检查是否有退货记录：Canceled 且有 tracking_number 视为发货后退货
                has_tracking = hist_tracking_number and str(hist_tracking_number).strip()
                is_canceled_with_tracking = (hist_status and hist_status.lower() == 'cancelled') and has_tracking

                if is_canceled_with_tracking:
                    # 退货重新下单，不计入高频复购
                    continue

                return True
    return False


def check_ph_remote_area(order: Dict, buyer_info: Dict) -> Dict:
    """
    检查菲律宾PH偏远地区订单
    返回: {'is_remote': bool, 'has_chat': bool, 'reason': str}
    """
    rating = order.get('rating')
    total_price = order.get('total_price', 0)
    currency = order.get('currency', '')
    shipping_address = order.get('shipping_address', '') or ''

    # 条件1: 顾客评分 = 0
    try:
        rating_is_zero = rating is not None and float(rating) == 0
    except:
        rating_is_zero = False

    # 条件2: 收货地址位于 Mindanao 或 Visayas 地区
    address_lower = shipping_address.lower()
    is_mindanao = 'mindanao' in address_lower
    is_visayas = 'visayas' in address_lower or 'cebu' in address_lower or 'iloilo' in address_lower
    is_remote_region = is_mindanao or is_visayas

    # 条件3: 订单金额 > 6000 PHP
    is_high_value = currency == 'PHP' and total_price and float(total_price) > 6000

    # 如果不满足任一条件，不属于偏远地区订单
    if not (rating_is_zero and is_remote_region and is_high_value):
        return {'is_remote': False, 'has_chat': False, 'reason': ''}

    # 检查是否有客服交流记录
    user_message_text = buyer_info.get('user_message_text', '') or ''
    has_chat = len(user_message_text.strip()) > 0

    if has_chat:
        return {'is_remote': False, 'has_chat': True, 'reason': '可通过'}
    else:
        return {'is_remote': True, 'has_chat': False, 'reason': '地址偏远'}


def get_buyer_history_orders(conn: str, order_sn: str, buyer_user_id: str) -> List[Tuple]:
    """
    获取买家的全部历史订单（排除当前订单）

    Returns:
        历史订单列表，每项为 (order_sn, status, tracking_number, order_create_time) 元组
    """
    if not buyer_user_id:
        return []

    # Access ODBC 不支持参数化查询，使用字符串拼接
    escaped_buyer = buyer_user_id.replace("'", "''")
    escaped_sn = order_sn.replace("'", "''")

    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT order_sn, status, tracking_number, order_create_time
        FROM shopee_orders
        WHERE buyer_user_id = '{escaped_buyer}' AND order_sn <> '{escaped_sn}'
    """)

    return cursor.fetchall()


def check_suspicious_customer(history_orders: List[Tuple]) -> bool:
    """
    检查可疑顾客：历史是否存在满足以下条件的订单：
    1. 订单状态不为 Completed
    2. 订单状态不为 Canceled
    3. 且无运单号
    """
    for row in history_orders:
        status = row[1]
        tracking_number = row[2]
        if status:
            status_lower = status.lower()

            # 条件1: 订单状态不为 Completed 且不为 To Ship
            is_not_completed_or_to_ship = (status_lower != 'completed' and status_lower != 'to_ship')

            # 条件2: 检查是否为可疑订单
            has_tracking = tracking_number and str(tracking_number).strip()
            is_canceled_with_tracking = (status_lower == 'cancelled') and has_tracking
            is_not_canceled_no_tracking = (status_lower != 'cancelled') and not has_tracking

            if is_not_completed_or_to_ship and (is_canceled_with_tracking or is_not_canceled_no_tracking):
                return True

    return False


def check_tax_request(order: Dict, buyer_info: Dict) -> bool:
    """
    检查税务相关请求
    检查订单备注或聊天记录中是否包含税务关键词
    """
    user_message_text = buyer_info.get('user_message_text', '') or ''

    text_lower = user_message_text.lower()
    for keyword in TAX_KEYWORDS:
        if keyword.lower() in text_lower:
            return True

    return False


def analyze_orders_from_db(db_path: str) -> List[Dict]:
    """
    从数据库分析订单标签

    Args:
        db_path: 数据库文件路径

    Returns:
        订单标签数据列表
        [
            {
                'platform_order_id': '订单ID',  # order_id 作为平台订单号
                'tags': ['标签1', '标签2'],
                'is_pass': False
            },
            ...
        ]
    """
    print("开始分析订单标签...")

    # 连接数据库
    conn = get_db_connection(db_path)

    try:
        # 获取数据
        orders = get_all_orders(conn)
        order_items = get_order_items(conn)
        order_buyers = get_order_buyers(conn)

        # 构建买家信息映射
        buyer_info_map = {b.get('order_sn'): b for b in order_buyers}

        # 构建订单商品映射
        order_items_map = {}
        for item in order_items:
            sn = item.get('order_sn')
            if sn not in order_items_map:
                order_items_map[sn] = []
            order_items_map[sn].append(item)

        # 为每个订单计算标签
        results = []

        for order in orders:
            order_sn = order.get('order_sn', '')
            buyer_user_id = order.get('buyer_user_id')
            rating = order.get('rating')
            order_create_time = order.get('order_create_time')
            tags = []

            # 1. 低分用户
            if check_low_score(rating):
                tags.append('低分不发')

            # 2a. 同单多件
            if check_same_order_multi_items(order_sn, order_items):
                tags.append('同单多件')

            # 2b & 4. 获取买家历史订单
            buyer_history_orders = []
            if buyer_user_id:
                buyer_history_orders = get_buyer_history_orders(conn, order_sn, buyer_user_id)

            # 2b. 高频复购
            if buyer_history_orders and check_high_frequency_repurchase(
                order_sn, order_create_time, buyer_history_orders, order_items_map
            ):
                tags.append('高频复购')

            # 3. 菲律宾偏远地区
            buyer_info = buyer_info_map.get(order_sn, {})
            ph_check = check_ph_remote_area(order, buyer_info)
            if ph_check.get('is_remote'):
                tags.append('地址偏远')

            # 4. 可疑顾客（历史退货退款派送失败）
            if buyer_history_orders and check_suspicious_customer(buyer_history_orders):
                tags.append('历史退货退款派送失败')

            # 5. 税务相关
            if check_tax_request(order, buyer_info):
                tags.append('顾客税务要求')

            # 判断是否为 pass 订单
            is_pass = len(tags) == 0

            results.append({
                'platform_order_id': order_sn,  # 使用 order_sn 作为平台订单号
                'order_sn': order_sn,
                'tags': tags,
                'is_pass': is_pass
            })

        print(f"分析完成: 共 {len(results)} 个订单")
        tag_counts = {}
        for r in results:
            for tag in r['tags']:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
        print(f"标签统计: {tag_counts}")

        # 给没有标签的订单添加 pass 标签
        pass_count = sum(1 for r in results if r['is_pass'])
        print(f"Pass 订单: {pass_count}")

        return results

    finally:
        conn.close()


def get_label_id_mapping() -> Dict[str, str]:
    """
    获取标签名到ID的映射

    Returns:
        标签名到ID的映射字典
    """
    from .config_loader import load_label_mapping
    return load_label_mapping()
