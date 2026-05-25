"""
SQLite 数据库存储模块
与 AccessDatabase 接口完全兼容
"""
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..utils.logger import default_logger as logger
from ..utils.performance_tracker import get_tracker

_sqlite_singleton_instance = None
_sqlite_singleton_lock = threading.Lock()


class SQLiteDatabaseError(Exception):
    pass


class SQLiteDatabase:
    TYPE_MAPPING = {
        'TEXT': 'TEXT',
        'LONGTEXT': 'TEXT',
        'INTEGER': 'INTEGER',
        'BIGINT': 'TEXT',
        'FLOAT': 'REAL',
        'BOOLEAN': 'INTEGER',
        'DATETIME': 'TEXT',
        'AUTO': 'INTEGER PRIMARY KEY AUTOINCREMENT',
    }

    def __init__(self, db_path: str, password: Optional[str] = None, pool_size: int = 1):
        self.db_path = os.path.abspath(db_path)
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None

    @classmethod
    def get_instance(cls, db_path: str, password: Optional[str] = None, pool_size: int = 1) -> 'SQLiteDatabase':
        global _sqlite_singleton_instance
        if _sqlite_singleton_instance is None:
            with _sqlite_singleton_lock:
                if _sqlite_singleton_instance is None:
                    _sqlite_singleton_instance = cls(db_path, password, pool_size)
                    logger.info(f"创建 SQLite 数据库单例实例: {db_path}")
        return _sqlite_singleton_instance

    @classmethod
    def reset_instance(cls):
        global _sqlite_singleton_instance
        with _sqlite_singleton_lock:
            if _sqlite_singleton_instance and _sqlite_singleton_instance._conn:
                try:
                    _sqlite_singleton_instance._conn.close()
                except Exception:
                    pass
            _sqlite_singleton_instance = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
        return self._conn

    @contextmanager
    def get_connection(self):
        yield self._get_conn()

    @contextmanager
    def transaction(self):
        conn = self._get_conn()
        with self._lock:
            try:
                conn.execute("BEGIN")
                yield conn
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def execute(self, sql: str, params: tuple = ()) -> int:
        with self._lock:
            conn = self._get_conn()
            try:
                cursor = conn.execute(sql, params)
                conn.commit()
                return cursor.rowcount
            except sqlite3.Error as e:
                conn.rollback()
                raise SQLiteDatabaseError(f"执行SQL失败: {e}\nSQL: {sql}")

    def execute_sql(self, sql: str) -> int:
        with self._lock:
            conn = self._get_conn()
            try:
                cursor = conn.execute(sql)
                conn.commit()
                return cursor.rowcount
            except sqlite3.Error as e:
                conn.rollback()
                logger.warning(f"执行SQL失败（可能已存在）: {e}\nSQL: {sql}")
                return 0

    def execute_many(self, sql: str, params_list: List[tuple]) -> int:
        with self._lock:
            conn = self._get_conn()
            try:
                conn.executemany(sql, params_list)
                conn.commit()
                return len(params_list)
            except sqlite3.Error as e:
                conn.rollback()
                raise SQLiteDatabaseError(f"批量执行失败: {e}")

    def query(self, sql: str, params: tuple = ()) -> List[Dict]:
        with self._lock:
            conn = self._get_conn()
            try:
                cursor = conn.execute(sql, params)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                result = []
                for row in rows:
                    result.append({
                        col: (None if row[col] is None else str(row[col]) if not isinstance(row[col], str) else row[col])
                        for col in columns
                    })
                return result
            except sqlite3.Error as e:
                raise SQLiteDatabaseError(f"查询失败: {e}\nSQL: {sql}")

    def query_one(self, sql: str, params: tuple = ()) -> Optional[Dict]:
        results = self.query(sql, params)
        return results[0] if results else None

    def query_value(self, sql: str, params: tuple = ()) -> Any:
        result = self.query_one(sql, params)
        if result:
            return list(result.values())[0]
        return None

    # ==================== 表操作 ====================

    def create_table(self, table_name: str, columns: Dict[str, str], primary_key: Optional[str] = None):
        column_defs = []
        for col_name, col_type in columns.items():
            sqlite_type = self.TYPE_MAPPING.get(col_type.upper(), col_type)
            if col_type.upper() == 'AUTO':
                column_defs.append(f"{col_name} {sqlite_type}")
            elif primary_key and col_name == primary_key:
                column_defs.append(f"{col_name} {sqlite_type} PRIMARY KEY")
            else:
                column_defs.append(f"{col_name} {sqlite_type}")

        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"
        self.execute_sql(sql)
        logger.info(f"已创建表: {table_name}")

    def drop_table(self, table_name: str):
        self.execute(f"DROP TABLE IF EXISTS {table_name}")
        logger.info(f"已删除表: {table_name}")

    def table_exists(self, table_name: str) -> bool:
        result = self.query_value(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return result is not None

    def get_table_columns(self, table_name: str) -> List[Dict]:
        rows = self.query(f"PRAGMA table_info({table_name})")
        return [{'name': r['name'], 'type': r['type'], 'nullable': r['notnull'] == '0'} for r in rows]

    # ==================== CRUD 操作 ====================

    def insert(self, table_name: str, data: Dict) -> int:
        columns = list(data.keys())
        placeholders = ', '.join(['?' for _ in columns])
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        return self.execute(sql, tuple(data[c] for c in columns))

    def upsert(self, table_name: str, data: Dict, primary_key: str = 'order_sn') -> int:
        columns = list(data.keys())
        placeholders = ', '.join(['?' for _ in columns])
        sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        return self.execute(sql, tuple(data[c] for c in columns))

    def insert_many(self, table_name: str, data_list: List[Dict]) -> int:
        if not data_list:
            return 0
        columns = list(data_list[0].keys())
        placeholders = ', '.join(['?' for _ in columns])
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        params_list = [tuple(d.get(c) for c in columns) for d in data_list]
        return self.execute_many(sql, params_list)

    def update(self, table_name: str, data: Dict, where: str, where_params: tuple = ()) -> int:
        set_clause = ', '.join(f"{k} = ?" for k in data.keys())
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {where}"
        return self.execute(sql, tuple(data.values()) + where_params)

    def delete(self, table_name: str, where: str, where_params: tuple = ()) -> int:
        sql = f"DELETE FROM {table_name} WHERE {where}"
        return self.execute(sql, where_params)

    def select(self, table_name: str, columns: str = '*', where: Optional[str] = None,
               where_params: tuple = (), order_by: Optional[str] = None,
               limit: Optional[int] = None) -> List[Dict]:
        sql = f"SELECT {columns} FROM {table_name}"
        if where:
            sql += f" WHERE {where}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit:
            sql += f" LIMIT {limit}"
        return self.query(sql, where_params)

    def count(self, table_name: str, where: Optional[str] = None, where_params: tuple = ()) -> int:
        sql = f"SELECT COUNT(*) FROM {table_name}"
        if where:
            sql += f" WHERE {where}"
        return int(self.query_value(sql, where_params) or 0)

    # ==================== 任务相关表 ====================

    def init_task_tables(self):
        if not self.table_exists('tasks'):
            self.create_table('tasks', {
                'id': 'AUTO', 'task_id': 'TEXT', 'env_id': 'TEXT', 'status': 'TEXT',
                'result': 'LONGTEXT', 'error': 'LONGTEXT', 'start_time': 'DATETIME',
                'end_time': 'DATETIME', 'retry_count': 'INTEGER', 'created_at': 'DATETIME'
            })
        if not self.table_exists('environments'):
            self.create_table('environments', {
                'id': 'AUTO', 'env_id': 'TEXT', 'env_name': 'TEXT', 'status': 'TEXT',
                'task_count': 'INTEGER', 'error_count': 'INTEGER',
                'last_used': 'DATETIME', 'created_at': 'DATETIME'
            })
        if not self.table_exists('request_logs'):
            self.create_table('request_logs', {
                'id': 'AUTO', 'env_id': 'TEXT', 'url': 'TEXT', 'method': 'TEXT',
                'status_code': 'INTEGER', 'duration': 'FLOAT', 'created_at': 'DATETIME'
            })
        logger.info("已初始化任务相关表")

    def save_task(self, task_data: Dict):
        task_data['created_at'] = datetime.now().isoformat()
        self.insert('tasks', task_data)

    def update_task_status(self, task_id: str, status: str,
                           result: Optional[str] = None, error: Optional[str] = None):
        data = {'status': status}
        if result:
            data['result'] = result
        if error:
            data['error'] = error
        data['end_time'] = datetime.now().isoformat()
        self.update('tasks', data, 'task_id = ?', (task_id,))

    def save_request_log(self, log_data: Dict):
        log_data['created_at'] = datetime.now().isoformat()
        self.insert('request_logs', log_data)

    # ==================== 订单相关表 ====================

    def init_order_tables(self):
        if not self.table_exists('shopee_orders'):
            self.create_table('shopee_orders', {
                'order_sn': 'TEXT', 'order_id': 'TEXT', 'shop_id': 'TEXT',
                'region_id': 'TEXT', 'env_name': 'TEXT', 'status': 'TEXT',
                'fulfilment_channel': 'TEXT', 'total_price': 'FLOAT', 'currency': 'TEXT',
                'shipping_name': 'TEXT', 'shipping_phone': 'TEXT', 'shipping_address': 'LONGTEXT',
                'tracking_number': 'TEXT', 'buyer_user_id': 'TEXT', 'rating': 'FLOAT',
                'update_time': 'DATETIME', 'order_create_time': 'DATETIME'
            }, primary_key='order_sn')

        if not self.table_exists('shopee_order_items'):
            self.create_table('shopee_order_items', {
                'item_id': 'TEXT', 'order_sn': 'TEXT', 'order_id': 'TEXT',
                'item_name': 'TEXT', 'item_description': 'TEXT',
                'amount': 'INTEGER', 'model_id': 'TEXT', 'created_at': 'DATETIME'
            })
            self.execute_sql("CREATE INDEX IF NOT EXISTS idx_order_sn ON shopee_order_items (order_sn)")
            self.execute_sql("CREATE INDEX IF NOT EXISTS idx_model_id ON shopee_order_items (model_id)")

        if not self.table_exists('shopee_order_buyer'):
            self.create_table('shopee_order_buyer', {
                'order_sn': 'TEXT', 'order_id': 'TEXT', 'buyer_user_id': 'TEXT',
                'buyer_username': 'TEXT', 'avatar': 'TEXT', 'rating': 'FLOAT',
                'country': 'TEXT', 'city': 'TEXT', 'conversation_id': 'TEXT',
                'total_messages': 'INTEGER', 'user_messages_count': 'INTEGER',
                'user_message_text': 'LONGTEXT', 'created_at': 'DATETIME'
            }, primary_key='order_sn')

        logger.info("已初始化订单相关表")

    def save_order(self, order_data: Dict, env_name: str = None) -> int:
        order_record = {
            'order_id': order_data.get('order_id'),
            'order_sn': order_data.get('order_sn'),
            'shop_id': order_data.get('shop_id'),
            'region_id': order_data.get('region_id'),
            'env_name': env_name or '',
            'status': order_data.get('status', ''),
            'fulfilment_channel': order_data.get('fulfilment_channel', ''),
            'total_price': order_data.get('total_price', 0),
            'currency': order_data.get('currency', 'MYR'),
            'shipping_name': order_data.get('shipping_name', ''),
            'shipping_phone': order_data.get('shipping_phone', ''),
            'shipping_address': order_data.get('shipping_address', ''),
            'tracking_number': order_data.get('tracking_number', ''),
            'order_create_time': order_data.get('create_time'),
            'update_time': order_data.get('update_time'),
        }
        return self.upsert('shopee_orders', order_record)

    def save_order_batch(self, orders: List[Dict], env_name: str = None) -> int:
        if not orders:
            return 0
        records = [{
            'order_id': o.get('order_id'), 'order_sn': o.get('order_sn'),
            'shop_id': o.get('shop_id'), 'region_id': o.get('region_id'),
            'env_name': env_name or '', 'status': o.get('status', ''),
            'fulfilment_channel': o.get('fulfilment_channel', ''),
            'total_price': o.get('total_price', 0), 'currency': o.get('currency', 'MYR'),
            'order_create_time': o.get('create_time'), 'update_time': o.get('update_time'),
        } for o in orders]
        return self.insert_many('shopee_orders', records)

    def save_order_items(self, order_id: str, order_sn: str, items: List[Dict]) -> int:
        if not items:
            return 0
        total = 0
        for item in items:
            record = {
                'item_id': item.get('item_id'), 'order_id': order_id, 'order_sn': order_sn,
                'item_name': item.get('name', ''), 'item_description': item.get('description', ''),
                'amount': item.get('amount', 1), 'model_id': item.get('model_id'),
                'created_at': datetime.now().isoformat(),
            }
            total += self.upsert('shopee_order_items', record, 'item_id')
        return total

    def save_order_buyer(self, order_id: str, order_sn: str, buyer_data: Dict) -> int:
        record = {
            'order_id': order_id, 'order_sn': order_sn,
            'buyer_user_id': buyer_data.get('buyer_user_id'),
            'buyer_username': buyer_data.get('buyer_username', ''),
            'avatar': buyer_data.get('avatar'), 'rating': buyer_data.get('rating'),
            'country': buyer_data.get('country'), 'city': buyer_data.get('city'),
            'created_at': datetime.now().isoformat(),
        }
        return self.upsert('shopee_order_buyer', record)

    def get_orders_by_env(self, env_name: str, status: str = None, limit: int = 100) -> List[Dict]:
        where = "env_name = ?"
        params = (env_name,)
        if status:
            where += " AND status = ?"
            params = (env_name, status)
        return self.select('shopee_orders', where=where, where_params=params,
                           order_by='order_create_time DESC', limit=limit)

    def get_order_by_sn(self, order_sn: str) -> Optional[Dict]:
        return self.query_one("SELECT * FROM shopee_orders WHERE order_sn = ?", (order_sn,))

    def order_exists(self, order_sn: str) -> bool:
        return self.count('shopee_orders', 'order_sn = ?', (order_sn,)) > 0

    def update_order_status(self, order_sn: str, status: str) -> int:
        return self.update('shopee_orders', {'status': status}, 'order_sn = ?', (order_sn,))

    # ==================== 批量操作方法 ====================

    def save_orders_batch_transaction(self, orders: List[Dict], env_name: str = None) -> int:
        if not orders:
            return 0
        tracker = get_tracker()
        tracker.start('DB:保存订单', env=env_name)
        saved_count = 0
        now = datetime.now().isoformat()
        with self._lock:
            with self.transaction() as conn:
                for order_data in orders:
                    try:
                        record = {
                            'order_id': order_data.get('order_id'),
                            'order_sn': order_data.get('order_sn'),
                            'shop_id': order_data.get('shop_id'),
                            'region_id': order_data.get('region_id'),
                            'env_name': env_name or '',
                            'status': order_data.get('status', ''),
                            'fulfilment_channel': order_data.get('fulfilment_channel', ''),
                            'total_price': order_data.get('total_price', 0),
                            'currency': order_data.get('currency', 'MYR'),
                            'shipping_name': order_data.get('shipping_name', ''),
                            'shipping_phone': order_data.get('shipping_phone', ''),
                            'shipping_address': order_data.get('shipping_address', ''),
                            'tracking_number': order_data.get('tracking_number', ''),
                            'buyer_user_id': order_data.get('buyer_user_id', ''),
                            'rating': order_data.get('rating'),
                            'order_create_time': order_data.get('order_create_time'),
                            'update_time': now,
                        }
                        self._upsert_single_in_transaction(conn.cursor(), 'shopee_orders', record, 'order_sn')
                        saved_count += 1
                    except Exception as e:
                        logger.warning(f"保存订单失败: {e}")
        tracker.end('DB:保存订单', {'count': saved_count}, env=env_name)
        return saved_count

    def update_orders_batch(self, orders: List[Dict], env_name: str = None) -> int:
        if not orders:
            return 0
        tracker = get_tracker()
        tracker.start('DB:更新订单', env=env_name)
        updated_count = 0
        now = datetime.now().isoformat()
        with self._lock:
            with self.transaction() as conn:
                for order_data in orders:
                    try:
                        record = {
                            'order_id': order_data.get('order_id'),
                            'shop_id': order_data.get('shop_id'),
                            'region_id': order_data.get('region_id'),
                            'env_name': env_name or '',
                            'status': order_data.get('status', ''),
                            'fulfilment_channel': order_data.get('fulfilment_channel', ''),
                            'total_price': order_data.get('total_price', 0),
                            'currency': order_data.get('currency', 'MYR'),
                            'shipping_name': order_data.get('shipping_name', ''),
                            'shipping_phone': order_data.get('shipping_phone', ''),
                            'shipping_address': order_data.get('shipping_address', ''),
                            'tracking_number': order_data.get('tracking_number', ''),
                            'buyer_user_id': order_data.get('buyer_user_id', ''),
                            'rating': order_data.get('rating'),
                            'order_create_time': order_data.get('order_create_time'),
                            'update_time': now,
                        }
                        order_sn = order_data.get('order_sn')
                        if order_sn:
                            self._update_single_in_transaction(conn.cursor(), 'shopee_orders', record, 'order_sn', order_sn)
                            updated_count += 1
                    except Exception as e:
                        logger.warning(f"更新订单失败: {e}")
        tracker.end('DB:更新订单', {'count': updated_count}, env=env_name)
        return updated_count

    def update_order_buyers_batch(self, buyers: List[Dict]) -> int:
        if not buyers:
            return 0
        tracker = get_tracker()
        tracker.start('DB:更新买家')
        total_affected = 0
        with self._lock:
            with self.transaction() as conn:
                for item in buyers:
                    try:
                        buyer_data = item.get('buyer_data', {})
                        chat_data = item.get('chat_data', {})
                        record = {
                            'buyer_user_id': buyer_data.get('buyer_user_id'),
                            'buyer_username': buyer_data.get('buyer_username', ''),
                            'avatar': buyer_data.get('avatar'),
                            'rating': buyer_data.get('rating'),
                            'country': buyer_data.get('country'),
                            'city': buyer_data.get('city'),
                            'conversation_id': chat_data.get('conversation_id'),
                            'total_messages': chat_data.get('total_messages', 0),
                            'user_messages_count': chat_data.get('user_messages_count', 0),
                            'user_message_text': chat_data.get('user_message_text', ''),
                        }
                        order_sn = item.get('order_sn')
                        if order_sn:
                            self._update_single_in_transaction(conn.cursor(), 'shopee_order_buyer', record, 'order_sn', order_sn)
                            total_affected += 1
                    except Exception as e:
                        logger.warning(f"更新买家失败: {e}")
        tracker.end('DB:更新买家', {'count': total_affected})
        return total_affected

    def _update_single_in_transaction(self, cursor, table_name: str, data: Dict, pk_field: str, pk_value: str):
        set_clause = ', '.join(f"{k} = ?" for k in data.keys())
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {pk_field} = ?"
        cursor.execute(sql, tuple(data.values()) + (pk_value,))

    def _insert_ignore_duplicates(self, cursor, table_name: str, data: Dict):
        columns = list(data.keys())
        placeholders = ', '.join(['?' for _ in columns])
        sql = f"INSERT OR IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.execute(sql, tuple(data[c] for c in columns))

    def _upsert_single_in_transaction(self, cursor, table_name: str, data: Dict, pk_field: str):
        columns = list(data.keys())
        placeholders = ', '.join(['?' for _ in columns])
        sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.execute(sql, tuple(data[c] for c in columns))

    def save_order_items_batch(self, order_id: str, order_sn: str, items: List[Dict]) -> int:
        if not items:
            return 0
        tracker = get_tracker()
        tracker.start('DB:保存商品')
        total_affected = 0
        now = datetime.now().isoformat()
        with self._lock:
            with self.transaction() as conn:
                for item in items:
                    try:
                        record = {
                            'item_id': item.get('item_id'), 'order_id': order_id,
                            'order_sn': order_sn, 'item_name': item.get('name', ''),
                            'item_description': item.get('description', ''),
                            'amount': item.get('amount', 1), 'model_id': item.get('model_id'),
                            'created_at': now,
                        }
                        self._insert_ignore_duplicates(conn.cursor(), 'shopee_order_items', record)
                        total_affected += 1
                    except Exception as e:
                        logger.warning(f"保存商品失败: {e}")
        tracker.end('DB:保存商品', {'count': total_affected})
        return total_affected

    def save_order_buyers_batch(self, buyers: List[Dict]) -> int:
        if not buyers:
            return 0
        tracker = get_tracker()
        tracker.start('DB:保存买家')
        buyer_map = {item.get('order_sn'): item for item in buyers if item.get('order_sn')}
        unique_buyers = list(buyer_map.values())
        logger.info(f"[sqlite_db] 买家数据去重: {len(buyers)} -> {len(unique_buyers)}")
        total_affected = 0
        now = datetime.now().isoformat()
        with self._lock:
            with self.transaction() as conn:
                for item in unique_buyers:
                    try:
                        buyer_data = item.get('buyer_data', {})
                        chat_data = item.get('chat_data', {})
                        record = {
                            'order_id': item.get('order_id'),
                            'order_sn': item.get('order_sn'),
                            'buyer_user_id': buyer_data.get('buyer_user_id'),
                            'buyer_username': buyer_data.get('buyer_username', ''),
                            'avatar': buyer_data.get('avatar'),
                            'rating': buyer_data.get('rating'),
                            'country': buyer_data.get('country'),
                            'city': buyer_data.get('city'),
                            'conversation_id': chat_data.get('conversation_id'),
                            'total_messages': chat_data.get('total_messages', 0),
                            'user_messages_count': chat_data.get('user_messages_count', 0),
                            'user_message_text': chat_data.get('user_message_text', ''),
                            'created_at': now,
                        }
                        self._upsert_single_in_transaction(conn.cursor(), 'shopee_order_buyer', record, 'order_sn')
                        total_affected += 1
                    except Exception as e:
                        logger.warning(f"保存买家失败: {e}")
        tracker.end('DB:保存买家', {'count': total_affected})
        return total_affected

    def check_orders_exist_batch(self, order_sns: List[str], batch_size: int = 500) -> Dict[str, bool]:
        if not order_sns:
            return {}
        result = {}
        for i in range(0, len(order_sns), batch_size):
            batch = order_sns[i:i + batch_size]
            placeholders = ', '.join(['?' for _ in batch])
            try:
                rows = self.query(
                    f"SELECT order_sn FROM shopee_orders WHERE order_sn IN ({placeholders})",
                    tuple(batch)
                )
                existing = {row['order_sn'] for row in rows}
                for oid in batch:
                    result[oid] = oid in existing
            except Exception as e:
                logger.warning(f"批量检查订单失败 (批次 {i // batch_size + 1}): {e}")
                for oid in batch:
                    result[oid] = False
        logger.info(f"[check_orders_exist_batch] Checked {len(order_sns)} order_sns, found {sum(result.values())} existing")
        return result
