"""
Access 数据库存储模块
使用 pyodbc 连接 Access 数据库
"""
from typing import Optional, Dict, List, Any, Union
from dataclasses import dataclass
import os
import threading
from contextlib import contextmanager
from datetime import datetime

try:
    import pyodbc
except ImportError:
    pyodbc = None

from ..utils.logger import default_logger as logger
from ..utils.performance_tracker import get_tracker

# 单例实例
_singleton_instance = None
_singleton_lock = threading.Lock()


@dataclass
class TableSchema:
    """表结构"""
    name: str
    columns: List[Dict[str, Any]]


class AccessDatabaseError(Exception):
    """Access 数据库错误"""
    pass


class AccessDatabase:
    """
    Access 数据库操作类

    支持 CRUD 操作和事务
    """

    # Access 数据类型映射
    TYPE_MAPPING = {
        'TEXT': 'TEXT',
        'LONGTEXT': 'MEMO',
        'INTEGER': 'INTEGER',
        'BIGINT': 'TEXT',  # Access LONG 最大约21亿，使用 TEXT 存储超长数字
        'FLOAT': 'DOUBLE',
        'BOOLEAN': 'BIT',
        'DATETIME': 'DATETIME',
        'AUTO': 'COUNTER PRIMARY KEY'
    }

    # Access 保留字列表
    RESERVED_WORDS = {
        'ADD', 'ALL', 'ALLOW', 'ALTER', 'AND', 'ANY', 'AS', 'ASC', 'AUTHORIZATION',
        'BETWEEN', 'BINARY', 'BIT', 'BOOLEAN', 'BY', 'BYTE', 'CHAR', 'CHARACTER',
        'COLUMN', 'CONSTRAINT', 'COUNT', 'COUNTER', 'CREATE', 'CURRENCY', 'DATABASE',
        'DATE', 'DATETIME', 'DEFAULT', 'DELETE', 'DESC', 'DISALLOW', 'DISTINCT',
        'DOUBLE', 'DROP', 'ELSE', 'END', 'EQV', 'ERROR', 'EXISTS', 'FALSE', 'FLOAT',
        'FOREIGN', 'FROM', 'GENERAL', 'GROUP', 'GUID', 'HAVING', 'IN', 'INDEX',
        'INNER', 'INSERT', 'INT', 'INTEGER', 'INTO', 'IS', 'JOIN', 'KEY', 'LAST',
        'LEFT', 'LEVEL', 'LIKE', 'LOGICAL', 'LONG', 'LONGTEXT', 'MAX', 'MIN',
        'MOD', 'MONEY', 'MULTIPLE', 'NAME', 'NO', 'NOT', 'NULL', 'NUMBER', 'OLE',
        'OBJECT', 'ON', 'OPTIMIZE', 'OPTION', 'OR', 'ORDER', 'OUTER', 'OWNERACCESS',
        'PARAM', 'PASSWORD', 'PERCENT', 'PIVOT', 'PRIMARY', 'PROCEDURE', 'PUBLIC',
        'REAL', 'REFERENCES', 'REFRESH', 'REQUERY', 'RIGHT', 'SCHEMA', 'SELECT',
        'SET', 'SHORT', 'SINGLE', 'SOME', 'SQL', 'STDEV', 'STDEVP', 'STRING', 'SUM',
        'TABLE', 'TEXT', 'TIME', 'TIMESTAMP', 'TOP', 'TRUE', 'TYPE', 'UNION', 'UNIQUE',
        'UPDATE', 'USER', 'VALUE', 'VALUES', 'VAR', 'VARBINARY', 'VARCHAR', 'VIEW',
        'WHERE', 'WITH', 'XOR', 'YEAR', 'YES', 'YESNO'
    }

    @staticmethod
    def _escape_name(name: str) -> str:
        """转义表名或列名（如果它们是保留字）"""
        if name.upper() in AccessDatabase.RESERVED_WORDS:
            return f"[{name}]"
        return name

    @staticmethod
    def _format_datetime(dt: datetime) -> str:
        """将 datetime 转换为 Access 格式 #yyyy-mm-dd hh:nn:ss#"""
        if dt is None:
            return "NULL"
        return f"#{dt.year:04d}-{dt.month:02d}-{dt.day:02d} {dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}#"

    def _format_sql_value(self, v):
        """将 Python 值转换为 SQL 字符串"""
        if v is None:
            return 'NULL'
        if isinstance(v, str):
            escaped = v.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(v, datetime):
            return self._format_datetime(v)
        if isinstance(v, (int, float)) and v > 1000000000:
            try:
                dt = datetime.fromtimestamp(v)
                return self._format_datetime(dt)
            except (ValueError, OSError):
                return 'NULL'
        if isinstance(v, bool):
            return '1' if v else '0'
        escaped = str(v).replace("'", "''")
        return f"'{escaped}'"

    def __init__(self, db_path: str, password: Optional[str] = None, pool_size: int = 3):
        """
        初始化数据库连接

        Args:
            db_path: 数据库文件路径 (.accdb 或 .mdb)
            password: 数据库密码
            pool_size: 连接池大小（默认3）
        """
        if pyodbc is None:
            raise ImportError("请安装 pyodbc: pip install pyodbc")

        self.db_path = db_path
        self.password = password
        self._lock = threading.RLock()
        self._pool_size = pool_size
        self._connection_pool = []  # 连接池

    @classmethod
    def get_instance(cls, db_path: str, password: Optional[str] = None, pool_size: int = 1) -> 'AccessDatabase':
        """
        获取单例实例（解决多进程并发写入问题）

        Args:
            db_path: 数据库文件路径
            password: 数据库密码
            pool_size: 连接池大小（默认1，确保串行访问）

        Returns:
            AccessDatabase 单例实例
        """
        global _singleton_instance
        if _singleton_instance is None:
            with _singleton_lock:
                if _singleton_instance is None:
                    _singleton_instance = cls(db_path, password, pool_size)
                    logger.info(f"创建数据库单例实例: {db_path}")
        return _singleton_instance

    @classmethod
    def reset_instance(cls):
        """重置单例实例（用于测试或重新初始化）"""
        global _singleton_instance
        with _singleton_lock:
            _singleton_instance = None

    def _get_connection_string(self) -> str:
        """获取连接字符串"""
        ext = os.path.splitext(self.db_path)[1].lower()

        # Access
        if ext == '.accdb':
            driver = 'Microsoft Access Driver (*.mdb, *.accdb)'
        else:
            driver = 'Microsoft Access Driver (*.mdb)'

        conn_str = f"DRIVER={{{driver}}};DBQ={self.db_path};charset=utf-8;"

        if self.password:
            conn_str += f"PWD={self.password};"

        return conn_str

    def _get_connection_from_pool(self) -> 'pyodbc.Connection':
        """从连接池获取连接，如果没有可用连接则创建新的"""
        with self._lock:
            # 尝试从池中获取连接
            while self._connection_pool:
                conn = self._connection_pool.pop()
                try:
                    # 验证连接是否有效
                    conn.cursor().close()
                    return conn
                except:
                    # 连接无效，丢弃并尝试下一个
                    try:
                        conn.close()
                    except:
                        pass
            # 池为空，创建新连接
            return self._create_connection()

    def _return_connection_to_pool(self, conn: 'pyodbc.Connection'):
        """将连接返回到连接池"""
        if conn is None:
            return
        with self._lock:
            if len(self._connection_pool) < self._pool_size:
                try:
                    conn.cursor().close()
                    self._connection_pool.append(conn)
                except:
                    try:
                        conn.close()
                    except:
                        pass
            else:
                try:
                    conn.close()
                except:
                    pass

    def _create_connection(self) -> 'pyodbc.Connection':
        """创建新的数据库连接"""
        conn_str = self._get_connection_string()
        conn = pyodbc.connect(conn_str)
        # 使用 latin1 编码（Access 默认编码）
        conn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding='latin1')
        return conn

    @contextmanager
    def get_connection(self):
        """获取数据库连接上下文管理器（支持连接池）"""
        conn = None
        try:
            conn = self._get_connection_from_pool()
            yield conn
        except pyodbc.Error as e:
            # 连接失败，关闭并抛出异常
            if conn:
                try:
                    conn.close()
                except:
                    pass
            raise AccessDatabaseError(f"数据库连接失败: {e}")
        finally:
            # 将连接返回到池中
            if conn:
                self._return_connection_to_pool(conn)

    @contextmanager
    def transaction(self):
        """事务上下文管理器"""
        conn = None
        try:
            conn = self._get_connection_from_pool()
            conn.autocommit = False
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise
        finally:
            if conn:
                conn.autocommit = True
                self._return_connection_to_pool(conn)

    def execute(self, sql: str, params: tuple = ()) -> int:
        """
        执行 SQL 语句

        Args:
            sql: SQL 语句
            params: 参数

        Returns:
            影响的行数
        """
        # Access ODBC 不支持参数化查询，需要直接替换参数
        if params:
            params_list = list(params)
            for param in params_list:
                if param is None:
                    sql = sql.replace('?', 'NULL', 1)
                elif isinstance(param, str):
                    escaped = param.replace("'", "''")
                    sql = sql.replace('?', f"'{escaped}'", 1)
                elif isinstance(param, datetime):
                    sql = sql.replace('?', self._format_datetime(param), 1)
                else:
                    sql = sql.replace('?', str(param), 1)

        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql)
                    conn.commit()
                    return cursor.rowcount
                except pyodbc.Error as e:
                    conn.rollback()
                    raise AccessDatabaseError(f"执行SQL失败: {e}\nSQL: {sql}")

    def execute_sql(self, sql: str) -> int:
        """
        执行任意 SQL 语句（用于 DDL 操作）

        Args:
            sql: SQL 语句

        Returns:
            影响的行数
        """
        with self._lock:
            with self.get_connection() as conn:
                try:
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    conn.commit()
                    return cursor.rowcount
                except pyodbc.Error as e:
                    conn.rollback()
                    logger.warning(f"执行SQL失败（可能已存在）: {e}\nSQL: {sql}")
                    return 0

    def execute_many(self, sql: str, params_list: List[tuple]) -> int:
        """
        批量执行 SQL 语句

        Args:
            sql: SQL 语句
            params_list: 参数列表

        Returns:
            影响的总行数
        """
        # Access ODBC 不支持参数化查询，需要直接替换参数
        processed_sql = sql
        if params_list and params_list[0]:
            # 获取第一个参数列表作为模板
            first_params = params_list[0]
            temp_sql = sql
            for param in first_params:
                if param is None:
                    temp_sql = temp_sql.replace('?', 'NULL', 1)
                elif isinstance(param, str):
                    escaped = param.replace("'", "''")
                    temp_sql = temp_sql.replace('?', f"'{escaped}'", 1)
                elif isinstance(param, datetime):
                    temp_sql = temp_sql.replace('?', self._format_datetime(param), 1)
                else:
                    temp_sql = temp_sql.replace('?', str(param), 1)
            processed_sql = temp_sql

        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(processed_sql)
                    conn.commit()
                    return len(params_list)
                except pyodbc.Error as e:
                    conn.rollback()
                    raise AccessDatabaseError(f"批量执行失败: {e}")

    def query(self, sql: str, params: tuple = ()) -> List[Dict]:
        """
        查询数据

        Args:
            sql: SQL 语句
            params: 参数

        Returns:
            查询结果列表
        """
        # Access ODBC 不支持参数化查询，需要直接替换参数
        if params:
            params_list = list(params)
            for param in params_list:
                if isinstance(param, str):
                    escaped = param.replace("'", "''")
                    sql = sql.replace('?', f"'{escaped}'", 1)
                elif isinstance(param, datetime):
                    sql = sql.replace('?', self._format_datetime(param), 1)
                else:
                    sql = sql.replace('?', str(param), 1)

        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                # 处理编码问题 - 直接转换 str，避免编码错误
                result = []
                for row in rows:
                    encoded_row = []
                    for value in row:
                        if value is None:
                            encoded_row.append(None)
                        elif isinstance(value, str):
                            encoded_row.append(value)
                        else:
                            # 其他类型转为字符串
                            encoded_row.append(str(value))
                    result.append(dict(zip(columns, encoded_row)))
                return result
            except Exception as e:
                raise AccessDatabaseError(f"查询失败: {e}\nSQL: {sql}")

    def query_one(self, sql: str, params: tuple = ()) -> Optional[Dict]:
        """
        查询单条数据

        Args:
            sql: SQL 语句
            params: 参数

        Returns:
            查询结果或 None
        """
        results = self.query(sql, params)
        return results[0] if results else None

    def query_value(self, sql: str, params: tuple = ()) -> Any:
        """
        查询单个值

        Args:
            sql: SQL 语句
            params: 参数

        Returns:
            查询值或 None
        """
        result = self.query_one(sql, params)
        if result:
            return list(result.values())[0]
        return None

    # ==================== 表操作 ====================

    def create_table(self, table_name: str,
                     columns: Dict[str, str],
                     primary_key: Optional[str] = None):
        """
        创建表

        Args:
            table_name: 表名
            columns: 列定义 {列名: 类型}
            primary_key: 主键列名
        """
        column_defs = []
        pk_column = None

        for col_name, col_type in columns.items():
            # 转换类型
            access_type = self.TYPE_MAPPING.get(col_type.upper(), col_type)
            # 转义列名
            safe_col_name = self._escape_name(col_name)

            if primary_key and col_name == primary_key:
                # 主键列单独处理
                pk_column = col_name
                if col_type.upper() in ('INTEGER', 'BIGINT', 'AUTO'):
                    # COUNTER 是 Access 的自增类型
                    column_defs.append(f"{safe_col_name} COUNTER")
                else:
                    column_defs.append(f"{safe_col_name} {access_type}")
            else:
                column_defs.append(f"{safe_col_name} {access_type}")

        # 构建 SQL
        safe_table_name = self._escape_name(table_name)
        sql = f"CREATE TABLE {safe_table_name} ({', '.join(column_defs)})"

        try:
            self.execute(sql)
        except AccessDatabaseError as e:
            # 如果创建失败，尝试分步创建：先创建表，再添加主键
            logger.warning(f"直接创建表失败，尝试分步创建: {e}")
            self._create_table_in_steps(table_name, columns, primary_key)
            return

        # 如果有主键，尝试添加主键约束
        if pk_column:
            try:
                # 使用 ALTER TABLE 添加主键
                safe_pk = self._escape_name(pk_column)
                self.execute(f"ALTER TABLE {safe_table_name} ADD PRIMARY KEY ({safe_pk})")
            except AccessDatabaseError:
                # 可能已经通过 COUNTER PRIMARY KEY 创建，忽略错误
                pass

        logger.info(f"已创建表: {table_name}")

    def _create_table_in_steps(self, table_name: str,
                                columns: Dict[str, str],
                                primary_key: Optional[str] = None):
        """
        分步创建表（用于解决 Access ODBC 驱动的限制）
        1. 先创建表，只包含少数列
        2. 使用 ALTER TABLE 添加其余列
        3. 添加主键约束
        """
        safe_table_name = self._escape_name(table_name)

        # 第一步：只创建主键列（如果存在）
        pk_column = None
        if primary_key and primary_key in columns:
            pk_column = primary_key
            col_type = columns[primary_key]
            access_type = self.TYPE_MAPPING.get(col_type.upper(), col_type)
            safe_pk = self._escape_name(pk_column)

            if col_type.upper() in ('INTEGER', 'BIGINT', 'AUTO'):
                sql = f"CREATE TABLE {safe_table_name} ({safe_pk} COUNTER)"
            else:
                sql = f"CREATE TABLE {safe_table_name} ({safe_pk} {access_type})"

            self.execute(sql)

            # 尝试添加主键
            try:
                self.execute(f"ALTER TABLE {safe_table_name} ADD PRIMARY KEY ({safe_pk})")
            except AccessDatabaseError:
                pass
        else:
            # 没有主键，创建前两列
            col_items = list(columns.items())
            if col_items:
                col_name, col_type = col_items[0]
                access_type = self.TYPE_MAPPING.get(col_type.upper(), col_type)
                safe_col_name = self._escape_name(col_name)
                sql = f"CREATE TABLE {safe_table_name} ({safe_col_name} {access_type})"
                self.execute(sql)

        # 第二步：逐个添加其余列
        for col_name, col_type in columns.items():
            if col_name == pk_column:
                continue

            access_type = self.TYPE_MAPPING.get(col_type.upper(), col_type)
            safe_col_name = self._escape_name(col_name)
            try:
                self.execute(f"ALTER TABLE {safe_table_name} ADD {safe_col_name} {access_type}")
            except AccessDatabaseError as e:
                logger.warning(f"添加列 {col_name} 失败: {e}")

        logger.info(f"已通过分步创建表: {table_name}")

    def drop_table(self, table_name: str):
        """删除表"""
        safe_table = self._escape_name(table_name)
        self.execute(f"DROP TABLE {safe_table}")
        logger.info(f"已删除表: {table_name}")

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            safe_table = self._escape_name(table_name)
            self.query(f"SELECT TOP 1 * FROM {safe_table}")
            return True
        except AccessDatabaseError:
            return False

    def get_table_columns(self, table_name: str) -> List[Dict]:
        """获取表的列信息"""
        safe_table = self._escape_name(table_name)
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {safe_table} WHERE 1=0")
            return [
                {
                    'name': desc[0],
                    'type': desc[1],
                    'nullable': not desc[3]
                }
                for desc in cursor.description
            ]

    # ==================== CRUD 操作 ====================

    def insert(self, table_name: str, data: Dict) -> int:
        """
        插入数据

        Args:
            table_name: 表名
            data: 数据字典

        Returns:
            影响的行数
        """
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ', '.join(['?' for _ in values])

        # 转义表名和列名
        safe_table = self._escape_name(table_name)
        safe_columns = [self._escape_name(col) for col in columns]

        sql = f"INSERT INTO {safe_table} ({', '.join(safe_columns)}) VALUES ({placeholders})"
        return self.execute(sql, tuple(values))

    def upsert(self, table_name: str, data: Dict, primary_key: str = 'order_sn') -> int:
        """
        插入或更新数据（UPSERT）

        Args:
            table_name: 表名
            data: 数据字典
            primary_key: 主键字段名

        Returns:
            影响的行数
        """
        # 先删除已存在的记录，然后插入
        pk_value = data.get(primary_key)
        if pk_value:
            try:
                self.execute(f"DELETE FROM {self._escape_name(table_name)} WHERE {self._escape_name(primary_key)} = ?", (pk_value,))
            except:
                pass

        # 插入新记录
        return self.insert(table_name, data)

    def insert_many(self, table_name: str, data_list: List[Dict]) -> int:
        """
        批量插入数据

        Args:
            table_name: 表名
            data_list: 数据字典列表

        Returns:
            影响的行数
        """
        if not data_list:
            return 0

        columns = list(data_list[0].keys())
        placeholders = ', '.join(['?' for _ in columns])

        # 转义表名和列名
        safe_table = self._escape_name(table_name)
        safe_columns = [self._escape_name(col) for col in columns]

        sql = f"INSERT INTO {safe_table} ({', '.join(safe_columns)}) VALUES ({placeholders})"

        params_list = [
            tuple(data.get(col) for col in columns)
            for data in data_list
        ]

        return self.execute_many(sql, params_list)

    def update(self, table_name: str, data: Dict,
               where: str, where_params: tuple = ()) -> int:
        """
        更新数据

        Args:
            table_name: 表名
            data: 更新的数据
            where: WHERE 条件
            where_params: WHERE 参数

        Returns:
            影响的行数
        """
        # 转义表名
        safe_table = self._escape_name(table_name)

        # Access ODBC 不支持参数化查询，需要直接替换参数
        set_parts = []
        params_list = list(data.values())
        for i, k in enumerate(data.keys()):
            v = params_list[i]
            safe_k = self._escape_name(k)
            if v is None:
                set_parts.append(f"{safe_k} = NULL")
            elif isinstance(v, str):
                escaped = v.replace("'", "''")
                set_parts.append(f"{safe_k} = '{escaped}'")
            elif isinstance(v, datetime):
                set_parts.append(f"{safe_k} = {self._format_datetime(v)}")
            elif isinstance(v, bool):
                set_parts.append(f"{safe_k} = {1 if v else 0}")
            else:
                set_parts.append(f"{safe_k} = {v}")
        set_clause = ', '.join(set_parts)

        # 处理 where 参数
        if where and where_params:
            for param in where_params:
                if isinstance(param, str):
                    escaped = param.replace("'", "''")
                    where = where.replace('?', f"'{escaped}'", 1)
                elif isinstance(param, datetime):
                    where = where.replace('?', self._format_datetime(param), 1)
                else:
                    where = where.replace('?', str(param), 1)

        sql = f"UPDATE {safe_table} SET {set_clause} WHERE {where}"
        return self.execute(sql)

    def delete(self, table_name: str, where: str,
               where_params: tuple = ()) -> int:
        """
        删除数据

        Args:
            table_name: 表名
            where: WHERE 条件
            where_params: WHERE 参数

        Returns:
            影响的行数
        """
        # 转义表名
        safe_table = self._escape_name(table_name)

        # Access ODBC 不支持参数化查询，需要直接替换参数
        if where and where_params:
            for param in where_params:
                if isinstance(param, str):
                    escaped = param.replace("'", "''")
                    where = where.replace('?', f"'{escaped}'", 1)
                elif isinstance(param, datetime):
                    where = where.replace('?', self._format_datetime(param), 1)
                else:
                    where = where.replace('?', str(param), 1)

        sql = f"DELETE FROM {safe_table} WHERE {where}"
        return self.execute(sql)

    def select(self, table_name: str, columns: str = '*',
               where: Optional[str] = None,
               where_params: tuple = (),
               order_by: Optional[str] = None,
               limit: Optional[int] = None) -> List[Dict]:
        """
        查询数据

        Args:
            table_name: 表名
            columns: 列名
            where: WHERE 条件
            where_params: WHERE 参数
            order_by: 排序
            limit: 限制数量

        Returns:
            查询结果
        """
        # 转义表名
        safe_table = self._escape_name(table_name)
        sql = f"SELECT {columns} FROM {safe_table}"

        # Access ODBC 不支持参数化查询，需要直接替换参数
        if where and where_params:
            for param in where_params:
                if isinstance(param, str):
                    escaped = param.replace("'", "''")
                    where = where.replace('?', f"'{escaped}'", 1)
                elif isinstance(param, datetime):
                    where = where.replace('?', self._format_datetime(param), 1)
                else:
                    where = where.replace('?', str(param), 1)
            sql += f" WHERE {where}"
        elif where:
            sql += f" WHERE {where}"

        if order_by:
            sql += f" ORDER BY {order_by}"

        if limit:
            sql = f"SELECT TOP {limit} * FROM ({sql}) AS subq"

        return self.query(sql, where_params)

    def count(self, table_name: str,
              where: Optional[str] = None,
              where_params: tuple = ()) -> int:
        """
        统计数量

        Args:
            table_name: 表名
            where: WHERE 条件
            where_params: WHERE 参数

        Returns:
            数量
        """
        # 转义表名
        safe_table = self._escape_name(table_name)
        sql = f"SELECT COUNT(*) FROM {safe_table}"
        if where:
            # Access ODBC 不支持参数化查询，需要直接替换参数
            if where_params:
                # 简单处理：将 ? 替换为参数值（仅适用于字符串和数字）
                params_list = list(where_params)
                for i, param in enumerate(params_list):
                    if isinstance(param, str):
                        # 对字符串参数加引号并转义单引号
                        escaped = param.replace("'", "''")
                        where = where.replace('?', f"'{escaped}'", 1)
                    elif isinstance(param, datetime):
                        where = where.replace('?', f"#{param}#", 1)
                    else:
                        # 数字直接替换
                        where = where.replace('?', str(param), 1)
            sql += f" WHERE {where}"

        return self.query_value(sql) or 0

    # ==================== 任务相关表 ====================

    def init_task_tables(self):
        """初始化任务相关表"""
        # 任务表
        if not self.table_exists('tasks'):
            self.create_table('tasks', {
                'id': 'AUTO',
                'task_id': 'TEXT',
                'env_id': 'TEXT',
                'status': 'TEXT',
                'result': 'LONGTEXT',
                'error': 'LONGTEXT',
                'start_time': 'DATETIME',
                'end_time': 'DATETIME',
                'retry_count': 'INTEGER',
                'created_at': 'DATETIME'
            }, primary_key='id')

        # 环境表
        if not self.table_exists('environments'):
            self.create_table('environments', {
                'id': 'AUTO',
                'env_id': 'TEXT',
                'env_name': 'TEXT',
                'status': 'TEXT',
                'task_count': 'INTEGER',
                'error_count': 'INTEGER',
                'last_used': 'DATETIME',
                'created_at': 'DATETIME'
            }, primary_key='id')

        # 请求日志表
        if not self.table_exists('request_logs'):
            self.create_table('request_logs', {
                'id': 'AUTO',
                'env_id': 'TEXT',
                'url': 'TEXT',
                'method': 'TEXT',
                'status_code': 'INTEGER',
                'duration': 'FLOAT',
                'created_at': 'DATETIME'
            }, primary_key='id')

        logger.info("已初始化任务相关表")

    def save_task(self, task_data: Dict):
        """保存任务数据"""
        task_data['created_at'] = datetime.now()
        self.insert('tasks', task_data)

    def update_task_status(self, task_id: str, status: str,
                           result: Optional[str] = None,
                           error: Optional[str] = None):
        """更新任务状态"""
        data = {'status': status}
        if result:
            data['result'] = result
        if error:
            data['error'] = error
        data['end_time'] = datetime.now()

        self.update('tasks', data, 'task_id = ?', (task_id,))

    def save_request_log(self, log_data: Dict):
        """保存请求日志"""
        log_data['created_at'] = datetime.now()
        self.insert('request_logs', log_data)

    # ==================== 订单相关表 ====================

    def init_order_tables(self):
        """初始化订单相关表"""
        # 订单主表 - 使用 order_sn 作为主键
        if not self.table_exists('shopee_orders'):
            self.create_table('shopee_orders', {
                'order_sn': 'TEXT PRIMARY KEY',
                'order_id': 'TEXT',
                'shop_id': 'TEXT',
                'region_id': 'TEXT',
                'env_name': 'TEXT',
                'status': 'TEXT',
                'fulfilment_channel': 'TEXT',
                'total_price': 'FLOAT',
                'currency': 'TEXT',
                'shipping_name': 'TEXT',
                'shipping_phone': 'TEXT',
                'shipping_address': 'LONGTEXT',
                'tracking_number': 'TEXT',
                'buyer_user_id': 'TEXT',
                'rating': 'FLOAT',
                'update_time': 'DATETIME',
                'order_create_time': 'DATETIME'
            }, primary_key='order_sn')

        # 订单商品表 - 无主键，使用 order_sn + item_id + model_id 组合索引
        if not self.table_exists('shopee_order_items'):
            self.create_table('shopee_order_items', {
                'item_id': 'TEXT',
                'order_sn': 'TEXT',
                'order_id': 'TEXT',
                'item_name': 'TEXT',
                'item_description': 'TEXT',
                'amount': 'INTEGER',
                'model_id': 'TEXT',
                'created_at': 'DATETIME'
            })
            # 创建索引
            self.execute_sql("CREATE INDEX idx_order_sn ON shopee_order_items (order_sn)")
            self.execute_sql("CREATE INDEX idx_model_id ON shopee_order_items (model_id)")

        # 订单买家信息表（合并聊天信息）- 使用 order_sn 作为主键
        if not self.table_exists('shopee_order_buyer'):
            self.create_table('shopee_order_buyer', {
                'order_sn': 'TEXT PRIMARY KEY',
                'order_id': 'TEXT',
                'buyer_user_id': 'TEXT',
                'buyer_username': 'TEXT',
                'avatar': 'TEXT',
                'rating': 'FLOAT',
                'country': 'TEXT',
                'city': 'TEXT',
                'conversation_id': 'TEXT',
                'total_messages': 'INTEGER',
                'user_messages_count': 'INTEGER',
                'user_message_text': 'LONGTEXT',
                'created_at': 'DATETIME'
            }, primary_key='order_sn')

        # 不再创建 shopee_order_chat 表（已合并到 shopee_order_buyer）

        logger.info("已初始化订单相关表")

    def save_order(self, order_data: Dict, env_name: str = None) -> int:
        """
        保存订单数据

        Args:
            order_data: 订单数据字典
            env_name: 环境名称

        Returns:
            影响的行数
        """
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
            'update_time': order_data.get('update_time')
        }
        return self.upsert('shopee_orders', order_record)

    def save_order_batch(self, orders: List[Dict], env_name: str = None) -> int:
        """
        批量保存订单数据

        Args:
            orders: 订单数据列表
            env_name: 环境名称

        Returns:
            影响的行数
        """
        if not orders:
            return 0

        order_records = []
        for order_data in orders:
            order_records.append({
                'order_id': order_data.get('order_id'),
                'order_sn': order_data.get('order_sn'),
                'shop_id': order_data.get('shop_id'),
                'region_id': order_data.get('region_id'),
                'env_name': env_name or '',
                'status': order_data.get('status', ''),
                'fulfilment_channel': order_data.get('fulfilment_channel', ''),
                'total_price': order_data.get('total_price', 0),
                'currency': order_data.get('currency', 'MYR'),
                'order_create_time': order_data.get('create_time'),
                'update_time': order_data.get('update_time')
            })

        return self.insert_many('shopee_orders', order_records)

    def save_order_items(self, order_id: str, order_sn: str, items: List[Dict]) -> int:
        """
        保存订单商品数据

        Args:
            order_id: 订单 ID
            order_sn: 订单号
            items: 商品列表

        Returns:
            影响的行数
        """
        if not items:
            return 0

        total_affected = 0
        for item in items:
            item_record = {
                'item_id': item.get('item_id'),
                'order_id': order_id,
                'order_sn': order_sn,
                'item_name': item.get('name', ''),
                'item_description': item.get('description', ''),
                'amount': item.get('amount', 1),
                'model_id': item.get('model_id'),
                'created_at': datetime.now()
            }
            total_affected += self.upsert('shopee_order_items', item_record, 'item_id')

        return total_affected

    def save_order_buyer(self, order_id: str, order_sn: str,
                        buyer_data: Dict) -> int:
        """
        保存订单买家信息

        Args:
            order_id: 订单 ID
            order_sn: 订单号
            buyer_data: 买家信息字典

        Returns:
            影响的行数
        """
        buyer_record = {
            'order_id': order_id,
            'order_sn': order_sn,
            'buyer_user_id': buyer_data.get('buyer_user_id'),
            'buyer_username': buyer_data.get('buyer_username', ''),
            'avatar': buyer_data.get('avatar'),
            'rating': buyer_data.get('rating'),
            'country': buyer_data.get('country'),
            'city': buyer_data.get('city'),
            'created_at': datetime.now()
        }
        return self.upsert('shopee_order_buyer', buyer_record)

    def get_orders_by_env(self, env_name: str,
                         status: str = None,
                         limit: int = 100) -> List[Dict]:
        """
        获取指定环境的订单

        Args:
            env_name: 环境名称
            status: 订单状态筛选
            limit: 限制数量

        Returns:
            订单列表
        """
        where = "env_name = ?"
        params = (env_name,)

        if status:
            where += " AND status = ?"
            params = (env_name, status)

        return self.select('shopee_orders',
                          where=where,
                          where_params=params,
                          order_by='create_time DESC',
                          limit=limit)

    def get_order_by_sn(self, order_sn: str) -> Optional[Dict]:
        """根据订单号查询订单"""
        # Access ODBC 不支持参数化查询，使用直接值
        escaped = order_sn.replace("'", "''")
        return self.query_one(
            f"SELECT * FROM shopee_orders WHERE order_sn = '{escaped}'"
        )

    def order_exists(self, order_sn: str) -> bool:
        """检查订单是否已存在"""
        return self.count('shopee_orders', 'order_sn = ?', (order_sn,)) > 0

    def update_order_status(self, order_sn: str, status: str) -> int:
        """更新订单状态"""
        return self.update(
            'shopee_orders',
            {'status': status},
            'order_sn = ?',
            (order_sn,)
        )

    # ==================== 批量操作方法 ====================

    def save_orders_batch_transaction(self, orders: List[Dict], env_name: str = None) -> int:
        """
        批量保存订单数据（使用事务）

        Args:
            orders: 订单数据列表
            env_name: 环境名称

        Returns:
            保存的订单数量
        """
        if not orders:
            return 0

        tracker = get_tracker()
        tracker.start('DB:保存订单')
        saved_count = 0
        now = datetime.now()
        with self.transaction() as conn:
            cursor = conn.cursor()

            # 准备批量插入的 SQL（使用 UNION ALL 子查询方式）
            for order_data in orders:
                try:
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
                        'buyer_user_id': order_data.get('buyer_user_id', ''),
                        'rating': order_data.get('rating'),
                        'order_create_time': order_data.get('order_create_time'),
                        'update_time': now,
                    }

                    # 使用 upsert：先尝试更新，再尝试插入
                    self._upsert_single_in_transaction(cursor, 'shopee_orders', order_record, 'order_sn')
                    saved_count += 1
                except Exception as e:
                    logger.warning(f"保存订单失败: {e}")

        tracker.end('DB:保存订单', {'count': saved_count})
        return saved_count

    def update_orders_batch(self, orders: List[Dict], env_name: str = None) -> int:
        """
        批量更新已存在的订单数据（使用事务）

        Args:
            orders: 订单数据列表
            env_name: 环境名称

        Returns:
            更新的订单数量
        """
        if not orders:
            return 0

        tracker = get_tracker()
        tracker.start('DB:更新订单')
        updated_count = 0
        with self.transaction() as conn:
            cursor = conn.cursor()
            now = datetime.now()

            for order_data in orders:
                try:
                    order_record = {
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
                        self._update_single_in_transaction(cursor, 'shopee_orders', order_record, 'order_sn', order_sn)
                        updated_count += 1
                except Exception as e:
                    logger.warning(f"更新订单失败: {e}")

        tracker.end('DB:更新订单', {'count': updated_count})
        return updated_count

    def update_order_buyers_batch(self, buyers: List[Dict]) -> int:
        """
        批量更新已存在的订单买家信息

        Args:
            buyers: 买家信息列表 [{order_id, order_sn, buyer_data, chat_data?}, ...]

        Returns:
            更新的买家数量
        """
        if not buyers:
            return 0

        tracker = get_tracker()
        tracker.start('DB:更新买家')

        # 按 order_sn 去重，保留最后一条记录（数据库主键是 order_sn）
        buyer_map = {}
        for item in buyers:
            order_sn = item.get('order_sn')
            if order_sn:
                buyer_map[order_sn] = item

        unique_buyers = list(buyer_map.values())
        logger.info(f"[acc_db] 买家数据去重: {len(buyers)} -> {len(unique_buyers)}")

        total_affected = 0
        now = datetime.now()

        with self.transaction() as conn:
            cursor = conn.cursor()

            for item in unique_buyers:
                try:
                    buyer_data = item.get('buyer_data', {})
                    chat_data = item.get('chat_data', {})
                    buyer_record = {
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
                        self._update_single_in_transaction(cursor, 'shopee_order_buyer', buyer_record, 'order_sn', order_sn)
                        total_affected += 1
                except Exception as e:
                    logger.warning(f"更新买家失败: {e}")
                    continue

        tracker.end('DB:更新买家', {'count': total_affected})
        return total_affected

    def _update_single_in_transaction(self, cursor, table_name: str, data: Dict, pk_field: str, pk_value: str):
        """在事务中执行单条更新"""
        safe_table = self._escape_name(table_name)

        set_parts = []
        for col, val in data.items():
            safe_col = self._escape_name(col)
            set_parts.append(f"{safe_col} = {self._format_sql_value(val)}")
        set_clause = ', '.join(set_parts)

        safe_pk = self._escape_name(pk_field)
        pk_formatted = self._format_sql_value(pk_value)
        sql = f"UPDATE {safe_table} SET {set_clause} WHERE {safe_pk} = {pk_formatted}"
        cursor.execute(sql)

    def _insert_ignore_duplicates(self, cursor, table_name: str, data: Dict):
        """在事务中执行插入，忽略重复键错误"""
        safe_table = self._escape_name(table_name)
        columns = list(data.keys())
        safe_columns = [self._escape_name(col) for col in columns]

        values_list = [self._format_sql_value(data.get(col)) for col in columns]
        sql = f"INSERT INTO {safe_table} ({', '.join(safe_columns)}) VALUES ({', '.join(values_list)})"
        cursor.execute(sql)

    def _upsert_single_in_transaction(self, cursor, table_name: str, data: Dict, pk_field: str):
        """在事务中执行单条 upsert"""
        safe_table = self._escape_name(table_name)

        # 构建字段列表和值 - 完全避免参数化查询，使用字符串拼接
        columns = list(data.keys())
        safe_columns = [self._escape_name(col) for col in columns]

        # 构建 INSERT 语句 - 使用 VALUES 字符串拼接
        values_list = [self._format_sql_value(data.get(col)) for col in columns]
        sql = f"INSERT INTO {safe_table} ({', '.join(safe_columns)}) VALUES ({', '.join(values_list)})"

        # 执行插入，忽略重复键错误
        try:
            cursor.execute(sql)
        except Exception as e:
            # 如果插入失败，尝试更新
            logger.error(f"插入失败:{str(e)}")
            pk_value = data.get(pk_field)
            if pk_value:
                set_parts = []
                for col in columns:
                    if col != pk_field:
                        safe_col = self._escape_name(col)
                        v = data.get(col)
                        set_parts.append(f"{safe_col} = {self._format_sql_value(v)}")
                set_clause = ', '.join(set_parts)
                safe_pk = self._escape_name(pk_field)
                pk_formatted = self._format_sql_value(pk_value)
                update_sql = f"UPDATE {safe_table} SET {set_clause} WHERE {safe_pk} = {pk_formatted}"
                cursor.execute(update_sql)

    def save_order_items_batch(self, order_id: str, order_sn: str, items: List[Dict]) -> int:
        """
        批量保存订单商品数据

        Args:
            order_id: 订单 ID
            order_sn: 订单号
            items: 商品列表

        Returns:
            影响的行数
        """
        if not items:
            return 0

        tracker = get_tracker()
        tracker.start('DB:保存商品')
        total_affected = 0
        now = datetime.now()

        with self.transaction() as conn:
            cursor = conn.cursor()

            for item in items:
                try:
                    item_record = {
                        'item_id': item.get('item_id'),
                        'order_id': order_id,
                        'order_sn': order_sn,
                        'item_name': item.get('name', ''),
                        'item_description': item.get('description', ''),
                        'amount': item.get('amount', 1),
                        'model_id': item.get('model_id'),
                        'created_at': now
                    }
                    # 直接插入，忽略重复键错误（表已无主键）
                    self._insert_ignore_duplicates(cursor, 'shopee_order_items', item_record)
                    total_affected += 1
                except Exception as e:
                    logger.warning(f"保存商品失败: {e}")
                    continue

        tracker.end('DB:保存商品', {'count': total_affected})
        return total_affected

    def save_order_buyers_batch(self, buyers: List[Dict]) -> int:
        """
        批量保存订单买家信息（包含聊天数据）

        Args:
            buyers: 买家信息列表 [{order_id, order_sn, buyer_data, chat_data?}, ...]

        Returns:
            影响的行数
        """
        if not buyers:
            return 0

        tracker = get_tracker()
        tracker.start('DB:保存买家')

        # 按 order_sn 去重，保留最后一条记录（数据库主键是 order_sn）
        buyer_map = {}
        for item in buyers:
            order_sn = item.get('order_sn')
            if order_sn:
                buyer_map[order_sn] = item

        unique_buyers = list(buyer_map.values())
        logger.info(f"[acc_db] 买家数据去重: {len(buyers)} -> {len(unique_buyers)}")

        total_affected = 0
        now = datetime.now()

        with self.transaction() as conn:
            cursor = conn.cursor()

            for item in unique_buyers:
                try:
                    buyer_data = item.get('buyer_data', {})
                    chat_data = item.get('chat_data', {})
                    buyer_record = {
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
                        'created_at': now
                    }
                    self._upsert_single_in_transaction(cursor, 'shopee_order_buyer', buyer_record, 'order_sn')
                    total_affected += 1
                except Exception as e:
                    logger.warning(f"保存买家失败: {e}")
                    continue

        tracker.end('DB:保存买家', {'count': total_affected})
        return total_affected

    def check_orders_exist_batch(self, order_sns: List[str], batch_size: int = 50) -> Dict[str, bool]:
        """
        批量检查订单是否存在（使用 order_sn 主键）

        Args:
            order_sns: 订单号列表
            batch_size: 每批处理数量（避免 IN 子句过长）

        Returns:
            {order_sn: exists}
        """
        if not order_sns:
            return {}

        result = {}

        # 分批处理，避免 IN 子句过长（Access 限制约 2048 字符）
        for i in range(0, len(order_sns), batch_size):
            batch = order_sns[i:i + batch_size]

            # 转换为字符串并加引号
            escaped_ids = []
            for oid in batch:
                escaped = str(oid).replace("'", "''")
                escaped_ids.append(f"'{escaped}'")
            in_clause = ', '.join(escaped_ids)

            try:
                rows = self.query(f"SELECT order_sn FROM shopee_orders WHERE order_sn IN ({in_clause})")
                existing_ids = {row.get('order_sn') for row in rows}
                for oid in batch:
                    result[oid] = oid in existing_ids
            except Exception as e:
                logger.warning(f"批量检查订单失败 (批次 {i//batch_size + 1}): {e}")
                for oid in batch:
                    result[oid] = False

        logger.info(f"[check_orders_exist_batch] Checked {len(order_sns)} order_sns, found {sum(result.values())} existing,res {len(result)}")
        return result
