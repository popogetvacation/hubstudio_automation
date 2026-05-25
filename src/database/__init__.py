from .access_db import (
    AccessDatabase,
    AccessDatabaseError,
    TableSchema
)
from .sqlite_db import SQLiteDatabase, SQLiteDatabaseError


def get_database(db_path: str = None, db_type: str = None):
    """工厂函数：根据配置返回 AccessDatabase 或 SQLiteDatabase 实例"""
    from ..config import load_config
    cfg = load_config()
    _type = db_type or cfg.database.db_type
    if _type == "sqlite":
        return SQLiteDatabase.get_instance(db_path or cfg.database.sqlite_path)
    return AccessDatabase.get_instance(db_path or cfg.database.access_path)


__all__ = [
    'AccessDatabase',
    'AccessDatabaseError',
    'TableSchema',
    'SQLiteDatabase',
    'SQLiteDatabaseError',
    'get_database',
]
