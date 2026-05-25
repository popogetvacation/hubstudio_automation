"""
一次性迁移脚本：将 Access 数据库数据迁移到 SQLite
用法: python scripts/migrate_access_to_sqlite.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.config import load_config
from src.database.access_db import AccessDatabase
from src.database.sqlite_db import SQLiteDatabase

TABLES_WITH_PK = [
    ('tasks', 'id'),
    ('environments', 'id'),
    ('request_logs', 'id'),
    ('shopee_orders', 'order_sn'),
    ('shopee_order_buyer', 'order_sn'),
]
TABLES_NO_PK = ['shopee_order_items']
BATCH_SIZE = 200


def migrate():
    cfg = load_config()
    print(f"源: {cfg.database.access_path}")
    print(f"目标: {cfg.database.sqlite_path}")

    src = AccessDatabase.get_instance(cfg.database.access_path)
    dst = SQLiteDatabase.get_instance(cfg.database.sqlite_path)

    dst.init_task_tables()
    dst.init_order_tables()

    for table, pk in TABLES_WITH_PK:
        if not src.table_exists(table):
            print(f"  跳过 {table}（源库不存在）")
            continue
        rows = src.query(f"SELECT * FROM {table}")
        print(f"  {table}: {len(rows)} 行")
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            with dst.transaction() as conn:
                for row in batch:
                    dst._upsert_single_in_transaction(conn.cursor(), table, row, pk)
        print(f"  {table}: 完成")

    for table in TABLES_NO_PK:
        if not src.table_exists(table):
            print(f"  跳过 {table}（源库不存在）")
            continue
        rows = src.query(f"SELECT * FROM {table}")
        print(f"  {table}: {len(rows)} 行")
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            with dst.transaction() as conn:
                for row in batch:
                    dst._insert_ignore_duplicates(conn.cursor(), table, row)
        print(f"  {table}: 完成")

    print("迁移完成")


if __name__ == '__main__':
    migrate()
