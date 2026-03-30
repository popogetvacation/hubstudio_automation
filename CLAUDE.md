# 项目上下文

## 数据库架构

参考 [docs/database_schema.md](docs/database_schema.md)

## 关键约束

- **订单去重**: 使用 `order_id` 主键检查订单是否已存在
- **批量操作**: Access 对 IN 子句长度有限制（约 2048 字符），大批量时需分批处理
- **主键策略**: 各表使用 `order_id` 或 `item_id` 作为主键，支持 upsert

## 核心模块

- `src/tasks/shopee_all_order_task.py` - Shopee 全部订单任务
- `src/database/access_db.py` - Access 数据库操作
- `src/api/shopee_api.py` - Shopee API 封装

## 注意事项

1. 数字类型 BIGINT 在 Access 中使用 TEXT 存储以避免溢出
2. LONGTEXT 对应 Access 的 MEMO 类型
3. 批量检查订单存在性使用 `check_orders_exist_batch(order_sns)`
