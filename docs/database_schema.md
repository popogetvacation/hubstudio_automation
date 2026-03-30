# 数据库架构文档

本文档记录 HubStudio 自动化项目使用的 Access 数据库架构。

## 数据库文件

- **路径**: `data/automation.accdb`
- **连接方式**: pyodbc + Microsoft Access Driver (*.mdb, *.accdb)

---

## 表结构

### 1. shopee_orders (订单主表)

| 字段名 | 类型 | 说明 | 主键 | 索引 |
|--------|------|------|------|------|
| order_sn | TEXT | 订单号 | YES | |
| order_id | TEXT | 订单ID | | |
| shop_id | TEXT | 店铺ID | | |
| region_id | TEXT | 区域ID | | |
| env_name | TEXT | 环境名称 | | |
| status | TEXT | 订单状态 | | |
| fulfilment_channel | TEXT | 发货渠道 | | |
| total_price | FLOAT | 订单总价 | | |
| currency | TEXT | 货币 | | |
| shipping_name | TEXT | 收货人姓名 | | |
| shipping_phone | TEXT | 收货人电话 | | |
| shipping_address | LONGTEXT | 收货地址 | | |
| tracking_number | TEXT | 追踪号 | | |
| buyer_user_id | TEXT | 买家用户ID | | 可重复索引 |
| rating | FLOAT | 评分 | | |
| update_time | DATETIME | 更新时间 | | |
| order_create_time | DATETIME | 订单创建时间 | | |

### 2. shopee_order_items (订单商品表)

| 字段名 | 类型 | 说明 | 索引 |
|--------|------|------|------|
| item_id | TEXT | 商品ID | |
| order_sn | TEXT | 订单号 | 可重复索引 |
| order_id | TEXT | 订单ID | |
| item_name | TEXT | 商品名称 | |
| item_description | TEXT | 商品描述 | |
| amount | INTEGER | 数量 | |
| model_id | TEXT | 规格ID | 可重复索引 |
| created_at | DATETIME | 记录创建时间 | |

### 3. shopee_order_buyer (订单买家信息表)

| 字段名 | 类型 | 说明 | 主键 |
|--------|------|------|------|
| order_sn | TEXT | 订单号 | YES |
| order_id | TEXT | 订单ID | |
| buyer_user_id | TEXT | 买家用户ID | |
| buyer_username | TEXT | 买家用户名 | |
| avatar | TEXT | 头像URL | |
| rating | FLOAT | 评分 | |
| country | TEXT | 国家 | |
| city | TEXT | 城市 | |
| conversation_id | TEXT | 对话ID | |
| total_messages | INTEGER | 消息总数 | |
| user_messages_count | INTEGER | 用户消息数 | |
| user_message_text | LONGTEXT | 用户消息内容 | |
| created_at | DATETIME | 记录创建时间 | |

> 注：聊天信息已合并到此表，不再单独创建 shopee_order_chat 表

### 4. tasks (任务表)

| 字段名 | 类型 | 说明 | 主键 |
|--------|------|------|------|
| id | AUTO | 自增ID | YES |
| task_id | TEXT | 任务ID | |
| env_id | TEXT | 环境ID | |
| status | TEXT | 状态 | |
| result | LONGTEXT | 结果 | |
| error | LONGTEXT | 错误信息 | |
| start_time | DATETIME | 开始时间 | |
| end_time | DATETIME | 结束时间 | |
| retry_count | INTEGER | 重试次数 | |
| created_at | DATETIME | 创建时间 | |

### 5. environments (环境表)

| 字段名 | 类型 | 说明 | 主键 |
|--------|------|------|------|
| id | AUTO | 自增ID | YES |
| env_id | TEXT | 环境ID | |
| env_name | TEXT | 环境名称 | |
| status | TEXT | 状态 | |
| task_count | INTEGER | 任务数量 | |
| error_count | INTEGER | 错误数量 | |
| last_used | DATETIME | 最后使用时间 | |
| created_at | DATETIME | 创建时间 | |

### 6. request_logs (请求日志表)

| 字段名 | 类型 | 说明 | 主键 |
|--------|------|------|------|
| id | AUTO | 自增ID | YES |
| env_id | TEXT | 环境ID | |
| url | TEXT | 请求URL | |
| method | TEXT | 请求方法 | |
| status_code | INTEGER | 状态码 | |
| duration | FLOAT | 耗时(秒) | |
| created_at | DATETIME | 创建时间 | |

---

## 核心操作

### 批量检查订单是否存在

```python
# 使用 order_id 主键批量检查
existing_map = database.check_orders_exist_batch(order_sns)
# 返回: {order_id: True/False}
```

### 批量保存订单 (事务)

```python
saved_count = database.save_orders_batch_transaction(orders, env_name)
```

### 关键字段说明

- **order_sn**: Shopee 订单号字符串，用于主键和去重检查
- **order_id**: Shopee 订单的13位数字ID
- **主键策略**: 各表使用 `order_sn` 作为主键，支持 upsert

---

## 注意事项

1. Access 对 IN 子句长度有限制，建议分批检查
2. 数字类型 BIGINT 在 Access 中使用 TEXT 存储
3.LONGTEXT 对应 Access 的 MEMO 类型
