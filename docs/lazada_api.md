# Lazada Seller Center API 分析

> 分析时间: 2026-04-09
> 站点: Philippines (Lazada PH)
> 页面: Order Management - 待发货 (topack/toship)

---

## 一、核心 API

### 1. 获取订单列表
**`mtop.lazada.seller.order.query.list`**

```
GET https://acs-m.lazada.com.ph/h5/mtop.lazada.seller.order.query.list/1.0/
```

#### Query 参数

| 参数 | 示例值 | 说明 |
|------|--------|------|
| `jsv` | `2.6.1` | JS 版本，固定值 |
| `appKey` | `4272` | Lazada 分配的应用 Key |
| `t` | `1775721305988` | 13位毫秒时间戳 |
| `sign` | `019002a0940432a3db4d357acd3d2f67` | MD5 签名，动态变化 |
| `v` | `1.0` | API 版本 |
| `timeout` | `30000` | 超时 30 秒 |
| `H5Request` | `true` | 固定 true |
| `url` | `mtop.lazada.seller.order.query.list` | API 名称 |
| `api` | `mtop.lazada.seller.order.query.list` | API 名称 |
| `type` | `originaljson` | 响应类型 |
| `dataType` | `json` | 数据类型 |
| `valueType` | `original` | 值类型 |
| `x-i18n-regionID` | `LAZADA_PH` | 站点 ID (菲律宾) |
| `data` | `%7B...%7D` | 请求体 JSON，URL 编码 |

#### data 请求体 (解码后)

```json
{
  "page": 1,
  "pageSize": 20,
  "filterOrderItems": true,
  "sort": "SHIPPING_SLA",
  "sortOrder": "ASC",
  "tab": "topack"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `page` | int | 页码，从 1 开始 |
| `pageSize` | int | 每页数量 |
| `filterOrderItems` | bool | 是否过滤订单商品 |
| `sort` | string | 排序字段，如 `SHIPPING_SLA` |
| `sortOrder` | string | `ASC` 或 `DESC` |
| `tab` | string | 订单状态标签 |

#### tab 可选值

| tab 值 | 含义 | 当前数量 (示例) |
|--------|------|----------------|
| `unpaid` | 待付款 | 5 |
| `topack` / `toship` | 待发货 (Pending Pack) | 43 |
| `toshiparrangeshipment` | 待安排发货 | 1 |
| `shipped` | 已发货 | 99 |
| `shipping` | 运输中 | 99 |
| `returnRefund` | 退货退款 | 2 |
| `canceled` | 已取消 | 6293 |
| `pending` / `processing` | 处理中 | 44 |

---

### 2. 获取各状态订单数量
**`mtop.lazada.seller.order.query.count`**

```
GET https://acs-m.lazada.com.ph/h5/mtop.lazada.seller.order.query.count/1.0/
```

#### data 请求体
```json
{}
```

#### 响应示例
```json
{
  "api": "mtop.lazada.seller.order.query.count",
  "data": {
    "data": {
      "shipment": 99,
      "topackapp": 1,
      "awbPrintedAndCanceled": 8,
      "pending": 44,
      "unpaid": 5,
      "returnRefund": 2,
      "toshiparrangeshipment": 1,
      "canceled": 6293,
      "toship": 1,
      "shipped": 99,
      "shipping": 99,
      "weeklyCanceled": 52,
      "topack": 43,
      "processing": 44,
      "tobepackedupapp": 99
    }
  }
}
```

---

### 3. 获取页面初始化配置
**`mtop.lazada.seller.order.query.list.init`**

```
GET https://acs-m.lazada.com.ph/h5/mtop.lazada.seller.order.query.list.init/1.0/
```

#### data 请求体
```json
{}
```

#### 响应关键字段
- `warehouseOptions` - 仓库选项列表
- `common3PLOptions` - 常用物流商选项
- `isShow3Pl` - 是否显示 3PL
- `enableSplitPackageFeatures` - 是否启用拆包功能
- `enableSlaBreachedFilter` - 是否启用 SLA 逾期筛选

---

### 4. SLA 逾期统计
**`mtop.lazada.seller.order.sla.breached.count`**

```
GET https://acs-m.lazada.com.ph/h5/mtop.lazada.seller.order.sla.breached.count/1.0/
```

#### data 请求体
```json
{"tab": "topack"}
```

---

## 二、认证机制

### 必需 Cookie

| Cookie 名 | 说明 |
|-----------|------|
| `_m_h5_tk` | 主 Token，格式: `{token}_{timestamp}` |
| `_m_h5_tk_enc` | Token 编码 |
| `asc_uid` | 卖家用户 ID |
| `asc_uid.sig` | UID 签名 |
| `asc_uid_enc` | UID 编码 |
| `asc_uid_enc.sig` | UID 编码签名 |
| `CSRFT` | CSRF Token |
| `TID` | Transaction ID |
| `t_sid` | Session ID |
| `epssw` | 加密验证串 |

### 签名机制 (sign)

`sign` 是 **纯 MD5** 哈希的结果（注意：不是 HMAC-MD5）。

**签名公式:**
```python
sign = MD5(token + "&" + timestamp + "&" + appKey + "&" + data)
```

**Python 实现:**
```python
import hashlib
import json

token = "72ad85c1d9fa57a5c777e0f835e7061c"  # 从 _m_h5_tk 中提取
timestamp = str(int(time.time() * 1000))  # 13位毫秒时间戳
app_key = "4272"  # 订单相关API固定值
data = {"page": 1, "pageSize": 20, ...}  # 请求体

# data 必须使用紧凑格式（无空格）
data_str = json.dumps(data, separators=(',', ':'))

# 计算 MD5 签名
message = f"{token}&{timestamp}&{app_key}&{data_str}"
sign = hashlib.md5(message.encode('utf-8')).hexdigest().lower()
```

**签名原材料格式:**
```
{_m_h5_tk的值(去掉时间戳部分)}&{timestamp}&{appKey}&{data参数JSON原文}
```

**示例:**
```
消息原文: 72ad85c1d9fa57a5c777e0f835e7061c&1776004563230&4272&{"page":1,"pageSize":20,"filterOrderItems":true,"sort":"SHIPPING_SLA","sortOrder":"ASC","tab":"topack"}
MD5结果: 924a1623bfb663f332e58362c85048af
```

**重要说明:**
- 使用 **MD5**，不是 HMAC-MD5
- `token` 从 `_m_h5_tk` Cookie 中提取（下划线前的部分）
- `timestamp` 使用当前时间戳（13位毫秒），**不是** Cookie 中的时间戳
- `data` 必须是紧凑 JSON 格式（`separators=(',', ':')`）
- `_m_h5_tk_enc` Cookie **不参与**签名计算

---

## 三、响应数据字段 (订单列表)

### 订单级别字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `orderNumber` | string | 订单号 |
| `buyerId` | long | 买家用户 ID |
| `buyerName` | string | 买家姓名 |
| `createDate` | string | 下单日期 (如 "02 Apr 2026") |
| `createTime` | string | 下单时间 |
| `creationTime` | long | 下单时间戳 (毫秒) |
| `paymentMethod` | string | 支付方式 (COD, COD_PLUS 等) |
| `cashOnDelivery` | bool | 是否货到付款 |
| `totalRetailPrice` | string | 订单总额 |
| `totalUnitPrice` | string | 商品总价 |
| `shippingFee` | string | 运费 |
| `totalSupplyPrice` | string | 供货价 |
| `totalQuantity` | int | 商品总数量 |
| `orderType` | string | 订单类型 (Normal 等) |
| `orderTypeName` | string | 订单类型名称 |
| `tabStatus` | string | 当前状态标签 |
| `holdStatus` | string | 冻结状态 |
| `warehouseName` | string | 仓库名称 |
| `warehouseCode` | string | 仓库代码 |
| `express` | bool | 是否急速发货 |
| `preOrder` | bool | 是否预售订单 |
| `guarantee` | bool | 是否担保 |
| `gc` | bool | 是否礼品卡 |
| `imChatLink` | string | 买家聊天链接 |

### 收货地址 (shippingAddress)

| 字段 | 说明 |
|------|------|
| `receiver` | 收货人 |
| `detailAddress` | 详细地址 |
| `isWork` | 是否工作地址 |

### 商品信息 (skus 数组)

每个订单的 `skus` 数组包含一个或多个商品:

| 字段 | 类型 | 说明 |
|------|------|------|
| `orderItemId` | string | 订单项 ID |
| `orderNumber` | string | 订单号 |
| `productName` | string | 商品名称 |
| `productTitle` | string | 商品标题 |
| `skuHiddenLevel` | int | SKU 隐藏级别 |
| `shopSku` | string | 店铺 SKU |
| `sellerSku` | string | 卖家 SKU |
| `skuInfo` | string | SKU 变体信息 |
| `image` | string | 商品图片 URL |
| `quantity` | int | 数量 |
| `unitPrice` | string | 单价 |
| `totalRetailPrice` | string | 零售价合计 |
| `totalUnitPrice` | string | 商品总价 |
| `adjustFee` | string | 调整费用 |
| `totalSupplyPrice` | string | 供货价 |
| `itemStatus` | string | 商品状态 (pending 等) |
| `packageStatusName` | string | 包裹状态名称 |
| `deliveryType` | string | 配送类型 (STANDARD 等) |
| `warehouseCode` | string | 仓库代码 |
| `warehouseType` | string | 仓库类型 |
| `shipmentProviderName` | string | 物流商名称 |
| `lastMileShipmentProviderName` | string | 最后一公里物流商 |
| `fulfillmentOrderId` | string | 履约单 ID |
| `sotSla` | string | 标准发货时效截止时间 |
| `ttsSla` | string | 实际发货截止时间 |
| `ttsSlaExtended` | bool | 是否已延长 TTSLA |
| `slaBreached` | bool | 是否已逾期 |
| `slaRuleInfo` | string | SLA 规则说明 |
| `slaOptions` | array | SLA 可选操作 |
| `createDate` | string | 创建日期 |
| `createTime` | string | 创建时间 |
| `updateDate` | string | 更新日期 |
| `updateTime` | string | 更新时间 |
| `updatedTime` | long | 更新时间戳 |
| `allowReturn` | bool | 是否允许退货 |
| `printedAwb` | bool | 是否已打印运单 |
| `printedInvoice` | bool | 是否已打印发票 |
| `printedCheckList` | bool | 是否已打印清单 |
| `needReprint` | bool | 是否需要重印 |

### SLA 相关字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `sotSla` | string | 标准发货时效截止时间 |
| `ttsSla` | string | 实际发货截止时间 |
| `ttsSlaExtended` | bool | TTSLA 是否已延长 |
| `maxSlaExtensionDays` | int | 最大可延长天数 |
| `slaExtensionTotalQuota` | int | SLA 延长配额总数 |
| `slaExtensionUsedQuota` | int | 已使用延长配额 |
| `slaBreached` | bool | 是否已逾期 |
| `hasSlaExtensionPrivilege` | bool | 是否有 SLA 延长权限 |
| `ffrTtsSla` | long | FFR TTSLA 时间戳 |
| `expectDeliveryMinTime` | string | 期望最晚发货时间 |
| `expectDeliveryMaxTime` | string | 期望最后发货时间 |
| `slaOptions` | array | SLA 选项列表 |

---

## 四、站点与域名对照

| 站点 | 国家 | Domain |
|------|------|--------|
| `LAZADA_PH` | 菲律宾 | `acs-m.lazada.com.ph` |
| `LAZADA_MY` | 马来西亚 | `acs-m.lazada.com.my` |
| `LAZADA_SG` | 新加坡 | `acs-m.lazada.sg` |
| `LAZADA_ID` | 印度尼西亚 | `acs-m.lazada.co.id` |
| `LAZADA_TH` | 泰国 | `acs-m.lazada.co.th` |
| `LAZADA_VN` | 越南 | `acs-m.lazada.vn` |

Seller Center 前缀: `https://sellercenter.lazada.{com.ph|com.my|...}`

---

## 五、API 通用请求头

```
Referer: https://sellercenter.lazada.com.ph/
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36
Accept: application/json
Content-Type: application/x-www-form-urlencoded
Origin: https://sellercenter.lazada.com.ph
```

---

## 六、调用流程建议

1. **登录获取 Cookie** - 通过 Seller Center 页面登录获取所有必需 Cookie
2. **构造时间戳** - 生成 13 位毫秒时间戳（当前时间，非 Cookie 时间戳）
3. **构造 data JSON** - 确定 page, pageSize, tab 等参数，使用紧凑格式 `separators=(',', ':')`
4. **计算签名** - 使用 **MD5** 计算 `MD5(token&timestamp&appKey&data)`
5. **发起请求** - 带上所有 Cookie 和构造好的参数
6. **解析响应** - 从 `data.data.dataSource` 获取订单列表
7. **分页处理** - 根据 `pageInfo.total` 和 `pageInfo.pageSize` 计算总页数

---

## 七、订单详情 API

订单详情页面共调用 3 个核心接口 + 1 个敏感信息接口。

### 1. 订单主详情
**`mtop.lazada.seller.order.main.detail`**

```
GET https://acs-m.lazada.com.ph/h5/mtop.lazada.seller.order.main.detail/1.0/
```

#### data 请求体
```json
{"tradeOrderId": "900548022891005"}
```

#### 响应结构 (data.data[].name 划分模块)

| name (模块名) | 说明 |
|---------------|------|
| `orderInfo` | 订单基本信息 (订单号、日期) |
| `basicInfo` | 买家基本信息 (姓名、ID、电话、标签) |
| `shippingAddress` | 收货地址 |
| `billAddress` | 账单地址 |
| `buyerPaymentInformation` | 买家支付信息 (价格明细) |
| `grandTotalInBPI` | Grand Total |
| `itemTable` | 商品明细表 |
| `order` | 完整订单信息 (同列表页订单结构) |

#### orderInfo 模块
```json
{
  "name": "orderInfo",
  "orderDate": "2024-09-27",
  "orderNumber": "900548022891005"
}
```

#### basicInfo 模块
```json
{
  "name": "basicInfo",
  "customName": "Fina Cabrera",
  "customerId": "500024991005",
  "finishOrderCount": 0,
  "isBlockedBySeller": false,
  "isNewBuyer": false,
  "phone": "0*1",
  "phoneLabel": "Reveiver Phone",
  "successfulDeliveryRate": "0%",
  "tags": [null]
}
```

#### shippingAddress / billAddress 模块
```json
{
  "name": "shippingAddress",
  "detailAddress": "e*y,Z*r,Z*a,T*n,P*s",
  "locationTree": "Zamboanga Del Sur, Zamboanga, Tetuan",
  "receiver": "D*a",
  "receiverPhone": "6*1",
  "isWork": false,
  "tags": [null]
}
```

#### buyerPaymentInformation 模块 (价格明细)
```json
{
  "name": "buyerPaymentInformation",
  "dataSource": [
    {"key": "subtotal", "name": "Subtotal:", "total": "1199.00", "isVat": true},
    {
      "key": "shipping_fee",
      "name": "Shipping Fee",
      "total": "308.00",
      "isVat": true,
      "children": [
        {"key": "shipping-fee-original", "name": "Shipping Fee Original:", "total": "408.00", "isVat": false},
        {"key": "shipping-fee-discount", "name": "Shipping Fee Discount:", "total": "-100.00", "isVat": false}
      ]
    },
    {"key": "lacing-discount-total", "name": "Lazada Discount Total:", "total": "-0.00", "isVat": false},
    {"key": "seller-discount-total", "name": "Seller Discount Total:", "total": "-49.60", "isVat": false},
    {"key": "adjust_amount_label", "name": "Adjust amount", "total": "0.00", "isVat": false},
    {"key": "grand_total", "name": "Grand Total:", "total": "1457.40", "isVat": false}
  ]
}
```

#### itemTable 模块 (商品明细)
```json
{
  "name": "itemTable",
  "dataSource": [{
    "orderLineId": "900548022991005",
    "itemId": "1954465077",
    "skuId": "24358790891",
    "itemName": "LAHOME 5L Electric Pressure Cooker...",
    "itemImgUrl": "https://ph-live.slatic.net/p/...",
    "sellerSku": "304SUSPot",
    "skuInfo": "Color Family:304 SUS Pot, Variation:Color",
    "status": "pending",
    "deliveryOption": "STANDARD",
    "quantity": 1,
    "totalAmount": "1199",
    "shippingFee": "408.00",
    "warehouseType": "dropshipping",
    "allowReturn": false,
    "promotions": "[{\"Promotion by\":\"SponsorType-seller\",\"ID\":\"900000027153068\",\"Discount(off)\":\"49.60\"}]",
    "shippingFeePromo": "[{\"Promotion by\":\"SponsorType-platform\",\"ID\":\"900000035742015\",\"Discount(off)\":\"100.00\"}]",
    "otherFees": ""
  }]
}
```

---

### 2. 订单补充详情
**`mtop.lazada.seller.order.extra.detail`**

```
GET https://acs-m.lazada.com.ph/h5/mtop.lazada.seller.order.extra.detail/1.0/
```

#### data 请求体
```json
{"tradeOrderId": "900548022891005"}
```

#### 响应结构

| name (模块名) | 说明 |
|---------------|------|
| `itemTable` | 商品表 (包含卖家收款金额) |
| `myPaymentInformation` | 我的支付信息 |
| `grandTotalInMPI` | Grand Total in MPI |
| `orderItemHistory` | 订单项状态变更历史 |

#### orderItemHistory 模块 (状态变更记录)
```json
{
  "name": "orderItemHistory",
  "dataSource": {
    "900548022991005": [
      {"createdAt": 1727418540907, "orderItemId": 900548022991005, "status": "DELIVERY_ORDER_CREATE", "statusDesc": "Delivery Order Create"},
      {"createdAt": 1727419199220, "orderItemId": 900548022991005, "status": "HANDLED_BY_SELLER", "statusDesc": "Handled By Seller"}
    ]
  }
}
```

---

### 3. 订单敏感信息
**`mtop.lazada.seller.order.query.sensitive`**

```
GET https://acs-m.lazada.com.ph/h5/mtop.lazada.seller.order.query.sensitive/1.0/
```

#### data 请求体
```json
{"tradeOrderId": "900548022891005", "needCiphertext": false}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `tradeOrderId` | string | 订单号 |
| `needCiphertext` | bool | 是否需要密文 (false = 明文) |

#### 响应结构

```json
{
  "data": {
    "buyerInfo": {
      "receiver": "Fina Cabrera",
      "receiverPhone": "0*1",
      "taxCode": ""
    },
    "billAddress": {
      "detailAddress": "estrada streets tetuan zamboanga city, Zamboanga Del Sur, Zamboanga, Tetuan, Philippines",
      "receiver": "Delfina  B cabrera",
      "receiverPhone": "6*1"
    },
    "shippingAddress": {
      "detailAddress": "estrada streets tetuan zamboanga city, Zamboanga Del Sur, Zamboanga, Tetuan, Philippines",
      "receiver": "Delfina  B cabrera",
      "receiverPhone": "6*1"
    },
    "reportInfo": {
      "operateDate": 1775723079989,
      "operationType": "open",
      "orderNumber": 900548022891005,
      "orderStatus": "pending",
      "sellerId": 500170530338,
      "userId": 500170530338,
      "venture": "PH"
    }
  }
}
```

> 注意: `needCiphertext: true` 时 phone 等字段会返回密文

---

### 4. AI 搜索指南等级
**`mtop.lazada.sdc.ai.search.guide.level`**

页面加载时调用，与订单详情关联不大，仅做记录。
```json
{"tradeOrderId": "900548022891005"}
```

---

## 八、订单状态流转 (orderItemHistory status 枚举)

| status | statusDesc | 说明 |
|--------|------------|------|
| `DELIVERY_ORDER_CREATE` | Delivery Order Create | 配送单创建 |
| `HANDLED_BY_SELLER` | Handled By Seller | 卖家已处理 |
| ... | ... | 待补充更多状态 |

---

## 九、买家聊天 API (IM 模块)

> 聊天模块使用不同的 appKey: `24813843` (订单详情用 `4272`)

### 前置: 打开会话获取 sessionViewId
**`mtop.im.use.web.seller.mtopimsessionviewservice.opensession`**

```
GET https://acs-m.lazada.com.ph/h5/mtop.im.use.web.seller.mtopimsessionviewservice.opensession/1.0/
```

#### data 请求体
```json
{
  "isWindowOpen": "true",
  "buyerId": "500024991005",
  "orderId": "900548022891005",
  "accessKey": "lazada-pc-h5",
  "accessToken": "lazada-test-secret",
  "toAccountId": "500024991005",
  "toAccountType": "1",
  "sessionType": 103,
  "fromCode": "sc_seller_order"
}
```

#### 响应中的关键字段
返回后会在响应中包含 `sessionViewId`，格式如:
```
1#103#500024991005#1#500170530338#2
```
该 sessionViewId 用于后续查询聊天记录。

---

### 1. 买家购买记录列表
**`mtop.global.im.web.card.order.list.get`**

```
GET https://acs-m.lazada.com.ph/h5/mtop.global.im.web.card.order.list.get/1.0/
```

#### data 请求体
```json
{
  "isWindowOpen": "true",
  "buyerId": "500024991005",
  "orderId": "900548022891005",
  "accessKey": "lazada-pc-h5",
  "accessToken": "lazada-test-secret",
  "page": 1,
  "pageNo": 1,
  "pageSize": 20,
  "customer": "500024991005"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `customer` | string | 买家 ID |
| `buyerId` | string | 买家 ID |
| `orderId` | string | 当前订单 ID |
| `page` / `pageNo` | int | 页码 |
| `pageSize` | int | 每页数量 |

#### 响应结构
```json
{
  "api": "mtop.global.im.web.card.order.list.get",
  "data": {
    "pageNo": 1,
    "pageSize": 20,
    "totalCount": 1,
    "totalPage": 1,
    "result": [{
      "id": "900548022891005",
      "buyerId": 500024991005,
      "buyerName": "Fina Cabrera",
      "sellerId": 500170530338,
      "gmtCreatedTime": 1727418540000,
      "orderType": "Normal",
      "status": "pending",
      "orderAmount": {
        "amount": "1457.40",
        "amountFRMStr": "1,457.40 PHP",
        "currencyCode": "PHP",
        "currencyCodeSimple": "₱"
      },
      "orderItemList": [{
        "productId": "900548022991005",
        "productName": "LAHOME 5L Electric Pressure Cooker...",
        "productPrice": {"amount": "1457.40", "currencyCode": "PHP"},
        "sellerSKU": "304SUSPot",
        "skuInfo": "Color Family:304 SUS Pot, Variation:Color",
        "imageUrl": "https://ph-live.slatic.net/p/...",
        "productCount": 1
      }],
      "logisticsFlag": false,
      "url": "/apps/order/detail?tradeOrderId=900548022891005"
    }]
  }
}
```

---

### 2. 和买家聊天记录
**`mtop.im.use.seller.messagebox.queryMessageListBySessionId`**

```
GET https://acs-m.lazada.com.ph/h5/mtop.im.use.seller.messagebox.queryMessageListBySessionId/1.0/
```

#### data 请求体
```json
{
  "isWindowOpen": "true",
  "buyerId": "500024991005",
  "orderId": "900548022891005",
  "accessKey": "lazada-pc-h5",
  "accessToken": "lazada-test-secret",
  "accountType": 2,
  "sessionViewId": "1#103#500024991005#1#500170530338#2",
  "nodeId": 1,
  "startTime": -1,
  "fetchCount": 50
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `sessionViewId` | string | 会话视图 ID (从 opensession 获取) |
| `startTime` | long | 开始时间戳，-1 表示最新 |
| `fetchCount` | int | 获取消息数量 |

#### 响应结构
```json
{
  "api": "mtop.im.use.seller.messagebox.queryMessageListBySessionId",
  "data": {
    "result": [
      {
        "msgId": "...",
        "content": "...",
        "msgType": "text",
        "fromId": "...",
        "fromType": "buyer",
        "toId": "...",
        "toType": "seller",
        "gmtCreate": 1727418540000,
        "status": "read"
      }
    ]
  }
}
```

> 注意: 该买家 (Fina Cabrera) 当前无聊天记录，返回空数组 `{"result":[]}`

---

### 3. 买家资料
**`mtop.global.im.biz.seller.buyerprofile.get`**

```
GET https://acs-m.lazada.com.ph/h5/mtop.global.im.biz.seller.buyerprofile.get/1.0/
```

#### data 请求体
```json
{
  "isWindowOpen": "true",
  "buyerId": "500024991005",
  "orderId": "900548022891005",
  "accessKey": "lazada-pc-h5",
  "accessToken": "lazada-test-secret",
  "buyerUserId": "500024991005",
  "imBuyerAccountTypeEnum": "LAZADA_BUYER"
}
```

---

### 4. 会话已读标记
**`mtop.lazada.im.web.seller.session.read`**

标记会话为已读。

```json
{
  "isWindowOpen": "true",
  "buyerId": "500024991005",
  "orderId": "900548022891005",
  "accessKey": "lazada-pc-h5",
  "accessToken": "lazada-test-secret",
  "sessionId": "1#103#500024991005#1#500170530338#2",
  "timestamp": 1775723698276,
  "ext": "{\"toUserType\":\"1\",\"toUserId\":\"500024991005\"}"
}
```

---

### 5. IM 模块通用 accessToken 说明

IM 模块 API 需要在请求 data 中携带以下通用参数:

| 参数 | 值 | 说明 |
|------|----|------|
| `accessKey` | `lazada-pc-h5` | 固定值 |
| `accessToken` | `lazada-test-secret` | 固定测试 Token |
| `isWindowOpen` | `true` | 是否打开窗口 |
| `buyerId` | 买家 ID | 当前买家 |
| `orderId` | 订单 ID | 当前订单 |

---

## 十、调试与验证

### 问题排查步骤

当遇到 `FAIL_SYS_ILLEGAL_ACCESS` 错误时，按以下步骤排查：

1. **使用 Chrome DevTools 捕获真实请求**
   - 打开 Lazada Seller Center
   - 导航到订单列表页面
   - 查看 Network 面板，找到 `mtop.lazada.seller.order.query.list` 请求
   - 记录完整的请求参数和签名值

2. **提取关键参数**
   ```python
   # 从请求 URL 中提取
   appKey = "4272"
   t = "1776004563230"  # 时间戳
   sign = "924a1623bfb663f332e58362c85048af"  # 签名

   # 从 Cookie 中提取
   _m_h5_tk = "72ad85c1d9fa57a5c777e0f835e7061c_1776008904341"
   token = _m_h5_tk.split('_')[0]  # "72ad85c1d9fa57a5c777e0f835e7061c"
   ```

3. **手动验证签名**
   ```python
   import hashlib
   import json

   data = {"page": 1, "pageSize": 20, ...}
   data_str = json.dumps(data, separators=(',', ':'))

   message = f"{token}&{t}&{appKey}&{data_str}"
   calculated_sign = hashlib.md5(message.encode('utf-8')).hexdigest().lower()

   # 对比计算结果与浏览器生成的签名
   assert calculated_sign == sign, "签名不匹配"
   ```

### 常见错误

| 错误 | 原因 | 解决方法 |
|------|------|----------|
| FAIL_SYS_ILLEGAL_ACCESS | 签名错误 | 检查是否使用 MD5 而非 HMAC-MD5 |
| 签名始终不匹配 | 数据格式错误 | 确认 JSON 使用 `separators=(',', ':')` |
| 签名每次变化但都错误 | 时间戳错误 | 使用当前时间戳，不是 Cookie 时间戳 |
| 无法提取 token | Cookie 格式错误 | `_m_h5_tk` 格式应为 `{token}_{timestamp}` |

### 关键发现 (2026-04-12 验证)

- ❌ **错误**: Lazada 使用 HMAC-MD5 签名
- ✅ **正确**: Lazada 使用**纯 MD5** 签名

- ❌ **错误**: `_m_h5_tk_enc` 参与签名计算
- ✅ **正确**: `_m_h5_tk_enc` **不参与**签名计算

### 验证命令示例

```bash
# 使用 Python 验证签名
python -c "
import hashlib, json
token = '72ad85c1d9fa57a5c777e0f835e7061c'
timestamp = '1776004563230'
app_key = '4272'
data = {'page': 1, 'pageSize': 20, 'filterOrderItems': True, 'sort': 'SHIPPING_SLA', 'sortOrder': 'ASC', 'tab': 'topack'}
data_str = json.dumps(data, separators=(',', ':'))
message = f'{token}&{timestamp}&{app_key}&{data_str}'
sign = hashlib.md5(message.encode('utf-8')).hexdigest().lower()
print(f'Calculated: {sign}')
print(f'Expected:   924a1623bfb663f332e58362c85048af')
print(f'Match: {sign == \"924a1623bfb663f332e58362c85048af\"}')"
```
