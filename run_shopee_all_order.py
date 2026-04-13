"""
Shopee 全部订单任务运行示例

演示如何获取 Shopee 全部订单列表
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import init_app, create_task


def run_shopee_all_order_task():
    """运行 Shopee 全部订单获取任务"""

    # 1. 初始化应用
    print("正在初始化...")
    config, client, runner = init_app()

    print(f"已加载配置:")
    print(f"  - API 地址: {config.hubstudio.api_url}")
    print(f"  - 最大并发: {config.scheduler.max_concurrent}")
    print()

    # 2. 创建全部订单任务
    task_config = {
        'page_size': 200,           # 每页订单数
        'max_pages': 1,            # 最大获取页数
        'order_list_tab': 100,     # 全部订单
        'sort_type': 3,            # 按更新时间排序
        'ascending': False,        # 降序（新订单在前）
        'fetch_detail': True,      # 获取订单详情
        'batch_size': 5,           # 批量大小 (API 上限 5)
        'capture_api': False,
        'save_to_db': True,        # 保存到数据库（需手动创建数据库文件）
    }
    task = create_task('shopee_all_order', task_config)

    print(f"任务: {task.task_name}")
    print(f"配置: {task_config}")
    print()

    # 3. 运行指定分组的任务
    group_name = "shopee"  # HubStudio 中的环境分组名称
    print(f"正在获取 '{group_name}' 分组的环境...")
    result = runner.run_task_by_group(task, group_name)

    # 4. 打印结果
    print()
    print("=" * 60)
    print("执行结果:")
    print(f"  分组: {result.group_name}")
    print(f"  任务: {result.task_name}")
    print(f"  总数: {result.total}")
    print(f"  成功: {result.success}")
    print(f"  失败: {result.failed}")
    print(f"  成功率: {result.success_rate:.1f}%")
    print()

    # 打印详细结果
    if result.results:
        print("详细结果:")
        for r in result.results:
            status = "[OK]" if r.success else "[FAIL]"
            print(f"  {status} {r.env_name}: ", end="")
            if r.success:
                order_count = len(r.result.get('orders', [])) if r.result else 0
                detail_count = len(r.result.get('order_details', [])) if r.result else 0
                total_count = r.result.get('total_count', 0) if r.result else 0

                # 打印订单状态统计
                status_summary = r.result.get('order_status_summary', {}) if r.result else {}

                print(f"订单 {order_count} 条, 详情 {detail_count} 条, 总计 {total_count}")
                if status_summary:
                    print(f"       订单状态统计: {status_summary}")

                # 打印订单详情示例
                details = r.result.get('order_details', []) if r.result else []
                if details:
                    print(f"       订单详情 (前3条):")
                    for i, detail in enumerate(details[:3]):
                        card = detail.get('package_card', {})
                        header = card.get('card_header', {})
                        order_sn = header.get('order_sn', '')
                        buyer = header.get('buyer_info', {}).get('username', '')
                        status_info = card.get('status_info', {})
                        status = status_info.get('status', '')
                        payment = card.get('payment_info', {})
                        total_price = payment.get('total_price', 0) / 100
                        print(f"         [{i+1}] {order_sn} | {buyer} | {status} | {total_price}")
                else:
                    # 打印订单列表
                    orders = r.result.get('orders', []) if r.result else []
                    if orders:
                        print(f"       订单列表 (前5条):")
                        for order in orders[:5]:
                            order_id = order.get('order_id')
                            region = order.get('region_id')
                            print(f"         - OrderID: {order_id}, Region: {region}")
            else:
                print(f"错误: {r.error}")
            print(f"       耗时: {r.duration:.2f}s")

    # 5. 关闭所有环境
    print()
    print("正在关闭环境...")
    closed = runner.close_all_environments()
    print(f"已关闭 {closed} 个环境")

    return result


if __name__ == "__main__":
    # 运行全部订单获取任务
    run_shopee_all_order_task()