"""
BigSeller 订单标签管理任务 - 运行示例

该脚本展示如何调用 BigSellerOrderLabelTask 进行订单标签管理
"""
import os
import sys

# 添加项目根目录到路径以导入配置
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config import load_config
from src.utils.order_tag_analyzer import analyze_orders_from_db, get_label_id_mapping
from src.tasks import TaskFactory, TaskRunner
from src.api import HubStudioClient


def main():
    """主函数"""
    print("=" * 60)
    print("BigSeller 订单标签管理任务")
    print("=" * 60)

    # 加载配置
    config = load_config()

    # 分析订单标签
    print("\n[1/3] 分析订单标签...")
    order_tags_data = analyze_orders_from_db(config.database.access_path)
    print(f"     共分析 {len(order_tags_data)} 个订单")

    # 获取标签ID映射
    print("\n[2/3] 获取标签ID映射...")
    label_id_mapping = get_label_id_mapping()
    print(f"     标签映射: {label_id_mapping}")

    # 创建任务
    print("\n[3/3] 创建任务...")
    task = TaskFactory.create('bigseller_order_label', {
        'order_tags_data': order_tags_data,
        'label_ids_to_remove': ['1657', '1848', '1825'],
        'pass_label_id': '1825',
        'low_score_label_id': '1657',
        'audit_label_id': '1848',
        'max_pages': 50,
        'page_size': 300,
        'batch_size': 300
    })

    # 创建任务运行器
    client = HubStudioClient(api_url=config.hubstudio.api_url, api_key=config.hubstudio.api_key)
    runner = TaskRunner(
        client=client,
        chromedriver_path=config.browser.chromedriver_path,
        max_workers=1,
        startup_timeout=60,
        task_timeout=300,
        max_retries=3
    )

    # 执行任务
    print("\n开始执行任务...")
    result = runner.run_task_by_group(task, 'bigseller')

    # 输出结果
    print("\n" + "=" * 60)
    print("任务执行结果")
    print("=" * 60)

    if result.total == 0:
        print("没有环境可执行任务")
        return

    for task_result in result.results:
        if task_result.success:
            task_result_data = task_result.result
            print(f"\n环境: {task_result.env_name}")
            print(f"  - 待处理订单: {task_result_data.get('total_pending_orders', 0)}")
            print(f"  - 匹配订单: {task_result_data.get('matched_orders', 0)}")
            print(f"  - 删除标签: {task_result_data.get('labels_removed', 0)}")
            print(f"  - 添加备注: {task_result_data.get('remarks_added', 0)}")
            print(f"  - 已有机审异常: {task_result_data.get('orders_with_existing_audit', 0)}")
            print(f"  - 添加标签: {task_result_data.get('labels_added', 0)}")
            print(f"  - 跳过订单: {task_result_data.get('skipped_orders', 0)}")
            print(f"  - 失败订单: {task_result_data.get('failed_orders', 0)}")

            if task_result_data.get('errors'):
                print(f"  - 错误信息:")
                for error in task_result_data['errors']:
                    print(f"    - {error}")
        else:
            print(f"\n环境: {task_result.env_name}")
            print(f"  - 执行失败: {task_result.error}")

    print("\n" + "=" * 60)
    if result.start_time and result.end_time:
        duration = (result.end_time - result.start_time).total_seconds()
        print(f"总计: 成功 {result.success}/{result.total}, 耗时 {duration:.2f}s")
    else:
        print(f"总计: 成功 {result.success}/{result.total}")
    print("=" * 60)


if __name__ == '__main__':
    main()
