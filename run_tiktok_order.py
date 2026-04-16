"""
TikTok/Tokopedia 订单任务测试脚本
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import init_app, create_task


def run_tiktok_order_task():
    """运行 TikTok/Tokopedia 订单获取任务"""

    # 1. 初始化应用
    print("正在初始化...")
    config, client, runner = init_app()

    print(f"已加载配置:")
    print(f"  - API 地址: {config.hubstudio.api_url}")
    print(f"  - 最大并发: {config.scheduler.max_concurrent}")
    print()

    # 2. 创建 TikTok/Tokopedia 订单任务
    task_config = {
        'max_pages': 10,           # 最大获取页数
        'page_size': 20,           # 每页订单数
        'save_to_file': True,      # 保存到 Excel 文件
        'output_dir': './data',    # 输出目录
    }
    task = create_task('tokopedia_order', task_config)

    print(f"任务: {task.task_name}")
    print(f"配置: {task_config}")
    print()

    # 3. 运行指定分组的任务 (group_code = "test")
    group_name = "test"  # HubStudio 中的环境分组名称
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
                total_count = r.result.get('total_count', 0) if r.result else 0
                tags_count = r.result.get('tags_count', {}) if r.result else {}
                output_file = r.result.get('output_file', '') if r.result else ''

                print(f"订单 {order_count} 条, 总计 {total_count}")
                if tags_count:
                    print(f"       标签统计: {tags_count}")
                if output_file:
                    print(f"       输出文件: {output_file}")
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
    run_tiktok_order_task()