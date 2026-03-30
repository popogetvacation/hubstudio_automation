"""
BigSeller 分组任务运行示例

调用 BigSeller API 上传 Excel 文件进行订单标记批量导入
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import init_app, create_task


def run_bigseller_task():
    """运行 BigSeller 订单标记导入任务"""

    # 1. 初始化应用
    print("正在初始化...")
    config, client, runner = init_app()

    print(f"已加载配置:")
    print(f"  - API 地址: {config.hubstudio.api_url}")
    print(f"  - 最大并发: {config.scheduler.max_concurrent}")
    print()

    # 2. 创建 BigSeller 任务
    task_config = {
        'excel_file': r'C:\Users\popo\Desktop\工作buf区\hubstudio_automation\data\order_tags.xlsx',  # Excel 文件路径
        'wait_completion': True,    # 是否等待导入完成
        'poll_interval': 2          # 轮询间隔（秒）
    }
    task = create_task('bigseller_import_order_mark', task_config)

    print(f"任务: {task.task_name}")
    print(f"配置: {task_config}")
    print()

    # 3. 运行 bigseller 分组的任务
    group_name = "bigseller"  # HubStudio 中的环境分组名称
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
                print(f"key={r.result.get('key', '')}, message={r.result.get('message', '')}")
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
    run_bigseller_task()