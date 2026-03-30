"""
定时调度脚本
顺序执行：shopee_all_order任务 → analyze_order_tags.py → bigseller任务推送报表
"""
import sys
import os
import time
import logging
from datetime import datetime

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import init_app, create_task

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Excel文件存放目录
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

# 定时间隔（秒）- 默认1小时
SCHEDULE_INTERVAL = 3600


def run_shopee_all_order_task():
    """执行 Shopee 全部订单同步任务"""
    logger.info("=" * 60)
    logger.info("[步骤1] 执行 Shopee 全部订单同步任务")
    logger.info("=" * 60)

    config, _, runner = init_app()

    task_config = {
        'page_size': 200,
        'max_pages': 5,
        'order_list_tab': 100,
        'sort_type': 3,
        'ascending': False,
        'fetch_detail': True,
        'batch_size': 5,
        'capture_api': False,
        'save_to_db': True,
    }
    task = create_task('shopee_all_order', task_config)

    group_name = "test"  # HubStudio 分组名称
    logger.info(f"正在获取 '{group_name}' 分组的环境...")

    result = runner.run_task_by_group(task, group_name)

    logger.info(f"订单同步完成: 总数={result.total}, 成功={result.success}, 失败={result.failed}")

    # 关闭环境
    closed = runner.close_all_environments()
    logger.info(f"已关闭 {closed} 个环境")

    return result.success > 0


def run_analyze_order_tags():
    """执行订单标签分析脚本"""
    logger.info("=" * 60)
    logger.info("[步骤2] 执行订单标签分析脚本")
    logger.info("=" * 60)

    # 导入并执行分析脚本
    from analyze_order_tags import main as analyze_main

    try:
        # 分析脚本现在会返回生成的文件列表
        generated_files = analyze_main()
        logger.info(f"订单标签分析完成，生成 {len(generated_files)} 个文件")
        return generated_files
    except Exception as e:
        logger.error(f"订单标签分析失败: {e}")
        return []


def run_bigseller_task(excel_files: list):
    """执行 BigSeller 报表推送任务（支持多个文件）"""
    logger.info("=" * 60)
    logger.info("[步骤3] 执行 BigSeller 报表推送任务")
    logger.info("=" * 60)

    if not excel_files:
        logger.error("没有需要上传的Excel文件")
        return False

    # 统计结果
    total_success = 0
    total_failed = 0

    for excel_file in excel_files:
        logger.info(f"正在上传: {excel_file}")

        if not os.path.exists(excel_file):
            logger.error(f"Excel文件不存在: {excel_file}")
            total_failed += 1
            continue

        config, _, runner = init_app()

        task_config = {
            'excel_file': excel_file,
            'wait_completion': True,
            'poll_interval': 2
        }
        task = create_task('bigseller_import_order_mark', task_config)

        group_name = "bigseller"  # HubStudio 分组名称
        logger.info(f"正在获取 '{group_name}' 分组的环境...")

        result = runner.run_task_by_group(task, group_name)

        logger.info(f"文件 {os.path.basename(excel_file)} 推送结果: 总数={result.total}, 成功={result.success}, 失败={result.failed}")

        if result.success > 0:
            total_success += 1
        else:
            total_failed += 1

        # 关闭环境
        closed = runner.close_all_environments()
        logger.info(f"已关闭 {closed} 个环境")

    logger.info(f"BigSeller推送完成: 成功={total_success}, 失败={total_failed}")
    return total_success > 0


def run_scheduler(interval: int = None):
    """
    运行定时调度器

    Args:
        interval: 调度间隔（秒），默认从环境变量或配置文件读取
    """
    if interval is None:
        interval = int(os.environ.get('SCHEDULE_INTERVAL', SCHEDULE_INTERVAL))

    logger.info("=" * 60)
    logger.info("定时调度器启动")
    logger.info(f"调度间隔: {interval} 秒")
    logger.info("=" * 60)

    run_count = 0

    while True:
        run_count += 1
        start_time = datetime.now()
        logger.info(f"\n>>> 第 {run_count} 次执行开始: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            # 步骤1: 执行 Shopee 订单同步
            step1_ok = run_shopee_all_order_task()
            if not step1_ok:
                logger.warning("步骤1 (Shopee订单同步) 执行失败，跳过后续步骤")

            # 步骤2: 执行订单标签分析（返回生成的文件列表）
            excel_files = run_analyze_order_tags()
            step2_ok = len(excel_files) > 0

            # 步骤3: 执行 BigSeller 推送（上传所有文件）
            if step2_ok:
                step3_ok = run_bigseller_task(excel_files)
            else:
                logger.warning("步骤2 (订单标签分析) 执行失败，跳过步骤3")
                step3_ok = False

            logger.info(f"执行完成 - 步骤1: {'成功' if step1_ok else '失败'}, 步骤2: {'成功' if step2_ok else '失败'}, 步骤3: {'成功' if step3_ok else '失败'}")

        except Exception as e:
            logger.error(f"执行过程中发生异常: {e}", exc_info=True)

        end_time = datetime.now()
        elapsed = (end_time - start_time).total_seconds()
        logger.info(f"本次执行耗时: {elapsed:.2f} 秒")

        # 计算下次执行时间
        next_run_time = end_time + interval
        logger.info(f"下次执行时间: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("-" * 60)

        # 等待下一次执行
        time.sleep(interval)


def run_once():
    """只执行一次（不循环）"""
    logger.info("=" * 60)
    logger.info("执行单次调度")
    logger.info("=" * 60)

    try:
        # 步骤1
        step1_ok = run_shopee_all_order_task()

        # 步骤2
        excel_files = run_analyze_order_tags()
        step2_ok = len(excel_files) > 0

        # 步骤3
        if step2_ok:
            step3_ok = run_bigseller_task(excel_files)
        else:
            step3_ok = False

        logger.info("=" * 60)
        logger.info(f"执行完成 - 步骤1: {'成功' if step1_ok else '失败'}, 步骤2: {'成功' if step2_ok else '失败'}, 步骤3: {'成功' if step3_ok else '失败'}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"执行失败: {e}", exc_info=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='定时调度脚本')
    parser.add_argument('--once', action='store_true', help='只执行一次，不循环')
    parser.add_argument('--interval', type=int, default=SCHEDULE_INTERVAL, help='调度间隔（秒）')
    parser.add_argument('--skip-order-sync', action='store_true', help='跳过订单同步步骤')
    parser.add_argument('--skip-analyze', action='store_true', help='跳过订单分析步骤')
    parser.add_argument('--skip-bigseller', action='store_true', help='跳过BigSeller推送步骤')

    args = parser.parse_args()

    if args.once:
        # 单次执行模式
        run_once()
    else:
        # 循环执行模式
        run_scheduler(args.interval)