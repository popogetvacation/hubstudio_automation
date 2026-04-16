"""
定时调度脚本
顺序执行：shopee_all_order任务 → analyze_order_tags.py → bigseller任务推送报表
"""
import sys
import os
import time
import logging
from datetime import datetime, timedelta

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
SCHEDULE_INTERVAL = 1200


def run_shopee_flow():
    """
    Shopee 独立流程：订单同步 → 订单标签分析 → 上传 BigSeller
    """
    logger.info("=" * 60)
    logger.info("[Shopee流程] 开始执行")
    logger.info("=" * 60)

    # 步骤1: 执行 Shopee 订单同步
    logger.info("=" * 60)
    logger.info("[Shopee流程 - 步骤1] 执行 Shopee 全部订单同步任务")
    logger.info("=" * 60)

    _, _, runner = init_app()

    task_config = {
        'page_size': 200,
        'max_pages': 10,
        'order_list_tab': 100,
        'sort_type': 3,
        'ascending': False,
        'fetch_detail': True,
        'batch_size': 5,
        'capture_api': False,
        'save_to_db': True,
    }
    task = create_task('shopee_all_order', task_config)

    group_name = "shopee"
    logger.info(f"正在获取 '{group_name}' 分组的环境...")

    result = runner.run_task_by_group(task, group_name)

    logger.info(f"Shopee订单同步完成: 总数={result.total}, 成功={result.success}, 失败={result.failed}")

    if result.success == 0:
        logger.error("[Shopee流程] 订单同步失败，终止流程")
        return False

    # 步骤2: 执行订单标签分析
    logger.info("=" * 60)
    logger.info("[Shopee流程 - 步骤2] 执行订单标签分析脚本")
    logger.info("=" * 60)

    from analyze_order_tags import main as analyze_main

    try:
        generated_files = analyze_main()
        logger.info(f"Shopee订单标签分析完成，生成 {len(generated_files)} 个文件")
    except Exception as e:
        logger.error(f"Shopee订单标签分析失败: {e}")
        return False

    if not generated_files:
        logger.error("[Shopee流程] 没有生成Excel文件，终止流程")
        return False

    # 步骤3: 上传 BigSeller
    logger.info("=" * 60)
    logger.info("[Shopee流程 - 步骤3] 执行 BigSeller 报表推送任务")
    logger.info("=" * 60)

    _, _, runner = init_app()

    total_success = 0
    total_failed = 0

    for excel_file in generated_files:
        logger.info(f"正在上传: {excel_file}")

        if not os.path.exists(excel_file):
            logger.error(f"Excel文件不存在: {excel_file}")
            total_failed += 1
            continue

        task_config = {
            'excel_file': excel_file,
            'wait_completion': True,
            'poll_interval': 2
        }
        task = create_task('bigseller_import_order_mark', task_config)

        group_name = "bigseller"
        logger.info(f"正在获取 '{group_name}' 分组的环境...")

        result = runner.run_task_by_group(task, group_name)

        logger.info(f"文件 {os.path.basename(excel_file)} 推送结果: 总数={result.total}, 成功={result.success}, 失败={result.failed}")

        if result.success > 0:
            total_success += 1
        else:
            total_failed += 1

    logger.info(f"[Shopee流程] BigSeller推送完成: 成功={total_success}, 失败={total_failed}")
    return total_success > 0


def run_tiktok_flow():
    """
    TikTok 独立流程：订单同步 → 生成标签文件 → 上传 BigSeller
    """
    logger.info("=" * 60)
    logger.info("[TikTok流程] 开始执行")
    logger.info("=" * 60)

    # 步骤1: 执行 TikTok/Tokopedia 订单同步
    logger.info("=" * 60)
    logger.info("[TikTok流程 - 步骤1] 执行 TikTok/Tokopedia 订单同步任务")
    logger.info("=" * 60)

    _, _, runner = init_app()

    task_config = {
        'max_pages': 10,
        'page_size': 20,
        'save_to_file': True,
        'output_dir': './data',
    }
    task = create_task('tokopedia_order', task_config)

    group_name = "tiktok"
    logger.info(f"正在获取 '{group_name}' 分组的环境...")

    result = runner.run_task_by_group(task, group_name)

    logger.info(f"TikTok订单同步完成: 总数={result.total}, 成功={result.success}, 失败={result.failed}")

    if result.success == 0:
        logger.error("[TikTok流程] 订单同步失败，终止流程")
        return False

    # 提取 TikTok 生成的标签文件路径（支持多环境）
    tiktok_output_files = []
    if result.results:
        for r in result.results:
            if r.result and r.result.get('output_file'):
                tiktok_output_files.append(r.result.get('output_file'))

    if not tiktok_output_files:
        logger.error("[TikTok流程] 没有生成标签文件，终止流程")
        return False

    logger.info(f"[TikTok流程] 找到 {len(tiktok_output_files)} 个标签文件")

    # 步骤2: 上传 BigSeller（遍历所有文件）
    logger.info("=" * 60)
    logger.info("[TikTok流程 - 步骤2] 执行 BigSeller 报表推送任务")
    logger.info("=" * 60)

    _, _, runner = init_app()

    total_success = 0
    total_failed = 0

    for excel_file in tiktok_output_files:
        logger.info(f"正在上传: {excel_file}")

        if not os.path.exists(excel_file):
            logger.error(f"Excel文件不存在: {excel_file}")
            total_failed += 1
            continue

        task_config = {
            'excel_file': excel_file,
            'wait_completion': True,
            'poll_interval': 2
        }
        task = create_task('bigseller_import_order_mark', task_config)

        group_name = "bigseller"
        logger.info(f"正在获取 '{group_name}' 分组的环境...")

        result = runner.run_task_by_group(task, group_name)

        logger.info(f"文件 {os.path.basename(excel_file)} 推送结果: 总数={result.total}, 成功={result.success}, 失败={result.failed}")

        if result.success > 0:
            total_success += 1
        else:
            total_failed += 1

    logger.info(f"[TikTok流程] BigSeller推送完成: 成功={total_success}, 失败={total_failed}")

    return total_success > 0


def run_lazada_flow():
    """
    Lazada 独立流程：订单同步 → 生成标签文件 → 上传 BigSeller
    """
    logger.info("=" * 60)
    logger.info("[Lazada流程] 开始执行")
    logger.info("=" * 60)

    # 步骤1: 执行 Lazada 订单同步
    logger.info("=" * 60)
    logger.info("[Lazada流程 - 步骤1] 执行 Lazada 订单同步任务")
    logger.info("=" * 60)

    _, _, runner = init_app()

    task_config = {
        'max_pages': 10,
        'page_size': 20,
        'save_to_file': True,
        'output_dir': './data',
    }
    task = create_task('lazada_order', task_config)

    group_name = "lazada"
    logger.info(f"正在获取 '{group_name}' 分组的环境...")

    result = runner.run_task_by_group(task, group_name)

    logger.info(f"Lazada订单同步完成: 总数={result.total}, 成功={result.success}, 失败={result.failed}")

    if result.success == 0:
        logger.error("[Lazada流程] 订单同步失败，终止流程")
        return False

    # 提取 Lazada 生成的标签文件路径（支持多环境）
    lazada_output_files = []
    if result.results:
        for r in result.results:
            if r.result and r.result.get('output_file'):
                lazada_output_files.append(r.result.get('output_file'))

    if not lazada_output_files:
        logger.error("[Lazada流程] 没有生成标签文件，终止流程")
        return False

    logger.info(f"[Lazada流程] 找到 {len(lazada_output_files)} 个标签文件")

    # 步骤2: 上传 BigSeller（遍历所有文件）
    logger.info("=" * 60)
    logger.info("[Lazada流程 - 步骤2] 执行 BigSeller 报表推送任务")
    logger.info("=" * 60)

    _, _, runner = init_app()

    total_success = 0
    total_failed = 0

    for excel_file in lazada_output_files:
        logger.info(f"正在上传: {excel_file}")

        if not os.path.exists(excel_file):
            logger.error(f"Excel文件不存在: {excel_file}")
            total_failed += 1
            continue

        task_config = {
            'excel_file': excel_file,
            'wait_completion': True,
            'poll_interval': 2
        }
        task = create_task('bigseller_import_order_mark', task_config)

        group_name = "bigseller"
        logger.info(f"正在获取 '{group_name}' 分组的环境...")

        result = runner.run_task_by_group(task, group_name)

        logger.info(f"文件 {os.path.basename(excel_file)} 推送结果: 总数={result.total}, 成功={result.success}, 失败={result.failed}")

        if result.success > 0:
            total_success += 1
        else:
            total_failed += 1

    logger.info(f"[Lazada流程] BigSeller推送完成: 成功={total_success}, 失败={total_failed}")

    return total_success > 0


def run_scheduler(interval: int = None, skip_tiktok: bool = False, skip_lazada: bool = False):
    """
    运行定时调度器

    Args:
        interval: 调度间隔（秒），默认从环境变量或配置文件读取
        skip_tiktok: 是否跳过 TikTok 订单同步步骤
        skip_lazada: 是否跳过 Lazada 订单同步步骤
    """
    if interval is None:
        interval = int(os.environ.get('SCHEDULE_INTERVAL', SCHEDULE_INTERVAL))

    logger.info("=" * 60)
    logger.info("定时调度器启动")
    logger.info(f"调度间隔: {interval} 秒")
    logger.info("=" * 60)

    run_count = 0

    def clear_xlsx_files():
        """清除 data 目录下的 xlsx 文件"""
        if os.path.exists(DATA_DIR):
            cleared = []
            for fname in os.listdir(DATA_DIR):
                if fname.endswith('.xlsx'):
                    fpath = os.path.join(DATA_DIR, fname)
                    try:
                        os.remove(fpath)
                        cleared.append(fname)
                    except Exception as e:
                        logger.warning(f"清除文件失败: {fname}, {e}")
            if cleared:
                logger.info(f"已清除 {len(cleared)} 个 xlsx 文件: {cleared}")
            else:
                logger.info("没有 xlsx 文件需要清除")

    while True:
        run_count += 1
        start_time = datetime.now()
        logger.info(f"\n>>> 第 {run_count} 次执行开始: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # 清除旧的 xlsx 文件
        clear_xlsx_files()

        try:
            # Shopee 独立流程
            shopee_ok = run_shopee_flow()

            # TikTok 独立流程
            if skip_tiktok:
                logger.info("跳过 TikTok 订单同步步骤")
                tiktok_ok = True
            else:
                tiktok_ok = run_tiktok_flow()

            # Lazada 独立流程
            if skip_lazada:
                logger.info("跳过 Lazada 订单同步步骤")
                lazada_ok = True
            else:
                lazada_ok = run_lazada_flow()

            logger.info(f"执行完成 - Shopee: {'成功' if shopee_ok else '失败'}, TikTok: {'成功' if tiktok_ok else '失败'}, Lazada: {'成功' if lazada_ok else '失败'}")

        except Exception as e:
            logger.error(f"执行过程中发生异常: {e}", exc_info=True)

        end_time = datetime.now()
        elapsed = (end_time - start_time).total_seconds()
        logger.info(f"本次执行耗时: {elapsed:.2f} 秒")

        # 计算下次执行时间
        next_run_time = end_time + timedelta(seconds=interval)
        logger.info(f"下次执行时间: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("-" * 60)

        # 等待下一次执行
        time.sleep(interval)


def run_once(skip_tiktok: bool = False, skip_lazada: bool = False):
    """只执行一次（不循环）"""
    logger.info("=" * 60)
    logger.info("执行单次调度")
    logger.info("=" * 60)

    try:
        # Shopee 独立流程
        shopee_ok = run_shopee_flow()

        # TikTok 独立流程
        if skip_tiktok:
            logger.info("跳过 TikTok 订单同步步骤")
            tiktok_ok = True
        else:
            tiktok_ok = run_tiktok_flow()

        # Lazada 独立流程
        if skip_lazada:
            logger.info("跳过 Lazada 订单同步步骤")
            lazada_ok = True
        else:
            lazada_ok = run_lazada_flow()

        logger.info("=" * 60)
        logger.info(f"执行完成 - Shopee: {'成功' if shopee_ok else '失败'}, TikTok: {'成功' if tiktok_ok else '失败'}, Lazada: {'成功' if lazada_ok else '失败'}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"执行失败: {e}", exc_info=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='定时调度脚本')
    parser.add_argument('--once', action='store_true', help='只执行一次，不循环')
    parser.add_argument('--interval', type=int, default=SCHEDULE_INTERVAL, help='调度间隔（秒）')
    parser.add_argument('--skip-order-sync', action='store_true', help='跳过订单同步步骤')
    parser.add_argument('--skip-tiktok', action='store_true', help='跳过TikTok订单同步步骤')
    parser.add_argument('--skip-lazada', action='store_true', help='跳过Lazada订单同步步骤')

    args = parser.parse_args()

    if args.once:
        # 单次执行模式
        run_once(skip_tiktok=args.skip_tiktok, skip_lazada=args.skip_lazada)
    else:
        # 循环执行模式
        run_scheduler(args.interval, skip_tiktok=args.skip_tiktok, skip_lazada=args.skip_lazada)