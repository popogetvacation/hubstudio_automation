"""性能统计模块 - 用于跟踪各流程执行时间"""
import time
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from contextlib import contextmanager
from collections import defaultdict

logger = logging.getLogger(__name__)


class PerformanceTracker:
    """性能统计追踪器"""

    def __init__(self):
        # 记录各阶段的耗时
        self.timings: Dict[str, List[float]] = defaultdict(list)
        # 记录各阶段的详细信息
        self.details: Dict[str, Any] = defaultdict(dict)
        # 开始时间
        self.start_times: Dict[str, float] = {}
        # 统计数据
        self.stats: Dict[str, Dict[str, Any]] = {}

    def start(self, name: str):
        """开始计时"""
        self.start_times[name] = time.time()

    def end(self, name: str, detail: Any = None) -> float:
        """结束计时，返回耗时（秒）"""
        if name not in self.start_times:
            logger.warning(f"[Performance] '{name}' 没有对应的 start 调用")
            return 0

        elapsed = time.time() - self.start_times[name]
        self.timings[name].append(elapsed)

        if detail is not None:
            self.details[name] = detail

        # 更新统计数据
        if name not in self.stats:
            self.stats[name] = {
                'count': 0,
                'total': 0,
                'min': float('inf'),
                'max': 0,
                'avg': 0
            }

        stats = self.stats[name]
        stats['count'] += 1
        stats['total'] += elapsed
        stats['min'] = min(stats['min'], elapsed)
        stats['max'] = max(stats['max'], elapsed)
        stats['avg'] = stats['total'] / stats['count']

        del self.start_times[name]
        return elapsed

    @contextmanager
    def measure(self, name: str, detail: Any = None):
        """上下文管理器，自动计时"""
        start = time.time()
        try:
            yield
        finally:
            elapsed = time.time() - start
            self.timings[name].append(elapsed)
            if detail is not None:
                self.details[name] = detail

            # 更新统计数据
            if name not in self.stats:
                self.stats[name] = {
                    'count': 0,
                    'total': 0,
                    'min': float('inf'),
                    'max': 0,
                    'avg': 0
                }

            stats = self.stats[name]
            stats['count'] += 1
            stats['total'] += elapsed
            stats['min'] = min(stats['min'], elapsed)
            stats['max'] = max(stats['max'], elapsed)
            stats['avg'] = stats['total'] / stats['count']

    def log_summary(self, title: str = "性能统计"):
        """输出统计摘要"""
        logger.info(f"========== {title} ==========")

        if not self.stats:
            logger.info("无统计数据")
            return

        # 按总耗时排序
        sorted_stats = sorted(
            self.stats.items(),
            key=lambda x: x[1]['total'],
            reverse=True
        )

        total_time = sum(s['total'] for s in self.stats.values())

        for name, stat in sorted_stats:
            percentage = (stat['total'] / total_time * 100) if total_time > 0 else 0
            logger.info(
                f"  {name}: "
                f"次数={stat['count']}, "
                f"总耗时={stat['total']:.2f}s({percentage:.1f}%), "
                f"平均={stat['avg']:.3f}s, "
                f"最快={stat['min']:.3f}s, "
                f"最慢={stat['max']:.3f}s"
            )

        logger.info(f"总耗时: {total_time:.2f}s")
        logger.info("=" * (len(title) + 16))

    def get_summary(self) -> Dict[str, Any]:
        """获取统计摘要"""
        total_time = sum(s['total'] for s in self.stats.values())

        summary = {
            'total_time': total_time,
            'phases': {}
        }

        for name, stat in self.stats.items():
            percentage = (stat['total'] / total_time * 100) if total_time > 0 else 0
            summary['phases'][name] = {
                'count': stat['count'],
                'total': round(stat['total'], 3),
                'avg': round(stat['avg'], 3),
                'min': round(stat['min'], 3),
                'max': round(stat['max'], 3),
                'percentage': round(percentage, 1),
                'detail': self.details.get(name)
            }

        return summary

    def reset(self):
        """重置统计数据"""
        self.timings.clear()
        self.details.clear()
        self.start_times.clear()
        self.stats.clear()


# 全局追踪器实例
_global_tracker = PerformanceTracker()


def get_tracker() -> PerformanceTracker:
    """获取全局追踪器"""
    return _global_tracker


def reset_tracker():
    """重置全局追踪器"""
    _global_tracker.reset()


@contextmanager
def measure_time(name: str, detail: Any = None):
    """全局上下文管理器计时"""
    with _global_tracker.measure(name, detail):
        yield
