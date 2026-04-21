"""
BigSeller 订单标签管理任务
批量删除标签、添加备注、添加新标签
"""
import time
import re
from typing import TYPE_CHECKING, Dict, Any, List

from .task_base import BaseTask, TaskFactory
from ..api.bigseller_api import BigSellerAPI
from ..utils.logger import default_logger as logger

if TYPE_CHECKING:
    from ..browser.selenium_driver import HubStudioSeleniumDriver


class BigSellerOrderLabelTask(BaseTask):
    """
    BigSeller 订单标签管理任务

    功能：
    1. 获取待处理订单列表
    2. 批量删除指定标签
    3. 批量添加备注（保留原备注，追加机审异常标记）
    4. 批量添加新标签
    """

    task_name = "bigseller_order_label"

    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化任务

        Args:
            config: 任务配置
                - order_tags_data: 订单标签数据列表（必填）
                - label_ids_to_remove: 需要删除的标签ID列表（默认 ['1657', '1848', '1825']）
                - pass_label_id: pass 标签ID（默认 '1825'）
                - low_score_label_id: 低分不发标签ID（默认 '1657'）
                - audit_label_id: 审核标签ID（默认 '1848'）
                - max_pages: 获取订单的最大页数（默认 50）
                - page_size: 每页订单数（默认 300）
                - batch_size: 批次大小（默认 300）
        """
        super().__init__(config)
        self.order_tags_data = self.config.get('order_tags_data', [])
        self.label_ids_to_remove = self.config.get('label_ids_to_remove', ['1657', '1848', '1825'])
        self.pass_label_id = self.config.get('pass_label_id', '1825')
        self.low_score_label_id = self.config.get('low_score_label_id', '1657')
        self.audit_label_id = self.config.get('audit_label_id', '1848')
        self.max_pages = self.config.get('max_pages', 50)
        self.page_size = self.config.get('page_size', 300)
        self.batch_size = self.config.get('batch_size', 300)

    def execute(self, driver: "HubStudioSeleniumDriver", env_info: Dict[str, Any]) -> Any:
        """
        执行订单标签管理任务

        Returns:
            执行结果字典
        """
        env_name = env_info.get('env_name', 'unknown')

        # 初始化结果
        result = {
            'total_pending_orders': 0,
            'matched_orders': 0,
            'labels_removed': 0,
            'remarks_added': 0,
            'orders_with_existing_audit': 0,
            'labels_added': 0,
            'skipped_orders': 0,
            'failed_orders': 0,
            'errors': []
        }

        try:
            # 步骤1: 跳转到 BigSeller 网站
            logger.info(f"[{env_name}] 步骤1: 跳转到 BigSeller...")
            try:
                driver.driver.set_page_load_timeout(30)
                driver.driver.get("https://www.bigseller.pro/")
                time.sleep(2)
            except Exception as e:
                logger.warning(f"[{env_name}] 页面加载超时或出错: {type(e).__name__}: {e}")

            # 步骤2: 创建 API 实例
            logger.info(f"[{env_name}] 步骤2: 创建 API 实例...")
            bigseller_api = BigSellerAPI(driver.driver)

            # 步骤3: 获取所有待处理订单
            logger.info(f"[{env_name}] 步骤3: 获取待处理订单列表...")
            pending_orders = bigseller_api.get_all_pending_orders(
                page_size=self.page_size,
                max_pages=self.max_pages
            )
            result['total_pending_orders'] = len(pending_orders)
            logger.info(f"[{env_name}] 获取到 {len(pending_orders)} 个待处理订单")

            # 步骤4: 构建订单映射
            logger.info(f"[{env_name}] 步骤4: 构建订单映射...")
            order_map = self._build_order_map(pending_orders)

            # 步骤5: 匹配订单
            logger.info(f"[{env_name}] 步骤5: 匹配订单...")
            matched_orders = self._match_orders(self.order_tags_data, order_map)
            result['matched_orders'] = len(matched_orders)
            logger.info(f"[{env_name}] 匹配到 {len(matched_orders)} 个订单")

            # 步骤6: 批量删除标签
            logger.info(f"[{env_name}] 步骤6: 批量删除标签...")
            labels_removed_count = self._batch_remove_labels(
                matched_orders,
                bigseller_api,
                env_name
            )
            result['labels_removed'] = labels_removed_count

            # 步骤7: 批量添加备注
            logger.info(f"[{env_name}] 步骤7: 批量添加备注...")
            remark_result = self._batch_add_remarks(
                matched_orders,
                bigseller_api,
                env_name
            )
            result['remarks_added'] = remark_result['added']
            result['orders_with_existing_audit'] = remark_result['existing_audit']
            result['skipped_orders'] = remark_result['skipped']
            result['failed_orders'] = remark_result['failed']

            # 步骤8: 批量添加新标签
            logger.info(f"[{env_name}] 步骤8: 批量添加新标签...")
            labels_added_count = self._batch_add_labels(
                matched_orders,
                bigseller_api,
                env_name
            )
            result['labels_added'] = labels_added_count

            logger.info(f"[{env_name}] 任务完成: "
                      f"匹配 {result['matched_orders']}, "
                      f"删除标签 {result['labels_removed']}, "
                      f"添加备注 {result['remarks_added']}, "
                      f"添加标签 {result['labels_added']}")

            return result

        except Exception as e:
            error_msg = f"任务执行失败: {type(e).__name__}: {e}"
            logger.error(f"[{env_name}] {error_msg}")
            result['errors'].append(error_msg)
            raise

    def _build_order_map(self, pending_orders: List[Dict]) -> Dict[str, Dict]:
        """
        构建订单映射（使用 platformOrderId 作为匹配键）

        Args:
            pending_orders: 待处理订单列表

        Returns:
            {platform_order_id: order_data}
        """
        order_map = {}

        for order in pending_orders:
            # 提取关键字段
            order_id = order.get('id')
            platform_order_id = order.get('platformOrderId')
            package_no = order.get('packageNo')
            item_total_num = order.get('itemTotalNum', 0)
            order_item_list = order.get('orderItemList', [])

            # 提取备注（使用 platformSellerNote）
            seller_remark = order.get('platformSellerNote', '') or order.get('sellerRemark', '')

            order_data = {
                'order_id': order_id,
                'platform_order_id': platform_order_id,
                'package_no': package_no,
                'item_total_num': item_total_num,
                'order_item_list': order_item_list,
                'seller_remark': seller_remark
            }

            # 使用 platformOrderId 作为主键
            if platform_order_id:
                order_map[platform_order_id] = order_data

            # 如果没有 platformOrderId，尝试使用 packageNo 作为备用键
            elif package_no:
                order_map[package_no] = order_data

        return order_map

    def _match_orders(self, order_tags_data: List[Dict], order_map: Dict[str, Dict]) -> List[Dict]:
        """
        匹配订单标签数据与待处理订单

        Args:
            order_tags_data: 订单标签数据列表
            order_map: 订单映射

        Returns:
            匹配到的订单列表
        """
        matched_orders = []

        for tag_data in order_tags_data:
            platform_order_id = tag_data.get('platform_order_id', '')

            # 在订单映射中查找（支持 platform_order_id 和 order_sn 备用）
            order_data = order_map.get(platform_order_id)

            # 如果没找到，尝试通过 order_sn 查找
            if not order_data:
                order_sn = tag_data.get('order_sn', '')
                # 遍历 order_map 查找匹配
                for value in order_map.values():
                    if value.get('order_sn') == order_sn:
                        order_data = value
                        break

            if order_data:
                # 添加标签信息
                matched_order = order_data.copy()
                matched_order.update({
                    'tags': tag_data.get('tags', []),
                    'is_pass': tag_data.get('is_pass', False)
                })
                matched_orders.append(matched_order)
            else:
                logger.warning(f"未找到匹配的订单: {platform_order_id}")

        return matched_orders

    def _batch_remove_labels(self, matched_orders: List[Dict],
                            bigseller_api: BigSellerAPI,
                            env_name: str) -> int:
        """
        批量删除标签

        Args:
            matched_orders: 匹配到的订单列表
            bigseller_api: BigSeller API 实例
            env_name: 环境名称

        Returns:
            删除标签的订单数
        """
        total_removed = 0

        # 提取订单 ID 列表
        order_ids = [order['order_id'] for order in matched_orders if order.get('order_id')]

        if not order_ids:
            return 0

        # 分批删除（所有标签ID用逗号拼接）
        for i in range(0, len(order_ids), self.batch_size):
            batch_ids = order_ids[i:i + self.batch_size]

            try:
                logger.info(f"[{env_name}] 删除标签 {self.label_ids_to_remove}: "
                          f"批次 {i//self.batch_size + 1}, 数量 {len(batch_ids)}")

                api_result = bigseller_api.batch_manage_order_labels(
                    order_ids=batch_ids,
                    label_ids=self.label_ids_to_remove,  # 传入列表，自动用逗号拼接
                    operation='delete'
                )

                if api_result.get('code') == 0:
                    total_removed += len(batch_ids)
                    logger.info(f"[{env_name}] 标签删除成功: {len(batch_ids)} 个订单")
                else:
                    logger.error(f"[{env_name}] 标签删除失败: {api_result.get('msg')}")

                # 延迟避免触发限流
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"[{env_name}] 删除标签异常: {e}")

        return total_removed

    def _batch_add_remarks(self, matched_orders: List[Dict],
                         bigseller_api: BigSellerAPI,
                         env_name: str) -> Dict[str, int]:
        """
        批量添加备注

        Args:
            matched_orders: 匹配到的订单列表
            bigseller_api: BigSeller API 实例
            env_name: 环境名称

        Returns:
            统计结果字典
        """
        result = {
            'added': 0,
            'existing_audit': 0,
            'skipped': 0,
            'failed': 0
        }

        # 筛选需要添加备注的订单（非 pass）
        orders_to_add = [
            order for order in matched_orders
            if not order.get('is_pass', False)
        ]

        result['skipped'] = len(matched_orders) - len(orders_to_add)

        # 分批添加备注
        for i in range(0, len(orders_to_add), self.batch_size):
            batch_orders = orders_to_add[i:i + self.batch_size]

            # 构建备注请求
            remark_requests = []

            for order in batch_orders:
                # 生成新备注内容
                remark_data = self._generate_remark(order)

                # 构建请求对象
                remark_request = bigseller_api.build_order_remark(
                    order_id=order['order_id'],
                    item_total_num=order['item_total_num'],
                    package_no=order['package_no'],
                    order_item_list=order['order_item_list'],
                    remark_type=1,  # 买家备注
                    content=remark_data['content'],
                    order_not_approved=False
                )

                remark_requests.append(remark_request)

                # 统计
                if remark_data['is_new']:
                    result['added'] += 1
                else:
                    result['existing_audit'] += 1

            try:
                logger.info(f"[{env_name}] 添加备注: "
                          f"批次 {i//self.batch_size + 1}, 数量 {len(remark_requests)}")

                api_result = bigseller_api.batch_edit_order_remarks(remark_requests)

                if api_result.get('code') == 0:
                    logger.info(f"[{env_name}] 备注添加成功: {len(remark_requests)} 个订单")
                else:
                    logger.error(f"[{env_name}] 备注添加失败: {api_result.get('msg')}")
                    result['failed'] += len(remark_requests)

                # 延迟避免触发限流
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"[{env_name}] 添加备注异常: {e}")
                result['failed'] += len(remark_requests)

        return result

    def _batch_add_labels(self, matched_orders: List[Dict],
                        bigseller_api: BigSellerAPI,
                        env_name: str) -> int:
        """
        批量添加标签（根据订单类型添加对应标签）

        规则：
        - pass 订单 → 添加 pass 标签 (1825)
        - 有"低分不发"标签 → 添加低分不发标签 (1657)
        - 其他有标签的订单 → 添加"需要审核/检查(机审)"标签 (1848)

        Args:
            matched_orders: 匹配到的订单列表
            bigseller_api: BigSeller API 实例
            env_name: 环境名称

        Returns:
            添加标签的订单数
        """
        total_added = 0

        for order in matched_orders:
            order_id = order.get('order_id')
            if not order_id:
                continue

            tags = order.get('tags', [])
            is_pass = order.get('is_pass', False)

            # 确定要添加的标签
            if is_pass:
                # pass 订单添加 pass 标签
                label_id = self.pass_label_id
                label_name = 'pass'
            elif '低分不发' in tags:
                # 有低分不发标签的订单添加低分不发标签
                label_id = self.low_score_label_id
                label_name = '低分不发'
            elif tags:
                # 其他有标签的订单添加审核标签
                label_id = self.audit_label_id
                label_name = '需要审核/检查(机审)'
            else:
                # 无标签，跳过
                continue

            try:
                logger.info(f"[{env_name}] 添加标签 '{label_name}' (ID: {label_id}) 到订单 {order_id}")

                api_result = bigseller_api.batch_manage_order_labels(
                    order_ids=[order_id],
                    label_ids=label_id,
                    operation='add'
                )

                if api_result.get('code') == 0:
                    total_added += 1
                    logger.info(f"[{env_name}] 标签添加成功")
                else:
                    logger.error(f"[{env_name}] 标签添加失败: {api_result.get('msg')}")

                # 延迟避免触发限流
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"[{env_name}] 添加标签异常: {e}")

        return total_added

    def _generate_remark(self, order: Dict) -> Dict[str, Any]:
        """
        生成备注内容

        备注规则：
        - pass 订单不添加备注
        - 排除低分不发标签
        - 格式：>>标签名<<
        - 先清除原备注中所有 >>...<< 格式的行，然后在最后追加新备注

        Args:
            order: 订单数据字典

        Returns:
            {
                'content': str,      # 最终备注内容
                'is_new': bool     # 是否为新添加的备注
            }
        """
        original_remark = order.get('seller_remark', '')
        tags = order.get('tags', [])
        is_pass = order.get('is_pass', False)

        # pass 订单不添加备注
        if is_pass:
            return {
                'content': original_remark,
                'is_new': False
            }

        # 排除低分不发标签
        remark_tags = [t for t in tags if t != '低分不发']

        # 没有标签可添加
        if not remark_tags:
            return {
                'content': original_remark,
                'is_new': False
            }

        # 使用原始标签名称，格式为 >>标签名<<
        remark_lines = [f">>{tag}<<" for tag in remark_tags]
        new_remark_block = chr(10).join(remark_lines)

        # 清除原备注中所有 >>...<< 格式的行
        cleaned_remark = re.sub(r'^>>.*<<\s*$', '', original_remark, flags=re.MULTILINE)
        # 清理多余的空行
        cleaned_remark = cleaned_remark.strip()

        # 在末尾追加新备注
        if cleaned_remark:
            final_remark = f"{cleaned_remark}\n{new_remark_block}"
        else:
            final_remark = new_remark_block

        # is_new 判断：检查是否需要添加新备注（有标签且与原备注不同）
        original_lines = set(re.findall(r'^>>(.*?)<<$', original_remark, flags=re.MULTILINE))
        new_lines = set(remark_tags)
        is_new = original_lines != new_lines and len(remark_tags) > 0

        return {
            'content': final_remark,
            'is_new': is_new
        }


# 注册任务到 TaskFactory
TaskFactory.register(BigSellerOrderLabelTask)
