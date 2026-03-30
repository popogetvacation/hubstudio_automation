"""
订单批量写入测试
测试场景：
1. 删除数据库里面的所有条目
2. 模拟500个订单批量写入数据库检查是否有500个
3. 再用250个重复订单和250个新订单写入再检查是否有750个
"""
import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.access_db import AccessDatabase


class TestOrderBatchWrite:
    """订单批量写入测试"""

    @pytest.fixture
    def db(self):
        """使用实际数据库"""
        db_path = './data/automation.accdb'
        db = AccessDatabase(db_path)
        db.init_order_tables()
        return db

    def _get_count(self, db) -> int:
        """获取订单数量"""
        try:
            result = db.query("SELECT COUNT(*) as cnt FROM shopee_orders", ())
            if result:
                return result[0].get('cnt', 0)
        except Exception as e:
            print(f"获取数量失败: {e}")
        return 0

    def _delete_all_orders(self, db):
        """删除所有订单"""
        try:
            db.execute("DELETE FROM shopee_orders")
        except Exception as e:
            print(f"删除失败: {e}")

    def _generate_order(self, index: int) -> dict:
        """生成模拟订单数据"""
        return {
            'order_id': 1000000000000 + index,  # 确保 order_id 唯一
            'order_sn': f'TEST_ORDER_{index:06d}',
            'shop_id': 12345678,
            'region_id': 'MY',
            'env_name': 'test_env',
            'status': 'READY_TO_SHIP',
            'fulfilment_channel': 'SHOPEE',
            'total_price': 99.90 + index,
            'currency': 'MYR',
            'shipping_name': f'Buyer {index}',
            'shipping_phone': f'+6012345{index:04d}',
            'shipping_address': f'Address {index}, Kuala Lumpur',
            'tracking_number': '',
        }

    def test_batch_write_500_orders(self, db):
        """测试：写入 500 个订单，检查是否有 500 条"""
        # 1. 删除所有订单
        print("\n[测试] 删除所有订单...")
        self._delete_all_orders(db)
        count_after_delete = self._get_count(db)
        print(f"删除后订单数量: {count_after_delete}")
        assert count_after_delete == 0, f"删除后应该有 0 条记录，实际 {count_after_delete}"

        # 2. 生成 500 个新订单
        print("[测试] 生成 500 个新订单...")
        orders_500 = [self._generate_order(i) for i in range(500)]

        # 3. 批量写入
        print("[测试] 批量写入 500 个订单...")
        saved_count = db.save_orders_batch_transaction(orders_500, 'test_env')
        print(f"实际保存数量: {saved_count}")

        # 4. 检查是否有 500 条
        count_after_500 = self._get_count(db)
        print(f"写入后订单数量: {count_after_500}")
        assert count_after_500 == 500, f"应该有 500 条记录，实际 {count_after_500} 条"

    def test_batch_write_250_duplicate_250_new(self, db):
        """测试：写入 250 个重复订单 + 250 个新订单，检查是否有 750 条"""
        # 确保初始有 500 条订单
        print("\n[测试] 确保初始有 500 条订单...")
        self._delete_all_orders(db)

        orders_500 = [self._generate_order(i) for i in range(500)]
        db.save_orders_batch_transaction(orders_500, 'test_env')

        count_before = self._get_count(db)
        print(f"初始订单数量: {count_before}")
        assert count_before == 500, f"初始应该有 500 条，实际 {count_before} 条"

        # 生成 250 个重复订单（使用前 250 个订单的 order_id）
        print("[测试] 生成 250 个重复订单...")
        duplicate_orders = []
        for i in range(250):
            # 使用已存在的 order_id
            duplicate_orders.append({
                'order_id': 1000000000000 + i,  # 与前 250 个相同
                'order_sn': f'TEST_ORDER_{i:06d}',
                'shop_id': 12345678,
                'region_id': 'MY',
                'env_name': 'test_env',
                'status': 'READY_TO_SHIP',
                'fulfilment_channel': 'SHOPEE',
                'total_price': 199.90,  # 价格可能不同
                'currency': 'MYR',
                'shipping_name': f'Buyer {i} Updated',
                'shipping_phone': f'+6098765{i:04d}',
                'shipping_address': f'Updated Address {i}',
                'tracking_number': '',
            })

        # 生成 250 个新订单（使用 500-749 的 order_id）
        print("[测试] 生成 250 个新订单...")
        new_orders_250 = [self._generate_order(i + 500) for i in range(250)]

        # 合并
        mixed_orders = duplicate_orders + new_orders_250

        # 批量写入
        print("[测试] 批量写入 250 重复 + 250 新订单...")
        saved_count = db.save_orders_batch_transaction(mixed_orders, 'test_env')
        print(f"实际保存数量: {saved_count}")

        # 检查是否有 750 条
        count_after_mixed = self._get_count(db)
        print(f"写入后订单数量: {count_after_mixed}")
        assert count_after_mixed == 750, f"应该有 750 条记录，实际 {count_after_mixed} 条"

    def test_full_scenario(self, db):
        """完整测试场景"""
        print("\n" + "="*50)
        print("完整测试场景开始")
        print("="*50)

        # Step 1: 删除所有订单
        print("\n[Step 1] 删除所有订单...")
        self._delete_all_orders(db)
        count = self._get_count(db)
        print(f"当前订单数量: {count}")
        assert count == 0, f"期望 0，实际 {count}"

        # Step 2: 写入 500 个新订单
        print("\n[Step 2] 写入 500 个新订单...")
        orders_500 = [self._generate_order(i) for i in range(500)]
        saved = db.save_orders_batch_transaction(orders_500, 'test_env')
        count = self._get_count(db)
        print(f"保存: {saved}, 当前数量: {count}")
        assert count == 500, f"期望 500，实际 {count}"

        # Step 3: 写入 250 重复 + 250 新
        print("\n[Step 3] 写入 250 重复 + 250 新订单...")
        duplicate_orders = []
        for i in range(250):
            duplicate_orders.append({
                'order_id': 1000000000000 + i,
                'order_sn': f'TEST_ORDER_{i:06d}',
                'shop_id': 12345678,
                'region_id': 'MY',
                'env_name': 'test_env',
                'status': 'READY_TO_SHIP',
                'fulfilment_channel': 'SHOPEE',
                'total_price': 299.90,
                'currency': 'MYR',
                'shipping_name': f'Buyer {i} Updated',
                'shipping_phone': f'+6098765{i:04d}',
                'shipping_address': f'Updated Address {i}',
                'tracking_number': '',
            })

        new_orders_250 = [self._generate_order(i + 500) for i in range(250)]
        mixed_orders = duplicate_orders + new_orders_250

        saved = db.save_orders_batch_transaction(mixed_orders, 'test_env')
        count = self._get_count(db)
        print(f"保存: {saved}, 当前数量: {count}")
        assert count == 750, f"期望 750，实际 {count}"

        print("\n" + "="*50)
        print("测试完成！")
        print("="*50)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])