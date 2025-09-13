"""
B+树索引测试脚本
验证B+树索引的创建、插入和查询功能
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.storage.index.bptree_index import BPTreeIndex, BPTreeIndexManager
from src.storage.buffer.buffer_manager import BufferManager, ReplacementPolicy
from src.storage.page.page import PageManager

def test_bptree_index():
    """测试B+树索引功能"""
    print("=" * 60)
    print("              B+树索引测试")
    print("=" * 60)
    
    # 创建页面管理器和缓存管理器
    page_manager = PageManager("test_bptree_index")
    buffer_manager = BufferManager(10, page_manager, ReplacementPolicy.LRU)
    
    print("\n1. 创建B+树索引...")
    
    # 创建B+树索引
    index = BPTreeIndex("test_index", "test_table", ["id"], buffer_manager, order=3)
    print("  ✓ B+树索引创建成功")
    
    print("\n2. 插入测试数据...")
    
    # 插入测试数据
    test_data = [
        (10, 1001),
        (20, 1002),
        (30, 1003),
        (40, 1004),
        (50, 1005),
        (60, 1006),
        (70, 1007),
        (80, 1008),
        (90, 1009),
        (100, 1010)
    ]
    
    for key, record_id in test_data:
        if index.insert(key, record_id):
            print(f"  ✓ 插入键值对: {key} -> {record_id}")
        else:
            print(f"  ✗ 插入失败: {key} -> {record_id}")
    
    print("\n3. 等值查询测试...")
    
    # 等值查询测试
    search_keys = [10, 30, 50, 70, 90, 110]  # 110不存在
    for key in search_keys:
        result = index.search(key)
        if result:
            print(f"  ✓ 查找键 {key}: 找到记录ID {result}")
        else:
            print(f"  ✗ 查找键 {key}: 未找到记录")
    
    print("\n4. 范围查询测试...")
    
    # 范围查询测试
    range_tests = [
        (20, 60),
        (40, 80),
        (10, 30)
    ]
    
    for start, end in range_tests:
        result = index.range_search(start, end)
        print(f"  ✓ 范围查询 [{start}, {end}]: 找到 {len(result)} 个记录ID")
        if result:
            print(f"    记录ID: {result}")
    
    print("\n5. 索引管理器测试...")
    
    # 测试索引管理器
    index_manager = BPTreeIndexManager(buffer_manager)
    
    # 创建索引
    if index_manager.create_index("users_name_idx", "users", ["name"]):
        print("  ✓ 创建索引 'users_name_idx' 成功")
    else:
        print("  ✗ 创建索引 'users_name_idx' 失败")
    
    # 获取索引
    retrieved_index = index_manager.get_index("users_name_idx")
    if retrieved_index:
        print("  ✓ 获取索引 'users_name_idx' 成功")
    else:
        print("  ✗ 获取索引 'users_name_idx' 失败")
    
    # 删除索引
    if index_manager.drop_index("users_name_idx"):
        print("  ✓ 删除索引 'users_name_idx' 成功")
    else:
        print("  ✗ 删除索引 'users_name_idx' 失败")
    
    print("\n6. 缓存状态...")
    buffer_manager.print_cache_status()
    
    print("\n✅ B+树索引测试完成!")

if __name__ == "__main__":
    test_bptree_index()