"""
简化版B+树索引测试
验证核心功能
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.storage.index.bptree_index import BPTreeIndex, BPTreeIndexManager
from src.storage.buffer.buffer_manager import BufferManager, ReplacementPolicy
from src.storage.page.page import PageManager

def test_simple_bptree():
    """测试简化版B+树索引"""
    print("=" * 60)
    print("              简化版B+树索引测试")
    print("=" * 60)
    
    # 创建页面管理器和缓存管理器
    page_manager = PageManager("test_simple_bptree")
    buffer_manager = BufferManager(10, page_manager, ReplacementPolicy.LRU)
    
    print("\n1. 创建B+树索引...")
    index = BPTreeIndex("test_index", "test_table", ["id"], buffer_manager, order=3)
    print("  ✓ B+树索引创建成功")
    
    print("\n2. 测试插入和查询...")
    
    # 插入少量数据
    test_data = [
        (10, 1001),
        (20, 1002),
        (30, 1003)
    ]
    
    for key, record_id in test_data:
        if index.insert(key, record_id):
            print(f"  ✓ 插入键值对: {key} -> {record_id}")
        else:
            print(f"  ✗ 插入失败: {key} -> {record_id}")
    
    print("\n3. 测试查询...")
    
    # 查询测试
    search_keys = [10, 20, 30, 40]  # 40不存在
    for key in search_keys:
        result = index.search(key)
        if result:
            print(f"  ✓ 查找键 {key}: 找到记录ID {result}")
        else:
            print(f"  ✗ 查找键 {key}: 未找到记录")
    
    print("\n4. 测试索引管理器...")
    
    # 测试索引管理器
    index_manager = BPTreeIndexManager(buffer_manager)
    
    # 创建索引
    if index_manager.create_index("simple_idx", "simple_table", ["name"]):
        print("  ✓ 创建索引 'simple_idx' 成功")
    else:
        print("  ✗ 创建索引 'simple_idx' 失败")
    
    # 获取索引
    retrieved_index = index_manager.get_index("simple_idx")
    if retrieved_index:
        print("  ✓ 获取索引 'simple_idx' 成功")
    else:
        print("  ✗ 获取索引 'simple_idx' 失败")
    
    print("\n✅ 简化版B+树索引测试完成!")

if __name__ == "__main__":
    test_simple_bptree()