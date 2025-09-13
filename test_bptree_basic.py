"""
B+树索引简化测试脚本
验证B+树索引的基本功能
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.storage.index.bptree_index import BPTreeIndexManager
from src.storage.buffer.buffer_manager import BufferManager, ReplacementPolicy
from src.storage.page.page import PageManager

def test_bptree_index_basic():
    """测试B+树索引基本功能"""
    print("=" * 60)
    print("              B+树索引基本功能测试")
    print("=" * 60)
    
    # 创建页面管理器和缓存管理器
    page_manager = PageManager("test_bptree_basic")
    buffer_manager = BufferManager(10, page_manager, ReplacementPolicy.LRU)
    
    print("\n1. 创建B+树索引管理器...")
    index_manager = BPTreeIndexManager(buffer_manager)
    print("  ✓ B+树索引管理器创建成功")
    
    print("\n2. 创建索引...")
    # 创建索引
    if index_manager.create_index("test_index", "test_table", ["id"]):
        print("  ✓ 索引 'test_index' 创建成功")
    else:
        print("  ✗ 索引 'test_index' 创建失败")
    
    # 获取索引
    index = index_manager.get_index("test_index")
    if index:
        print("  ✓ 获取索引 'test_index' 成功")
    else:
        print("  ✗ 获取索引 'test_index' 失败")
    
    print("\n3. 测试索引管理器功能...")
    # 测试创建重复索引
    if not index_manager.create_index("test_index", "test_table", ["id"]):
        print("  ✓ 防止重复创建索引功能正常")
    else:
        print("  ✗ 防止重复创建索引功能异常")
    
    # 删除索引
    if index_manager.drop_index("test_index"):
        print("  ✓ 删除索引 'test_index' 成功")
    else:
        print("  ✗ 删除索引 'test_index' 失败")
    
    # 再次删除已删除的索引
    if not index_manager.drop_index("test_index"):
        print("  ✓ 防止删除不存在索引功能正常")
    else:
        print("  ✗ 防止删除不存在索引功能异常")
    
    print("\n✅ B+树索引基本功能测试完成!")

if __name__ == "__main__":
    test_bptree_index_basic()