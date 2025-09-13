"""
存储引擎B+树索引集成测试
验证存储引擎中B+树索引的完整功能
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.storage.storage_engine import StorageEngine

def test_storage_engine_with_bptree_index():
    """测试存储引擎中的B+树索引"""
    print("=" * 60)
    print("              存储引擎B+树索引集成测试")
    print("=" * 60)
    
    # 创建存储引擎
    storage = StorageEngine("test_storage_bptree", buffer_size=20)
    
    print("\n1. 创建测试表...")
    
    # 创建用户表
    users_columns = [
        {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
        {'name': 'username', 'type': 'STRING', 'max_length': 50, 'nullable': False},
        {'name': 'email', 'type': 'STRING', 'max_length': 100},
        {'name': 'age', 'type': 'INTEGER'},
        {'name': 'salary', 'type': 'FLOAT'}
    ]
    
    if storage.create_table("users", users_columns):
        print("  ✓ 用户表创建成功")
    else:
        print("  ✗ 用户表创建失败")
        return
    
    print("\n2. 插入测试数据...")
    
    # 插入用户数据
    test_users = [
        {'id': 1, 'username': 'alice', 'email': 'alice@example.com', 'age': 25, 'salary': 5000.0},
        {'id': 2, 'username': 'bob', 'email': 'bob@example.com', 'age': 30, 'salary': 6000.0},
        {'id': 3, 'username': 'charlie', 'email': 'charlie@example.com', 'age': 28, 'salary': 5500.0},
        {'id': 4, 'username': 'diana', 'email': 'diana@example.com', 'age': 26, 'salary': 5200.0},
        {'id': 5, 'username': 'eve', 'email': 'eve@example.com', 'age': 32, 'salary': 7000.0},
        {'id': 6, 'username': 'frank', 'email': 'frank@example.com', 'age': 29, 'salary': 5800.0},
        {'id': 7, 'username': 'grace', 'email': 'grace@example.com', 'age': 27, 'salary': 5400.0},
        {'id': 8, 'username': 'henry', 'email': 'henry@example.com', 'age': 31, 'salary': 6500.0}
    ]
    
    for user in test_users:
        if storage.insert("users", user):
            print(f"  ✓ 插入用户: {user['username']}")
        else:
            print(f"  ✗ 插入失败: {user['username']}")
    
    print("\n3. 创建B+树索引...")
    
    # 创建基于用户名的索引
    if storage.create_index("idx_username", "users", ["username"]):
        print("  ✓ 用户名索引创建成功")
    else:
        print("  ✗ 用户名索引创建失败")
    
    # 创建基于薪资的索引
    if storage.create_index("idx_salary", "users", ["salary"]):
        print("  ✓ 薪资索引创建成功")
    else:
        print("  ✗ 薪资索引创建失败")
    
    print("\n4. 测试索引查询...")
    
    # 获取索引实例
    username_index = storage.get_index("idx_username")
    if username_index:
        print("  ✓ 成功获取用户名索引")
        # 测试搜索功能
        result = username_index.search("alice")
        print(f"  → 搜索用户名 'alice': {result}")
    else:
        print("  ✗ 无法获取用户名索引")
    
    salary_index = storage.get_index("idx_salary")
    if salary_index:
        print("  ✓ 成功获取薪资索引")
        # 测试范围搜索功能
        result = salary_index.range_search(5000.0, 6000.0)
        print(f"  → 薪资范围搜索 [5000.0, 6000.0]: {result}")
    else:
        print("  ✗ 无法获取薪资索引")
    
    print("\n5. 性能对比测试...")
    
    # 不使用索引的查询
    print("  → 全表扫描查询所有用户...")
    all_users = storage.select("users")
    print(f"    返回 {len(all_users)} 条记录")
    
    print("\n6. 索引管理测试...")
    
    # 列出所有索引（需要扩展功能）
    print("  → 索引管理功能...")
    print("    当前版本支持创建和使用B+树索引")
    
    print("\n7. 缓存状态...")
    storage.buffer_manager.print_cache_status()
    
    print("\n✅ 存储引擎B+树索引集成测试完成!")

if __name__ == "__main__":
    test_storage_engine_with_bptree_index()