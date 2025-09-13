"""
B+树索引系统综合测试
展示完整的功能和性能优势
"""

import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.storage.storage_engine import StorageEngine

def comprehensive_bptree_test():
    """B+树索引系统综合测试"""
    print("=" * 70)
    print("              B+树索引系统综合测试")
    print("=" * 70)
    
    # 创建存储引擎
    storage = StorageEngine("comprehensive_bptree_test", buffer_size=50)
    
    print("\n1. 创建测试表和索引...")
    
    # 创建用户表
    users_columns = [
        {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
        {'name': 'username', 'type': 'STRING', 'max_length': 50, 'nullable': False},
        {'name': 'email', 'type': 'STRING', 'max_length': 100},
        {'name': 'age', 'type': 'INTEGER'},
        {'name': 'salary', 'type': 'FLOAT'},
        {'name': 'department', 'type': 'STRING', 'max_length': 50}
    ]
    
    if storage.create_table("employees", users_columns):
        print("  ✓ 员工表创建成功")
    else:
        print("  ✗ 员工表创建失败")
        return
    
    # 创建索引
    if storage.create_index("idx_username", "employees", ["username"]):
        print("  ✓ 用户名索引创建成功")
    else:
        print("  ✗ 用户名索引创建失败")
    
    if storage.create_index("idx_salary", "employees", ["salary"]):
        print("  ✓ 薪资索引创建成功")
    else:
        print("  ✗ 薪资索引创建失败")
    
    if storage.create_index("idx_department", "employees", ["department"]):
        print("  ✓ 部门索引创建成功")
    else:
        print("  ✗ 部门索引创建失败")
    
    print("\n2. 插入大量测试数据...")
    
    # 生成测试数据
    departments = ["IT", "HR", "Finance", "Marketing", "Operations"]
    test_employees = []
    
    for i in range(1000):
        employee = {
            'id': i + 1,
            'username': f'user{i+1:04d}',
            'email': f'user{i+1:04d}@company.com',
            'age': 22 + (i % 40),
            'salary': 3000.0 + (i % 50) * 100.0,
            'department': departments[i % len(departments)]
        }
        test_employees.append(employee)
    
    # 插入数据
    start_time = time.time()
    inserted_count = 0
    for employee in test_employees:
        if storage.insert("employees", employee):
            inserted_count += 1
            if inserted_count % 200 == 0:
                print(f"  ✓ 已插入 {inserted_count} 条记录")
        else:
            print(f"  ✗ 插入失败: {employee['username']}")
    
    insert_time = time.time() - start_time
    print(f"  ✓ 总共插入 {inserted_count} 条记录，耗时 {insert_time:.2f} 秒")
    
    print("\n3. 测试索引查询性能...")
    
    # 测试等值查询
    print("  → 测试等值查询...")
    
    # 使用索引的查询（理论上）
    start_time = time.time()
    user_result = storage.select("employees", where={'username': 'user0100'})
    index_query_time = time.time() - start_time
    print(f"    等值查询 'user0100': 找到 {len(user_result)} 条记录，耗时 {index_query_time:.4f} 秒")
    
    # 范围查询测试
    print("  → 测试范围查询...")
    start_time = time.time()
    salary_result = storage.select("employees", where={'salary': {'>': 5000.0}})
    range_query_time = time.time() - start_time
    print(f"    薪资>5000: 找到 {len(salary_result)} 条记录，耗时 {range_query_time:.4f} 秒")
    
    print("\n4. 测试全表扫描性能...")
    
    # 全表扫描
    start_time = time.time()
    all_employees = storage.select("employees")
    full_scan_time = time.time() - start_time
    print(f"  → 全表扫描: 返回 {len(all_employees)} 条记录，耗时 {full_scan_time:.4f} 秒")
    
    print("\n5. 测试投影查询...")
    
    # 列投影查询
    start_time = time.time()
    projected_result = storage.select("employees", columns=['username', 'salary', 'department'])
    projection_time = time.time() - start_time
    print(f"  → 投影查询: 返回 {len(projected_result)} 条记录，耗时 {projection_time:.4f} 秒")
    if projected_result:
        print(f"    示例记录: {projected_result[0]}")
    
    print("\n6. 测试复合条件查询...")
    
    # 复合条件查询
    start_time = time.time()
    complex_result = storage.select("employees", 
                                   columns=['username', 'salary'], 
                                   where={'department': 'IT', 'salary': {'>': 4000.0}})
    complex_time = time.time() - start_time
    print(f"  → 复合条件查询 (IT部门且薪资>4000): 返回 {len(complex_result)} 条记录，耗时 {complex_time:.4f} 秒")
    
    print("\n7. 索引系统状态...")
    
    # 获取索引信息
    username_index = storage.get_index("idx_username")
    salary_index = storage.get_index("idx_salary")
    department_index = storage.get_index("idx_department")
    
    print(f"  → 用户名索引: {'可用' if username_index else '不可用'}")
    print(f"  → 薪资索引: {'可用' if salary_index else '不可用'}")
    print(f"  → 部门索引: {'可用' if department_index else '不可用'}")
    
    print("\n8. 缓存系统状态...")
    storage.buffer_manager.print_cache_status()
    
    print("\n9. 存储引擎统计...")
    stats = storage.get_stats()
    print(f"  → 运行时间: {stats['uptime_seconds']} 秒")
    print(f"  → 表数量: {stats['tables']}")
    print(f"  → 执行查询: {stats['storage_stats']['queries_executed']}")
    print(f"  → 插入记录: {stats['storage_stats']['records_inserted']}")
    
    print("\n" + "=" * 70)
    print("              性能分析")
    print("=" * 70)
    print("  B+树索引系统的主要优势:")
    print("  1. 等值查询时间复杂度: O(log n)")
    print("  2. 范围查询时间复杂度: O(log n + k)")
    print("  3. 相比全表扫描O(n)有显著性能提升")
    print("  4. 支持高效的排序和分组操作")
    print("  5. 与缓存系统良好集成，减少磁盘I/O")
    
    print("\n✅ B+树索引系统综合测试完成!")

if __name__ == "__main__":
    comprehensive_bptree_test()