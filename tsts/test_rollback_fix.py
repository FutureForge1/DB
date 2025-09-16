"""
测试事务回滚修复
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

def test_rollback_fix():
    print("🔧 测试事务回滚修复")
    print("=" * 50)
    
    try:
        from src.unified_sql_processor import UnifiedSQLProcessor
        processor = UnifiedSQLProcessor()
        print("✅ 数据库系统初始化成功")
        
        # 1. 创建测试表
        print("\n1. 创建测试表...")
        success, results, error = processor.process_sql(
            "CREATE TABLE test_rollback (id INTEGER PRIMARY KEY, name VARCHAR(50), value INTEGER);"
        )
        if success:
            print("✅ 表创建成功")
        else:
            print(f"❌ 表创建失败: {error}")
            return
        
        # 2. 插入初始数据
        print("\n2. 插入初始数据...")
        init_sqls = [
            "INSERT INTO test_rollback (id, name, value) VALUES (1, 'Alice', 100);",
            "INSERT INTO test_rollback (id, name, value) VALUES (2, 'Bob', 200);"
        ]
        
        for sql in init_sqls:
            success, results, error = processor.process_sql(sql)
            if success:
                print(f"✅ 插入成功")
            else:
                print(f"❌ 插入失败: {error}")
        
        # 3. 查看初始数据
        print("\n3. 查看初始数据...")
        success, results, error = processor.process_sql("SELECT * FROM test_rollback ORDER BY id;")
        if success and results:
            print("初始数据:")
            for result in results:
                if result.get('type') == 'select_result':
                    for record in result.get('records', []):
                        print(f"  ID: {record.get('id')}, 姓名: {record.get('name')}, 值: {record.get('value')}")
        
        # 4. 开始事务并进行更新
        print("\n4. 开始事务并进行更新...")
        transaction_sqls = [
            "BEGIN;",
            "UPDATE test_rollback SET value = 999 WHERE id = 1;",  # Alice的值改为999
            "UPDATE test_rollback SET name = 'Bobby' WHERE id = 2;",  # Bob的名字改为Bobby
        ]
        
        for sql in transaction_sqls:
            print(f"   执行: {sql}")
            success, results, error = processor.process_sql(sql)
            if success:
                if results:
                    for result in results:
                        if result.get('message'):
                            print(f"   ✅ {result['message']}")
                        elif result.get('type') in ['update_result']:
                            rows = result.get('affected_rows', 0)
                            print(f"   ✅ 更新了 {rows} 行")
                else:
                    print("   ✅ 执行成功")
            else:
                print(f"   ❌ 执行失败: {error}")
        
        # 5. 查看事务中的数据（更新后）
        print("\n5. 查看事务中的数据（更新后）...")
        success, results, error = processor.process_sql("SELECT * FROM test_rollback ORDER BY id;")
        if success and results:
            print("更新后数据:")
            for result in results:
                if result.get('type') == 'select_result':
                    for record in result.get('records', []):
                        print(f"  ID: {record.get('id')}, 姓名: {record.get('name')}, 值: {record.get('value')}")
        
        # 6. 回滚事务
        print("\n6. 回滚事务...")
        success, results, error = processor.process_sql("ROLLBACK;")
        if success:
            if results:
                for result in results:
                    if result.get('message'):
                        print(f"✅ {result['message']}")
            else:
                print("✅ 回滚成功")
        else:
            print(f"❌ 回滚失败: {error}")
        
        # 7. 查看回滚后的数据
        print("\n7. 查看回滚后的数据...")
        success, results, error = processor.process_sql("SELECT * FROM test_rollback ORDER BY id;")
        if success and results:
            print("回滚后数据:")
            for result in results:
                if result.get('type') == 'select_result':
                    for record in result.get('records', []):
                        print(f"  ID: {record.get('id')}, 姓名: {record.get('name')}, 值: {record.get('value')}")
        
        # 8. 验证数据是否完全恢复
        print("\n8. 验证数据恢复情况...")
        success, results, error = processor.process_sql("SELECT * FROM test_rollback WHERE id = 1 AND name = 'Alice' AND value = 100;")
        alice_restored = False
        if success and results:
            for result in results:
                if result.get('type') == 'select_result' and result.get('records'):
                    alice_restored = True
                    break
        
        success, results, error = processor.process_sql("SELECT * FROM test_rollback WHERE id = 2 AND name = 'Bob' AND value = 200;")
        bob_restored = False
        if success and results:
            for result in results:
                if result.get('type') == 'select_result' and result.get('records'):
                    bob_restored = True
                    break
        
        print(f"Alice数据恢复: {'✅ 成功' if alice_restored else '❌ 失败'}")
        print(f"Bob数据恢复: {'✅ 成功' if bob_restored else '❌ 失败'}")
        
        if alice_restored and bob_restored:
            print("\n🎉 事务回滚修复成功！数据完全恢复到原始状态")
        else:
            print("\n❌ 事务回滚仍有问题，数据未完全恢复")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_rollback_fix()
