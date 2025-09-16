"""
清理并重新测试事务回滚修复
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

def clean_and_test_rollback():
    print("🧹 清理并测试事务回滚修复")
    print("=" * 50)
    
    try:
        from src.unified_sql_processor import UnifiedSQLProcessor
        processor = UnifiedSQLProcessor()
        print("✅ 数据库系统初始化成功")
        
        # 1. 清理已存在的表
        print("\n1. 清理已存在的表...")
        try:
            success, results, error = processor.process_sql("DROP TABLE test_rollback;")
            if success:
                print("✅ 旧表删除成功")
            else:
                print(f"ℹ️ 旧表不存在或删除失败: {error}")
        except:
            print("ℹ️ 旧表不存在")
        
        # 2. 创建新的测试表
        print("\n2. 创建新的测试表...")
        success, results, error = processor.process_sql(
            "CREATE TABLE rollback_test (id INTEGER PRIMARY KEY, name VARCHAR(50), value INTEGER);"
        )
        if success:
            print("✅ 表创建成功")
        else:
            print(f"❌ 表创建失败: {error}")
            return
        
        # 3. 插入初始数据
        print("\n3. 插入初始数据...")
        init_sqls = [
            "INSERT INTO rollback_test (id, name, value) VALUES (1, 'Alice', 100);",
            "INSERT INTO rollback_test (id, name, value) VALUES (2, 'Bob', 200);"
        ]
        
        for sql in init_sqls:
            success, results, error = processor.process_sql(sql)
            if success:
                print(f"✅ 插入成功")
            else:
                print(f"❌ 插入失败: {error}")
        
        # 4. 查看初始数据
        print("\n4. 查看初始数据...")
        success, results, error = processor.process_sql("SELECT * FROM rollback_test ORDER BY id;")
        if success and results:
            print("📊 初始数据:")
            for result in results:
                if result.get('type') == 'select_result':
                    records = result.get('records', [])
                    for record in records:
                        print(f"   ID: {record.get('id')}, 姓名: {record.get('name')}, 值: {record.get('value')}")
        
        # 5. 开始事务并进行更新
        print("\n5. 开始事务并进行更新...")
        
        # 开始事务
        print("   执行: BEGIN;")
        success, results, error = processor.process_sql("BEGIN;")
        if success:
            print("   ✅ 事务开始")
        else:
            print(f"   ❌ 事务开始失败: {error}")
            return
        
        # 更新Alice的值
        print("   执行: UPDATE rollback_test SET value = 999 WHERE name = 'Alice';")
        success, results, error = processor.process_sql("UPDATE rollback_test SET value = 999 WHERE name = 'Alice';")
        if success:
            print("   ✅ Alice的值更新为999")
        else:
            print(f"   ❌ 更新失败: {error}")
        
        # 更新Bob的名字
        print("   执行: UPDATE rollback_test SET name = 'Bobby' WHERE id = 2;")
        success, results, error = processor.process_sql("UPDATE rollback_test SET name = 'Bobby' WHERE id = 2;")
        if success:
            print("   ✅ Bob的名字更新为Bobby")
        else:
            print(f"   ❌ 更新失败: {error}")
        
        # 6. 查看事务中的数据（更新后）
        print("\n6. 查看事务中的数据（更新后）...")
        success, results, error = processor.process_sql("SELECT * FROM rollback_test ORDER BY id;")
        if success and results:
            print("📊 更新后数据:")
            for result in results:
                if result.get('type') == 'select_result':
                    records = result.get('records', [])
                    for record in records:
                        print(f"   ID: {record.get('id')}, 姓名: {record.get('name')}, 值: {record.get('value')}")
        
        # 7. 回滚事务
        print("\n7. 回滚事务...")
        print("   执行: ROLLBACK;")
        success, results, error = processor.process_sql("ROLLBACK;")
        if success:
            print("   ✅ 事务回滚成功")
        else:
            print(f"   ❌ 回滚失败: {error}")
        
        # 8. 查看回滚后的数据
        print("\n8. 查看回滚后的数据...")
        success, results, error = processor.process_sql("SELECT * FROM rollback_test ORDER BY id;")
        if success and results:
            print("📊 回滚后数据:")
            rollback_records = []
            for result in results:
                if result.get('type') == 'select_result':
                    records = result.get('records', [])
                    rollback_records = records
                    for record in records:
                        print(f"   ID: {record.get('id')}, 姓名: {record.get('name')}, 值: {record.get('value')}")
            
            # 9. 验证数据是否完全恢复
            print("\n9. 验证数据恢复情况...")
            alice_correct = False
            bob_correct = False
            
            for record in rollback_records:
                if (record.get('id') == 1 and 
                    record.get('name') == 'Alice' and 
                    record.get('value') == 100):
                    alice_correct = True
                elif (record.get('id') == 2 and 
                      record.get('name') == 'Bob' and 
                      record.get('value') == 200):
                    bob_correct = True
            
            print(f"Alice数据恢复: {'✅ 正确 (ID:1, 姓名:Alice, 值:100)' if alice_correct else '❌ 错误'}")
            print(f"Bob数据恢复: {'✅ 正确 (ID:2, 姓名:Bob, 值:200)' if bob_correct else '❌ 错误'}")
            
            if alice_correct and bob_correct:
                print("\n🎉 事务回滚修复成功！数据完全恢复到原始状态")
            else:
                print("\n❌ 事务回滚仍有问题，数据未完全恢复")
                print("期望: Alice(ID:1, value:100), Bob(ID:2, value:200)")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    clean_and_test_rollback()
