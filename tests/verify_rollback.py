"""
验证事务回滚是否正确工作
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

def verify_rollback():
    print("🔍 验证事务回滚功能")
    print("=" * 40)
    
    try:
        from src.unified_sql_processor import UnifiedSQLProcessor
        processor = UnifiedSQLProcessor()
        
        # 清理并创建测试表
        try:
            processor.process_sql("DROP TABLE verify_rollback;")
        except:
            pass
            
        # 创建表
        success, results, error = processor.process_sql(
            "CREATE TABLE verify_rollback (id INTEGER PRIMARY KEY, name VARCHAR(50), balance INTEGER);"
        )
        if not success:
            print(f"❌ 创建表失败: {error}")
            return
        print("✅ 测试表创建成功")
        
        # 插入初始数据
        processor.process_sql("INSERT INTO verify_rollback (id, name, balance) VALUES (1, 'Alice', 1000);")
        processor.process_sql("INSERT INTO verify_rollback (id, name, balance) VALUES (2, 'Bob', 2000);")
        print("✅ 初始数据插入完成")
        
        # 查看初始状态
        print("\n📊 初始状态:")
        success, results, error = processor.process_sql("SELECT id, name, balance FROM verify_rollback ORDER BY id;")
        initial_data = []
        if success:
            for result in results:
                if result.get('type') == 'select_result':
                    initial_data = result.get('records', [])
                    for record in initial_data:
                        print(f"   ID: {record['id']}, 姓名: {record['name']}, 余额: {record['balance']}")
        
        # 开始事务并修改数据
        print("\n🔄 开始事务并修改数据...")
        processor.process_sql("BEGIN;")
        processor.process_sql("UPDATE verify_rollback SET balance = 999999 WHERE id = 1;")  # Alice余额改为999999
        processor.process_sql("UPDATE verify_rollback SET name = 'Robert' WHERE id = 2;")    # Bob名字改为Robert
        print("   ✅ 数据修改完成")
        
        # 查看修改后状态
        print("\n📊 修改后状态:")
        success, results, error = processor.process_sql("SELECT id, name, balance FROM verify_rollback ORDER BY id;")
        if success:
            for result in results:
                if result.get('type') == 'select_result':
                    for record in result.get('records', []):
                        print(f"   ID: {record['id']}, 姓名: {record['name']}, 余额: {record['balance']}")
        
        # 回滚事务
        print("\n↩️  回滚事务...")
        success, results, error = processor.process_sql("ROLLBACK;")
        if success:
            print("   ✅ 回滚命令执行成功")
        else:
            print(f"   ❌ 回滚失败: {error}")
        
        # 查看回滚后状态
        print("\n📊 回滚后状态:")
        success, results, error = processor.process_sql("SELECT id, name, balance FROM verify_rollback ORDER BY id;")
        rollback_data = []
        if success:
            for result in results:
                if result.get('type') == 'select_result':
                    rollback_data = result.get('records', [])
                    for record in rollback_data:
                        print(f"   ID: {record['id']}, 姓名: {record['name']}, 余额: {record['balance']}")
        
        # 验证数据是否完全恢复
        print("\n🎯 验证结果:")
        if len(initial_data) == len(rollback_data):
            all_correct = True
            for i in range(len(initial_data)):
                initial = initial_data[i]
                rollback = rollback_data[i]
                
                # 按ID匹配记录
                initial_record = None
                rollback_record = None
                
                for record in initial_data:
                    if record['id'] == 1:
                        initial_alice = record
                    elif record['id'] == 2:
                        initial_bob = record
                
                for record in rollback_data:
                    if record['id'] == 1:
                        rollback_alice = record
                    elif record['id'] == 2:
                        rollback_bob = record
                
            # 验证Alice的数据
            alice_correct = (initial_alice['id'] == rollback_alice['id'] and 
                           initial_alice['name'] == rollback_alice['name'] and 
                           initial_alice['balance'] == rollback_alice['balance'])
            
            # 验证Bob的数据
            bob_correct = (initial_bob['id'] == rollback_bob['id'] and 
                          initial_bob['name'] == rollback_bob['name'] and 
                          initial_bob['balance'] == rollback_bob['balance'])
            
            print(f"   Alice数据恢复: {'✅ 正确' if alice_correct else '❌ 错误'}")
            print(f"   Bob数据恢复: {'✅ 正确' if bob_correct else '❌ 错误'}")
            
            if alice_correct and bob_correct:
                print("\n🎉 事务回滚功能完全正常！数据已完全恢复到原始状态")
            else:
                print("\n❌ 事务回滚存在问题")
                print(f"   期望Alice: ID=1, 姓名=Alice, 余额=1000")
                print(f"   实际Alice: ID={rollback_alice['id']}, 姓名={rollback_alice['name']}, 余额={rollback_alice['balance']}")
                print(f"   期望Bob: ID=2, 姓名=Bob, 余额=2000")
                print(f"   实际Bob: ID={rollback_bob['id']}, 姓名={rollback_bob['name']}, 余额={rollback_bob['balance']}")
        else:
            print("❌ 记录数量不匹配")
            
    except Exception as e:
        print(f"❌ 验证失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify_rollback()
