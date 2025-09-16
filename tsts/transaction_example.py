"""
事务功能演示示例
展示BEGIN、COMMIT、ROLLBACK的实际使用
"""

import sys
from pathlib import Path

# 确保可以导入项目模块
sys.path.insert(0, str(Path(__file__).parent))

from src.unified_sql_processor import UnifiedSQLProcessor
from src.storage.storage_engine import StorageEngine

def demonstrate_transaction():
    """演示事务功能"""
    print("=" * 60)
    print("           数据库事务功能演示")
    print("=" * 60)
    
    # 创建SQL处理器
    try:
        processor = UnifiedSQLProcessor()
        print("✅ SQL处理器创建成功")
    except Exception as e:
        print(f"❌ SQL处理器创建失败: {e}")
        return
    
    # 1. 创建测试表
    print("\n1. 创建测试表...")
    create_table_sql = """
    CREATE TABLE accounts (
        id INTEGER PRIMARY KEY,
        name VARCHAR(50),
        balance DECIMAL(10,2)
    );
    """
    
    success, results, error = processor.process_sql(create_table_sql)
    if success:
        print("✅ 表创建成功")
    else:
        print(f"❌ 表创建失败: {error}")
        return
    
    # 2. 插入初始数据
    print("\n2. 插入初始数据...")
    insert_sqls = [
        "INSERT INTO accounts (id, name, balance) VALUES (1, 'Alice', 1000.00);",
        "INSERT INTO accounts (id, name, balance) VALUES (2, 'Bob', 500.00);"
    ]
    
    for sql in insert_sqls:
        success, results, error = processor.process_sql(sql)
        if success:
            print(f"✅ 插入成功: {sql.split('VALUES')[1].strip()}")
        else:
            print(f"❌ 插入失败: {error}")
    
    # 3. 查看初始数据
    print("\n3. 查看初始数据...")
    success, results, error = processor.process_sql("SELECT * FROM accounts;")
    if success and results:
        print("初始账户数据:")
        for result in results:
            if result.get('type') == 'select_result':
                for record in result.get('records', []):
                    print(f"  ID: {record.get('id')}, 姓名: {record.get('name')}, 余额: {record.get('balance')}")
    
    # 4. 演示成功的事务提交
    print("\n" + "="*50)
    print("4. 演示成功的事务提交 (Alice转账100给Bob)")
    print("="*50)
    
    transaction_sqls = [
        "BEGIN;",
        "UPDATE accounts SET balance = balance - 100 WHERE id = 1;",  # Alice减少100
        "UPDATE accounts SET balance = balance + 100 WHERE id = 2;",  # Bob增加100
        "COMMIT;"
    ]
    
    for sql in transaction_sqls:
        print(f"\n执行: {sql}")
        success, results, error = processor.process_sql(sql)
        if success:
            if results:
                for result in results:
                    if result.get('message'):
                        print(f"✅ {result['message']}")
                    elif result.get('type') == 'update_result':
                        print(f"✅ 更新了 {result.get('affected_rows', 0)} 行")
            else:
                print("✅ 执行成功")
        else:
            print(f"❌ 执行失败: {error}")
    
    # 5. 查看事务提交后的数据
    print("\n5. 查看事务提交后的数据...")
    success, results, error = processor.process_sql("SELECT * FROM accounts;")
    if success and results:
        print("事务提交后的账户数据:")
        for result in results:
            if result.get('type') == 'select_result':
                for record in result.get('records', []):
                    print(f"  ID: {record.get('id')}, 姓名: {record.get('name')}, 余额: {record.get('balance')}")
    
    # 6. 演示事务回滚
    print("\n" + "="*50)
    print("6. 演示事务回滚 (尝试无效操作后回滚)")
    print("="*50)
    
    rollback_sqls = [
        "BEGIN;",
        "UPDATE accounts SET balance = balance - 200 WHERE id = 1;",  # Alice减少200
        "UPDATE accounts SET balance = balance + 200 WHERE id = 2;",  # Bob增加200
        "ROLLBACK;"  # 回滚所有更改
    ]
    
    for sql in rollback_sqls:
        print(f"\n执行: {sql}")
        success, results, error = processor.process_sql(sql)
        if success:
            if results:
                for result in results:
                    if result.get('message'):
                        print(f"✅ {result['message']}")
                    elif result.get('type') == 'update_result':
                        print(f"✅ 更新了 {result.get('affected_rows', 0)} 行")
            else:
                print("✅ 执行成功")
        else:
            print(f"❌ 执行失败: {error}")
    
    # 7. 查看回滚后的数据
    print("\n7. 查看回滚后的数据...")
    success, results, error = processor.process_sql("SELECT * FROM accounts;")
    if success and results:
        print("事务回滚后的账户数据:")
        for result in results:
            if result.get('type') == 'select_result':
                for record in result.get('records', []):
                    print(f"  ID: {record.get('id')}, 姓名: {record.get('name')}, 余额: {record.get('balance')}")
    
    # 8. 演示复杂事务场景
    print("\n" + "="*50)
    print("8. 演示复杂事务场景 (多步操作)")
    print("="*50)
    
    complex_sqls = [
        "BEGIN;",
        "INSERT INTO accounts (id, name, balance) VALUES (3, 'Charlie', 300.00);",
        "UPDATE accounts SET balance = balance + 50 WHERE name = 'Alice';",
        "DELETE FROM accounts WHERE balance < 100;",
        "COMMIT;"
    ]
    
    for sql in complex_sqls:
        print(f"\n执行: {sql}")
        success, results, error = processor.process_sql(sql)
        if success:
            if results:
                for result in results:
                    if result.get('message'):
                        print(f"✅ {result['message']}")
                    elif result.get('type') in ['insert_result', 'update_result', 'delete_result']:
                        rows = result.get('affected_rows', result.get('inserted_rows', result.get('deleted_rows', 0)))
                        print(f"✅ 影响了 {rows} 行")
            else:
                print("✅ 执行成功")
        else:
            print(f"❌ 执行失败: {error}")
    
    # 9. 查看最终数据
    print("\n9. 查看最终数据...")
    success, results, error = processor.process_sql("SELECT * FROM accounts ORDER BY id;")
    if success and results:
        print("最终账户数据:")
        for result in results:
            if result.get('type') == 'select_result':
                for record in result.get('records', []):
                    print(f"  ID: {record.get('id')}, 姓名: {record.get('name')}, 余额: {record.get('balance')}")
    
    print("\n" + "="*60)
    print("           事务功能演示完成！")
    print("="*60)
    
    print("""
📝 事务使用总结:

1. BEGIN; - 开始事务
2. 执行多个SQL操作 (INSERT, UPDATE, DELETE)
3. COMMIT; - 提交所有更改
   或 ROLLBACK; - 回滚所有更改

💡 事务的好处:
- 确保数据一致性
- 支持复杂的多步操作
- 出错时可以完全回滚
- 支持并发控制
    """)

if __name__ == "__main__":
    try:
        demonstrate_transaction()
    except Exception as e:
        print(f"演示过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
