"""
简单的事务功能演示
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

def main():
    print("🎯 数据库事务功能演示")
    print("=" * 50)
    
    try:
        from src.unified_sql_processor import UnifiedSQLProcessor
        processor = UnifiedSQLProcessor()
        print("✅ 数据库系统初始化成功")
        
        # 演示SQL语句
        demo_sqls = [
            # 1. 创建表
            "CREATE TABLE demo_users (id INTEGER PRIMARY KEY, name VARCHAR(50), age INTEGER);",
            
            # 2. 开始事务
            "BEGIN;",
            
            # 3. 插入数据
            "INSERT INTO demo_users (id, name, age) VALUES (1, 'Alice', 25);",
            "INSERT INTO demo_users (id, name, age) VALUES (2, 'Bob', 30);",
            
            # 4. 提交事务
            "COMMIT;",
            
            # 5. 查看数据
            "SELECT * FROM demo_users;",
            
            # 6. 开始新事务
            "BEGIN;",
            
            # 7. 更新数据
            "UPDATE demo_users SET age = 26 WHERE name = 'Alice';",
            
            # 8. 回滚事务
            "ROLLBACK;",
            
            # 9. 再次查看数据（应该没有变化）
            "SELECT * FROM demo_users;"
        ]
        
        for i, sql in enumerate(demo_sqls, 1):
            print(f"\n{i}. 执行: {sql}")
            try:
                success, results, error = processor.process_sql(sql)
                if success:
                    print("   ✅ 执行成功")
                    if results:
                        for result in results:
                            if 'message' in result:
                                print(f"   📝 {result['message']}")
                            elif result.get('type') == 'select_result':
                                records = result.get('records', [])
                                if records:
                                    print("   📊 查询结果:")
                                    for record in records:
                                        print(f"      {record}")
                else:
                    print(f"   ❌ 执行失败: {error}")
            except Exception as e:
                print(f"   ❌ 异常: {e}")
        
        print("\n" + "=" * 50)
        print("🎉 事务演示完成！")
        
    except ImportError as e:
        print(f"❌ 模块导入失败: {e}")
    except Exception as e:
        print(f"❌ 演示失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
