"""
存储引擎主接口
整合页管理器、缓存管理器和表管理器，提供统一的存储服务接口
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import time
from typing import Dict, List, Optional, Any, Tuple
from src.storage.page.page import PageManager, PageType
from src.storage.buffer.buffer_manager import BufferManager, ReplacementPolicy
from src.storage.table.table_manager import TableManager, TableSchema, ColumnDefinition, ColumnType

class StorageEngine:
    """存储引擎主类"""
    
    def __init__(self, 
                 data_dir: str = "data",
                 buffer_size: int = 100,
                 replacement_policy: ReplacementPolicy = ReplacementPolicy.LRU):
        """
        初始化存储引擎
        
        Args:
            data_dir: 数据目录
            buffer_size: 缓存大小（页数）
            replacement_policy: 页面替换策略
        """
        self.data_dir = data_dir
        
        # 初始化组件
        self.page_manager = PageManager(data_dir)
        self.buffer_manager = BufferManager(buffer_size, self.page_manager, replacement_policy)
        self.table_manager = TableManager(self.buffer_manager)
        
        # 统计信息
        self.stats = {
            'queries_executed': 0,
            'records_inserted': 0,
            'records_updated': 0,
            'records_deleted': 0,
            'start_time': time.time()
        }
    
    def create_table(self, table_name: str, columns: List[Dict[str, Any]]) -> bool:
        """
        创建表
        
        Args:
            table_name: 表名
            columns: 列定义列表，格式：[{'name': 'id', 'type': 'INTEGER', 'primary_key': True}, ...]
            
        Returns:
            是否创建成功
        """
        try:
            schema = TableSchema(table_name)
            schema.created_time = time.strftime('%Y-%m-%d %H:%M:%S')
            
            for col_def in columns:
                column = ColumnDefinition(
                    name=col_def['name'],
                    column_type=ColumnType(col_def['type']),
                    max_length=col_def.get('max_length'),
                    nullable=col_def.get('nullable', True),
                    default_value=col_def.get('default_value'),
                    is_primary_key=col_def.get('primary_key', False),
                    is_unique=col_def.get('unique', False)
                )
                schema.add_column(column)
            
            return self.table_manager.create_table(schema)
            
        except Exception as e:
            print(f"Error creating table: {e}")
            return False
    
    def drop_table(self, table_name: str) -> bool:
        """删除表"""
        return self.table_manager.drop_table(table_name)
    
    def insert(self, table_name: str, record: Dict[str, Any]) -> bool:
        """
        插入记录
        
        Args:
            table_name: 表名
            record: 记录数据
            
        Returns:
            是否插入成功
        """
        if self.table_manager.insert_record(table_name, record):
            self.stats['records_inserted'] += 1
            return True
        return False
    
    def select(self, table_name: str, 
              columns: Optional[List[str]] = None,
              where: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        查询记录
        
        Args:
            table_name: 表名
            columns: 要查询的列，None表示所有列
            where: 查询条件
            
        Returns:
            查询结果列表
        """
        self.stats['queries_executed'] += 1
        return self.table_manager.select_records(table_name, where, columns)
    
    def update(self, table_name: str, 
              values: Dict[str, Any],
              where: Optional[Dict[str, Any]] = None) -> int:
        """
        更新记录
        
        Args:
            table_name: 表名
            values: 要更新的值
            where: 更新条件
            
        Returns:
            更新的记录数
        """
        updated = self.table_manager.update_records(table_name, values, where)
        self.stats['records_updated'] += updated
        return updated
    
    def delete(self, table_name: str, 
              where: Optional[Dict[str, Any]] = None) -> int:
        """
        删除记录
        
        Args:
            table_name: 表名
            where: 删除条件
            
        Returns:
            删除的记录数
        """
        deleted = self.table_manager.delete_records(table_name, where)
        self.stats['records_deleted'] += deleted
        return deleted
    
    def list_tables(self) -> List[str]:
        """列出所有表"""
        return self.table_manager.list_tables()
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取表信息"""
        return self.table_manager.get_table_info(table_name)
    
    def add_column(self, table_name: str, column_def: Dict[str, Any]) -> bool:
        """
        为表添加列
        
        Args:
            table_name: 表名
            column_def: 列定义，格式：{'name': 'description', 'type': 'STRING', 'nullable': True, 'default_value': None}
            
        Returns:
            是否添加成功
        """
        try:
            from src.storage.table.table_manager import ColumnDefinition, ColumnType
            
            column = ColumnDefinition(
                name=column_def['name'],
                column_type=ColumnType(column_def['type']),
                max_length=column_def.get('max_length'),
                nullable=column_def.get('nullable', True),
                default_value=column_def.get('default_value'),
                is_primary_key=column_def.get('primary_key', False),
                is_unique=column_def.get('unique', False)
            )
            
            return self.table_manager.add_column(table_name, column)
            
        except Exception as e:
            print(f"Error adding column: {e}")
            return False
    
    def create_index(self, index_name: str, table_name: str, columns: List[str]) -> bool:
        """
        创建索引
        
        Args:
            index_name: 索引名
            table_name: 表名
            columns: 列名列表
            
        Returns:
            是否创建成功
        """
        return self.table_manager.create_index(index_name, table_name, columns)
    
    def flush_all(self) -> int:
        """将所有脏页刷新到磁盘"""
        return self.buffer_manager.flush_all_pages()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取存储引擎统计信息"""
        uptime = time.time() - self.stats['start_time']
        
        cache_stats = self.buffer_manager.get_cache_stats()
        page_stats = self.page_manager.get_page_stats()
        
        return {
            'uptime_seconds': round(uptime, 2),
            'tables': len(self.table_manager.list_tables()),
            'storage_stats': self.stats.copy(),
            'cache_stats': cache_stats,
            'page_stats': page_stats
        }
    
    def print_status(self):
        """打印存储引擎状态"""
        stats = self.get_stats()
        
        print("\n" + "=" * 60)
        print("              存储引擎状态")
        print("=" * 60)
        
        print(f"运行时间: {stats['uptime_seconds']} 秒")
        print(f"表数量: {stats['tables']}")
        print(f"执行查询: {stats['storage_stats']['queries_executed']}")
        print(f"插入记录: {stats['storage_stats']['records_inserted']}")
        print(f"更新记录: {stats['storage_stats']['records_updated']}")
        print(f"删除记录: {stats['storage_stats']['records_deleted']}")
        
        print("\n--- 缓存统计 ---")
        cache_stats = stats['cache_stats']
        print(f"缓存命中率: {cache_stats['cache_hit_rate']}%")
        print(f"缓存命中: {cache_stats['cache_hits']}")
        print(f"缓存未命中: {cache_stats['cache_misses']}")
        print(f"已使用帧: {cache_stats['used_frames']}/{cache_stats['buffer_size']}")
        print(f"脏页数: {cache_stats['dirty_frames']}")
        
        print("\n--- 页面统计 ---")
        page_stats = stats['page_stats']
        print(f"总页数: {page_stats['total_pages']}")
        print(f"总记录数: {page_stats['total_records']}")
        print(f"下一页ID: {page_stats['next_page_id']}")
    
    def shutdown(self):
        """关闭存储引擎"""
        print("正在关闭存储引擎...")
        
        # 刷新所有脏页
        flushed = self.flush_all()
        print(f"刷新了 {flushed} 个脏页到磁盘")
        
        print("存储引擎已关闭")

def test_storage_engine():
    """测试存储引擎"""
    print("=" * 70)
    print("              存储引擎完整测试")
    print("=" * 70)
    
    # 创建存储引擎
    storage = StorageEngine("test_storage", buffer_size=10)
    
    print("\n1. 创建表...")
    
    # 创建用户表
    users_columns = [
        {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
        {'name': 'username', 'type': 'STRING', 'max_length': 50, 'nullable': False},
        {'name': 'email', 'type': 'STRING', 'max_length': 100},
        {'name': 'age', 'type': 'INTEGER'},
        {'name': 'salary', 'type': 'FLOAT'},
        {'name': 'active', 'type': 'BOOLEAN', 'default_value': True}
    ]
    
    if storage.create_table("users", users_columns):
        print("  ✓ 用户表创建成功")
    else:
        print("  ✗ 用户表创建失败")
    
    # 创建订单表
    orders_columns = [
        {'name': 'order_id', 'type': 'INTEGER', 'primary_key': True},
        {'name': 'user_id', 'type': 'INTEGER', 'nullable': False},
        {'name': 'product', 'type': 'STRING', 'max_length': 100},
        {'name': 'amount', 'type': 'FLOAT'},
        {'name': 'status', 'type': 'STRING', 'max_length': 20, 'default_value': 'pending'}
    ]
    
    if storage.create_table("orders", orders_columns):
        print("  ✓ 订单表创建成功")
    else:
        print("  ✗ 订单表创建失败")
    
    print("\n2. 插入测试数据...")
    
    # 插入用户数据
    test_users = [
        {'id': 1, 'username': 'alice', 'email': 'alice@example.com', 'age': 25, 'salary': 5000.0},
        {'id': 2, 'username': 'bob', 'email': 'bob@example.com', 'age': 30, 'salary': 6000.0},
        {'id': 3, 'username': 'charlie', 'email': 'charlie@example.com', 'age': 28, 'salary': 5500.0},
        {'id': 4, 'username': 'diana', 'email': 'diana@example.com', 'age': 26, 'salary': 5200.0},
        {'id': 5, 'username': 'eve', 'email': 'eve@example.com', 'age': 32, 'salary': 7000.0}
    ]
    
    for user in test_users:
        if storage.insert("users", user):
            print(f"  ✓ 插入用户: {user['username']}")
        else:
            print(f"  ✗ 插入失败: {user['username']}")
    
    # 插入订单数据
    test_orders = [
        {'order_id': 101, 'user_id': 1, 'product': 'Laptop', 'amount': 1200.0, 'status': 'completed'},
        {'order_id': 102, 'user_id': 2, 'product': 'Mouse', 'amount': 25.0, 'status': 'pending'},
        {'order_id': 103, 'user_id': 1, 'product': 'Keyboard', 'amount': 80.0, 'status': 'completed'},
        {'order_id': 104, 'user_id': 3, 'product': 'Monitor', 'amount': 300.0, 'status': 'shipping'},
        {'order_id': 105, 'user_id': 4, 'product': 'Headphones', 'amount': 150.0, 'status': 'completed'}
    ]
    
    for order in test_orders:
        if storage.insert("orders", order):
            print(f"  ✓ 插入订单: {order['order_id']}")
        else:
            print(f"  ✗ 插入失败: {order['order_id']}")
    
    print("\n3. 查询测试...")
    
    # 查询所有用户
    all_users = storage.select("users")
    print(f"  所有用户 ({len(all_users)} 条):")
    for user in all_users:
        print(f"    {user['username']} - {user['email']} - 年龄:{user['age']}")
    
    # 条件查询
    high_salary_users = storage.select("users", where={'salary': {'>': 5500}})
    print(f"\n  高薪用户 (薪资>5500, {len(high_salary_users)} 条):")
    for user in high_salary_users:
        print(f"    {user['username']} - 薪资:{user['salary']}")
    
    # 列投影
    user_names = storage.select("users", columns=['username', 'age'])
    print(f"\n  用户姓名和年龄:")
    for user in user_names:
        print(f"    {user['username']} - {user['age']}")
    
    # 查询订单
    completed_orders = storage.select("orders", where={'status': 'completed'})
    print(f"\n  已完成订单 ({len(completed_orders)} 条):")
    for order in completed_orders:
        print(f"    订单{order['order_id']} - {order['product']} - {order['amount']}")
    
    print("\n4. 更新测试...")
    
    # 更新用户年龄
    updated = storage.update("users", {'age': 31}, where={'username': 'bob'})
    print(f"  更新了 {updated} 个用户的年龄")
    
    # 更新订单状态
    updated = storage.update("orders", {'status': 'shipped'}, where={'status': 'shipping'})
    print(f"  更新了 {updated} 个订单状态")
    
    print("\n5. 删除测试...")
    
    # 删除待处理订单
    deleted = storage.delete("orders", where={'status': 'pending'})
    print(f"  删除了 {deleted} 个待处理订单")
    
    print("\n6. 表信息...")
    
    tables = storage.list_tables()
    print(f"  数据库中的表: {tables}")
    
    for table in tables:
        info = storage.describe_table(table)
        if info:
            print(f"\n  表 '{table}' 信息:")
            print(f"    列: {info['columns']}")
            print(f"    主键: {info['primary_key']}")
            print(f"    页数: {info['page_count']}")
            print(f"    记录数: {info['record_count']}")
    
    print("\n7. 性能统计...")
    storage.print_status()
    
    print("\n8. 关闭存储引擎...")
    storage.shutdown()
    
    print("\n✅ 存储引擎测试完成!")

if __name__ == "__main__":
    test_storage_engine()