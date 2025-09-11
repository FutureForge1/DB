"""
表管理系统实现
管理数据表的结构定义、记录的增删查改操作
与页管理器和缓存管理器集成，提供高级的数据操作接口
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import json
import os
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum
from src.storage.page.page import Page, PageType
from src.storage.buffer.buffer_manager import BufferManager

class ColumnType(Enum):
    """列数据类型"""
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"

@dataclass
class ColumnDefinition:
    """列定义"""
    name: str
    column_type: ColumnType
    max_length: Optional[int] = None  # 字符串类型的最大长度
    nullable: bool = True
    default_value: Optional[Any] = None
    is_primary_key: bool = False
    is_unique: bool = False

@dataclass
class TableSchema:
    """表结构定义"""
    table_name: str
    columns: List[ColumnDefinition] = field(default_factory=list)
    primary_key: Optional[str] = None
    created_time: Optional[str] = None
    
    def add_column(self, column: ColumnDefinition):
        """添加列定义"""
        self.columns.append(column)
        if column.is_primary_key:
            self.primary_key = column.name
    
    def get_column(self, column_name: str) -> Optional[ColumnDefinition]:
        """获取列定义"""
        for column in self.columns:
            if column.name == column_name:
                return column
        return None
    
    def get_column_names(self) -> List[str]:
        """获取所有列名"""
        return [col.name for col in self.columns]
    
    def validate_record(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        """验证记录是否符合表结构"""
        # 检查必需字段
        for column in self.columns:
            if not column.nullable and column.name not in record:
                if column.default_value is None:
                    return False, f"Column '{column.name}' cannot be null"
        
        # 检查数据类型
        for field_name, value in record.items():
            column = self.get_column(field_name)
            if column is None:
                return False, f"Unknown column '{field_name}'"
            
            if value is not None:
                if not self._validate_type(value, column):
                    return False, f"Invalid type for column '{field_name}'"
        
        return True, ""
    
    def _validate_type(self, value: Any, column: ColumnDefinition) -> bool:
        """验证值的数据类型"""
        if column.column_type == ColumnType.INTEGER:
            return isinstance(value, int)
        elif column.column_type == ColumnType.FLOAT:
            return isinstance(value, (int, float))
        elif column.column_type == ColumnType.STRING:
            if isinstance(value, str):
                if column.max_length and len(value) > column.max_length:
                    return False
                return True
            return False
        elif column.column_type == ColumnType.BOOLEAN:
            return isinstance(value, bool)
        else:
            return True  # 其他类型暂时允许
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'table_name': self.table_name,
            'columns': [
                {
                    'name': col.name,
                    'column_type': col.column_type.value,
                    'max_length': col.max_length,
                    'nullable': col.nullable,
                    'default_value': col.default_value,
                    'is_primary_key': col.is_primary_key,
                    'is_unique': col.is_unique
                }
                for col in self.columns
            ],
            'primary_key': self.primary_key,
            'created_time': self.created_time
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableSchema':
        """从字典创建"""
        schema = cls(
            table_name=data['table_name'],
            primary_key=data.get('primary_key'),
            created_time=data.get('created_time')
        )
        
        for col_data in data.get('columns', []):
            column = ColumnDefinition(
                name=col_data['name'],
                column_type=ColumnType(col_data['column_type']),
                max_length=col_data.get('max_length'),
                nullable=col_data.get('nullable', True),
                default_value=col_data.get('default_value'),
                is_primary_key=col_data.get('is_primary_key', False),
                is_unique=col_data.get('is_unique', False)
            )
            schema.add_column(column)
        
        return schema

class TableManager:
    """表管理器"""
    
    def __init__(self, buffer_manager: Optional[BufferManager] = None):
        """初始化表管理器"""
        self.buffer_manager = buffer_manager or BufferManager()
        self.tables: Dict[str, TableSchema] = {}  # 表名到表结构的映射
        self.table_pages: Dict[str, List[int]] = {}  # 表名到页ID列表的映射
        self._load_schemas()
    
    def _get_schema_file_path(self) -> str:
        """获取表结构文件路径"""
        data_dir = self.buffer_manager.page_manager.data_dir
        return str(data_dir / "table_schemas.json")
    
    def _load_schemas(self):
        """加载表结构"""
        schema_file = self._get_schema_file_path()
        if os.path.exists(schema_file):
            try:
                with open(schema_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # 加载表结构
                    for table_data in data.get('tables', []):
                        schema = TableSchema.from_dict(table_data)
                        self.tables[schema.table_name] = schema
                    
                    # 加载表页映射
                    self.table_pages = data.get('table_pages', {})
                    
            except Exception as e:
                print(f"Error loading schemas: {e}")
    
    def _save_schemas(self):
        """保存表结构"""
        schema_file = self._get_schema_file_path()
        try:
            data = {
                'tables': [schema.to_dict() for schema in self.tables.values()],
                'table_pages': self.table_pages
            }
            
            with open(schema_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            print(f"Error saving schemas: {e}")
    
    def create_table(self, schema: TableSchema) -> bool:
        """创建表"""
        if schema.table_name in self.tables:
            return False  # 表已存在
        
        # 保存表结构
        self.tables[schema.table_name] = schema
        self.table_pages[schema.table_name] = []
        
        # 创建第一个数据页
        page = self.buffer_manager.create_page(PageType.DATA_PAGE)
        if page:
            self.table_pages[schema.table_name].append(page.header.page_id)
            self.buffer_manager.unpin_page(page.header.page_id, is_dirty=True)
        
        self._save_schemas()
        return True
    
    def drop_table(self, table_name: str) -> bool:
        """删除表"""
        if table_name not in self.tables:
            return False
        
        # 删除所有相关页面（这里只是从映射中移除，实际删除可以在后台进行）
        if table_name in self.table_pages:
            del self.table_pages[table_name]
        
        del self.tables[table_name]
        self._save_schemas()
        return True
    
    def insert_record(self, table_name: str, record: Dict[str, Any]) -> bool:
        """插入记录"""
        if table_name not in self.tables:
            return False
        
        schema = self.tables[table_name]
        
        # 验证记录
        is_valid, error_msg = schema.validate_record(record)
        if not is_valid:
            print(f"Record validation failed: {error_msg}")
            return False
        
        # 补充默认值
        complete_record = self._apply_defaults(record, schema)
        
        # 找到可以插入记录的页面
        page_id = self._find_page_for_insert(table_name, complete_record)
        if page_id is None:
            # 创建新页面
            page = self.buffer_manager.create_page(PageType.DATA_PAGE)
            if page:
                page_id = page.header.page_id
                self.table_pages[table_name].append(page_id)
                self._save_schemas()
            else:
                return False
        
        # 获取页面并插入记录
        page = self.buffer_manager.get_page(page_id)
        if page and page.add_record(complete_record):
            self.buffer_manager.unpin_page(page_id, is_dirty=True)
            return True
        
        return False
    
    def select_records(self, table_name: str, 
                      where_condition: Optional[Dict[str, Any]] = None,
                      columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """查询记录"""
        if table_name not in self.tables:
            return []
        
        schema = self.tables[table_name]
        results = []
        
        # 遍历表的所有页面
        for page_id in self.table_pages.get(table_name, []):
            page = self.buffer_manager.get_page(page_id)
            if page:
                records = page.get_records()
                
                # 应用WHERE条件
                for record in records:
                    if self._match_condition(record, where_condition):
                        # 应用列投影
                        if columns:
                            projected_record = {col: record.get(col) for col in columns if col in record}
                        else:
                            projected_record = record.copy()
                        results.append(projected_record)
                
                self.buffer_manager.unpin_page(page_id)
        
        return results
    
    def update_records(self, table_name: str, 
                      update_values: Dict[str, Any],
                      where_condition: Optional[Dict[str, Any]] = None) -> int:
        """更新记录"""
        if table_name not in self.tables:
            return 0
        
        schema = self.tables[table_name]
        updated_count = 0
        
        # 遍历表的所有页面
        for page_id in self.table_pages.get(table_name, []):
            page = self.buffer_manager.get_page(page_id)
            if page:
                records = page.get_records()
                page_modified = False
                
                for i, record in enumerate(records):
                    if self._match_condition(record, where_condition):
                        # 更新记录
                        updated_record = record.copy()
                        updated_record.update(update_values)
                        
                        # 验证更新后的记录
                        is_valid, error_msg = schema.validate_record(updated_record)
                        if is_valid:
                            records[i] = updated_record
                            updated_count += 1
                            page_modified = True
                        else:
                            print(f"Update validation failed: {error_msg}")
                
                if page_modified:
                    # 重建页面数据
                    page.data = bytearray(page.data.__class__(b'\x00' * len(page.data)))
                    page.header.record_count = 0
                    page.header.free_space = len(page.data)
                    page.records.clear()
                    
                    for record in records:
                        page.add_record(record)
                    
                    self.buffer_manager.unpin_page(page_id, is_dirty=True)
                else:
                    self.buffer_manager.unpin_page(page_id)
        
        return updated_count
    
    def delete_records(self, table_name: str, 
                      where_condition: Optional[Dict[str, Any]] = None) -> int:
        """删除记录"""
        if table_name not in self.tables:
            return 0
        
        deleted_count = 0
        
        # 遍历表的所有页面
        for page_id in self.table_pages.get(table_name, []):
            page = self.buffer_manager.get_page(page_id)
            if page:
                records = page.get_records()
                new_records = []
                
                for record in records:
                    if not self._match_condition(record, where_condition):
                        new_records.append(record)
                    else:
                        deleted_count += 1
                
                # 如果有记录被删除，重建页面
                if len(new_records) != len(records):
                    page.data = bytearray(page.data.__class__(b'\x00' * len(page.data)))
                    page.header.record_count = 0
                    page.header.free_space = len(page.data)
                    page.records.clear()
                    
                    for record in new_records:
                        page.add_record(record)
                    
                    self.buffer_manager.unpin_page(page_id, is_dirty=True)
                else:
                    self.buffer_manager.unpin_page(page_id)
        
        return deleted_count
    
    def _apply_defaults(self, record: Dict[str, Any], schema: TableSchema) -> Dict[str, Any]:
        """应用默认值"""
        complete_record = record.copy()
        
        for column in schema.columns:
            if column.name not in complete_record:
                if column.default_value is not None:
                    complete_record[column.name] = column.default_value
                elif not column.nullable:
                    # 这里应该在验证阶段就被捕获，但为了安全起见
                    if column.column_type == ColumnType.INTEGER:
                        complete_record[column.name] = 0
                    elif column.column_type == ColumnType.STRING:
                        complete_record[column.name] = ""
                    elif column.column_type == ColumnType.BOOLEAN:
                        complete_record[column.name] = False
        
        return complete_record
    
    def _find_page_for_insert(self, table_name: str, record: Dict[str, Any]) -> Optional[int]:
        """找到可以插入记录的页面"""
        record_size = len(json.dumps(record, ensure_ascii=False).encode('utf-8')) + 4
        
        for page_id in self.table_pages.get(table_name, []):
            page = self.buffer_manager.get_page(page_id)
            if page and page.header.free_space >= record_size:
                self.buffer_manager.unpin_page(page_id)
                return page_id
            elif page:
                self.buffer_manager.unpin_page(page_id)
        
        return None
    
    def _match_condition(self, record: Dict[str, Any], condition: Optional[Dict[str, Any]]) -> bool:
        """检查记录是否匹配条件"""
        if condition is None:
            return True
        
        for field, expected_value in condition.items():
            if field not in record:
                return False
            
            record_value = record[field]
            
            # 简单的相等比较，实际实现中需要支持更多运算符
            if isinstance(expected_value, dict):
                # 支持运算符，如 {"age": {">": 18}}
                for op, value in expected_value.items():
                    if op == ">":
                        if not (record_value > value):
                            return False
                    elif op == ">=":
                        if not (record_value >= value):
                            return False
                    elif op == "<":
                        if not (record_value < value):
                            return False
                    elif op == "<=":
                        if not (record_value <= value):
                            return False
                    elif op == "=":
                        if not (record_value == value):
                            return False
                    elif op == "!=":
                        if not (record_value != value):
                            return False
            else:
                if record_value != expected_value:
                    return False
        
        return True
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取表信息"""
        if table_name not in self.tables:
            return None
        
        schema = self.tables[table_name]
        page_count = len(self.table_pages.get(table_name, []))
        
        # 统计记录数
        record_count = 0
        for page_id in self.table_pages.get(table_name, []):
            page = self.buffer_manager.get_page(page_id)
            if page:
                record_count += page.header.record_count
                self.buffer_manager.unpin_page(page_id)
        
        # 构建列信息列表
        columns_info = []
        for col in schema.columns:
            col_info = {
                'name': col.name,
                'type': col.column_type.value,
                'max_length': col.max_length,
                'nullable': col.nullable,
                'default_value': col.default_value,
                'primary_key': col.is_primary_key,
                'unique': col.is_unique
            }
            columns_info.append(col_info)
        
        return {
            'table_name': table_name,
            'columns': columns_info,  # 返回列详细信息列表
            'primary_key': schema.primary_key,
            'page_count': page_count,
            'record_count': record_count,
            'created_time': schema.created_time
        }
    
    def add_column(self, table_name: str, column: ColumnDefinition) -> bool:
        """为表添加列"""
        if table_name not in self.tables:
            return False
        
        schema = self.tables[table_name]
        schema.add_column(column)
        
        # 更新现有记录，为新列添加默认值
        for page_id in self.table_pages.get(table_name, []):
            page = self.buffer_manager.get_page(page_id)
            if page:
                records = page.get_records()
                page_modified = False
                
                for record in records:
                    # 为新列添加默认值
                    if column.name not in record:
                        if column.default_value is not None:
                            record[column.name] = column.default_value
                        elif not column.nullable:
                            # 添加非空列的默认值
                            if column.column_type == ColumnType.INTEGER:
                                record[column.name] = 0
                            elif column.column_type == ColumnType.STRING:
                                record[column.name] = ""
                            elif column.column_type == ColumnType.BOOLEAN:
                                record[column.name] = False
                            elif column.column_type == ColumnType.FLOAT:
                                record[column.name] = 0.0
                        else:
                            record[column.name] = None
                        page_modified = True
                
                if page_modified:
                    # 重建页面数据
                    page.data = bytearray(page.data.__class__(b'\x00' * len(page.data)))
                    page.header.record_count = 0
                    page.header.free_space = len(page.data)
                    page.records.clear()
                    
                    for record in records:
                        page.add_record(record)
                    
                    self.buffer_manager.unpin_page(page_id, is_dirty=True)
                else:
                    self.buffer_manager.unpin_page(page_id)
        
        self._save_schemas()
        return True
    
    def create_index(self, index_name: str, table_name: str, columns: List[str]) -> bool:
        """创建索引（简化实现，仅记录索引信息）"""
        # 在实际实现中，这里会创建索引结构
        # 目前我们只是记录索引信息到一个索引文件中
        
        # 检查表是否存在
        if table_name not in self.tables:
            return False
        
        # 检查列是否存在
        schema = self.tables[table_name]
        for col_name in columns:
            if not schema.get_column(col_name):
                return False
        
        # 创建索引信息（简化实现，实际应创建索引结构）
        import time
        index_info = {
            'index_name': index_name,
            'table_name': table_name,
            'columns': columns,
            'created_time': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # 保存索引信息到文件
        index_file = self._get_index_file_path()
        indexes = {}
        if os.path.exists(index_file):
            try:
                with open(index_file, 'r', encoding='utf-8') as f:
                    indexes = json.load(f)
            except:
                pass
        
        indexes[index_name] = index_info
        
        try:
            with open(index_file, 'w', encoding='utf-8') as f:
                json.dump(indexes, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving index info: {e}")
            return False
        
        return True
    
    def _get_index_file_path(self) -> str:
        """获取索引文件路径"""
        data_dir = self.buffer_manager.page_manager.data_dir
        return str(data_dir / "indexes.json")
    
    def list_tables(self) -> List[str]:
        """列出所有表"""
        return list(self.tables.keys())

def test_table_manager():
    """测试表管理器"""
    print("=" * 60)
    print("              表管理器测试")
    print("=" * 60)
    
    # 创建表管理器
    table_manager = TableManager()
    
    print("\n1. 创建表结构...")
    
    # 创建学生表
    students_schema = TableSchema("students")
    students_schema.add_column(ColumnDefinition("id", ColumnType.INTEGER, is_primary_key=True))
    students_schema.add_column(ColumnDefinition("name", ColumnType.STRING, max_length=50, nullable=False))
    students_schema.add_column(ColumnDefinition("age", ColumnType.INTEGER, nullable=False))
    students_schema.add_column(ColumnDefinition("grade", ColumnType.FLOAT))
    students_schema.add_column(ColumnDefinition("active", ColumnType.BOOLEAN, default_value=True))
    
    if table_manager.create_table(students_schema):
        print("  创建学生表成功")
    else:
        print("  创建学生表失败")
    
    print("\n2. 插入测试数据...")
    
    test_students = [
        {"id": 1, "name": "张三", "age": 20, "grade": 85.5},
        {"id": 2, "name": "李四", "age": 21, "grade": 92.0},
        {"id": 3, "name": "王五", "age": 19, "grade": 78.5},
        {"id": 4, "name": "赵六", "age": 22, "grade": 88.0},
        {"id": 5, "name": "钱七", "age": 20, "grade": 95.5}
    ]
    
    for student in test_students:
        if table_manager.insert_record("students", student):
            print(f"  插入学生: {student}")
        else:
            print(f"  插入失败: {student}")
    
    print("\n3. 查询记录...")
    
    # 查询所有记录
    all_students = table_manager.select_records("students")
    print(f"  所有学生 ({len(all_students)} 条记录):")
    for student in all_students:
        print(f"    {student}")
    
    # 条件查询
    high_grade_students = table_manager.select_records(
        "students", 
        where_condition={"grade": {">": 90}}
    )
    print(f"\n  成绩 > 90 的学生 ({len(high_grade_students)} 条记录):")
    for student in high_grade_students:
        print(f"    {student}")
    
    # 列投影
    names_and_grades = table_manager.select_records(
        "students",
        columns=["name", "grade"]
    )
    print(f"\n  姓名和成绩:")
    for record in names_and_grades:
        print(f"    {record}")
    
    print("\n4. 更新记录...")
    
    # 更新年龄为20的学生的成绩
    updated = table_manager.update_records(
        "students",
        update_values={"grade": 90.0},
        where_condition={"age": 20}
    )
    print(f"  更新了 {updated} 条记录")
    
    # 查看更新结果
    updated_students = table_manager.select_records(
        "students",
        where_condition={"age": 20}
    )
    print("  更新后的记录:")
    for student in updated_students:
        print(f"    {student}")
    
    print("\n5. 删除记录...")
    
    # 删除成绩低于80的学生
    deleted = table_manager.delete_records(
        "students",
        where_condition={"grade": {"<": 80}}
    )
    print(f"  删除了 {deleted} 条记录")
    
    # 查看剩余记录
    remaining_students = table_manager.select_records("students")
    print(f"  剩余学生 ({len(remaining_students)} 条记录):")
    for student in remaining_students:
        print(f"    {student}")
    
    print("\n6. 表信息统计...")
    
    table_info = table_manager.get_table_info("students")
    if table_info:
        print("  表信息:")
        for key, value in table_info.items():
            print(f"    {key}: {value}")
    
    # 缓存管理器状态
    table_manager.buffer_manager.print_cache_status()
    
    print("\n✅ 表管理器测试完成!")

if __name__ == "__main__":
    test_table_manager()