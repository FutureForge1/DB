"""
统一SQL处理器
集成所有最新功能的SQL处理接口，支持复杂查询、JOIN、聚合函数等
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from typing import List, Dict, Any, Tuple, Optional
from src.compiler.lexer.lexer import Lexer
from src.compiler.parser.unified_parser import UnifiedSQLParser
from src.compiler.semantic.ddl_dml_analyzer import DDLDMLSemanticAnalyzer
from src.compiler.codegen.translator import IntegratedCodeGenerator
from src.execution.execution_engine import ExecutionEngine
from src.storage.storage_engine import StorageEngine

class UnifiedSQLProcessor:
    """统一SQL处理器"""
    
    def __init__(self, storage_engine: Optional[StorageEngine] = None):
        """
        初始化统一SQL处理器
        
        Args:
            storage_engine: 存储引擎实例
        """
        self.storage_engine = storage_engine or StorageEngine()
        self.execution_engine = ExecutionEngine(self.storage_engine)
        
        # 复杂查询关键字检测
        self.complex_keywords = {
            'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL',
            'GROUP', 'ORDER', 'HAVING', 'ASC', 'DESC',
            'LIMIT', 'OFFSET'
        }
    
    def _is_complex_query(self, sql: str) -> bool:
        """检测是否为复杂查询"""
        sql_upper = sql.upper()
        
        # 检查聚合函数
        aggregate_functions = ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']
        if any(f" {func}(" in sql_upper or f"{func}(" in sql_upper for func in aggregate_functions):
            return True
            
        # 检查GROUP BY
        if " GROUP BY " in sql_upper:
            return True
            
        # 检查其他复杂查询关键字（使用更宽松的匹配）
        import re
        for keyword in self.complex_keywords:
            # 使用正则表达式匹配，允许前后是空格、换行符或字符串开始/结束
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, sql_upper):
                return True
        return False
    
    def process_sql(self, sql: str) -> Tuple[bool, List[Dict[str, Any]], str]:
        """
        处理SQL语句
        
        Args:
            sql: SQL查询语句
            
        Returns:
            (是否成功, 结果列表, 错误信息)
        """
        try:
            print(f"\n处理SQL语句: {sql}")
            
            # 1. 使用统一解析器进行词法和语法分析
            unified_parser = UnifiedSQLParser(sql)
            ast, sql_type = unified_parser.parse()
            
            if ast is None:
                return False, [], "语法分析失败"
            
            print(f"  → 检测到{sql_type}语句")
            
            # 2. 根据SQL类型选择语义分析器
            if sql_type == "SELECT":
                # SELECT查询根据复杂性选择分析器
                is_complex = self._is_complex_query(sql)
                print(f"  → 复杂查询检测: {is_complex}")
                if is_complex:
                    from src.compiler.parser.extended_parser import ExtendedParser
                    from src.compiler.semantic.extended_analyzer import ExtendedSemanticAnalyzer
                    # 对于复杂查询，重新进行词法和语法分析
                    lexer = Lexer(sql)
                    tokens = lexer.tokenize()
                    parser = ExtendedParser(tokens)
                    ast = parser.parse()
                    if ast is None:
                        return False, [], "复杂查询语法分析失败"
                    semantic_analyzer = ExtendedSemanticAnalyzer(self.storage_engine)
                else:
                    from src.compiler.semantic.analyzer import SemanticAnalyzer
                    # 修正：传入存储引擎实例
                    semantic_analyzer = SemanticAnalyzer(self.storage_engine)
                    
                quadruples = semantic_analyzer.analyze(ast)
                
            elif sql_type in ["DDL", "DML"]:
                # DDL/DML语句使用新的语义分析器
                semantic_analyzer = DDLDMLSemanticAnalyzer()
                quadruples = semantic_analyzer.analyze(ast)
                
                # 检查语义错误
                errors = semantic_analyzer.get_errors()
                if errors:
                    return False, [], f"语义分析失败: {'; '.join(errors)}"
                    
            else:
                return False, [], f"不支持的SQL类型: {sql_type}"
            
            if not quadruples:
                return False, [], "语义分析失败"
            
            # 3. 目标代码生成和执行
            if sql_type == "SELECT":
                # SELECT查询需要目标代码生成和执行
                translator = IntegratedCodeGenerator()
                target_instructions = translator.generate_target_code(quadruples)
                results = self.execution_engine.execute(target_instructions, translator)
                return True, results, ""
                
            else:
                # DDL/DML语句直接执行四元式
                results = self._execute_ddl_dml(quadruples, sql_type)
                return True, results, ""
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return False, [], str(e)

    def execute_sql_with_details(self, sql: str) -> Dict[str, Any]:
        """
        执行SQL并返回详细信息
        
        Args:
            sql: SQL查询语句
            
        Returns:
            详细的执行结果信息
        """
        result = {
            'sql': sql,
            'success': False,
            'results': [],
            'error': '',
            'tokens_count': 0,
            'quadruples_count': 0,
            'instructions_count': 0,
            'is_complex': False
        }
        
        try:
            # 检测复杂查询
            result['is_complex'] = self._is_complex_query(sql)
            
            # 词法分析
            lexer = Lexer(sql)
            tokens = lexer.tokenize()
            result['tokens_count'] = len(tokens)
            
            # 选择分析器
            if result['is_complex']:
                from src.compiler.parser.extended_parser import ExtendedParser
                from src.compiler.semantic.extended_analyzer import ExtendedSemanticAnalyzer
                parser = ExtendedParser(tokens)
                # 修正：传入存储引擎实例
                semantic_analyzer = ExtendedSemanticAnalyzer(self.storage_engine)
            else:
                from src.compiler.parser.parser import Parser
                from src.compiler.semantic.analyzer import SemanticAnalyzer
                parser = Parser(tokens)
                # 修正：传入存储引擎实例
                semantic_analyzer = SemanticAnalyzer(self.storage_engine)
            
            # 语法分析
            ast = parser.parse()
            if ast is None:
                result['error'] = "语法分析失败"
                return result
            
            # 语义分析
            quadruples = semantic_analyzer.analyze(ast)
            if not quadruples:
                result['error'] = "语义分析失败"
                return result
            
            result['quadruples_count'] = len(quadruples)
            
            # 目标代码生成
            from src.compiler.codegen.translator import IntegratedCodeGenerator
            translator = IntegratedCodeGenerator()
            target_instructions = translator.generate_target_code(quadruples)
            result['instructions_count'] = len(target_instructions)
            
            # 执行
            results = self.execution_engine.execute(target_instructions, translator)
            
            result['success'] = True
            result['results'] = results
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            result['error'] = str(e)
        
        return result
    
    def _execute_ddl_dml(self, quadruples: List, sql_type: str) -> List[Dict[str, Any]]:
        """
        执行DDL/DML四元式
        
        Args:
            quadruples: 四元式列表
            sql_type: SQL类型
            
        Returns:
            执行结果
        """
        results = []
        
        for quad in quadruples:
            try:
                if quad.op == "CREATE_TABLE":
                    result = self._execute_create_table(quad)
                elif quad.op == "DROP_TABLE":
                    result = self._execute_drop_table(quad)
                elif quad.op == "ALTER_TABLE_ADD":
                    result = self._execute_alter_table(quad)
                elif quad.op == "CREATE_INDEX":
                    result = self._execute_create_index(quad)
                elif quad.op == "INSERT":
                    result = self._execute_insert(quad)
                elif quad.op == "UPDATE":
                    result = self._execute_update(quad)
                elif quad.op == "DELETE":
                    result = self._execute_delete(quad)
                else:
                    result = {"error": f"Unsupported operation: {quad.op}"}
                    
                results.append(result)
                
            except Exception as e:
                results.append({"error": str(e)})
        
        return results
    
    def _execute_create_table(self, quad) -> Dict[str, Any]:
        """执行创建表操作"""
        try:
            table_name = quad.arg1
            # 解析列定义
            import ast
            columns_data = ast.literal_eval(quad.arg2)
            
            # 转换列定义格式以匹配存储引擎的要求
            columns = []
            for col_info in columns_data:
                # 转换数据类型
                col_type = col_info['type']
                if col_type.upper() == 'INT':
                    col_type = 'INTEGER'
                elif col_type.upper().startswith('VARCHAR'):
                    col_type = 'STRING'
                elif col_type.upper().startswith('DECIMAL'):
                    col_type = 'FLOAT'
                
                column_def = {
                    'name': col_info['name'],
                    'type': col_type
                }
                
                # 处理约束
                for constraint in col_info.get('constraints', []):
                    if constraint == 'PRIMARY_KEY':
                        column_def['primary_key'] = True
                    elif constraint == 'NOT_NULL':
                        column_def['nullable'] = False
                    elif constraint.startswith('DEFAULT='):
                        default_val = constraint.split('=', 1)[1]
                        # 尝试转换默认值类型
                        if default_val.isdigit():
                            column_def['default_value'] = int(default_val)
                        elif default_val.replace('.', '').isdigit():
                            column_def['default_value'] = float(default_val)
                        else:
                            # 移除引号
                            if default_val.startswith('"') and default_val.endswith('"'):
                                default_val = default_val[1:-1]
                            elif default_val.startswith("'") and default_val.endswith("'"):
                                default_val = default_val[1:-1]
                            column_def['default_value'] = default_val
                
                # 处理VARCHAR长度
                if col_info['type'].upper().startswith('VARCHAR'):
                    import re
                    match = re.search(r'VARCHAR\((\d+)\)', col_info['type'], re.IGNORECASE)
                    if match:
                        column_def['max_length'] = int(match.group(1))
                
                columns.append(column_def)
            
            # 调用存储引擎创建表
            success = self.storage_engine.create_table(table_name, columns)
            
            if success:
                return {"message": f"Table '{table_name}' created successfully"}
            else:
                return {"error": f"Failed to create table '{table_name}'"}
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"error": f"Error creating table: {str(e)}"}
    
    def _execute_drop_table(self, quad) -> Dict[str, Any]:
        """执行删除表操作"""
        try:
            table_name = quad.arg1
            # 调用存储引擎删除表
            success = self.storage_engine.drop_table(table_name)
            
            if success:
                return {"message": f"Table '{table_name}' dropped successfully"}
            else:
                return {"error": f"Failed to drop table '{table_name}'"}
        except Exception as e:
            return {"error": f"Error dropping table: {str(e)}"}
    
    def _execute_alter_table(self, quad) -> Dict[str, Any]:
        """执行修改表操作"""
        try:
            table_name = quad.arg1
            # 解析列定义
            import ast
            column_info = ast.literal_eval(quad.arg2)
            
            # 转换列定义格式
            col_type = column_info['type']
            if col_type.upper() == 'INT':
                col_type = 'INTEGER'
            elif col_type.upper().startswith('VARCHAR'):
                col_type = 'STRING'
            elif col_type.upper().startswith('DECIMAL'):
                col_type = 'FLOAT'
            
            column_def = {
                'name': column_info['name'],
                'type': col_type
            }
            
            # 处理约束
            for constraint in column_info.get('constraints', []):
                if constraint == 'PRIMARY_KEY':
                    column_def['primary_key'] = True
                elif constraint == 'NOT_NULL':
                    column_def['nullable'] = False
                elif constraint.startswith('DEFAULT='):
                    default_val = constraint.split('=', 1)[1]
                    # 尝试转换默认值类型
                    if default_val.isdigit():
                        column_def['default_value'] = int(default_val)
                    elif default_val.replace('.', '').isdigit():
                        column_def['default_value'] = float(default_val)
                    else:
                        # 移除引号
                        if default_val.startswith('"') and default_val.endswith('"'):
                            default_val = default_val[1:-1]
                        elif default_val.startswith("'") and default_val.endswith("'"):
                            default_val = default_val[1:-1]
                        column_def['default_value'] = default_val
            
            # 处理VARCHAR长度
            if column_info['type'].upper().startswith('VARCHAR'):
                import re
                match = re.search(r'VARCHAR\((\d+)\)', column_info['type'], re.IGNORECASE)
                if match:
                    column_def['max_length'] = int(match.group(1))
            
            # 调用存储引擎添加列
            success = self.storage_engine.add_column(table_name, column_def)
            
            if success:
                return {"message": f"Column added to table '{table_name}' successfully"}
            else:
                return {"error": f"Failed to add column to table '{table_name}'"}
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"error": f"Error altering table: {str(e)}"}
    
    def _execute_create_index(self, quad) -> Dict[str, Any]:
        """执行创建索引操作"""
        try:
            index_name = quad.arg1
            # 解析表名和列信息
            table_and_columns = quad.arg2
            # 格式: "table_name(column1,column2)"
            import re
            match = re.match(r'(.+?)\((.+)\)', table_and_columns)
            if match:
                table_name = match.group(1)
                columns = match.group(2).split(',')
                # 调用存储引擎创建索引
                success = self.storage_engine.create_index(index_name, table_name, columns)
                
                if success:
                    return {"message": f"Index '{index_name}' created successfully"}
                else:
                    return {"error": f"Failed to create index '{index_name}'"}
            else:
                return {"error": "Invalid index format"}
        except Exception as e:
            return {"error": f"Error creating index: {str(e)}"}
    
    def _execute_insert(self, quad) -> Dict[str, Any]:
        """执行插入操作"""
        try:
            table_name = quad.arg1
            # 解析列和值
            data_str = quad.arg2
            # 格式: "COLUMNS=ALL;VALUES=['1', 'Laptop', '999.99']" 或 "COLUMNS=['col1','col2'];VALUES=['val1','val2']"
            import re
            match = re.match(r'COLUMNS=(.+?);VALUES=(.+)', data_str)
            if match:
                columns_part = match.group(1)
                values_part = match.group(2)
                
                # 解析值
                import ast
                values = ast.literal_eval(values_part)
                
                # 构造记录字典
                record = {}
                
                # 如果指定了列名
                if columns_part != 'ALL' and columns_part != "'ALL'":
                    columns = ast.literal_eval(columns_part)
                    for i, (col, val) in enumerate(zip(columns, values)):
                        # 尝试转换数据类型
                        if isinstance(val, str):
                            if val.isdigit():
                                record[col] = int(val)
                            elif val.replace('.', '').replace('-', '').isdigit():
                                record[col] = float(val)
                            else:
                                # 移除引号
                                if val.startswith('"') and val.endswith('"'):
                                    val = val[1:-1]
                                elif val.startswith("'") and val.endswith("'"):
                                    val = val[1:-1]
                                record[col] = val
                        else:
                            record[col] = val
                else:
                    # 没有指定列名，需要从表结构获取列名
                    # 这种情况下我们暂时无法处理，因为不知道表的列顺序
                    # 简单处理：假设值按表定义的列顺序排列
                    # 这需要访问存储引擎获取表结构信息
                    try:
                        # 尝试获取表信息
                        table_info = self.storage_engine.get_table_info(table_name)
                        if table_info and 'columns' in table_info:
                            columns = [col['name'] for col in table_info['columns']]
                            for i, (col, val) in enumerate(zip(columns, values)):
                                # 尝试转换数据类型
                                if isinstance(val, str):
                                    if val.isdigit():
                                        record[col] = int(val)
                                    elif val.replace('.', '').replace('-', '').isdigit():
                                        record[col] = float(val)
                                    else:
                                        # 移除引号
                                        if val.startswith('"') and val.endswith('"'):
                                            val = val[1:-1]
                                        elif val.startswith("'") and val.endswith("'"):
                                            val = val[1:-1]
                                        record[col] = val
                                else:
                                    record[col] = val
                        else:
                            # 如果无法获取表信息，使用默认方式
                            for i, val in enumerate(values):
                                col_name = f'col_{i}'
                                # 尝试转换数据类型
                                if isinstance(val, str):
                                    if val.isdigit():
                                        record[col_name] = int(val)
                                    elif val.replace('.', '').replace('-', '').isdigit():
                                        record[col_name] = float(val)
                                    else:
                                        # 移除引号
                                        if val.startswith('"') and val.endswith('"'):
                                            val = val[1:-1]
                                        elif val.startswith("'") and val.endswith("'"):
                                            val = val[1:-1]
                                        record[col_name] = val
                                else:
                                    record[col_name] = val
                    except:
                        # 出错时使用默认方式
                        for i, val in enumerate(values):
                            col_name = f'col_{i}'
                            # 尝试转换数据类型
                            if isinstance(val, str):
                                if val.isdigit():
                                    record[col_name] = int(val)
                                elif val.replace('.', '').replace('-', '').isdigit():
                                    record[col_name] = float(val)
                                else:
                                    # 移除引号
                                    if val.startswith('"') and val.endswith('"'):
                                        val = val[1:-1]
                                    elif val.startswith("'") and val.endswith("'"):
                                        val = val[1:-1]
                                    record[col_name] = val
                            else:
                                record[col_name] = val
                
                # 调用存储引擎插入记录
                success = self.storage_engine.insert(table_name, record)
                
                if success:
                    return {"message": f"Record inserted successfully"}
                else:
                    return {"error": "Failed to insert record"}
            else:
                return {"error": "Invalid insert format"}
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"error": f"Error inserting record: {str(e)}"}
    
    def _execute_update(self, quad) -> Dict[str, Any]:
        """执行更新操作"""
        try:
            table_name = quad.arg1
            # 解析SET和WHERE子句
            data_str = quad.arg2
            # 格式: "SET=price=899.99;WHERE=id=1"
            import re
            match = re.match(r'SET=(.+?);WHERE=(.+)', data_str)
            if match:
                set_part = match.group(1)
                where_part = match.group(2)
                
                # 解析SET子句
                updates = {}
                # 处理多个赋值，用分号分隔
                assignments = set_part.split(';')
                for assignment in assignments:
                    if '=' in assignment:
                        col, val = assignment.split('=', 1)
                        col = col.strip()
                        val = val.strip()
                        # 尝试转换数据类型
                        if val.isdigit():
                            updates[col] = int(val)
                        elif val.replace('.', '').replace('-', '').isdigit():
                            updates[col] = float(val)
                        else:
                            # 移除引号
                            if val.startswith('"') and val.endswith('"'):
                                val = val[1:-1]
                            elif val.startswith("'") and val.endswith("'"):
                                val = val[1:-1]
                            updates[col] = val
                
                # 解析WHERE子句
                condition = None
                if where_part != 'ALL':
                    # 简单解析条件字符串，格式如 "id=1" 或 "price>1000"
                    # 支持多种比较操作符
                    match = re.match(r'(.+?)(>=|<=|>|<|<>|=)(.+)', where_part)
                    if match:
                        column = match.group(1).strip()
                        operator = match.group(2)
                        value = match.group(3).strip()
                        
                        # 尝试转换值的类型
                        if value.isdigit():
                            value = int(value)
                        elif value.replace('.', '').replace('-', '').isdigit():
                            value = float(value)
                        else:
                            # 移除引号
                            if value.startswith('"') and value.endswith('"'):
                                value = value[1:-1]
                            elif value.startswith("'") and value.endswith("'"):
                                value = value[1:-1]
                        
                        # 构造条件字典
                        condition = {column: {operator: value}}
                    else:
                        # 如果无法解析，直接传递原始字符串
                        condition = where_part
                else:
                    condition = None  # 无条件更新所有记录
                
                # 调用存储引擎更新记录
                count = self.storage_engine.update(table_name, updates, condition)
                
                return {"message": f"{count} record(s) updated successfully"}
            else:
                # 处理没有WHERE子句的情况
                match = re.match(r'SET=(.+)', data_str)
                if match:
                    set_part = match.group(1)
                    
                    # 解析SET子句
                    updates = {}
                    # 处理多个赋值，用分号分隔
                    assignments = set_part.split(';')
                    for assignment in assignments:
                        if '=' in assignment:
                            col, val = assignment.split('=', 1)
                            col = col.strip()
                            val = val.strip()
                            # 尝试转换数据类型
                            if val.isdigit():
                                updates[col] = int(val)
                            elif val.replace('.', '').replace('-', '').isdigit():
                                updates[col] = float(val)
                            else:
                                # 移除引号
                                if val.startswith('"') and val.endswith('"'):
                                    val = val[1:-1]
                                elif val.startswith("'") and val.endswith("'"):
                                    val = val[1:-1]
                                updates[col] = val
                    
                    # 无条件更新所有记录
                    count = self.storage_engine.update(table_name, updates, None)
                    
                    return {"message": f"{count} record(s) updated successfully"}
                else:
                    return {"error": "Invalid update format"}
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"error": f"Error updating records: {str(e)}"}
    
    def _execute_delete(self, quad) -> Dict[str, Any]:
        """执行删除操作"""
        try:
            table_name = quad.arg1
            condition_str = quad.arg2
            
            # 解析WHERE条件
            condition = None
            if condition_str and condition_str != "ALL":
                # 简单解析条件字符串，格式如 "id=1" 或 "price>1000"
                import re
                # 支持多种比较操作符
                match = re.match(r'(.+?)(>=|<=|>|<|<>|=)(.+)', condition_str)
                if match:
                    column = match.group(1).strip()
                    operator = match.group(2)
                    value = match.group(3).strip()
                    
                    # 尝试转换值的类型
                    if value.isdigit():
                        value = int(value)
                    elif value.replace('.', '').replace('-', '').isdigit():
                        value = float(value)
                    else:
                        # 移除引号
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        elif value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                    
                    # 构造条件字典
                    condition = {column: {operator: value}}
                else:
                    # 如果无法解析，直接传递原始字符串
                    condition = condition_str
            
            # 调用存储引擎删除记录
            count = self.storage_engine.delete(table_name, condition)
            
            return {"message": f"{count} record(s) deleted successfully"}
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"error": f"Error deleting records: {str(e)}"}

    def get_stats(self) -> Dict[str, Any]:
        """获取处理器统计信息"""
        return {
            'storage_stats': self.storage_engine.get_stats(),
            'execution_stats': self.execution_engine.get_stats()
        }

def test_unified_sql_processor():
    """测试统一SQL处理器"""
    print("=" * 80)
    print("              统一SQL处理器测试")
    print("=" * 80)
    
    # 创建存储引擎和测试数据
    storage = StorageEngine("test_unified_processor")
    
    # 创建测试表
    students_columns = [
        {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
        {'name': 'name', 'type': 'STRING', 'max_length': 50},
        {'name': 'age', 'type': 'INTEGER'},
        {'name': 'grade', 'type': 'FLOAT'}
    ]
    
    storage.create_table("students", students_columns)
    
    # 插入测试数据
    test_students = [
        {'id': 1, 'name': 'Alice', 'age': 20, 'grade': 85.5},
        {'id': 2, 'name': 'Bob', 'age': 22, 'grade': 92.0},
        {'id': 3, 'name': 'Charlie', 'age': 19, 'grade': 78.5},
        {'id': 4, 'name': 'Diana', 'age': 21, 'grade': 96.0},
        {'id': 5, 'name': 'Eve', 'age': 23, 'grade': 88.0}
    ]
    
    for student in test_students:
        storage.insert("students", student)
    
    print(f"准备了 {len(test_students)} 条测试数据")
    
    # 创建统一SQL处理器
    processor = UnifiedSQLProcessor(storage)
    
    # 测试查询列表 - 使用更简单的语法避免扩展解析器的问题
    test_queries = [
        # 基础查询
        "SELECT * FROM students;",
        "SELECT name FROM students;", 
        "SELECT name, age FROM students;",
        
        # 简单聚合查询
        "SELECT COUNT(*) FROM students;",
        "SELECT AVG(grade) FROM students;",
        
        # 复杂查询
        "SELECT name, grade FROM students ORDER BY grade DESC;",
        "SELECT age, COUNT(*) FROM students GROUP BY age;"
    ]
    
    print(f"\n开始测试 {len(test_queries)} 个SQL查询...")
    print("=" * 80)
    
    for i, sql in enumerate(test_queries, 1):
        print(f"\n查询 {i}: {sql}")
        print("-" * 60)
        
        # 使用详细执行方法
        result = processor.execute_sql_with_details(sql)
        
        print(f"  类型: {'复杂查询' if result['is_complex'] else '基础查询'}")
        print(f"  Token数: {result['tokens_count']}")
        print(f"  四元式数: {result['quadruples_count']}")
        print(f"  指令数: {result['instructions_count']}")
        
        if result['success']:
            print(f"  ✅ 执行成功，返回 {len(result['results'])} 条结果")
            # 显示前3条结果
            for j, record in enumerate(result['results'][:3], 1):
                print(f"    {j}. {record}")
            if len(result['results']) > 3:
                print(f"    ... 还有 {len(result['results']) - 3} 条记录")
        else:
            print(f"  ❌ 执行失败: {result['error']}")
    
    print("\n" + "=" * 80)
    print("处理器统计信息")
    print("=" * 80)
    
    stats = processor.get_stats()
    execution_stats = stats['execution_stats']
    print(f"执行指令数: {execution_stats['instructions_executed']}")
    print(f"扫描记录数: {execution_stats['records_scanned']}")
    print(f"输出记录数: {execution_stats['records_output']}")
    
    # 关闭存储引擎
    storage.shutdown()
    
    print("\n✅ 统一SQL处理器测试完成!")

if __name__ == "__main__":
    test_unified_sql_processor()