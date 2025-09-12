"""
统一SQL处理器
集成基础和扩展的SQL语法分析器，提供统一的SQL处理接口
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from typing import List, Dict, Any, Tuple, Optional
from src.compiler.lexer.lexer import Lexer
from src.compiler.parser.unified_parser import UnifiedSQLParser
from src.compiler.semantic.ddl_dml_analyzer import DDLDMLSemanticAnalyzer
from src.compiler.codegen.translator import QuadrupleTranslator
from src.compiler.codegen.translator import IntegratedCodeGenerator
from src.execution.execution_engine import ExecutionEngine
from src.storage.storage_engine import StorageEngine

class SQLProcessor:
    """统一SQL处理器"""
    
    def __init__(self, storage_engine: Optional[StorageEngine] = None):
        """
        初始化SQL处理器
        
        Args:
            storage_engine: 存储引擎实例
        """
        self.storage_engine = storage_engine or StorageEngine()
        self.execution_engine = ExecutionEngine(self.storage_engine)
        
        # 复杂查询关键字检测
        self.complex_keywords = {
            'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL',
            'COUNT', 'SUM', 'AVG', 'MAX', 'MIN',
            'GROUP', 'ORDER', 'HAVING', 'ASC', 'DESC',
            'LIMIT', 'OFFSET'
        }
    
    def _is_complex_query_from_tokens(self, tokens) -> bool:
        """
        检测是否为复杂查询
        
        Args:
            tokens: Token列表
            
        Returns:
            是否为复杂查询
        """
        # 检查WHERE子句
        has_where = False
        for token in tokens:
            if token.value.upper() == 'WHERE':
                has_where = True
                break
        
        # 如果有WHERE子句，检查是否为简单比较条件
        if has_where:
            # 简单启发式：WHERE column operator value 形式的条件认为是简单查询
            # 这里我们先尝试用基础分析器处理
            return False  # 暂时将WHERE查询视为可以由基础分析器处理
        
        # 检查其他复杂查询关键字
        for token in tokens:
            if token.value.upper() in self.complex_keywords:
                return True
        return False
    
    def _is_complex_query(self, sql: str) -> bool:
        """检测是否为复杂查询"""
        sql_upper = sql.upper()
        for keyword in self.complex_keywords:
            if keyword in sql_upper:
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
            # 检测是否为复杂查询
            is_complex = self._is_complex_query(sql)
            
            # 1. 使用统一解析器进行词法和语法分析
            unified_parser = UnifiedSQLParser(sql)
            ast, sql_type = unified_parser.parse()
            
            if ast is None:
                return False, [], "语法分析失败"
            
            print(f"  → 检测到{sql_type}语句")
            
            # 2. 根据SQL类型和复杂性选择语义分析器
            if sql_type == "SELECT":
                # SELECT查询根据复杂性选择分析器
                if is_complex:
                    from src.compiler.semantic.extended_analyzer import ExtendedSemanticAnalyzer
                    semantic_analyzer = ExtendedSemanticAnalyzer()
                else:
                    from src.compiler.semantic.analyzer import SemanticAnalyzer
                    semantic_analyzer = SemanticAnalyzer()
                    
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
                # 根据复杂性选择代码生成器
                if is_complex:
                    translator = IntegratedCodeGenerator()
                else:
                    translator = QuadrupleTranslator()
                    
                target_instructions = translator.generate_target_code(quadruples)
                results = self.execution_engine.execute(target_instructions)
                return True, results, ""
                
            else:
                # DDL/DML语句直接执行四元式
                results = self._execute_ddl_dml(quadruples, sql_type)
                return True, results, ""
            
        except Exception as e:
            return False, [], str(e)
    
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
        """执行CREATE TABLE操作"""
        table_name = quad.arg1
        
        try:
            # 解析列信息
            import ast
            columns_info = ast.literal_eval(quad.arg2)
            
            # 转换为存储引擎需要的格式
            columns = []
            for col_info in columns_info:
                column = {
                    'name': col_info['name'],
                    'type': self._map_data_type(col_info['type']),
                    'primary_key': 'PRIMARY_KEY' in col_info.get('constraints', []),
                    'not_null': 'NOT_NULL' in col_info.get('constraints', []),
                    'unique': 'UNIQUE' in col_info.get('constraints', [])
                }
                
                # 处理VARCHAR类型的长度
                if col_info['type'].upper().startswith('VARCHAR'):
                    # 提取长度信息
                    import re
                    match = re.search(r'VARCHAR\((\d+)\)', col_info['type'].upper())
                    if match:
                        column['max_length'] = int(match.group(1))
                
                # 处理DECIMAL类型的精度
                elif col_info['type'].upper().startswith('DECIMAL'):
                    # 提取精度和标度信息
                    import re
                    match = re.search(r'DECIMAL\((\d+),(\d+)\)', col_info['type'].upper())
                    if match:
                        column['max_length'] = int(match.group(1))  # 使用max_length存储精度
                        # 标度信息暂时忽略
                
                columns.append(column)
            
            # 创建表
            self.storage_engine.create_table(table_name, columns)
            
            return {
                "operation": "CREATE_TABLE",
                "table_name": table_name,
                "columns_count": len(columns),
                "status": "success",
                "message": f"数据表 '{table_name}' 创建成功"
            }
            
        except Exception as e:
            return {
                "operation": "CREATE_TABLE",
                "table_name": table_name,
                "status": "error",
                "message": str(e)
            }
    
    def _execute_drop_table(self, quad) -> Dict[str, Any]:
        """执行DROP TABLE操作"""
        table_name = quad.arg1
        
        try:
            self.storage_engine.drop_table(table_name)
            return {
                "operation": "DROP_TABLE",
                "table_name": table_name,
                "status": "success",
                "message": f"数据表 '{table_name}' 删除成功"
            }
        except Exception as e:
            return {
                "operation": "DROP_TABLE",
                "table_name": table_name,
                "status": "error",
                "message": str(e)
            }
    
    def _execute_alter_table(self, quad) -> Dict[str, Any]:
        """执行ALTER TABLE操作"""
        table_name = quad.arg1
        
        try:
            # 解析列信息
            import ast
            column_info = ast.literal_eval(quad.arg2)
            
            # 转换为存储引擎需要的格式
            column_def = {
                'name': column_info['name'],
                'type': self._map_data_type(column_info['type']),
                'nullable': not column_info.get('not_null', False),
                'primary_key': 'PRIMARY_KEY' in column_info.get('constraints', []),
                'unique': 'UNIQUE' in column_info.get('constraints', [])
            }
            
            # 处理VARCHAR类型的长度
            if column_info['type'].upper().startswith('VARCHAR'):
                # 提取长度信息
                import re
                match = re.search(r'VARCHAR\((\d+)\)', column_info['type'].upper())
                if match:
                    column_def['max_length'] = int(match.group(1))
            
            # 处理DECIMAL类型的精度
            elif column_info['type'].upper().startswith('DECIMAL'):
                # 提取精度和标度信息
                import re
                match = re.search(r'DECIMAL\((\d+),(\d+)\)', column_info['type'].upper())
                if match:
                    column_def['max_length'] = int(match.group(1))  # 使用max_length存储精度
            
            # 添加列
            success = self.storage_engine.add_column(table_name, column_def)
            
            if success:
                return {
                    "operation": "ALTER_TABLE",
                    "table_name": table_name,
                    "column_name": column_info['name'],
                    "status": "success",
                    "message": f"列 '{column_info['name']}' 添加到数据表 '{table_name}' 成功"
                }
            else:
                return {
                    "operation": "ALTER_TABLE",
                    "table_name": table_name,
                    "column_name": column_info['name'],
                    "status": "error",
                    "message": f"添加列 '{column_info['name']}' 到数据表 '{table_name}' 失败"
                }
                
        except Exception as e:
            return {
                "operation": "ALTER_TABLE",
                "table_name": table_name,
                "status": "error",
                "message": str(e)
            }
    
    def _execute_create_index(self, quad) -> Dict[str, Any]:
        """执行CREATE INDEX操作"""
        index_name = quad.arg1
        
        try:
            # 解析表名和列信息
            import re
            match = re.match(r'(.+?)\((.+)\)', quad.arg2)
            if not match:
                raise ValueError("索引格式无效")
            
            table_name = match.group(1)
            columns_str = match.group(2)
            columns = [col.strip() for col in columns_str.split(',')]
            
            # 创建索引
            success = self.storage_engine.create_index(index_name, table_name, columns)
            
            if success:
                return {
                    "operation": "CREATE_INDEX",
                    "index_name": index_name,
                    "table_name": table_name,
                    "columns": columns,
                    "status": "success",
                    "message": f"索引 '{index_name}' 在数据表 '{table_name}' 上创建成功"
                }
            else:
                return {
                    "operation": "CREATE_INDEX",
                    "index_name": index_name,
                    "table_name": table_name,
                    "columns": columns,
                    "status": "error",
                    "message": f"在数据表 '{table_name}' 上创建索引 '{index_name}' 失败"
                }
                
        except Exception as e:
            return {
                "operation": "CREATE_INDEX",
                "index_name": index_name,
                "status": "error",
                "message": str(e)
            }
    
    def _execute_insert(self, quad) -> Dict[str, Any]:
        """执行INSERT操作"""
        table_name = quad.arg1
        
        try:
            # 解析参数
            params = quad.arg2.split(';')
            columns_str = params[0].split('=')[1] if '=' in params[0] else 'ALL'
            values_str = params[1].split('=')[1] if len(params) > 1 and '=' in params[1] else '[]'
            
            # 解析值列表
            import ast
            values = ast.literal_eval(values_str)
            
            # 构建记录字典
            columns = []
            if columns_str == 'ALL':
                # 获取表的所有列
                try:
                    table_info = self.storage_engine.get_table_info(table_name)
                    if table_info and 'columns' in table_info and isinstance(table_info['columns'], list):
                        columns = [col['name'] for col in table_info['columns']]
                except Exception as e:
                    print(f"Warning: Failed to get table info: {e}")
                    # 如果无法获取表信息，使用默认列名
                    columns = []
            else:
                try:
                    columns = ast.literal_eval(columns_str)
                except Exception as e:
                    print(f"Warning: Failed to parse columns: {e}")
                    columns = []
            
            # 构建记录
            record = {}
            for i, value in enumerate(values):
                if i < len(columns):
                    # 类型转换
                    if value == 'NULL':
                        record[columns[i]] = None
                    elif isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit():
                        record[columns[i]] = float(value) if '.' in value else int(value)
                    else:
                        record[columns[i]] = value
            
            # 插入记录
            self.storage_engine.insert(table_name, record)
            
            return {
                "operation": "INSERT",
                "table_name": table_name,
                "affected_rows": 1,
                "status": "success",
                "message": f"1 行数据插入到 '{table_name}' 表中"
            }
            
        except Exception as e:
            return {
                "operation": "INSERT",
                "table_name": table_name,
                "status": "error",
                "message": str(e)
            }
    
    def _execute_update(self, quad) -> Dict[str, Any]:
        """执行UPDATE操作"""
        table_name = quad.arg1
        
        try:
            # 解析参数
            params = quad.arg2.split(';')
            set_clause = None
            where_clause = None
            
            for param in params:
                if param.startswith('SET='):
                    set_clause = param[4:]
                elif param.startswith('WHERE='):
                    where_clause = param[6:]
            
            if not set_clause:
                raise ValueError("UPDATE语句缺少SET子句")
            
            # 解析SET子句
            assignments = {}
            for assignment in set_clause.split(';'):
                if '=' in assignment:
                    key, value = assignment.split('=', 1)
                    # 类型转换
                    if value == 'NULL':
                        assignments[key] = None
                    elif isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit():
                        assignments[key] = float(value) if '.' in value else int(value)
                    else:
                        assignments[key] = value.strip("'").strip('"')
            
            # 解析WHERE子句
            condition = None
            if where_clause and where_clause != "ALL":
                # 解析WHERE条件 (例如: "id=1")
                if any(op in where_clause for op in ['=', '!=', '<', '>', '<=', '>=']):
                    # 找到操作符
                    operators = ['<=', '>=', '!=', '<=', '>=', '=', '<', '>']
                    operator = None
                    for op in operators:
                        if op in where_clause:
                            operator = op
                            break
                    
                    if operator:
                        key, value = where_clause.split(operator, 1)
                        key = key.strip()
                        value = value.strip()
                        
                        # 类型转换
                        if isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit():
                            condition = {key: {operator: float(value) if '.' in value else int(value)}}
                        else:
                            condition = {key: {operator: value.strip("'").strip('"')}}
            
            # 更新记录
            updated_count = self.storage_engine.update(table_name, assignments, condition)
            
            return {
                "operation": "UPDATE",
                "table_name": table_name,
                "affected_rows": updated_count,
                "status": "success",
                "message": f"{updated_count} 行数据在 '{table_name}' 表中更新"
            }
            
        except Exception as e:
            return {
                "operation": "UPDATE",
                "table_name": table_name,
                "status": "error",
                "message": str(e)
            }
    
    def _execute_delete(self, quad) -> Dict[str, Any]:
        """执行DELETE操作"""
        table_name = quad.arg1
        
        try:
            # 解析参数
            where_clause = quad.arg2  # DELETE操作的WHERE子句直接在arg2中
            
            # 解析WHERE子句
            condition = None
            if where_clause and where_clause != "ALL":
                # 解析WHERE条件 (例如: "id=1" 或 "price<50")
                if any(op in where_clause for op in ['=', '!=', '<', '>', '<=', '>=']):
                    # 找到操作符
                    operators = ['<=', '>=', '!=', '<=', '>=', '=', '<', '>']
                    operator = None
                    for op in operators:
                        if op in where_clause:
                            operator = op
                            break
                    
                    if operator:
                        key, value = where_clause.split(operator, 1)
                        key = key.strip()
                        value = value.strip()
                        
                        # 类型转换
                        if isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit():
                            condition = {key: {operator: float(value) if '.' in value else int(value)}}
                        else:
                            condition = {key: {operator: value.strip("'").strip('"')}}
            
            # 删除记录
            deleted_count = self.storage_engine.delete(table_name, condition)
            
            return {
                "operation": "DELETE",
                "table_name": table_name,
                "affected_rows": deleted_count,
                "status": "success",
                "message": f"{deleted_count} 行数据从 '{table_name}' 表中删除"
            }
            
        except Exception as e:
            return {
                "operation": "DELETE",
                "table_name": table_name,
                "status": "error",
                "message": str(e)
            }
    
    def _map_data_type(self, type_str: str) -> str:
        """映射数据类型"""
        type_upper = type_str.upper()
        
        if type_upper in ['INT', 'INTEGER']:
            return 'INTEGER'
        elif type_upper.startswith('VARCHAR'):
            return 'STRING'
        elif type_upper.startswith('CHAR'):
            return 'STRING'
        elif type_upper.startswith('DECIMAL'):
            return 'FLOAT'  # 使用FLOAT替代DECIMAL
        elif type_upper in ['FLOAT', 'DOUBLE']:
            return 'FLOAT'
        elif type_upper == 'TEXT':
            return 'STRING'
        else:
            return 'STRING'  # 默认类型
    
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
            # 词法分析
            lexer = Lexer(sql)
            tokens = lexer.tokenize()
            result['tokens_count'] = len(tokens)
            
            # 检测复杂查询
            result['is_complex'] = self._is_complex_query(sql)
            
            # 选择分析器
            if result['is_complex']:
                from src.compiler.parser.extended_parser import ExtendedParser
                from src.compiler.semantic.extended_analyzer import ExtendedSemanticAnalyzer
                parser = ExtendedParser(tokens)
                semantic_analyzer = ExtendedSemanticAnalyzer()
            else:
                from src.compiler.parser.parser import Parser
                from src.compiler.semantic.analyzer import SemanticAnalyzer
                parser = Parser(tokens)
                semantic_analyzer = SemanticAnalyzer()
            
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
            if result['is_complex']:
                from src.compiler.codegen.translator import IntegratedCodeGenerator
                translator = IntegratedCodeGenerator()
            else:
                from src.compiler.codegen.translator import QuadrupleTranslator
                translator = QuadrupleTranslator()
                
            target_instructions = translator.generate_target_code(quadruples)
            result['instructions_count'] = len(target_instructions)
            
            # 执行
            results = self.execution_engine.execute(target_instructions)
            
            result['success'] = True
            result['results'] = results
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def get_stats(self) -> Dict[str, Any]:
        """获取处理器统计信息"""
        return {
            'storage_stats': self.storage_engine.get_stats(),
            'execution_stats': self.execution_engine.get_stats()
        }

def test_sql_processor():
    """测试SQL处理器"""
    print("=" * 80)
    print("              SQL处理器测试")
    print("=" * 80)
    
    # 创建存储引擎和测试数据
    storage = StorageEngine("test_sql_processor")
    
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
    
    # 创建SQL处理器
    processor = SQLProcessor(storage)
    
    # 测试查询列表
    test_queries = [
        # 基础查询
        "SELECT * FROM students;",
        "SELECT name FROM students;", 
        "SELECT name, age FROM students;",
        
        # 复杂查询
        "SELECT COUNT(*) FROM students;",
        "SELECT AVG(grade) FROM students;",
        "SELECT name, grade FROM students ORDER BY grade DESC;",
        "SELECT major, COUNT(*) FROM students GROUP BY major;"
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
    
    print("\n✅ SQL处理器测试完成!")

if __name__ == "__main__":
    test_sql_processor()