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
                translator = IntegratedCodeGenerator()
                    
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
    
    def _execute_create_table(self, quad: Any) -> Dict[str, Any]:
        """
        执行CREATE TABLE操作
        
        Args:
            quad: CREATE_TABLE四元式
            
        Returns:
            执行结果
        """
        try:
            table_name = quad.arg1
            # 解析列定义
            import ast
            columns = ast.literal_eval(quad.arg2)
            
            # 调用存储引擎创建表
            success = self.storage_engine.create_table(table_name, columns)
            
            if success:
                return {"message": f"Table '{table_name}' created successfully"}
            else:
                return {"error": f"Failed to create table '{table_name}'"}
        except Exception as e:
            return {"error": f"Error creating table: {str(e)}"}
    
    def _execute_drop_table(self, quad: Any) -> Dict[str, Any]:
        """
        执行DROP TABLE操作
        
        Args:
            quad: DROP_TABLE四元式
            
        Returns:
            执行结果
        """
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
    
    def _execute_alter_table(self, quad: Any) -> Dict[str, Any]:
        """
        执行ALTER TABLE操作
        
        Args:
            quad: ALTER_TABLE_ADD四元式
            
        Returns:
            执行结果
        """
        try:
            table_name = quad.arg1
            # 解析列定义
            import ast
            column_info = ast.literal_eval(quad.arg2)
            
            # 调用存储引擎添加列
            success = self.storage_engine.add_column(table_name, column_info)
            
            if success:
                return {"message": f"Column added to table '{table_name}' successfully"}
            else:
                return {"error": f"Failed to add column to table '{table_name}'"}
        except Exception as e:
            return {"error": f"Error altering table: {str(e)}"}
    
    def _execute_create_index(self, quad: Any) -> Dict[str, Any]:
        """
        执行CREATE INDEX操作
        
        Args:
            quad: CREATE_INDEX四元式
            
        Returns:
            执行结果
        """
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
                success = self.storage_engine.create_index(table_name, index_name, columns)
                
                if success:
                    return {"message": f"Index '{index_name}' created successfully"}
                else:
                    return {"error": f"Failed to create index '{index_name}'"}
            else:
                return {"error": "Invalid index format"}
        except Exception as e:
            return {"error": f"Error creating index: {str(e)}"}
    
    def _execute_insert(self, quad: Any) -> Dict[str, Any]:
        """
        执行INSERT操作
        
        Args:
            quad: INSERT四元式
            
        Returns:
            执行结果
        """
        try:
            table_name = quad.arg1
            # 解析列和值
            data_str = quad.arg2
            # 格式: "COLUMNS=[col1,col2];VALUES=[val1,val2]"
            import re
            match = re.match(r'COLUMNS=(.+?);VALUES=(.+)', data_str)
            if match:
                columns_part = match.group(1)
                values_part = match.group(2)
                
                # 解析值
                import ast
                if columns_part == 'ALL':
                    columns = None
                else:
                    columns = ast.literal_eval(columns_part)
                
                values = ast.literal_eval(values_part)
                
                # 构造记录字典
                if columns:
                    record = dict(zip(columns, values))
                else:
                    record = {}
                    for i, value in enumerate(values):
                        # 尝试转换数据类型
                        if isinstance(value, str) and value.isdigit():
                            record[f'col_{i}'] = int(value)
                        elif isinstance(value, str) and value.replace('.', '').isdigit():
                            record[f'col_{i}'] = float(value)
                        else:
                            record[f'col_{i}'] = value
                
                # 调用存储引擎插入记录
                record_id = self.storage_engine.insert(table_name, record)
                
                if record_id:
                    return {"message": f"Record inserted successfully with ID: {record_id}"}
                else:
                    return {"error": "Failed to insert record"}
            else:
                return {"error": "Invalid insert format"}
        except Exception as e:
            return {"error": f"Error inserting record: {str(e)}"}
    
    def _execute_update(self, quad: Any) -> Dict[str, Any]:
        """
        执行UPDATE操作
        
        Args:
            quad: UPDATE四元式
            
        Returns:
            执行结果
        """
        try:
            table_name = quad.arg1
            # 解析SET和WHERE子句
            data_str = quad.arg2
            # 格式: "SET=col1=val1;col2=val2;WHERE=condition"
            import re
            match = re.match(r'SET=(.+?);WHERE=(.+)', data_str)
            if match:
                set_part = match.group(1)
                where_part = match.group(2)
                
                # 解析SET子句
                updates = {}
                for assignment in set_part.split(';'):
                    if '=' in assignment:
                        col, val = assignment.split('=', 1)
                        # 尝试转换数据类型
                        if val.isdigit():
                            updates[col] = int(val)
                        elif val.replace('.', '').isdigit():
                            updates[col] = float(val)
                        else:
                            updates[col] = val
                
                # 解析WHERE子句
                condition = None
                if where_part != 'ALL':
                    condition = where_part
                
                # 调用存储引擎更新记录
                count = self.storage_engine.update(table_name, updates, condition)
                
                return {"message": f"{count} record(s) updated successfully"}
            else:
                return {"error": "Invalid update format"}
        except Exception as e:
            return {"error": f"Error updating records: {str(e)}"}
    
    def _execute_delete(self, quad: Any) -> Dict[str, Any]:
        """
        执行DELETE操作
        
        Args:
            quad: DELETE四元式
            
        Returns:
            执行结果
        """
        try:
            table_name = quad.arg1
            condition = quad.arg2
            
            # 调用存储引擎删除记录
            count = self.storage_engine.delete(table_name, condition)
            
            return {"message": f"{count} record(s) deleted successfully"}
        except Exception as e:
            return {"error": f"Error deleting records: {str(e)}"}
    
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
            from src.compiler.codegen.translator import IntegratedCodeGenerator
            translator = IntegratedCodeGenerator()
           

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