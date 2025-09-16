"""
DDL/DML语义分析器
处理CREATE TABLE、INSERT、UPDATE、DELETE等语句的语义分析和四元式生成
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from typing import List, Dict, Any, Optional
from src.common.types import ASTNode, ASTNodeType, Quadruple, SemanticError

class DDLDMLSemanticAnalyzer:
    """DDL/DML语义分析器"""
    
    def __init__(self, storage_engine=None):
        """初始化语义分析器"""
        self.quadruples = []
        self.temp_counter = 0
        self.errors = []
        self.storage_engine = storage_engine
    
    def analyze(self, ast: ASTNode) -> List[Quadruple]:
        """
        分析AST并生成四元式
        
        Args:
            ast: 抽象语法树根节点
            
        Returns:
            四元式列表
        """
        self.quadruples = []
        self.temp_counter = 0
        self.errors = []
        
        if not ast:
            return []
        
        try:
            if ast.value == "CREATE_TABLE":
                self._analyze_create_table(ast)
            elif ast.value == "DROP_TABLE":
                self._analyze_drop_table(ast)
            elif ast.value == "ALTER_TABLE":
                self._analyze_alter_table(ast)
            elif ast.value == "CREATE_INDEX":
                self._analyze_create_index(ast)
            elif ast.value == "DROP_INDEX":
                self._analyze_drop_index(ast)
            elif ast.value == "SHOW_INDEX":
                self._analyze_show_index(ast)
            elif ast.value == "BEGIN":
                self.quadruples.append(Quadruple(op="BEGIN", arg1=None, arg2=None, result=self._next_temp()))
            elif ast.value == "COMMIT":
                self.quadruples.append(Quadruple(op="COMMIT", arg1=None, arg2=None, result=self._next_temp()))
            elif ast.value == "ROLLBACK":
                self.quadruples.append(Quadruple(op="ROLLBACK", arg1=None, arg2=None, result=self._next_temp()))
            elif ast.value == "INSERT":
                self._analyze_insert(ast)
            elif ast.value == "UPDATE":
                self._analyze_update(ast)
            elif ast.value == "DELETE":
                self._analyze_delete(ast)
            else:
                raise SemanticError(
                    "操作错误",
                    f"不支持的操作: {ast.value}",
                    context="DDL/DML分析"
                )
                
        except SemanticError as e:
            # 格式化详细的语义错误信息
            detailed_error = e.format_message()
            self.errors.append(detailed_error)
        except Exception as e:
            self.errors.append(f"语义分析异常: {str(e)}")
            
        return self.quadruples
    
    def _next_temp(self) -> str:
        """生成下一个临时变量名"""
        self.temp_counter += 1
        return f"T{self.temp_counter}"
    
    def _analyze_create_table(self, ast: ASTNode):
        """分析CREATE TABLE语句"""
        # 获取表名
        table_name = ast.children[0].value  # TABLE_NAME节点
        
        # 检查表是否已经存在
        self._check_table_not_exists(table_name, "CREATE TABLE")
        
        # 获取列定义
        columns_node = ast.children[1]  # COLUMN_LIST节点
        columns = []
        
        for column_node in columns_node.children:
            column_info = self._extract_column_info(column_node)
            columns.append(column_info)
        
        # 验证列定义的有效性
        self._validate_column_definitions(columns, table_name)
        
        # 生成CREATE_TABLE四元式
        temp_result = self._next_temp()
        quad = Quadruple(
            op="CREATE_TABLE",
            arg1=table_name,
            arg2=str(columns),  # 将列信息序列化
            result=temp_result
        )
        self.quadruples.append(quad)
    
    def _extract_column_info(self, column_node: ASTNode) -> Dict[str, Any]:
        """提取列信息"""
        column_info = {
            'name': column_node.value,
            'type': '',
            'constraints': []
        }
        
        # 数据类型
        if column_node.children:
            data_type_node = column_node.children[0]
            column_info['type'] = data_type_node.value
            
            # 约束
            if len(column_node.children) > 1:
                constraints_node = column_node.children[1]
                for constraint_node in constraints_node.children:
                    if constraint_node.value == "PRIMARY_KEY":
                        column_info['constraints'].append('PRIMARY_KEY')
                    elif constraint_node.value == "NOT_NULL":
                        column_info['constraints'].append('NOT_NULL')
                    elif constraint_node.value == "UNIQUE":
                        column_info['constraints'].append('UNIQUE')
                    elif constraint_node.value == "DEFAULT":
                        default_value = constraint_node.children[0].value if constraint_node.children else None
                        column_info['constraints'].append(f'DEFAULT={default_value}')
        
        return column_info
    
    def _analyze_drop_table(self, ast: ASTNode):
        """分析DROP TABLE语句"""
        table_name = ast.children[0].value  # TABLE_NAME节点
        
        # 检查表是否存在
        self._check_table_exists(table_name, "DROP TABLE")
        
        temp_result = self._next_temp()
        quad = Quadruple(
            op="DROP_TABLE",
            arg1=table_name,
            arg2=None,
            result=temp_result
        )
        self.quadruples.append(quad)
    
    def _analyze_alter_table(self, ast: ASTNode):
        """分析ALTER TABLE语句"""
        table_name = ast.children[0].value  # TABLE_NAME节点
        operation = ast.children[1].value  # 操作类型
        
        if operation == "ADD_COLUMN":
            column_node = ast.children[2]
            column_info = self._extract_column_info(column_node)
            
            temp_result = self._next_temp()
            quad = Quadruple(
                op="ALTER_TABLE_ADD",
                arg1=table_name,
                arg2=str(column_info),
                result=temp_result
            )
            self.quadruples.append(quad)
            
        elif operation == "DROP_COLUMN":
            column_name = ast.children[2].value  # 列名节点
            
            temp_result = self._next_temp()
            quad = Quadruple(
                op="ALTER_TABLE_DROP",
                arg1=table_name,
                arg2=column_name,
                result=temp_result
            )
            self.quadruples.append(quad)
        else:
            self.errors.append(f"Unsupported ALTER TABLE operation: {operation}")
    
    def _analyze_create_index(self, ast: ASTNode):
        """分析CREATE INDEX语句"""
        index_name = ast.children[0].value  # 索引名
        table_name = ast.children[1].value  # 表名
        columns_node = ast.children[2]  # 列列表
        
        columns = [child.value for child in columns_node.children]
        
        temp_result = self._next_temp()
        quad = Quadruple(
            op="CREATE_INDEX",
            arg1=index_name,
            arg2=f"{table_name}({','.join(columns)})",
            result=temp_result
        )
        self.quadruples.append(quad)
    
    def _analyze_drop_index(self, ast: ASTNode):
        """分析DROP INDEX语句"""
        index_name = ast.children[0].value  # 索引名
        
        # 如果有表名信息
        table_name = None
        if len(ast.children) > 1:
            table_name = ast.children[1].value
        
        temp_result = self._next_temp()
        quad = Quadruple(
            op="DROP_INDEX",
            arg1=index_name,
            arg2=table_name if table_name else "-",
            result=temp_result
        )
        self.quadruples.append(quad)
    
    def _analyze_show_index(self, ast: ASTNode):
        """分析SHOW INDEX语句"""
        table_name = ast.children[0].value  # 表名
        
        temp_result = self._next_temp()
        quad = Quadruple(
            op="SHOW_INDEX",
            arg1=table_name,
            arg2="-",
            result=temp_result
        )
        self.quadruples.append(quad)
    
    def _analyze_insert(self, ast: ASTNode):
        """分析INSERT语句"""
        table_name = ast.children[0].value  # TABLE_NAME节点
        
        # 查找列列表和值列表
        columns = None
        values_list = None
        
        for child in ast.children:
            if child.type == ASTNodeType.COLUMN_LIST:
                columns = [col.value for col in child.children]
            elif child.value == "VALUES_LIST":
                values_list = child
        
        if not values_list:
            raise SemanticError(
                "语法错误",
                "INSERT语句缺少VALUES子句",
                context="INSERT语句分析"
            )
        
        # 处理每一行值
        for row_node in values_list.children:
            if row_node.value == "VALUE_ROW":
                values = [val.value for val in row_node.children]
                
                temp_result = self._next_temp()
                quad = Quadruple(
                    op="INSERT",
                    arg1=table_name,
                    arg2=f"COLUMNS={columns or 'ALL'};VALUES={values}",
                    result=temp_result
                )
                self.quadruples.append(quad)
    
    def _analyze_update(self, ast: ASTNode):
        """分析UPDATE语句"""
        table_name = ast.children[0].value  # TABLE_NAME节点
        
        # 查找SET子句和WHERE子句
        set_clause = None
        where_clause = None
        
        for child in ast.children:
            if child.value == "SET_CLAUSE":
                set_clause = child
            elif child.type == ASTNodeType.WHERE_CLAUSE:
                where_clause = child
        
        if not set_clause:
            raise SemanticError(
                "语法错误",
                "UPDATE语句缺少SET子句",
                context="UPDATE语句分析"
            )
        
        # 处理SET赋值
        assignments = []
        for assignment_node in set_clause.children:
            if assignment_node.value == "ASSIGNMENT":
                column = assignment_node.children[0].value
                value = assignment_node.children[1].value
                assignments.append(f"{column}={value}")
        
        # 处理WHERE条件
        condition = None
        if where_clause:
            condition_node = where_clause.children[0]
            column = condition_node.children[0].value
            operator = condition_node.value
            value = condition_node.children[1].value
            condition = f"{column}{operator}{value}"
        
        temp_result = self._next_temp()
        quad = Quadruple(
            op="UPDATE",
            arg1=table_name,
            arg2=f"SET={';'.join(assignments)};WHERE={condition or 'ALL'}",
            result=temp_result
        )
        self.quadruples.append(quad)
    
    def _analyze_delete(self, ast: ASTNode):
        """分析DELETE语句"""
        table_name = ast.children[0].value  # TABLE_NAME节点
        
        # 查找WHERE子句
        where_clause = None
        for child in ast.children:
            if child.type == ASTNodeType.WHERE_CLAUSE:
                where_clause = child
                break
        
        # 处理WHERE条件
        condition = None
        if where_clause:
            condition_node = where_clause.children[0]
            column = condition_node.children[0].value
            operator = condition_node.value
            value = condition_node.children[1].value
            condition = f"{column}{operator}{value}"
        
        temp_result = self._next_temp()
        quad = Quadruple(
            op="DELETE",
            arg1=table_name,
            arg2=condition or "ALL",
            result=temp_result
        )
        self.quadruples.append(quad)
    
    def get_errors(self) -> List[str]:
        """获取语义错误列表"""
        return self.errors
    
    def _check_table_not_exists(self, table_name: str, operation: str):
        """检查表不存在（用于CREATE TABLE等操作）"""
        # 基本验证
        if not table_name or not table_name.strip():
            raise SemanticError(
                "表名错误",
                f"{operation} 操作中表名不能为空",
                context=f"DDL语义检查 - {operation}"
            )
        
        # 检查表名格式
        import re
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
            raise SemanticError(
                "表名格式错误",
                f"表名 '{table_name}' 格式无效，表名必须以字母或下划线开头，只能包含字母、数字和下划线",
                context=f"DDL语义检查 - {operation}"
            )
        
        # 如果有存储引擎，检查表是否已存在
        if self.storage_engine:
            try:
                existing_tables = self.storage_engine.list_tables()
                if table_name in existing_tables:
                    raise SemanticError(
                        "表已存在错误",
                        f"表 '{table_name}' 已经存在，无法重复创建",
                        context=f"DDL语义检查 - {operation} - 现有表: {', '.join(existing_tables)}"
                    )
            except SemanticError:
                raise  # 重新抛出语义错误
            except Exception as e:
                # 存储引擎访问失败时，记录警告但不阻止执行
                print(f"  警告: 无法验证表存在性 - {e}")
    
    def _check_table_exists(self, table_name: str, operation: str):
        """检查表存在（用于DROP TABLE等操作）"""
        # 基本验证
        if not table_name or not table_name.strip():
            raise SemanticError(
                "表名错误",
                f"{operation} 操作中表名不能为空",
                context=f"DDL语义检查 - {operation}"
            )
        
        # 如果有存储引擎，检查表是否存在
        if self.storage_engine:
            try:
                existing_tables = self.storage_engine.list_tables()
                if table_name not in existing_tables:
                    raise SemanticError(
                        "表不存在错误",
                        f"表 '{table_name}' 不存在，无法执行 {operation} 操作",
                        context=f"DDL语义检查 - {operation} - 现有表: {', '.join(existing_tables)}"
                    )
            except SemanticError:
                raise  # 重新抛出语义错误
            except Exception as e:
                # 存储引擎访问失败时，记录警告但不阻止执行
                print(f"  警告: 无法验证表存在性 - {e}")
    
    def _validate_column_definitions(self, columns: List[Dict], table_name: str):
        """验证列定义的有效性"""
        if not columns:
            raise SemanticError(
                "列定义错误",
                f"表 '{table_name}' 必须至少包含一个列定义",
                context="CREATE TABLE语义检查"
            )
        
        # 检查重复列名
        column_names = [col['name'] for col in columns]
        duplicate_names = set([name for name in column_names if column_names.count(name) > 1])
        if duplicate_names:
            raise SemanticError(
                "列定义错误",
                f"表 '{table_name}' 中存在重复的列名: {', '.join(duplicate_names)}",
                context="CREATE TABLE语义检查"
            )
        
        # 验证每个列定义
        for col in columns:
            col_name = col.get('name', '')
            col_type = col.get('type', '')
            
            if not col_name or not col_name.strip():
                raise SemanticError(
                    "列定义错误",
                    f"表 '{table_name}' 中存在空的列名",
                    context="CREATE TABLE语义检查"
                )
            
            if not col_type or not col_type.strip():
                raise SemanticError(
                    "列定义错误", 
                    f"表 '{table_name}' 中列 '{col_name}' 缺少数据类型",
                    context="CREATE TABLE语义检查"
                )


def test_ddl_dml_analyzer():
    """测试DDL/DML语义分析器"""
    from src.compiler.parser.unified_parser import UnifiedSQLParser
    
    print("=" * 80)
    print("              DDL/DML语义分析器测试")
    print("=" * 80)
    
    test_cases = [
        # DDL
        "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL);",
        "DROP TABLE products;",
        "ALTER TABLE products ADD COLUMN price DECIMAL(10,2);",
        
        # DML
        "INSERT INTO products VALUES (1, 'Laptop', 999.99);",
        "UPDATE products SET price = 899.99 WHERE id = 1;",
        "DELETE FROM products WHERE price > 1000;",
    ]
    
    for i, sql in enumerate(test_cases, 1):
        print(f"\\n测试用例 {i}: {sql}")
        print("-" * 60)
        
        try:
            # 语法分析
            parser = UnifiedSQLParser(sql)
            ast, sql_type = parser.parse()
            
            if ast and sql_type in ["DDL", "DML"]:
                # 语义分析
                analyzer = DDLDMLSemanticAnalyzer()
                quadruples = analyzer.analyze(ast)
                
                print(f"✅ 语义分析成功")
                print(f"   SQL类型: {sql_type}")
                print(f"   生成四元式: {len(quadruples)}个")
                
                for j, quad in enumerate(quadruples, 1):
                    print(f"   {j}: {quad}")
                
                # 检查错误
                errors = analyzer.get_errors()
                if errors:
                    print(f"   ⚠️ 发现错误: {'; '.join(errors)}")
            else:
                print(f"❌ 语法分析失败或不支持的SQL类型")
                
        except Exception as e:
            print(f"❌ 分析失败: {e}")


if __name__ == "__main__":
    test_ddl_dml_analyzer()