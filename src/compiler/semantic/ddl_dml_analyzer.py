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
    
    def __init__(self):
        """初始化语义分析器"""
        self.quadruples = []
        self.temp_counter = 0
        self.errors = []
    
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
            elif ast.value == "INSERT":
                self._analyze_insert(ast)
            elif ast.value == "UPDATE":
                self._analyze_update(ast)
            elif ast.value == "DELETE":
                self._analyze_delete(ast)
            else:
                raise SemanticError(f"Unsupported operation: {ast.value}")
                
        except Exception as e:
            self.errors.append(str(e))
            
        return self.quadruples
    
    def _next_temp(self) -> str:
        """生成下一个临时变量名"""
        self.temp_counter += 1
        return f"T{self.temp_counter}"
    
    def _analyze_create_table(self, ast: ASTNode):
        """分析CREATE TABLE语句"""
        # 获取表名
        table_name = ast.children[0].value  # TABLE_NAME节点
        
        # 获取列定义
        columns_node = ast.children[1]  # COLUMN_LIST节点
        columns = []
        
        for column_node in columns_node.children:
            column_info = self._extract_column_info(column_node)
            columns.append(column_info)
        
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
            raise SemanticError("Missing VALUES clause in INSERT statement")
        
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
            raise SemanticError("Missing SET clause in UPDATE statement")
        
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