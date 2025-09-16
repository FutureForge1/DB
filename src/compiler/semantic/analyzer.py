"""
语义分析器实现
进行语义检查和符号表管理，生成四元式中间代码
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from typing import List, Optional, Dict, Any
from src.common.types import (
    Token, TokenType, ASTNode, ASTNodeType, 
    SemanticError, SymbolTable, Symbol, Quadruple
)

class SemanticAnalyzer:
    """语义分析器"""
    
    def __init__(self, storage_engine=None):
        """初始化语义分析器"""
        self.symbol_table = SymbolTable()
        self.quadruples = []  # 四元式中间代码列表
        self.temp_counter = 0  # 临时变量计数器
        self.current_scope = "global"  # 当前作用域
        self.storage_engine = storage_engine  # 存储引擎实例
        self.current_table_name = None  # 存储当前分析的表名，用于WHERE子句验证
        
        # 预定义的表结构（模拟数据库schema）
        self.table_schemas = {
            "students": {
                "columns": ["id", "name", "age", "grade", "major"],
                "types": {"id": "int", "name": "string", "age": "int", "grade": "float", "major": "string"}
            },
            "courses": {
                "columns": ["course_id", "student_id", "course_name", "score"],
                "types": {"course_id": "int", "student_id": "int", "course_name": "string", "score": "float"}
            },
            "teachers": {
                "columns": ["id", "name", "department", "salary"],
                "types": {"id": "int", "name": "string", "department": "string", "salary": "float"}
            },
            "users": {
                "columns": ["id", "username", "email", "status"],
                "types": {"id": "int", "username": "string", "email": "string", "status": "string"}
            },
            "products": {
                "columns": ["id", "name", "price", "category", "status"],
                "types": {"id": "int", "name": "string", "price": "float", "category": "string", "status": "string"}
            },
            "people": {
                "columns": ["id", "name", "age", "grade"],
                "types": {"id": "int", "name": "string", "age": "int", "grade": "float"}
            }
        }
    
    def generate_temp_var(self) -> str:
        """生成临时变量名"""
        self.temp_counter += 1
        return f"T{self.temp_counter}"
    
    def add_symbol(self, name: str, symbol_type: str, line: int = 0, column: int = 0):
        """添加符号到符号表"""
        symbol = Symbol(name, symbol_type, self.current_scope, line, column)
        self.symbol_table.add_symbol(symbol)
    
    def lookup_symbol(self, name: str) -> Optional[Symbol]:
        """查找符号"""
        return self.symbol_table.lookup(name)
    
    def emit_quadruple(self, op: str, arg1: Optional[str], arg2: Optional[str], result: str):
        """生成四元式"""
        quad = Quadruple(op, arg1, arg2, result)
        self.quadruples.append(quad)
        return quad
    
    def analyze(self, ast: ASTNode) -> bool:
        """
        对抽象语法树进行语义分析
        
        Args:
            ast: 抽象语法树根节点
            
        Returns:
            生成的四元式列表
        """
        try:
            print("\n开始语义分析:")
            print("-" * 60)
            
            # 清空之前的分析结果
            self.quadruples = []
            self.temp_counter = 0
            
            # 分析AST
            result_temp = self._analyze_node(ast)
            
            # 添加OUTPUT四元式
            self.emit_quadruple("OUTPUT", result_temp, None, "RESULT")
            
            print("✅ 语义分析完成!")
            return self.quadruples
            
        except SemanticError as e:
            # 格式化详细的语义错误信息
            detailed_error = e.format_message()
            print(f"❌ 语义错误: {detailed_error}")
            # 重新抛出异常，保留详细错误信息
            raise SemanticError(e.error_type, detailed_error, e.line, e.column, e.context)
        except Exception as e:
            print(f"❌ 语义分析失败: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _analyze_node(self, node: ASTNode) -> str:
        """
        分析AST节点，返回节点的值或临时变量名
        
        Args:
            node: 要分析的AST节点
            
        Returns:
            节点值或临时变量名
        """
        if node.type == ASTNodeType.SELECT_STMT:
            return self._analyze_select_statement(node)
        elif node.type == ASTNodeType.COLUMN_LIST:
            return self._analyze_column_list(node)
        elif node.type == ASTNodeType.TABLE_NAME:
            return self._analyze_table_name(node)
        elif node.type == ASTNodeType.WHERE_CLAUSE:
            return self._analyze_where_clause(node)
        elif node.type == ASTNodeType.CONDITION:
            return self._analyze_condition(node)
        elif node.type == ASTNodeType.IDENTIFIER:
            return self._analyze_identifier(node)
        elif node.type == ASTNodeType.LITERAL:
            return self._analyze_literal(node)
        else:
            raise SemanticError(
                "类型错误",
                f"未知的AST节点类型: {node.type}",
                context="AST节点分析"
            )
    
    def _analyze_select_statement(self, node: ASTNode) -> str:
        """分析SELECT语句"""
        print("分析SELECT语句...")
        
        # 分析各个子节点
        column_list_result = None
        table_name_result = None
        where_result = None
        
        for child in node.children:
            if child.type == ASTNodeType.COLUMN_LIST:
                column_list_result = self._analyze_node(child)
            elif child.type == ASTNodeType.TABLE_NAME:
                table_name_result = self._analyze_node(child)
            elif child.type == ASTNodeType.WHERE_CLAUSE:
                where_result = self._analyze_node(child)
        
        # 在获得表名和列列表后，验证列是否存在
        if column_list_result and table_name_result:
            self._validate_columns_in_table(column_list_result, table_name_result)
        
        # 生成SELECT的四元式
        select_temp = self.generate_temp_var()
        self.emit_quadruple("SELECT", column_list_result, table_name_result, select_temp)
        
        # 确定当前数据源
        current_temp = select_temp
        
        # 如果有WHERE条件，生成FILTER四元式
        if where_result:
            filter_temp = self.generate_temp_var()
            self.emit_quadruple("FILTER", current_temp, where_result, filter_temp)
            current_temp = filter_temp
        
        # 生成PROJECT四元式进行列投影（除非是SELECT *）
        if column_list_result and column_list_result != "*":
            project_temp = self.generate_temp_var()
            self.emit_quadruple("PROJECT", current_temp, column_list_result, project_temp)
            current_temp = project_temp
        
        return current_temp
    
    def _analyze_column_list(self, node: ASTNode) -> str:
        """分析列列表"""
        print("分析列列表...")
        
        columns = []
        for child in node.children:
            if child.type == ASTNodeType.IDENTIFIER:
                col_name = child.value
                if col_name == "*":
                    columns.append("*")
                else:
                    # 先收集列名，稍后在知道表名后进行验证
                    columns.append(col_name)
        
        # 将列列表转换为字符串
        columns_str = ",".join(columns)
        
        # 添加到符号表
        self.add_symbol(f"columns_{self.temp_counter}", "column_list")
        
        return columns_str
    
    def _analyze_table_name(self, node: ASTNode) -> str:
        """分析表名"""
        print(f"分析表名: {node.value}")
        
        table_name = node.value
        
        # 检查表是否存在 - 修正：使用存储引擎动态验证
        table_exists = False
        if self.storage_engine:
            # 使用存储引擎检查表是否存在
            try:
                tables = self.storage_engine.list_tables()
                table_exists = table_name in tables
                print(f"  从存储引擎查找表: {table_name} -> {table_exists}")
                print(f"  存储引擎中的表: {tables}")
            except Exception as e:
                print(f"  存储引擎查询失败: {e}")
                # 如果无法访问存储引擎，回退到静态检查
                table_exists = table_name in self.table_schemas
        else:
            # 没有存储引擎时，使用静态检查
            table_exists = table_name in self.table_schemas
        
        if not table_exists:
            raise SemanticError(
                "表不存在错误",
                f"表 '{table_name}' 不存在",
                context="表名验证"
            )
        
        # 添加到符号表
        self.add_symbol(table_name, "table")
        
        # 存储当前表名，用于后续的列验证
        self.current_table_name = table_name
        
        print(f"  表 '{table_name}' 验证成功")
        if self.storage_engine:
            try:
                tables = self.storage_engine.list_tables()
                print(f"  数据库中的表: {tables}")
            except:
                if table_name in self.table_schemas:
                    print(f"  可用列: {', '.join(self.table_schemas[table_name]['columns'])}")
        elif table_name in self.table_schemas:
            print(f"  可用列: {', '.join(self.table_schemas[table_name]['columns'])}")
        
        return table_name
    
    def _validate_columns_in_table(self, columns_str: str, table_name: str):
        """验证列是否存在于指定表中"""
        if not columns_str or columns_str == "*":
            return  # *通配符或空列表不需要验证
        
        if not self.storage_engine:
            return  # 没有存储引擎时跳过验证
        
        try:
            # 获取表信息
            table_info = self.storage_engine.get_table_info(table_name)
            if not table_info or 'columns' not in table_info:
                return  # 无法获取表信息时跳过验证
            
            # 获取表中的所有列名
            available_columns = [col['name'] for col in table_info['columns']]
            
            # 检查每个请求的列
            requested_columns = [col.strip() for col in columns_str.split(',')]
            for col_name in requested_columns:
                if col_name != "*" and col_name not in available_columns:
                    raise SemanticError(
                        "列不存在错误",
                        f"表 '{table_name}' 中不存在列 '{col_name}'",
                        context=f"列验证 - 可用列: {', '.join(available_columns)}"
                    )
                    
        except SemanticError:
            raise  # 重新抛出语义错误
        except Exception as e:
            # 其他异常时跳过验证，不影响主流程
            print(f"  列验证跳过: {e}")
            return
    
    def _validate_column_in_condition(self, column_name: str, node: ASTNode):
        """验证WHERE条件中的列名是否存在于当前表中"""
        # 验证标识符或字面值类型的节点（列名可能被解析为任一类型）
        if node.type not in [ASTNodeType.IDENTIFIER, ASTNodeType.LITERAL]:
            return
            
        # 如果没有当前表名或存储引擎，跳过验证
        if not self.current_table_name or not self.storage_engine:
            return
        
        # 如果是字符串字面值（带引号），不验证（这是真正的字符串常量）
        if (isinstance(column_name, str) and 
            ((column_name.startswith("'") and column_name.endswith("'")) or
             (column_name.startswith('"') and column_name.endswith('"')))):
            return
            
        try:
            # 获取表信息
            table_info = self.storage_engine.get_table_info(self.current_table_name)
            if not table_info or 'columns' not in table_info:
                return  # 无法获取表信息时跳过验证
            
            # 获取表中的所有列名
            available_columns = [col['name'] for col in table_info['columns']]
            
            # 检查列名是否存在
            if column_name not in available_columns:
                raise SemanticError(
                    "列不存在错误",
                    f"WHERE子句中的列 '{column_name}' 在表 '{self.current_table_name}' 中不存在",
                    context=f"WHERE子句验证 - 可用列: {', '.join(available_columns)}"
                )
                
        except SemanticError:
            raise  # 重新抛出语义错误
        except Exception as e:
            # 其他异常时跳过验证，不影响主流程
            print(f"  WHERE子句列验证跳过: {e}")
            return
    
    def _analyze_where_clause(self, node: ASTNode) -> str:
        """分析WHERE子句"""
        print("分析WHERE子句...")
        
        if not node.children:
            return ""
        
        # 分析条件
        condition_result = None
        for child in node.children:
            if child.type == ASTNodeType.CONDITION:
                condition_result = self._analyze_node(child)
        
        return condition_result
    
    def _analyze_condition(self, node: ASTNode) -> str:
        """分析条件表达式"""
        print("分析条件表达式...")
        
        if len(node.children) >= 3:
            # 三元条件: operand1 operator operand2
            left_operand = self._analyze_node(node.children[0])
            
            # 验证左操作数（通常是列名）是否存在
            self._validate_column_in_condition(left_operand, node.children[0])
            
            # 获取操作符，直接从节点值获取
            if hasattr(node.children[1], 'value'):
                operator = node.children[1].value
            else:
                # 如果不是直接值，递归分析
                operator = self._analyze_node(node.children[1])
            
            right_operand = self._analyze_node(node.children[2])
            
            # 根据运算符生成相应的四元式
            temp_var = self.generate_temp_var()
            
            # 映射运算符
            op_mapping = {
                '>': 'GT',
                '>=': 'GE', 
                '<': 'LT',
                '<=': 'LE',
                '=': 'EQ',
                '!=': 'NE',
                '<>': 'NE'
            }
            
            final_op = op_mapping.get(operator, 'GT')  # 默认为GT
            self.emit_quadruple(final_op, left_operand, right_operand, temp_var)
            
            print(f"  生成条件: {left_operand} {final_op} {right_operand} -> {temp_var}")
            
            return temp_var
        elif len(node.children) >= 2:
            # 简单的二元条件
            left_operand = self._analyze_node(node.children[0])
            right_operand = self._analyze_node(node.children[1])
            
            # 生成比较操作的四元式
            temp_var = self.generate_temp_var()
            self.emit_quadruple("GT", left_operand, right_operand, temp_var)  # 默认为大于
            
            print(f"  生成条件: {left_operand} GT {right_operand} -> {temp_var}")
            
            return temp_var
        
        return ""
    
    def _analyze_identifier(self, node: ASTNode) -> str:
        """分析标识符"""
        identifier = node.value
        print(f"分析标识符: {identifier}")
        
        # 添加到符号表（如果不是特殊符号*）
        if identifier != "*":
            self.add_symbol(identifier, "identifier")
        
        return identifier
    
    def _analyze_literal(self, node: ASTNode) -> str:
        """分析字面值"""
        literal = node.value
        print(f"分析字面值: {literal}")
        
        # 根据字面值类型进行处理
        if literal.isdigit() or (literal.count('.') == 1 and literal.replace('.', '').isdigit()):
            # 数字字面值
            self.add_symbol(f"literal_{literal}", "number")
        else:
            # 字符串字面值
            self.add_symbol(f"literal_{literal}", "string")
        
        return literal
    
    def print_symbol_table(self):
        """打印符号表"""
        print("\n符号表:")
        print("-" * 50)
        print(self.symbol_table)
    
    def print_quadruples(self):
        """打印四元式中间代码"""
        print("\n四元式中间代码:")
        print("-" * 50)
        if not self.quadruples:
            print("无四元式生成")
            return
        
        for i, quad in enumerate(self.quadruples, 1):
            print(f"{i:2d}: {quad}")
    
    def get_intermediate_code(self) -> List[Quadruple]:
        """获取生成的中间代码"""
        return self.quadruples.copy()
    
    def validate_column_references(self, table_name: str, column_name: str) -> bool:
        """验证列引用是否有效"""
        if table_name not in self.table_schemas:
            return False
        
        if column_name == "*":
            return True
        
        return column_name in self.table_schemas[table_name]["columns"]
    
    def get_column_type(self, table_name: str, column_name: str) -> Optional[str]:
        """获取列的数据类型"""
        if table_name not in self.table_schemas:
            return None
        
        return self.table_schemas[table_name]["types"].get(column_name)


def test_semantic_analyzer():
    """测试语义分析器"""
    from src.compiler.lexer.lexer import Lexer
    from src.compiler.parser.parser import Parser
    
    test_cases = [
        "SELECT name FROM students WHERE age > 18;",
        "SELECT * FROM users;",
        "SELECT name, age FROM people WHERE grade >= 90;",
        "SELECT id FROM products WHERE price <= 100;"
    ]
    
    print("=" * 80)
    print("              语义分析器测试")
    print("=" * 80)
    
    for i, sql in enumerate(test_cases, 1):
        print(f"\n\n测试用例 {i}: {sql}")
        print("=" * 80)
        
        try:
            # 词法分析
            lexer = Lexer(sql)
            tokens = lexer.tokenize()
            
            # 语法分析
            parser = Parser(tokens)
            ast = parser.parse()
            
            if ast:
                print("AST构建成功!")
                
                # 语义分析
                analyzer = SemanticAnalyzer()
                success = analyzer.analyze(ast)
                
                if success:
                    analyzer.print_symbol_table()
                    analyzer.print_quadruples()
                
        except Exception as e:
            print(f"❌ 错误: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    test_semantic_analyzer()