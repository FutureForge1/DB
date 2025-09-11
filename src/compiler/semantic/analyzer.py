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
    
    def __init__(self):
        """初始化语义分析器"""
        self.symbol_table = SymbolTable()
        self.quadruples = []  # 四元式中间代码列表
        self.temp_counter = 0  # 临时变量计数器
        self.current_scope = "global"  # 当前作用域
        
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
            print(f"❌ 语义错误: {e}")
            return []
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
            raise SemanticError(f"Unknown AST node type: {node.type}")
    
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
        
        # 生成SELECT的四元式
        select_temp = self.generate_temp_var()
        self.emit_quadruple("SELECT", column_list_result, table_name_result, select_temp)
        
        # 如果有WHERE条件，生成FILTER四元式
        if where_result:
            filter_temp = self.generate_temp_var()
            self.emit_quadruple("FILTER", select_temp, where_result, filter_temp)
            return filter_temp
        
        return select_temp
    
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
                    # 验证列名（这里简化处理，实际应该在知道表名后验证）
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
        
        # 检查表是否存在
        if table_name not in self.table_schemas:
            raise SemanticError(f"Table '{table_name}' does not exist")
        
        # 添加到符号表
        self.add_symbol(table_name, "table")
        
        print(f"  表 '{table_name}' 验证成功")
        print(f"  可用列: {', '.join(self.table_schemas[table_name]['columns'])}")
        
        return table_name
    
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