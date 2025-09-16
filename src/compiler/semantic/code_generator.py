"""
中间代码生成器
负责生成优化的四元式中间代码
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from typing import List, Dict, Optional
from src.common.types import ASTNode, ASTNodeType, Quadruple, Token, TokenType

class IntermediateCodeGenerator:
    """中间代码生成器"""
    
    def __init__(self):
        """初始化代码生成器"""
        self.quadruples = []
        self.temp_counter = 0
        self.label_counter = 0
        
        # 运算符映射
        self.operator_map = {
            ">": "GT",    # Greater Than
            ">=": "GE",   # Greater Equal
            "<": "LT",    # Less Than
            "<=": "LE",   # Less Equal
            "=": "EQ",    # Equal
            "<>": "NE",   # Not Equal
            "AND": "AND", # Logical AND
            "OR": "OR"    # Logical OR
        }
    
    def generate_temp_var(self) -> str:
        """生成临时变量"""
        self.temp_counter += 1
        return f"T{self.temp_counter}"
    
    def generate_label(self) -> str:
        """生成标签"""
        self.label_counter += 1
        return f"L{self.label_counter}"
    
    def emit(self, op: str, arg1: Optional[str], arg2: Optional[str], result: str) -> Quadruple:
        """生成四元式"""
        quad = Quadruple(op, arg1, arg2, result)
        self.quadruples.append(quad)
        return quad
    
    def generate_select_code(self, columns: str, table: str, condition_var: Optional[str] = None) -> str:
        """生成SELECT操作的中间代码"""
        # 生成基本SELECT操作
        result_var = self.generate_temp_var()
        self.emit("SELECT", columns, table, result_var)
        
        # 如果有WHERE条件，生成FILTER操作
        if condition_var:
            filtered_var = self.generate_temp_var()
            self.emit("FILTER", result_var, condition_var, filtered_var)
            return filtered_var
        
        return result_var
    
    def generate_comparison_code(self, left: str, operator: str, right: str) -> str:
        """生成比较操作的中间代码"""
        result_var = self.generate_temp_var()
        op = self.operator_map.get(operator, "CMP")
        self.emit(op, left, right, result_var)
        return result_var
    
    def generate_logical_code(self, left: str, operator: str, right: str) -> str:
        """生成逻辑操作的中间代码"""
        result_var = self.generate_temp_var()
        op = self.operator_map.get(operator, "AND")
        self.emit(op, left, right, result_var)
        return result_var
    
    def generate_projection_code(self, source: str, columns: List[str]) -> str:
        """生成投影操作的中间代码"""
        result_var = self.generate_temp_var()
        columns_str = ",".join(columns)
        self.emit("PROJECT", source, columns_str, result_var)
        return result_var
    
    def optimize_quadruples(self) -> List[Quadruple]:
        """简单的四元式优化"""
        # 这里可以实现一些基本的优化，比如：
        # 1. 常量折叠
        # 2. 死代码消除
        # 3. 公共子表达式消除
        
        optimized = []
        for quad in self.quadruples:
            # 简单示例：跳过无用的赋值
            if not (quad.op == "ASSIGN" and quad.arg1 == quad.result):
                optimized.append(quad)
        
        return optimized
    
    def get_quadruples(self) -> List[Quadruple]:
        """获取生成的四元式列表"""
        return self.quadruples.copy()
    
    def print_code(self):
        """打印生成的中间代码"""
        print("\n生成的中间代码:")
        print("-" * 50)
        if not self.quadruples:
            print("无中间代码生成")
            return
        
        for i, quad in enumerate(self.quadruples, 1):
            print(f"{i:3d}: {quad}")
    
    def clear(self):
        """清空生成器状态"""
        self.quadruples = []
        self.temp_counter = 0
        self.label_counter = 0


class EnhancedSemanticAnalyzer:
    """增强的语义分析器，集成改进的代码生成"""
    
    def __init__(self):
        """初始化增强的语义分析器"""
        self.code_gen = IntermediateCodeGenerator()
        self.operators_stack = []  # 运算符栈，用于处理复杂表达式
        
        # 预定义表结构
        self.table_schemas = {
            "students": {
                "columns": ["id", "name", "age", "grade"],
                "types": {"id": "int", "name": "string", "age": "int", "grade": "float"}
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
    
    def analyze_with_tokens(self, ast: ASTNode, tokens: List[Token]) -> bool:
        """
        使用Token信息进行增强的语义分析
        
        Args:
            ast: 抽象语法树
            tokens: 原始Token列表
            
        Returns:
            分析是否成功
        """
        try:
            print("\n开始增强语义分析:")
            print("-" * 60)
            
            self.code_gen.clear()
            self.tokens = tokens
            self.token_pos = 0
            
            # 分析AST并生成代码
            result = self._analyze_node_enhanced(ast)
            
            # 生成最终输出操作
            if result:
                self.code_gen.emit("OUTPUT", result, None, "RESULT")
            
            print("增强语义分析完成!")
            return True
            
        except Exception as e:
            print(f"增强语义分析失败: {e}")
            return False
    
    def _analyze_node_enhanced(self, node: ASTNode) -> Optional[str]:
        """增强的AST节点分析"""
        if node.type == ASTNodeType.SELECT_STMT:
            return self._analyze_select_enhanced(node)
        elif node.type == ASTNodeType.COLUMN_LIST:
            return self._extract_columns(node)
        elif node.type == ASTNodeType.TABLE_NAME:
            return node.value
        elif node.type == ASTNodeType.WHERE_CLAUSE:
            return self._analyze_where_enhanced(node)
        elif node.type == ASTNodeType.CONDITION:
            return self._analyze_condition_enhanced(node)
        elif node.type == ASTNodeType.IDENTIFIER:
            return node.value
        elif node.type == ASTNodeType.LITERAL:
            return node.value
        
        return None
    
    def _analyze_select_enhanced(self, node: ASTNode) -> str:
        """增强的SELECT分析"""
        columns = "*"
        table = ""
        condition_var = None
        
        for child in node.children:
            if child.type == ASTNodeType.COLUMN_LIST:
                columns = self._extract_columns(child)
            elif child.type == ASTNodeType.TABLE_NAME:
                table = child.value
                # 验证表存在
                if table not in self.table_schemas:
                    raise Exception(f"Table '{table}' does not exist")
                print(f"  验证表 '{table}' - 成功")
            elif child.type == ASTNodeType.WHERE_CLAUSE:
                condition_var = self._analyze_where_enhanced(child)
        
        # 生成SELECT代码
        return self.code_gen.generate_select_code(columns, table, condition_var)
    
    def _extract_columns(self, node: ASTNode) -> str:
        """提取列名列表"""
        columns = []
        for child in node.children:
            if child.type == ASTNodeType.IDENTIFIER:
                columns.append(child.value)
        return ",".join(columns) if columns else "*"
    
    def _analyze_where_enhanced(self, node: ASTNode) -> Optional[str]:
        """增强的WHERE子句分析"""
        if not node.children:
            return None
        
        for child in node.children:
            if child.type == ASTNodeType.CONDITION:
                return self._analyze_condition_enhanced(child)
        
        return None
    
    def _analyze_condition_enhanced(self, node: ASTNode) -> str:
        """增强的条件分析，支持从Token获取运算符"""
        if len(node.children) >= 2:
            left = self._analyze_node_enhanced(node.children[0])
            right = self._analyze_node_enhanced(node.children[1])
            
            # 从tokens中找到运算符
            operator = self._find_operator_between_operands(left, right)
            
            # 生成比较代码
            return self.code_gen.generate_comparison_code(left, operator, right)
        
        return ""
    
    def _find_operator_between_operands(self, left: str, right: str) -> str:
        """从Token流中找到两个操作数之间的运算符"""
        # 这是一个简化的实现，实际中应该更精确地匹配
        operator_tokens = [">", ">=", "<", "<=", "=", "<>", "AND", "OR"]
        
        for token in self.tokens:
            if token.value in operator_tokens:
                return token.value
        
        return "="  # 默认运算符
    
    def print_results(self):
        """打印分析结果"""
        self.code_gen.print_code()
    
    def get_quadruples(self) -> List[Quadruple]:
        """获取生成的四元式"""
        return self.code_gen.get_quadruples()


def test_enhanced_analyzer():
    """测试增强的语义分析器"""
    from src.compiler.lexer.lexer import Lexer
    from src.compiler.parser.parser import Parser
    
    test_cases = [
        "SELECT name FROM students WHERE age > 18;",
        "SELECT * FROM users WHERE status = 'active';",
        "SELECT name, age FROM people WHERE grade >= 90;"
    ]
    
    print("=" * 80)
    print("              增强语义分析器测试")
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
                # 增强语义分析
                analyzer = EnhancedSemanticAnalyzer()
                success = analyzer.analyze_with_tokens(ast, tokens)
                
                if success:
                    analyzer.print_results()
                
        except Exception as e:
            print(f"❌ 错误: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    test_enhanced_analyzer()