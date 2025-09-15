"""
统一SQL分析器
根据SQL语句类型自动选择合适的分析器（SELECT、DDL、DML）
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from typing import List, Optional, Tuple
from src.common.types import Token, TokenType, SyntaxError, ASTNode
from src.compiler.lexer.lexer import Lexer
from src.compiler.parser.parser import Parser
from src.compiler.parser.extended_parser import ExtendedParser
from src.compiler.parser.ddl_parser import DDLParser
from src.compiler.parser.dml_parser import DMLParser

class UnifiedSQLParser:
    """统一SQL分析器"""
    
    def __init__(self, sql: str):
        """
        初始化统一SQL分析器
        
        Args:
            sql: SQL语句字符串
        """
        self.sql = sql
        self.tokens = []
        self.sql_type = None
        self.parser = None
    
    def parse(self) -> Tuple[Optional[ASTNode], str]:
        """
        解析SQL语句
        
        Returns:
            (AST根节点, SQL类型)
        """
        try:
            # 1. 词法分析
            lexer = Lexer(self.sql)
            self.tokens = lexer.tokenize()
            
            if not self.tokens:
                return None, "EMPTY"
            
            # 2. 确定SQL类型
            self.sql_type = self._determine_sql_type()
            
            # 3. 选择合适的分析器
            if self.sql_type == "SELECT":
                return self._parse_select(), self.sql_type
            elif self.sql_type == "DDL":
                return self._parse_ddl(), self.sql_type
            elif self.sql_type == "DML":
                return self._parse_dml(), self.sql_type
            else:
                raise SyntaxError(f"Unsupported SQL type: {self.sql_type}")
                
        except Exception as e:
            raise SyntaxError(f"SQL parsing failed: {e}")
    
    def _determine_sql_type(self) -> str:
        """
        根据第一个关键字确定SQL类型
        
        Returns:
            SQL类型字符串
        """
        if not self.tokens or self.tokens[0].type == TokenType.EOF:
            return "EMPTY"
        
        first_token = self.tokens[0]
        
        # SELECT查询
        if first_token.type == TokenType.SELECT:
            return "SELECT"
        
        # DDL语句
        if first_token.type in [TokenType.CREATE, TokenType.DROP, TokenType.ALTER]:
            return "DDL"
        
        # DML语句
        if first_token.type in [TokenType.INSERT, TokenType.UPDATE, TokenType.DELETE]:
            return "DML"
        
        return "UNKNOWN"
    
    def _parse_select(self) -> Optional[ASTNode]:
        """解析SELECT语句"""
        # 检查是否为复杂查询
        is_complex = self._is_complex_query()
        
        if is_complex:
            parser = ExtendedParser(self.tokens)
        else:
            parser = Parser(self.tokens)
        
        return parser.parse()
    
    def _parse_ddl(self) -> Optional[ASTNode]:
        """解析DDL语句"""
        parser = DDLParser(self.tokens)
        return parser.parse()
    
    def _parse_dml(self) -> Optional[ASTNode]:
        """解析DML语句"""
        parser = DMLParser(self.tokens)
        return parser.parse()
    
    def _is_complex_query(self) -> bool:
        """
        检测是否为复杂查询
        
        Returns:
            是否为复杂查询
        """
        complex_keywords = {
            'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL',
            'COUNT', 'SUM', 'AVG', 'MAX', 'MIN',
            'GROUP', 'ORDER', 'HAVING', 'ASC', 'DESC'
        }
        
        # 检查是否有复杂查询关键字
        for token in self.tokens:
            if token.value.upper() in complex_keywords:
                return True
                
        # 检查是否有表别名.列名的形式（点号）
        for i, token in enumerate(self.tokens):
            if (token.type == TokenType.DOT and 
                i > 0 and i < len(self.tokens) - 1 and
                self.tokens[i-1].type == TokenType.IDENTIFIER and
                self.tokens[i+1].type == TokenType.IDENTIFIER):
                return True
                
        return False
    
    def get_sql_type(self) -> str:
        """获取SQL类型"""
        return self.sql_type or "UNKNOWN"
    
    def get_tokens(self) -> List[Token]:
        """获取Token列表"""
        return self.tokens


def test_unified_parser():
    """测试统一SQL分析器"""
    print("=" * 80)
    print("              统一SQL分析器测试")
    print("=" * 80)
    
    test_cases = [
        # SELECT查询
        ("SELECT * FROM students;", "基础SELECT查询"),
        ("SELECT name, age FROM students WHERE age > 20;", "带WHERE的SELECT查询"),
        ("SELECT COUNT(*) FROM students;", "复杂SELECT查询"),
        
        # DDL语句
        ("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(255));", "CREATE TABLE"),
        ("DROP TABLE products;", "DROP TABLE"),
        ("ALTER TABLE products ADD COLUMN price DECIMAL(10,2);", "ALTER TABLE"),
        ("CREATE INDEX idx_name ON products (name);", "CREATE INDEX"),
        
        # DML语句
        ("INSERT INTO products VALUES (1, 'Laptop', 999.99);", "INSERT"),
        ("UPDATE products SET price = 899.99 WHERE id = 1;", "UPDATE"),
        ("DELETE FROM products WHERE price > 1000;", "DELETE"),
    ]
    
    for i, (sql, description) in enumerate(test_cases, 1):
        print(f"\\n测试用例 {i}: {description}")
        print(f"SQL: {sql}")
        print("-" * 60)
        
        try:
            parser = UnifiedSQLParser(sql)
            ast, sql_type = parser.parse()
            
            if ast:
                print(f"✅ 解析成功")
                print(f"   SQL类型: {sql_type}")
                print(f"   AST根节点: {ast.type.value}")
                print(f"   操作类型: {ast.value}")
                print(f"   子节点数: {len(ast.children)}")
            else:
                print(f"❌ 解析失败: AST为空")
                
        except Exception as e:
            print(f"❌ 解析失败: {e}")
    
    print(f"\\n{'='*80}")
    print("统一SQL分析器测试完成")
    print(f"{'='*80}")


if __name__ == "__main__":
    test_unified_parser()