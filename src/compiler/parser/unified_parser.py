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
                self._provide_sql_suggestion(self.sql_type)
                raise SyntaxError(f"不支持的SQL语句类型: {self.sql_type}")
                
        except SyntaxError as e:
            # 保留原始的语法错误信息
            raise e
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
        
        # DDL语句（含事务控制）
        if first_token.type in [TokenType.CREATE, TokenType.DROP, TokenType.ALTER, TokenType.SHOW,
                                 TokenType.BEGIN, TokenType.COMMIT, TokenType.ROLLBACK]:
            return "DDL"
        
        # DML语句
        if first_token.type in [TokenType.INSERT, TokenType.UPDATE, TokenType.DELETE]:
            return "DML"
        
        return "UNKNOWN"
    
    def _provide_sql_suggestion(self, sql_type: str):
        """为无法识别的SQL提供建议"""
        if not self.tokens:
            print("💡 提示: 请输入有效的SQL语句")
            return
            
        first_token = self.tokens[0].value.upper()
        suggestions = {
            'SELEC': 'SELECT',
            'FORM': 'FROM', 
            'WERE': 'WHERE',
            'CREAT': 'CREATE',
            'DELET': 'DELETE',
            'UPDAT': 'UPDATE',
            'INSER': 'INSERT'
        }
        
        if first_token in suggestions:
            print(f"💡 提示: 您是否想要输入 '{suggestions[first_token]}' 而不是 '{first_token}'?")
        else:
            print(f"💡 提示: 无法识别的SQL关键字 '{first_token}'")
            print("   支持的SQL语句类型:")
            print("   - SELECT: 查询数据")
            print("   - INSERT: 插入数据") 
            print("   - UPDATE: 更新数据")
            print("   - DELETE: 删除数据")
            print("   - CREATE: 创建表/索引")
            print("   - DROP: 删除表/索引")
    
    def _parse_select(self) -> Optional[ASTNode]:
        """解析SELECT语句"""
        # 检查是否为复杂查询
        is_complex = self._is_complex_query()
        
        try:
            if is_complex:
                parser = ExtendedParser(self.tokens)
            else:
                parser = Parser(self.tokens)
            
            return parser.parse()
        except SyntaxError as e:
            # 重新抛出语法错误，保留详细信息
            raise e
        except Exception as e:
            raise SyntaxError(f"SELECT语句解析失败: {e}")
    
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
            'GROUP', 'ORDER', 'HAVING', 'ASC', 'DESC',
            'LIMIT', 'OFFSET'
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


