"""
词法分析器实现
将SQL查询字符串分解为Token序列
"""

import re
from typing import List, Iterator
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.common.types import (
    Token, TokenType, LexicalError, 
    SQL_KEYWORDS, OPERATORS, DELIMITERS
)

# 关键字映射表
KEYWORD_MAP = {
    # 基本查询关键字
    'SELECT': TokenType.SELECT,
    'FROM': TokenType.FROM,
    'WHERE': TokenType.WHERE,
    'AND': TokenType.AND,
    'OR': TokenType.OR,
    'NOT': TokenType.NOT,
    
    # 复杂查询关键字
    'JOIN': TokenType.JOIN,
    'INNER': TokenType.INNER,
    'LEFT': TokenType.LEFT,
    'RIGHT': TokenType.RIGHT,
    'OUTER': TokenType.OUTER,
    'FULL': TokenType.FULL,
    'ON': TokenType.ON,
    
    # 聚合函数关键字
    'GROUP': TokenType.GROUP,
    'BY': TokenType.BY,
    'HAVING': TokenType.HAVING,
    'COUNT': TokenType.COUNT,
    'SUM': TokenType.SUM,
    'AVG': TokenType.AVG,
    'MAX': TokenType.MAX,
    'MIN': TokenType.MIN,
    
    # 排序关键字
    'ORDER': TokenType.ORDER,
    'ASC': TokenType.ASC,
    'DESC': TokenType.DESC,
    
    # 限制关键字
    'LIMIT': TokenType.LIMIT,
    'OFFSET': TokenType.OFFSET,
    
    # 子查询关键字
    'IN': TokenType.IN,
    'EXISTS': TokenType.EXISTS,
    'ALL': TokenType.ALL,
    'ANY': TokenType.ANY,
    'SOME': TokenType.SOME,
    
}

class Lexer:    
    def __init__(self, source: str):
        """
        初始化词法分析器
        参数是要分析的SQL源代码
        """
        self.source = source
        self.position = 0
        self.line = 1
        self.column = 1
        self.tokens: List[Token] = []
    
    def tokenize(self) -> List[Token]:
        """
        对输入源代码进行词法分析，返回Token列表
        """
        self.tokens = []
        self.position = 0
        self.line = 1
        self.column = 1
        
        while self.position < len(self.source):
            self._skip_whitespace()
            
            if self.position >= len(self.source):
                break
            
            # 跳过注释
            if self._skip_comment():
                continue
            
            # 识别字符串字面值
            if self.current_char() == "'":
                self._read_string()
                continue
            
            # 识别数字
            if self.current_char().isdigit():
                self._read_number()
                continue
            
            # 识别标识符和关键字
            if self.current_char().isalpha() or self.current_char() == '_':
                self._read_identifier()
                continue
            
            # 识别运算符
            if self._read_operator():
                continue
            
            # 识别分隔符
            if self._read_delimiter():
                continue
            
            # 未知字符
            raise LexicalError(
                f"错误字符: '{self.current_char()}'",
                self.line, self.column
            )
        
        # 添加EOF标记
        self.tokens.append(Token(TokenType.EOF, "", self.line, self.column))
        return self.tokens
    
    def current_char(self) -> str:
        """获取当前位置的字符"""
        if self.position >= len(self.source):
            return '\0'
        return self.source[self.position]
    
    def peek_char(self, offset: int = 1) -> str:
        """向前查看字符"""
        pos = self.position + offset
        if pos >= len(self.source):
            return '\0'
        return self.source[pos]
    
    def advance(self) -> str:
        """前进一个字符位置"""
        if self.position < len(self.source):
            char = self.source[self.position]
            self.position += 1
            if char == '\n':
                self.line += 1
                self.column = 1
            else:
                self.column += 1
            return char
        return '\0'
    
    def _skip_whitespace(self):
        """跳过空白字符"""
        while (self.position < len(self.source) and 
               self.current_char().isspace()):
            self.advance()
    
    def _skip_comment(self) -> bool:
        """跳过注释，返回是否跳过了注释"""
        if (self.current_char() == '-' and 
            self.peek_char() == '-'):
            # 单行注释，跳过到行末
            while (self.position < len(self.source) and 
                   self.current_char() != '\n'):
                self.advance()
            return True
        return False
    
    def _read_string(self):
        """读取字符串字面值"""
        start_line = self.line
        start_column = self.column
        value = ""
        self.advance()  # 跳过开始的单引号
        
        while (self.position < len(self.source) and 
               self.current_char() != "'"):
            if self.current_char() == '\\':
                # 处理转义字符
                self.advance()
                if self.position < len(self.source):
                    escaped_char = self.current_char()
                    if escaped_char == 'n':
                        value += '\n'
                    elif escaped_char == 't':
                        value += '\t'
                    elif escaped_char == 'r':
                        value += '\r'
                    elif escaped_char == '\\':
                        value += '\\'
                    elif escaped_char == "'":
                        value += "'"
                    else:
                        value += escaped_char
                    self.advance()
            else:
                value += self.current_char()
                self.advance()
        
        if self.position >= len(self.source):
            raise LexicalError(
                "字符串没有正确结束",
                start_line, start_column
            )
         # 跳过结束的单引号
        self.advance() 
        
        token = Token(TokenType.STRING, value, start_line, start_column)
        self.tokens.append(token)
    
    def _read_number(self):
        """读取数字"""
        start_line = self.line
        start_column = self.column
        value = ""
        
        # 读取整数部分
        while (self.position < len(self.source) and self.current_char().isdigit()):
            value += self.current_char()
            self.advance()
        
        # 检查小数点
        if (self.position < len(self.source) and self.current_char() == '.' and
            self.peek_char().isdigit()):
            value += self.current_char()
            self.advance()
            
            # 读取小数部分
            while (self.position < len(self.source) and  self.current_char().isdigit()):
                value += self.current_char()
                self.advance()
        
        token = Token(TokenType.NUMBER, value, start_line, start_column)
        self.tokens.append(token)
    
    def _read_identifier(self):
        """读取标识符或关键字"""
        start_line = self.line
        start_column = self.column
        value = ""
        
        # 读取标识符字符
        while (self.position < len(self.source) and (self.current_char().isalnum() or self.current_char() == '_')):
            value += self.current_char()
            self.advance()
        
        # 检查是否为关键字
        upper_value = value.upper()
        if upper_value in SQL_KEYWORDS:
            token_type = TokenType(upper_value)
        else:
            token_type = TokenType.IDENTIFIER
        
        token = Token(token_type, value, start_line, start_column)
        self.tokens.append(token)
    
    def _read_operator(self) -> bool:
        """读取运算符"""
        start_line = self.line
        start_column = self.column
        
        # 先检查双字符运算符
        two_char = self.current_char() + self.peek_char()
        if two_char in OPERATORS:
            self.advance()
            self.advance()
            token = Token(OPERATORS[two_char], two_char, start_line, start_column)
            self.tokens.append(token)
            return True
        
        # 检查单字符运算符
        one_char = self.current_char()
        if one_char in OPERATORS:
            self.advance()
            token = Token(OPERATORS[one_char], one_char, start_line, start_column)
            self.tokens.append(token)
            return True
        
        return False
    
    def _read_delimiter(self) -> bool:
        """读取分隔符"""
        start_line = self.line
        start_column = self.column
        char = self.current_char()
        
        if char in DELIMITERS:
            self.advance()
            token = Token(DELIMITERS[char], char, start_line, start_column)
            self.tokens.append(token)
            return True
        
        return False
    
    def print_tokens(self):
        """打印所有Token（调试用）"""
        print("词法分析结果:")
        print("-" * 60)
        print(f"{'序号':<4} {'类型':<15} {'值':<20} {'位置':<10}")
        print("-" * 60)
        
        for i, token in enumerate(self.tokens):
            location = f"{token.line}:{token.column}"
            print(f"{i+1:<4} {token.type.value:<15} {repr(token.value):<20} {location:<10}")
        
        print("-" * 60)
        print(f"总共识别了 {len(self.tokens)} 个Token")
    
    def get_token_tuples(self) -> List[tuple]:
        """返回四元式形式的Token信息 (type, value, line, column)"""
        return [(token.type.value, token.value, token.line, token.column) 
                for token in self.tokens]
