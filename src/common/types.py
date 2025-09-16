"""
公共数据结构和常量定义
"""

from enum import Enum
from dataclasses import dataclass
from typing import Any, Optional

#Token类型定义  枚举
class TokenType(Enum):
    # 查询
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    
    # 复杂查询关
    JOIN = "JOIN"
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    OUTER = "OUTER"
    FULL = "FULL"
    ON = "ON"
    
    # 聚合函数
    GROUP = "GROUP"
    BY = "BY"
    HAVING = "HAVING"
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MAX = "MAX"
    MIN = "MIN"
    
    # 排序
    ORDER = "ORDER"
    ASC = "ASC"
    DESC = "DESC"
    
    # 限制
    LIMIT = "LIMIT"
    OFFSET = "OFFSET"
    
    # 子查询
    IN = "IN"
    EXISTS = "EXISTS"
    ALL = "ALL"
    ANY = "ANY"
    SOME = "SOME"
    
    # DDL的
    CREATE = "CREATE"
    TABLE = "TABLE"
    DROP = "DROP"
    ALTER = "ALTER"
    INDEX = "INDEX"
    SHOW = "SHOW"
    
    # DML的
    INSERT = "INSERT"
    INTO = "INTO"
    VALUES = "VALUES"
    UPDATE = "UPDATE"
    SET = "SET"
    DELETE = "DELETE"
    
    # 数据类型
    INT = "INT"
    INTEGER = "INTEGER"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    DECIMAL = "DECIMAL"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    TEXT = "TEXT"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"
    
    # 约束关键字
    PRIMARY = "PRIMARY"
    KEY = "KEY"
    FOREIGN = "FOREIGN"
    REFERENCES = "REFERENCES"
    UNIQUE = "UNIQUE"
    CHECK = "CHECK"
    DEFAULT = "DEFAULT"
    NULL = "NULL"
    
    # 其他
    DISTINCT = "DISTINCT"
    AS = "AS"
    UNION = "UNION"
    INTERSECT = "INTERSECT"
    EXCEPT = "EXCEPT"
    
    # 事务控制
    BEGIN = "BEGIN"
    COMMIT = "COMMIT"
    ROLLBACK = "ROLLBACK"
    TRANSACTION = "TRANSACTION"
    
    # 标识符和字面值
    IDENTIFIER = "IDENTIFIER"  # 表名、列名等
    NUMBER = "NUMBER"          # 数字
    STRING = "STRING"          # 字符串
    
    # 运算符
    EQUALS = "="             
    NOT_EQUALS = "<>"         
    LESS_THAN = "<"          
    LESS_EQUAL = "<="         
    GREATER_THAN = ">"
    GREATER_EQUAL = ">="      
    
    # 分隔符
    COMMA = ","               
    SEMICOLON = ";"           
    LEFT_PAREN = "("          
    RIGHT_PAREN = ")"         
    ASTERISK = "*"            
    DOT = "."                
    
    # 特殊标记
    EOF = "EOF"               # 文件结束
    UNKNOWN = "UNKNOWN"       # 未知字符


# Token数据结构
@dataclass
class Token:
    type: TokenType
    value: str
    line: int  # 行号
    column: int    # 列号
    
    def __str__(self):
        return f"Token({self.type.value}, '{self.value}', {self.line}:{self.column})"

#中间代码定义     构建一个四元式中间代码        eg:[op, arg1, arg2, result]

@dataclass
class Quadruple:
    op: str              
    arg1: Optional[str] 
    arg2: Optional[str]  
    result: str         
    
    def __str__(self):
        return f"({self.op}, {self.arg1 or '-'}, {self.arg2 or '-'}, {self.result})"

#标代码指令定义
class InstructionType(Enum):
    """目标代码指令类型"""
    OPEN = "OPEN"         # 打开表
    SCAN = "SCAN"         # 扫描记录
    FILTER = "FILTER"     # 按条件过滤
    PROJECT = "PROJECT"   # 投影列
    OUTPUT = "OUTPUT"     # 输出结果
    CLOSE = "CLOSE"       # 关闭表


#目标代码指令
@dataclass
class Instruction:
    type: InstructionType
    operands: list[str]
    
    def __str__(self):
        operands_str = ", ".join(self.operands)
        return f"{self.type.value} {operands_str}"

#SQL语法树节点定义
class ASTNodeType(Enum):
    """抽象语法树节点类型"""
    SELECT_STMT = "SELECT_STATEMENT"    # SELECT 查询语句
    COLUMN_LIST = "COLUMN_LIST"    # 列列表
    TABLE_NAME = "TABLE_NAME"    # 表名
    WHERE_CLAUSE = "WHERE_CLAUSE"    # WHERE 子句
    CONDITION = "CONDITION"    # 条件
    IDENTIFIER = "IDENTIFIER"    # 标识符
    LITERAL = "LITERAL"    # 字面量
    
    # 复杂查询节点类型
    JOIN_CLAUSE = "JOIN_CLAUSE"
    JOIN_TYPE = "JOIN_TYPE"
    ON_CLAUSE = "ON_CLAUSE"
    
    # 聚合函数节点类型
    GROUP_BY_CLAUSE = "GROUP_BY_CLAUSE"
    HAVING_CLAUSE = "HAVING_CLAUSE"
    AGGREGATE_FUNCTION = "AGGREGATE_FUNCTION"
    AGGREGATE_ARG = "AGGREGATE_ARG"
    
    # 排序节点类型
    ORDER_BY_CLAUSE = "ORDER_BY_CLAUSE"
    ORDER_BY_LIST = "ORDER_BY_LIST"
    ORDER_BY_SPEC = "ORDER_BY_SPEC"
    ORDER_SPEC = "ORDER_SPEC"
    
    # 限制节点类型
    LIMIT_CLAUSE = "LIMIT_CLAUSE"
    LIMIT_VALUE = "LIMIT_VALUE"
    OFFSET_VALUE = "OFFSET_VALUE"
    
    # 子查询节点类型
    SUBQUERY = "SUBQUERY"
    
    # 表连接节点类型
    TABLE_REF = "TABLE_REF"
    TABLE_ALIAS = "TABLE_ALIAS"
    COLUMN_REF = "COLUMN_REF"
    COLUMN_ALIAS = "COLUMN_ALIAS"  # 列别名
    AGGREGATE_ARG_LIST = "AGGREGATE_ARG_LIST"
    JOIN_CONDITION = "JOIN_CONDITION"  # 添加JOIN条件节点类型

# 抽象语法树节点
@dataclass
class ASTNode:
    type: ASTNodeType
    value: Optional[str] = None
    children: list['ASTNode'] = None
    
    def __post_init__(self):
        if self.children is None:
            self.children = []
    
    def add_child(self, child: 'ASTNode'):
        self.children.append(child)
    
    def __str__(self, level=0):
        indent = "  " * level
        result = f"{indent}{self.type.value}"
        if self.value:
            result += f": {self.value}"
        result += "\n"
        for child in self.children:
            result += child.__str__(level + 1)
        return result

# 符号表定义
@dataclass
class Symbol:
    name: str
    type: str
    scope: str
    line: int
    column: int

class SymbolTable:
    def __init__(self):
        self.symbols: dict[str, Symbol] = {}
    
    def add_symbol(self, symbol: Symbol):
        """添加符号"""
        self.symbols[symbol.name] = symbol
    
    def lookup(self, name: str) -> Optional[Symbol]:
        """查找符号"""
        return self.symbols.get(name)
    
    def __str__(self):
        result = "符号表:\n"
        for name, symbol in self.symbols.items():
            result += f"  {name}: {symbol.type} (line {symbol.line})\n"
        return result

#错误处理
class CompilerError(Exception):
    """编译器错误基类"""
    def __init__(self, message: str, line: int = 0, column: int = 0):
        self.message = message
        self.line = line
        self.column = column
        super().__init__(self.format_message())
    
    def format_message(self):
        if self.line > 0:
            return f"Error at line {self.line}, column {self.column}: {self.message}"
        return f"Error: {self.message}"

class LexicalError(CompilerError):
    """词法分析错误"""
    pass

class SyntaxError(CompilerError):
    """语法分析错误"""
    pass

class SemanticError(CompilerError):
    """语义分析错误"""
    def __init__(self, error_type: str, message: str, line: int = 0, column: int = 0, context: str = ""):
        self.error_type = error_type
        self.context = context
        super().__init__(message, line, column)
    
    def format_message(self):
        parts = [f"[{self.error_type}]"]
        if self.line > 0:
            parts.append(f"行 {self.line}")
            if self.column > 0:
                parts.append(f"列 {self.column}")
        parts.append(f": {self.message}")
        if self.context:
            parts.append(f" (上下文: {self.context})")
        return " ".join(parts)



#常量定义


# SQL关键字列表
SQL_KEYWORDS = {
    'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT',
    # 复杂查询关键字
    'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'FULL', 'ON',
    # 聚合函数关键字
    'GROUP', 'BY', 'HAVING', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN',
    # 排序关键字
    'ORDER', 'ASC', 'DESC',
    # 限制关键字
    'LIMIT', 'OFFSET',
    # 子查询关键字
    'IN', 'EXISTS', 'ALL', 'ANY', 'SOME',
    # DDL关键字
    'CREATE', 'TABLE', 'DROP', 'ALTER', 'INDEX', 'SHOW',
    # 事务关键字
    'BEGIN', 'COMMIT', 'ROLLBACK', 'TRANSACTION',
    # DML关键字
    'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
    # 数据类型关键字
    'INT', 'INTEGER', 'VARCHAR', 'CHAR', 'DECIMAL', 'FLOAT', 'DOUBLE',
    'TEXT', 'DATE', 'TIME', 'DATETIME', 'TIMESTAMP',
    # 约束关键字
    'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'UNIQUE', 'CHECK', 'DEFAULT', 'NULL',
    # 其他关键字
    'DISTINCT', 'AS', 'UNION', 'INTERSECT', 'EXCEPT'
}

# 运算符映射
OPERATORS = {
    '=': TokenType.EQUALS,
    '<>': TokenType.NOT_EQUALS,
    '<': TokenType.LESS_THAN,
    '<=': TokenType.LESS_EQUAL,
    '>': TokenType.GREATER_THAN,
    '>=': TokenType.GREATER_EQUAL,
}

# 分隔符映射
DELIMITERS = {
    ',': TokenType.COMMA,
    ';': TokenType.SEMICOLON,
    '(': TokenType.LEFT_PAREN,
    ')': TokenType.RIGHT_PAREN,
    '*': TokenType.ASTERISK,
    '.': TokenType.DOT,
}