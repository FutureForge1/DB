"""
DDL语法分析器
支持CREATE TABLE、DROP TABLE、ALTER TABLE、CREATE INDEX等数据定义语言语句
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from typing import List, Optional, Dict, Any
from src.common.types import Token, TokenType, SyntaxError, ASTNode, ASTNodeType

class DDLParser:
    """DDL语法分析器"""
    
    def __init__(self, tokens: List[Token]):
        """
        初始化DDL语法分析器
        
        Args:
            tokens: 词法分析器产生的Token列表
        """
        self.tokens = tokens
        self.position = 0
        self.current_token = self.tokens[0] if tokens else None
    
    def current_token_type(self) -> TokenType:
        """获取当前Token的类型"""
        if self.current_token:
            return self.current_token.type
        return TokenType.EOF
    
    def advance(self):
        """前进到下一个Token"""
        if self.position < len(self.tokens) - 1:
            self.position += 1
            self.current_token = self.tokens[self.position]
        else:
            self.current_token = None
    
    def expect(self, token_type: TokenType) -> Token:
        """期望特定类型的Token"""
        if self.current_token_type() != token_type:
            raise SyntaxError(
                f"Expected {token_type.value}, got {self.current_token_type().value}",
                self.current_token.line if self.current_token else 0,
                self.current_token.column if self.current_token else 0
            )
        token = self.current_token
        self.advance()
        return token
    
    def match(self, token_type: TokenType) -> bool:
        """检查当前Token是否匹配指定类型"""
        return self.current_token_type() == token_type
    
    def parse(self) -> Optional[ASTNode]:
        """
        解析DDL语句
        
        Returns:
            AST根节点
        """
        if not self.current_token:
            return None
        
        if self.current_token_type() == TokenType.CREATE:
            return self._parse_create_statement()
        elif self.current_token_type() == TokenType.DROP:
            return self._parse_drop_statement()
        elif self.current_token_type() == TokenType.ALTER:
            return self._parse_alter_statement()
        else:
            raise SyntaxError(
                f"Unexpected DDL statement starting with {self.current_token_type().value}",
                self.current_token.line,
                self.current_token.column
            )
    
    def _parse_create_statement(self) -> ASTNode:
        """解析CREATE语句"""
        # CREATE
        self.expect(TokenType.CREATE)
        
        if self.current_token_type() == TokenType.TABLE:
            return self._parse_create_table()
        elif self.current_token_type() == TokenType.INDEX:
            return self._parse_create_index()
        else:
            raise SyntaxError(
                f"Unsupported CREATE statement: {self.current_token_type().value}",
                self.current_token.line,
                self.current_token.column
            )
    
    def _parse_create_table(self) -> ASTNode:
        """
        解析CREATE TABLE语句
        语法: CREATE TABLE table_name ( column_def [, column_def]* );
        """
        # TABLE
        self.expect(TokenType.TABLE)
        
        # 表名
        table_name_token = self.expect(TokenType.IDENTIFIER)
        
        # 创建CREATE TABLE节点
        create_table_node = ASTNode(ASTNodeType.SELECT_STMT)
        create_table_node.value = f"CREATE_TABLE"
        
        # 表名节点
        table_name_node = ASTNode(ASTNodeType.TABLE_NAME, table_name_token.value)
        create_table_node.add_child(table_name_node)
        
        # 左括号
        self.expect(TokenType.LEFT_PAREN)
        
        # 列定义列表
        columns_node = ASTNode(ASTNodeType.COLUMN_LIST)
        create_table_node.add_child(columns_node)
        
        # 解析第一个列定义
        column_def = self._parse_column_definition()
        columns_node.add_child(column_def)
        
        # 解析其余列定义
        while self.match(TokenType.COMMA):
            self.advance()  # 跳过逗号
            column_def = self._parse_column_definition()
            columns_node.add_child(column_def)
        
        # 右括号
        self.expect(TokenType.RIGHT_PAREN)
        
        # 分号
        self.expect(TokenType.SEMICOLON)
        
        return create_table_node
    
    def _parse_column_definition(self) -> ASTNode:
        """
        解析列定义
        语法: column_name data_type [constraints]
        """
        # 列名
        column_name_token = self.expect(TokenType.IDENTIFIER)
        
        # 创建列定义节点
        column_node = ASTNode(ASTNodeType.IDENTIFIER, column_name_token.value)
        
        # 数据类型
        data_type_node = self._parse_data_type()
        column_node.add_child(data_type_node)
        
        # 解析约束
        constraints_node = self._parse_column_constraints()
        if constraints_node.children:  # 只有在有约束时才添加
            column_node.add_child(constraints_node)
        
        return column_node
    
    def _parse_data_type(self) -> ASTNode:
        """
        解析数据类型
        支持: INT, INTEGER, VARCHAR(n), CHAR(n), DECIMAL(p,s), FLOAT, DOUBLE, TEXT
        """
        data_type_token = self.current_token
        
        if self.match(TokenType.INT) or self.match(TokenType.INTEGER):
            self.advance()
            return ASTNode(ASTNodeType.LITERAL, data_type_token.value)
        
        elif self.match(TokenType.VARCHAR) or self.match(TokenType.CHAR):
            self.advance()
            type_node = ASTNode(ASTNodeType.LITERAL, data_type_token.value)
            
            # 检查长度参数
            if self.match(TokenType.LEFT_PAREN):
                self.advance()
                length_token = self.expect(TokenType.NUMBER)
                self.expect(TokenType.RIGHT_PAREN)
                type_node.value += f"({length_token.value})"
            
            return type_node
        
        elif self.match(TokenType.DECIMAL):
            self.advance()
            type_node = ASTNode(ASTNodeType.LITERAL, data_type_token.value)
            
            # 检查精度和标度参数
            if self.match(TokenType.LEFT_PAREN):
                self.advance()
                precision_token = self.expect(TokenType.NUMBER)
                self.expect(TokenType.COMMA)
                scale_token = self.expect(TokenType.NUMBER)
                self.expect(TokenType.RIGHT_PAREN)
                type_node.value += f"({precision_token.value},{scale_token.value})"
            
            return type_node
        
        elif self.match(TokenType.FLOAT) or self.match(TokenType.DOUBLE) or \
             self.match(TokenType.TEXT) or self.match(TokenType.DATE) or \
             self.match(TokenType.TIME) or self.match(TokenType.DATETIME):
            self.advance()
            return ASTNode(ASTNodeType.LITERAL, data_type_token.value)
        
        else:
            raise SyntaxError(
                f"Expected data type, got {self.current_token_type().value}",
                self.current_token.line,
                self.current_token.column
            )
    
    def _parse_column_constraints(self) -> ASTNode:
        """
        解析列约束
        支持: PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT value
        """
        constraints_node = ASTNode(ASTNodeType.CONDITION)  # 复用条件节点类型
        
        while True:
            if self.match(TokenType.PRIMARY):
                self.advance()
                self.expect(TokenType.KEY)
                constraint_node = ASTNode(ASTNodeType.LITERAL, "PRIMARY_KEY")
                constraints_node.add_child(constraint_node)
            
            elif self.match(TokenType.NOT):
                self.advance()
                self.expect(TokenType.NULL)
                constraint_node = ASTNode(ASTNodeType.LITERAL, "NOT_NULL")
                constraints_node.add_child(constraint_node)
            
            elif self.match(TokenType.UNIQUE):
                self.advance()
                constraint_node = ASTNode(ASTNodeType.LITERAL, "UNIQUE")
                constraints_node.add_child(constraint_node)
            
            elif self.match(TokenType.DEFAULT):
                self.advance()
                constraint_node = ASTNode(ASTNodeType.LITERAL, "DEFAULT")
                
                # 默认值
                if self.match(TokenType.NUMBER) or self.match(TokenType.STRING):
                    value_token = self.current_token
                    self.advance()
                    value_node = ASTNode(ASTNodeType.LITERAL, value_token.value)
                    constraint_node.add_child(value_node)
                
                constraints_node.add_child(constraint_node)
            
            else:
                break
        
        return constraints_node
    
    def _parse_create_index(self) -> ASTNode:
        """
        解析CREATE INDEX语句
        语法: CREATE INDEX index_name ON table_name (column_list);
        """
        # INDEX
        self.expect(TokenType.INDEX)
        
        # 索引名
        index_name_token = self.expect(TokenType.IDENTIFIER)
        
        # ON
        self.expect(TokenType.ON)
        
        # 表名
        table_name_token = self.expect(TokenType.IDENTIFIER)
        
        # 左括号
        self.expect(TokenType.LEFT_PAREN)
        
        # 列列表
        columns = []
        column_token = self.expect(TokenType.IDENTIFIER)
        columns.append(column_token.value)
        
        while self.match(TokenType.COMMA):
            self.advance()
            column_token = self.expect(TokenType.IDENTIFIER)
            columns.append(column_token.value)
        
        # 右括号
        self.expect(TokenType.RIGHT_PAREN)
        
        # 分号
        self.expect(TokenType.SEMICOLON)
        
        # 创建INDEX节点
        index_node = ASTNode(ASTNodeType.SELECT_STMT)
        index_node.value = f"CREATE_INDEX"
        
        # 索引名节点
        index_name_node = ASTNode(ASTNodeType.IDENTIFIER, index_name_token.value)
        index_node.add_child(index_name_node)
        
        # 表名节点
        table_name_node = ASTNode(ASTNodeType.TABLE_NAME, table_name_token.value)
        index_node.add_child(table_name_node)
        
        # 列列表节点
        columns_node = ASTNode(ASTNodeType.COLUMN_LIST)
        for column in columns:
            column_node = ASTNode(ASTNodeType.IDENTIFIER, column)
            columns_node.add_child(column_node)
        index_node.add_child(columns_node)
        
        return index_node
    
    def _parse_drop_statement(self) -> ASTNode:
        """解析DROP语句"""
        # DROP
        self.expect(TokenType.DROP)
        
        if self.current_token_type() == TokenType.TABLE:
            return self._parse_drop_table()
        else:
            raise SyntaxError(
                f"Unsupported DROP statement: {self.current_token_type().value}",
                self.current_token.line,
                self.current_token.column
            )
    
    def _parse_drop_table(self) -> ASTNode:
        """
        解析DROP TABLE语句
        语法: DROP TABLE table_name;
        """
        # TABLE
        self.expect(TokenType.TABLE)
        
        # 表名
        table_name_token = self.expect(TokenType.IDENTIFIER)
        
        # 分号
        self.expect(TokenType.SEMICOLON)
        
        # 创建DROP TABLE节点
        drop_table_node = ASTNode(ASTNodeType.SELECT_STMT)
        drop_table_node.value = f"DROP_TABLE"
        
        # 表名节点
        table_name_node = ASTNode(ASTNodeType.TABLE_NAME, table_name_token.value)
        drop_table_node.add_child(table_name_node)
        
        return drop_table_node
    
    def _parse_alter_statement(self) -> ASTNode:
        """解析ALTER语句"""
        # ALTER
        self.expect(TokenType.ALTER)
        
        if self.current_token_type() == TokenType.TABLE:
            return self._parse_alter_table()
        else:
            raise SyntaxError(
                f"Unsupported ALTER statement: {self.current_token_type().value}",
                self.current_token.line,
                self.current_token.column
            )
    
    def _parse_alter_table(self) -> ASTNode:
        """
        解析ALTER TABLE语句
        语法: ALTER TABLE table_name ADD COLUMN column_def;
        """
        # TABLE
        self.expect(TokenType.TABLE)
        
        # 表名
        table_name_token = self.expect(TokenType.IDENTIFIER)
        
        # ADD (简化实现，只支持ADD COLUMN)
        if not self.match(TokenType.IDENTIFIER) or self.current_token.value.upper() != 'ADD':
            raise SyntaxError(
                f"Expected ADD, got {self.current_token.value}",
                self.current_token.line,
                self.current_token.column
            )
        self.advance()
        
        # COLUMN (可选)
        if self.match(TokenType.IDENTIFIER) and self.current_token.value.upper() == 'COLUMN':
            self.advance()
        
        # 列定义
        column_def = self._parse_column_definition()
        
        # 分号
        self.expect(TokenType.SEMICOLON)
        
        # 创建ALTER TABLE节点
        alter_table_node = ASTNode(ASTNodeType.SELECT_STMT)
        alter_table_node.value = f"ALTER_TABLE"
        
        # 表名节点
        table_name_node = ASTNode(ASTNodeType.TABLE_NAME, table_name_token.value)
        alter_table_node.add_child(table_name_node)
        
        # 操作类型节点
        operation_node = ASTNode(ASTNodeType.LITERAL, "ADD_COLUMN")
        alter_table_node.add_child(operation_node)
        
        # 列定义节点
        alter_table_node.add_child(column_def)
        
        return alter_table_node


def test_ddl_parser():
    """测试DDL语法分析器"""
    from src.compiler.lexer.lexer import Lexer
    
    print("=" * 80)
    print("              DDL语法分析器测试")
    print("=" * 80)
    
    test_cases = [
        # CREATE TABLE
        """CREATE TABLE products (
            product_id INT PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            price DECIMAL(10, 2),
            category_id INT
        );""",
        
        # CREATE INDEX
        "CREATE INDEX idx_product_name ON products (product_name);",
        
        # DROP TABLE
        "DROP TABLE products;",
        
        # ALTER TABLE
        "ALTER TABLE products ADD COLUMN description TEXT;"
    ]
    
    for i, sql in enumerate(test_cases, 1):
        print(f"\n测试用例 {i}: {sql}")
        print("-" * 60)
        
        try:
            lexer = Lexer(sql)
            tokens = lexer.tokenize()
            
            parser = DDLParser(tokens)
            ast = parser.parse()
            
            if ast:
                print("✅ DDL语法分析成功")
                print(f"AST根节点: {ast.type.value}")
                print(f"操作类型: {ast.value}")
                print(f"子节点数: {len(ast.children)}")
                
                # 显示AST结构
                def print_ast(node, indent=0):
                    prefix = "  " * indent
                    print(f"{prefix}+ {node.type.value}: {node.value or ''}")
                    for child in node.children:
                        print_ast(child, indent + 1)
                
                print_ast(ast)
            else:
                print("❌ DDL语法分析失败")
                
        except Exception as e:
            print(f"❌ 解析失败: {e}")


if __name__ == "__main__":
    test_ddl_parser()