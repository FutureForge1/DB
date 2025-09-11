"""
DML语法分析器
支持INSERT INTO、UPDATE、DELETE FROM等数据操作语言语句
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from typing import List, Optional, Dict, Any
from src.common.types import Token, TokenType, SyntaxError, ASTNode, ASTNodeType

class DMLParser:
    """DML语法分析器"""
    
    def __init__(self, tokens: List[Token]):
        """
        初始化DML语法分析器
        
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
    
    def match_keyword(self, keyword: str) -> bool:
        """检查当前Token是否为指定关键字"""
        return (self.current_token and 
                self.current_token.type == TokenType.IDENTIFIER and
                self.current_token.value.upper() == keyword.upper())
    
    def parse(self) -> Optional[ASTNode]:
        """
        解析DML语句
        
        Returns:
            AST根节点
        """
        if not self.current_token:
            return None
        
        if self.current_token_type() == TokenType.INSERT:
            return self._parse_insert_statement()
        elif self.current_token_type() == TokenType.UPDATE:
            return self._parse_update_statement()
        elif self.current_token_type() == TokenType.DELETE:
            return self._parse_delete_statement()
        else:
            raise SyntaxError(
                f"Unexpected DML statement starting with {self.current_token.value}",
                self.current_token.line,
                self.current_token.column
            )
    
    def _parse_insert_statement(self) -> ASTNode:
        """
        解析INSERT语句
        语法: INSERT INTO table_name [(column_list)] VALUES (value_list) [, (value_list)]*;
        """
        # INSERT
        self.expect(TokenType.INSERT)
        
        # INTO
        self.expect(TokenType.INTO)
        
        # 表名
        table_name_token = self.expect(TokenType.IDENTIFIER)
        
        # 创建INSERT节点
        insert_node = ASTNode(ASTNodeType.SELECT_STMT)
        insert_node.value = "INSERT"
        
        # 表名节点
        table_name_node = ASTNode(ASTNodeType.TABLE_NAME, table_name_token.value)
        insert_node.add_child(table_name_node)
        
        # 可选的列列表
        columns_node = None
        if self.match(TokenType.LEFT_PAREN):
            self.advance()
            columns_node = ASTNode(ASTNodeType.COLUMN_LIST)
            
            # 第一个列名
            column_token = self.expect(TokenType.IDENTIFIER)
            column_node = ASTNode(ASTNodeType.IDENTIFIER, column_token.value)
            columns_node.add_child(column_node)
            
            # 其余列名
            while self.match(TokenType.COMMA):
                self.advance()
                column_token = self.expect(TokenType.IDENTIFIER)
                column_node = ASTNode(ASTNodeType.IDENTIFIER, column_token.value)
                columns_node.add_child(column_node)
            
            self.expect(TokenType.RIGHT_PAREN)
            insert_node.add_child(columns_node)
        
        # VALUES
        self.expect(TokenType.VALUES)
        
        # 值列表
        values_container = ASTNode(ASTNodeType.CONDITION)  # 复用条件节点类型
        values_container.value = "VALUES_LIST"
        
        # 第一组值
        values_row = self._parse_values_row()
        values_container.add_child(values_row)
        
        # 其余值组
        while self.match(TokenType.COMMA):
            self.advance()
            values_row = self._parse_values_row()
            values_container.add_child(values_row)
        
        insert_node.add_child(values_container)
        
        # 分号
        self.expect(TokenType.SEMICOLON)
        
        return insert_node
    
    def _parse_values_row(self) -> ASTNode:
        """解析一行值"""
        # 左括号
        self.expect(TokenType.LEFT_PAREN)
        
        values_row = ASTNode(ASTNodeType.CONDITION)
        values_row.value = "VALUE_ROW"
        
        # 第一个值
        value_node = self._parse_value()
        values_row.add_child(value_node)
        
        # 其余值
        while self.match(TokenType.COMMA):
            self.advance()
            value_node = self._parse_value()
            values_row.add_child(value_node)
        
        # 右括号
        self.expect(TokenType.RIGHT_PAREN)
        
        return values_row
    
    def _parse_value(self) -> ASTNode:
        """解析值"""
        if self.match(TokenType.NUMBER):
            token = self.current_token
            self.advance()
            return ASTNode(ASTNodeType.LITERAL, token.value)
        elif self.match(TokenType.STRING):
            token = self.current_token
            self.advance()
            return ASTNode(ASTNodeType.LITERAL, token.value)
        elif self.match_keyword("NULL"):
            self.advance()
            return ASTNode(ASTNodeType.LITERAL, "NULL")
        else:
            raise SyntaxError(
                f"Expected value, got {self.current_token.value}",
                self.current_token.line,
                self.current_token.column
            )
    
    def _parse_update_statement(self) -> ASTNode:
        """
        解析UPDATE语句
        语法: UPDATE table_name SET column = value [, column = value]* [WHERE condition];
        """
        # UPDATE
        self.expect(TokenType.UPDATE)
        
        # 表名
        table_name_token = self.expect(TokenType.IDENTIFIER)
        
        # SET
        self.expect(TokenType.SET)
        
        # 创建UPDATE节点
        update_node = ASTNode(ASTNodeType.SELECT_STMT)
        update_node.value = "UPDATE"
        
        # 表名节点
        table_name_node = ASTNode(ASTNodeType.TABLE_NAME, table_name_token.value)
        update_node.add_child(table_name_node)
        
        # SET子句
        set_clause = ASTNode(ASTNodeType.CONDITION)
        set_clause.value = "SET_CLAUSE"
        
        # 第一个赋值
        assignment = self._parse_assignment()
        set_clause.add_child(assignment)
        
        # 其余赋值
        while self.match(TokenType.COMMA):
            self.advance()
            assignment = self._parse_assignment()
            set_clause.add_child(assignment)
        
        update_node.add_child(set_clause)
        
        # 可选的WHERE子句
        if self.current_token_type() == TokenType.WHERE:
            self.advance()
            where_clause = self._parse_where_condition()
            update_node.add_child(where_clause)
        
        # 分号
        self.expect(TokenType.SEMICOLON)
        
        return update_node
    
    def _parse_assignment(self) -> ASTNode:
        """解析赋值表达式 column = value"""
        # 列名
        column_token = self.expect(TokenType.IDENTIFIER)
        
        # 等号
        self.expect(TokenType.EQUALS)
        
        # 值
        value_node = self._parse_value()
        
        # 创建赋值节点
        assignment_node = ASTNode(ASTNodeType.CONDITION)
        assignment_node.value = "ASSIGNMENT"
        
        column_node = ASTNode(ASTNodeType.IDENTIFIER, column_token.value)
        assignment_node.add_child(column_node)
        assignment_node.add_child(value_node)
        
        return assignment_node
    
    def _parse_delete_statement(self) -> ASTNode:
        """
        解析DELETE语句
        语法: DELETE FROM table_name [WHERE condition];
        """
        # DELETE
        self.expect(TokenType.DELETE)
        
        # FROM
        self.expect(TokenType.FROM)
        
        # 表名
        table_name_token = self.expect(TokenType.IDENTIFIER)
        
        # 创建DELETE节点
        delete_node = ASTNode(ASTNodeType.SELECT_STMT)
        delete_node.value = "DELETE"
        
        # 表名节点
        table_name_node = ASTNode(ASTNodeType.TABLE_NAME, table_name_token.value)
        delete_node.add_child(table_name_node)
        
        # 可选的WHERE子句
        if self.current_token_type() == TokenType.WHERE:
            self.advance()
            where_clause = self._parse_where_condition()
            delete_node.add_child(where_clause)
        
        # 分号
        self.expect(TokenType.SEMICOLON)
        
        return delete_node
    
    def _parse_where_condition(self) -> ASTNode:
        """解析WHERE条件（简化版本）"""
        # 列名
        column_token = self.expect(TokenType.IDENTIFIER)
        
        # 比较运算符
        if not (self.match(TokenType.EQUALS) or self.match(TokenType.NOT_EQUALS) or
                self.match(TokenType.LESS_THAN) or self.match(TokenType.LESS_EQUAL) or
                self.match(TokenType.GREATER_THAN) or self.match(TokenType.GREATER_EQUAL)):
            raise SyntaxError(
                f"Expected comparison operator, got {self.current_token.value}",
                self.current_token.line,
                self.current_token.column
            )
        
        operator_token = self.current_token
        self.advance()
        
        # 值
        value_node = self._parse_value()
        
        # 创建WHERE条件节点
        where_node = ASTNode(ASTNodeType.WHERE_CLAUSE)
        
        condition_node = ASTNode(ASTNodeType.CONDITION)
        condition_node.value = operator_token.value
        
        column_node = ASTNode(ASTNodeType.IDENTIFIER, column_token.value)
        condition_node.add_child(column_node)
        condition_node.add_child(value_node)
        
        where_node.add_child(condition_node)
        
        return where_node


def test_dml_parser():
    """测试DML语法分析器"""
    from src.compiler.lexer.lexer import Lexer
    
    print("=" * 80)
    print("              DML语法分析器测试")
    print("=" * 80)
    
    test_cases = [
        # INSERT
        "INSERT INTO products (product_id, product_name, price) VALUES (1, 'Laptop', 999.99);",
        
        # INSERT (多行)
        "INSERT INTO products VALUES (1, 'Laptop', 999.99), (2, 'Mouse', 29.99);",
        
        # UPDATE
        "UPDATE products SET price = 899.99 WHERE product_id = 1;",
        
        # UPDATE (多列)
        "UPDATE products SET price = 899.99, product_name = 'Gaming Laptop' WHERE product_id = 1;",
        
        # DELETE
        "DELETE FROM products WHERE price > 1000;",
        
        # DELETE (全部)
        "DELETE FROM products;"
    ]
    
    for i, sql in enumerate(test_cases, 1):
        print(f"\n测试用例 {i}: {sql}")
        print("-" * 60)
        
        try:
            lexer = Lexer(sql)
            tokens = lexer.tokenize()
            
            parser = DMLParser(tokens)
            ast = parser.parse()
            
            if ast:
                print("✅ DML语法分析成功")
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
                print("❌ DML语法分析失败")
                
        except Exception as e:
            print(f"❌ 解析失败: {e}")


if __name__ == "__main__":
    test_dml_parser()