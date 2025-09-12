"""
扩展的语法分析器实现
支持JOIN、聚合函数、ORDER BY、GROUP BY等复杂查询语法
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from typing import List, Optional
from src.common.types import Token, TokenType, SyntaxError, ASTNode, ASTNodeType
from src.compiler.parser.extended_grammar import ExtendedSQLGrammar

class ExtendedParser:
    """扩展的语法分析器"""
    
    def __init__(self, tokens: List[Token]):
        """
        初始化扩展语法分析器
        
        Args:
            tokens: 词法分析器产生的Token列表
        """
        self.tokens = tokens
        self.position = 0
        self.current_token = self.tokens[0] if tokens else None
        self.grammar = ExtendedSQLGrammar()
        self.parse_stack = []
        self.parse_steps = []
        self.ast_root = None
    
    def current_token_type(self) -> str:
        """获取当前Token的类型字符串"""
        if self.current_token:
            if self.current_token.type == TokenType.EOF:
                return '$'
            return self.current_token.type.value
        return '$'
    
    def advance(self):
        """前进到下一个Token"""
        if self.position < len(self.tokens) - 1:
            self.position += 1
            self.current_token = self.tokens[self.position]
        else:
            self.current_token = None
    
    def parse(self) -> Optional[ASTNode]:
        """
        执行扩展语法分析
        
        Returns:
            抽象语法树的根节点，如果分析失败则返回None
        """
        try:
            # 初始化分析栈
            self.parse_stack = ['$', self.grammar.start_symbol]
            self.parse_steps = []
            
            print("\\n开始扩展LL(1)语法分析:")
            print("-" * 80)
            print(f"{'步骤':<4} {'分析栈':<40} {'剩余输入':<25} {'动作':<20}")
            print("-" * 80)
            
            step = 1
            
            while len(self.parse_stack) > 1:
                stack_top = self.parse_stack[-1]
                current_input = self.current_token_type()
                
                # 记录当前状态
                stack_display = ' '.join(reversed(self.parse_stack))
                remaining_input = self._get_remaining_input()
                
                if self.grammar.is_terminal(stack_top):
                    # 栈顶是终结符
                    if stack_top == current_input:
                        # 匹配成功
                        action = f"匹配 {stack_top}"
                        self.parse_stack.pop()
                        if current_input != '$':
                            self.advance()
                    elif stack_top == 'ε':
                        # 空产生式
                        action = "匹配 ε"
                        self.parse_stack.pop()
                    else:
                        # 匹配失败
                        raise SyntaxError(
                            f"Expected '{stack_top}', found '{current_input}'",
                            self.current_token.line if self.current_token else 0,
                            self.current_token.column if self.current_token else 0
                        )
                else:
                    # 栈顶是非终结符
                    production = self.grammar.get_production(stack_top, current_input)
                    
                    if production is None:
                        # 尝试使用默认的ε产生式
                        production = self._get_default_production(stack_top, current_input)
                        
                        if production is None:
                            raise SyntaxError(
                                f"No production for {stack_top} with input {current_input}",
                                self.current_token.line if self.current_token else 0,
                                self.current_token.column if self.current_token else 0
                            )
                    
                    # 应用产生式
                    action = f"{stack_top} -> {' '.join(production)}"
                    self.parse_stack.pop()  # 弹出非终结符
                    
                    # 如果产生式右部不是ε，则将其逆序压栈
                    if production != ['ε']:
                        for symbol in reversed(production):
                            self.parse_stack.append(symbol)
                
                # 记录分析步骤
                self.parse_steps.append({
                    'step': step,
                    'stack': stack_display,
                    'input': remaining_input,
                    'action': action
                })
                
                print(f"{step:<4} {stack_display:<40} {remaining_input:<25} {action:<20}")
                step += 1
                
                # 防止死循环
                if step > 100:
                    print("⚠️ 分析步骤过多，可能存在问题")
                    break
            
            # 检查是否成功分析
            if (len(self.parse_stack) == 1 and 
                self.parse_stack[0] == '$' and 
                self.current_token_type() == '$'):
                print("-" * 80)
                print("✅ 扩展语法分析成功!")
                
                # 构建AST
                self.ast_root = self._build_extended_ast()
                return self.ast_root
            else:
                raise SyntaxError("Unexpected end of input or stack not empty")
                
        except SyntaxError as e:
            print("-" * 80)
            print(f"❌ 扩展语法分析失败: {e}")
            return None
    
    def _get_default_production(self, nonterminal: str, terminal: str):
        """获取默认的ε产生式"""
        # 对于可选子句，返回ε产生式
        optional_clauses = {
            "where_clause": ["ε"],
            "group_by_clause": ["ε"],
            "having_clause": ["ε"],
            "order_by_clause": ["ε"],
            "limit_clause": ["ε"],
            "join_list": ["ε"],
            "table_alias": ["ε"],
            "column_alias": ["ε"],
            "join_type": ["ε"],
            "order_direction": ["ε"]
        }
        
        if nonterminal in optional_clauses:
            return optional_clauses[nonterminal]
        
        # 对于列表的尾部，通常也是ε
        tail_productions = {
            "column_list_tail": ["ε"],
            "group_by_list_tail": ["ε"],
            "order_by_list_tail": ["ε"],
            "value_list_tail": ["ε"],
            "or_condition_tail": ["ε"],
            "and_condition_tail": ["ε"]
        }
        
        if nonterminal in tail_productions:
            return tail_productions[nonterminal]
        
        return None
    
    def _get_remaining_input(self) -> str:
        """获取剩余输入串的显示"""
        remaining = []
        for i in range(self.position, min(self.position + 5, len(self.tokens))):
            token = self.tokens[i]
            if token.type == TokenType.EOF:
                remaining.append('$')
                break
            else:
                remaining.append(token.value)
        return ' '.join(remaining)
    
    def _build_extended_ast(self) -> ASTNode:
        """
        构建扩展的抽象语法树
        """
        # 简化的AST构建，基于Token序列
        root = ASTNode(ASTNodeType.SELECT_STMT)
        
        pos = 0
        
        # 跳过SELECT关键字
        if pos < len(self.tokens) and self.tokens[pos].type == TokenType.SELECT:
            pos += 1
            
            # 检查DISTINCT
            if pos < len(self.tokens) and self.tokens[pos].type == TokenType.DISTINCT:
                distinct_node = ASTNode(ASTNodeType.IDENTIFIER, "DISTINCT")
                root.add_child(distinct_node)
                pos += 1
            
            # 解析列列表
            column_list = ASTNode(ASTNodeType.COLUMN_LIST)
            pos = self._parse_column_list(pos, column_list)
            root.add_child(column_list)
            
            # 解析FROM子句
            if pos < len(self.tokens) and self.tokens[pos].type == TokenType.FROM:
                pos += 1
                
                # 解析表名
                if pos < len(self.tokens) and self.tokens[pos].type == TokenType.IDENTIFIER:
                    table_node = ASTNode(ASTNodeType.TABLE_NAME, self.tokens[pos].value)
                    root.add_child(table_node)
                    pos += 1
                
                # 解析表别名（如果存在）
                if pos < len(self.tokens) and self.tokens[pos].type == TokenType.IDENTIFIER:
                    # 表别名
                    pos += 1
                
                # 解析JOIN子句
                pos = self._parse_joins(pos, root)
            
            # 解析WHERE子句
            pos = self._parse_where_clause(pos, root)
            
            # 解析GROUP BY子句
            pos = self._parse_group_by_clause(pos, root)
            
            # 解析HAVING子句
            pos = self._parse_having_clause(pos, root)
            
            # 解析ORDER BY子句
            pos = self._parse_order_by_clause(pos, root)
            
            # 解析LIMIT/OFFSET子句
            pos = self._parse_limit_clause(pos, root)
        
        return root
    
    def _parse_column_list(self, pos: int, column_list: ASTNode) -> int:
        """解析列列表"""
        while pos < len(self.tokens) and self.tokens[pos].type != TokenType.FROM:
            token = self.tokens[pos]
            
            if token.type == TokenType.ASTERISK:
                col_node = ASTNode(ASTNodeType.IDENTIFIER, "*")
                column_list.add_child(col_node)
                pos += 1
            elif token.type == TokenType.IDENTIFIER:
                # 检查是否是表别名.列名的形式
                if (pos + 2 < len(self.tokens) and 
                    self.tokens[pos + 1].type == TokenType.DOT and
                    self.tokens[pos + 2].type == TokenType.IDENTIFIER):
                    # 表别名.列名形式
                    table_alias = self.tokens[pos].value
                    column_name = self.tokens[pos + 2].value
                    col_ref = f"{table_alias}.{column_name}"
                    col_node = ASTNode(ASTNodeType.COLUMN_REF, col_ref)
                    column_list.add_child(col_node)
                    pos += 3  # 跳过表别名、点号和列名
                else:
                    # 简单的列名
                    col_node = ASTNode(ASTNodeType.IDENTIFIER, token.value)
                    column_list.add_child(col_node)
                    pos += 1
            elif token.type in [TokenType.COUNT, TokenType.SUM, TokenType.AVG, TokenType.MAX, TokenType.MIN]:
                # 聚合函数
                agg_node = ASTNode(ASTNodeType.AGGREGATE_FUNCTION, token.value)
                column_list.add_child(agg_node)
                pos += 1
                
                # 解析括号和参数
                if pos < len(self.tokens) and self.tokens[pos].type == TokenType.LEFT_PAREN:
                    pos += 1  # 跳过左括号
                    
                    # 创建聚合参数节点
                    arg_node = ASTNode(ASTNodeType.AGGREGATE_ARG)
                    agg_node.add_child(arg_node)
                    
                    # 解析参数
                    if pos < len(self.tokens):
                        if self.tokens[pos].type == TokenType.ASTERISK:
                            # 处理 * 参数
                            star_node = ASTNode(ASTNodeType.IDENTIFIER, "*")
                            arg_node.add_child(star_node)
                            pos += 1
                        elif self.tokens[pos].type == TokenType.IDENTIFIER:
                            # 处理列名参数
                            col_ref_node = ASTNode(ASTNodeType.IDENTIFIER, self.tokens[pos].value)
                            arg_node.add_child(col_ref_node)
                            pos += 1
                    
                    # 跳过右括号
                    if pos < len(self.tokens) and self.tokens[pos].type == TokenType.RIGHT_PAREN:
                        pos += 1
            elif token.type == TokenType.COMMA:
                pos += 1
            else:
                pos += 1
        
        return pos
    
    def _parse_joins(self, pos: int, root: ASTNode) -> int:
        """解析JOIN子句"""
        while pos < len(self.tokens):
            token = self.tokens[pos]
            
            # 检查JOIN关键字
            if token.type == TokenType.JOIN:
                join_node = ASTNode(ASTNodeType.JOIN_CLAUSE)
                root.add_child(join_node)
                pos += 1
                
                # 解析表名
                if pos < len(self.tokens) and self.tokens[pos].type == TokenType.IDENTIFIER:
                    table_node = ASTNode(ASTNodeType.TABLE_NAME, self.tokens[pos].value)
                    join_node.add_child(table_node)
                    pos += 1
                
                # 解析表别名（如果存在）
                if pos < len(self.tokens) and self.tokens[pos].type == TokenType.IDENTIFIER:
                    # 表别名
                    pos += 1
                
                # 解析ON子句
                if pos < len(self.tokens) and self.tokens[pos].type == TokenType.ON:
                    pos += 1
                    on_node = ASTNode(ASTNodeType.ON_CLAUSE)
                    join_node.add_child(on_node)
                    
                    # 解析连接条件
                    condition_start = pos
                    while (pos < len(self.tokens) and 
                           self.tokens[pos].type not in [TokenType.WHERE, TokenType.GROUP, 
                                                        TokenType.ORDER, TokenType.SEMICOLON, TokenType.JOIN]):
                        pos += 1
                    
                    # 构造条件字符串
                    condition_tokens = self.tokens[condition_start:pos]
                    condition_str = ' '.join(token.value for token in condition_tokens)
                    condition_node = ASTNode(ASTNodeType.CONDITION, condition_str)
                    on_node.add_child(condition_node)
            elif token.type in [TokenType.INNER, TokenType.LEFT, TokenType.RIGHT, TokenType.FULL]:
                # JOIN类型，继续处理
                join_type_node = ASTNode(ASTNodeType.IDENTIFIER, token.value)
                root.add_child(join_type_node)
                pos += 1
            else:
                break
        
        return pos
    
    def _parse_where_clause(self, pos: int, root: ASTNode) -> int:
        """解析WHERE子句"""
        if pos < len(self.tokens) and self.tokens[pos].type == TokenType.WHERE:
            pos += 1
            where_node = ASTNode(ASTNodeType.WHERE_CLAUSE)
            
            # 简化的条件解析
            while (pos < len(self.tokens) and 
                   self.tokens[pos].type not in [TokenType.GROUP, TokenType.ORDER, TokenType.SEMICOLON]):
                pos += 1
            
            root.add_child(where_node)
        
        return pos
    
    def _parse_group_by_clause(self, pos: int, root: ASTNode) -> int:
        """解析GROUP BY子句"""
        if (pos < len(self.tokens) and 
            self.tokens[pos].type == TokenType.GROUP and
            pos + 1 < len(self.tokens) and
            self.tokens[pos + 1].type == TokenType.BY):
            
            pos += 2
            group_node = ASTNode(ASTNodeType.GROUP_BY_CLAUSE)
            
            # 解析列列表
            while (pos < len(self.tokens) and 
                   self.tokens[pos].type not in [TokenType.HAVING, TokenType.ORDER, TokenType.SEMICOLON]):
                if self.tokens[pos].type == TokenType.IDENTIFIER:
                    # 检查是否是表别名.列名的形式
                    if (pos + 2 < len(self.tokens) and 
                        self.tokens[pos + 1].type == TokenType.DOT and
                        self.tokens[pos + 2].type == TokenType.IDENTIFIER):
                        # 表别名.列名形式
                        table_alias = self.tokens[pos].value
                        column_name = self.tokens[pos + 2].value
                        col_ref = f"{table_alias}.{column_name}"
                        col_node = ASTNode(ASTNodeType.COLUMN_REF, col_ref)
                        group_node.add_child(col_node)
                        pos += 3  # 跳过表别名、点号和列名
                    else:
                        col_node = ASTNode(ASTNodeType.IDENTIFIER, self.tokens[pos].value)
                        group_node.add_child(col_node)
                        pos += 1
                else:
                    pos += 1
            
            root.add_child(group_node)
            
            # 处理HAVING子句
            if pos < len(self.tokens) and self.tokens[pos].type == TokenType.HAVING:
                pos += 1
                having_node = ASTNode(ASTNodeType.HAVING_CLAUSE)
                root.add_child(having_node)
                
                # 跳过HAVING条件
                while (pos < len(self.tokens) and 
                       self.tokens[pos].type not in [TokenType.ORDER, TokenType.SEMICOLON]):
                    pos += 1
        
        return pos
    
    def _parse_order_by_clause(self, pos: int, root: ASTNode) -> int:
        """解析ORDER BY子句"""
        if (pos < len(self.tokens) and 
            self.tokens[pos].type == TokenType.ORDER and
            pos + 1 < len(self.tokens) and
            self.tokens[pos + 1].type == TokenType.BY):
            
            pos += 2
            order_node = ASTNode(ASTNodeType.ORDER_BY_CLAUSE)
            
            # 解析排序列表
            order_list_node = ASTNode(ASTNodeType.ORDER_BY_LIST)
            order_node.add_child(order_list_node)
            
            while (pos < len(self.tokens) and 
                   self.tokens[pos].type != TokenType.SEMICOLON):
                if self.tokens[pos].type == TokenType.IDENTIFIER:
                    # 检查是否是表别名.列名的形式
                    if (pos + 2 < len(self.tokens) and 
                        self.tokens[pos + 1].type == TokenType.DOT and
                        self.tokens[pos + 2].type == TokenType.IDENTIFIER):
                        # 表别名.列名形式
                        order_spec = ASTNode(ASTNodeType.ORDER_BY_SPEC)
                        table_alias = self.tokens[pos].value
                        column_name = self.tokens[pos + 2].value
                        col_ref = f"{table_alias}.{column_name}"
                        column_node = ASTNode(ASTNodeType.COLUMN_REF, col_ref)
                        order_spec.add_child(column_node)
                        pos += 3  # 跳过表别名、点号和列名
                        
                        # 检查排序方向
                        direction = "ASC"  # 默认升序
                        if (pos < len(self.tokens) and 
                            self.tokens[pos].type in [TokenType.ASC, TokenType.DESC]):
                            direction = self.tokens[pos].value.upper()
                            pos += 1
                        
                        direction_node = ASTNode(ASTNodeType.ORDER_SPEC, direction)
                        order_spec.add_child(direction_node)
                        order_list_node.add_child(order_spec)
                    else:
                        # 处理列名
                        order_spec = ASTNode(ASTNodeType.ORDER_BY_SPEC)
                        column_node = ASTNode(ASTNodeType.COLUMN_REF, self.tokens[pos].value)
                        order_spec.add_child(column_node)
                        pos += 1
                        
                        # 检查排序方向
                        direction = "ASC"  # 默认升序
                        if (pos < len(self.tokens) and 
                            self.tokens[pos].type in [TokenType.ASC, TokenType.DESC]):
                            direction = self.tokens[pos].value.upper()
                            pos += 1
                        
                        direction_node = ASTNode(ASTNodeType.ORDER_SPEC, direction)
                        order_spec.add_child(direction_node)
                        order_list_node.add_child(order_spec)
                elif self.tokens[pos].type == TokenType.COMMA:
                    pos += 1
                else:
                    # 处理聚合函数
                    if self.tokens[pos].type in [TokenType.COUNT, TokenType.SUM, TokenType.AVG, TokenType.MAX, TokenType.MIN]:
                        order_spec = ASTNode(ASTNodeType.ORDER_BY_SPEC)
                        agg_node = ASTNode(ASTNodeType.AGGREGATE_FUNCTION, self.tokens[pos].value)
                        order_spec.add_child(agg_node)
                        pos += 1
                        
                        # 解析聚合函数参数
                        if pos < len(self.tokens) and self.tokens[pos].type == TokenType.LEFT_PAREN:
                            pos += 1  # 跳过左括号
                            
                            # 解析参数
                            if pos < len(self.tokens):
                                if self.tokens[pos].type == TokenType.ASTERISK:
                                    # 处理 * 参数
                                    star_node = ASTNode(ASTNodeType.IDENTIFIER, "*")
                                    agg_node.add_child(star_node)
                                    pos += 1
                                elif self.tokens[pos].type == TokenType.IDENTIFIER:
                                    # 处理列名参数
                                    col_ref_node = ASTNode(ASTNodeType.IDENTIFIER, self.tokens[pos].value)
                                    agg_node.add_child(col_ref_node)
                                    pos += 1
                            
                            # 跳过右括号
                            if pos < len(self.tokens) and self.tokens[pos].type == TokenType.RIGHT_PAREN:
                                pos += 1
                        
                        # 检查排序方向
                        direction = "ASC"  # 默认升序
                        if (pos < len(self.tokens) and 
                            self.tokens[pos].type in [TokenType.ASC, TokenType.DESC]):
                            direction = self.tokens[pos].value.upper()
                            pos += 1
                        
                        direction_node = ASTNode(ASTNodeType.ORDER_SPEC, direction)
                        order_spec.add_child(direction_node)
                        order_list_node.add_child(order_spec)
                    else:
                        pos += 1
            
            root.add_child(order_node)
        
        return pos
    
    def _parse_limit_clause(self, pos: int, root: ASTNode) -> int:
        """解析LIMIT/OFFSET子句"""
        limit_node = ASTNode(ASTNodeType.LIMIT_CLAUSE)
        
        # 解析LIMIT
        if pos < len(self.tokens) and self.tokens[pos].type == TokenType.LIMIT:
            pos += 1
            if pos < len(self.tokens) and self.tokens[pos].type == TokenType.NUMBER:
                limit_value = self.tokens[pos].value
                limit_node.add_child(ASTNode(ASTNodeType.LIMIT_VALUE, limit_value))
                pos += 1
                
                # 检查是否有OFFSET
                if pos < len(self.tokens) and self.tokens[pos].type == TokenType.OFFSET:
                    pos += 1
                    if pos < len(self.tokens) and self.tokens[pos].type == TokenType.NUMBER:
                        offset_value = self.tokens[pos].value
                        limit_node.add_child(ASTNode(ASTNodeType.OFFSET_VALUE, offset_value))
                        pos += 1
        # 只有OFFSET的情况
        elif pos < len(self.tokens) and self.tokens[pos].type == TokenType.OFFSET:
            pos += 1
            if pos < len(self.tokens) and self.tokens[pos].type == TokenType.NUMBER:
                offset_value = self.tokens[pos].value
                limit_node.add_child(ASTNode(ASTNodeType.OFFSET_VALUE, offset_value))
                pos += 1
        
        # 只有当有LIMIT/OFFSET时才添加节点
        if limit_node.children:
            root.add_child(limit_node)
        
        return pos
    
    def _parse_having_clause(self, pos: int, root: ASTNode) -> int:
        """解析HAVING子句"""
        if pos < len(self.tokens) and self.tokens[pos].type == TokenType.HAVING:
            pos += 1
            having_node = ASTNode(ASTNodeType.HAVING_CLAUSE)
            
            # 解析HAVING条件
            condition_start = pos
            while (pos < len(self.tokens) and 
                   self.tokens[pos].type not in [TokenType.ORDER, TokenType.LIMIT, TokenType.OFFSET, TokenType.SEMICOLON]):
                pos += 1
            
            # 构造条件字符串
            if pos > condition_start:
                condition_tokens = self.tokens[condition_start:pos]
                condition_str = ' '.join(token.value for token in condition_tokens)
                condition_node = ASTNode(ASTNodeType.CONDITION, condition_str)
                having_node.add_child(condition_node)
            
            root.add_child(having_node)
        
        return pos
    
    def print_parse_steps(self):
        """打印分析步骤"""
        if not self.parse_steps:
            print("无分析步骤记录")
            return
        
        print("\\n详细分析步骤:")
        print("-" * 100)
        print(f"{'步骤':<4} {'分析栈':<40} {'剩余输入':<25} {'动作':<30}")
        print("-" * 100)
        
        for step_info in self.parse_steps:
            print(f"{step_info['step']:<4} {step_info['stack']:<40} "
                  f"{step_info['input']:<25} {step_info['action']:<30}")
    
    def print_ast(self):
        """打印扩展的抽象语法树"""
        if self.ast_root:
            print("\\n扩展抽象语法树:")
            print("-" * 50)
            print(self.ast_root)
        else:
            print("\\n无抽象语法树")

def test_extended_parser():
    """测试扩展语法分析器"""
    from src.compiler.lexer.lexer import Lexer
    
    test_cases = [
        # 基本查询
        "SELECT name FROM students;",
        
        # 聚合函数
        "SELECT COUNT(*), AVG(grade) FROM students;",
        
        # JOIN查询
        "SELECT s.name, c.course_name FROM students s JOIN courses c ON s.id = c.student_id;",
        
        # GROUP BY
        "SELECT major, COUNT(*) FROM students GROUP BY major;",
        
        # ORDER BY
        "SELECT name, grade FROM students ORDER BY grade DESC;",
        
        # 复杂查询
        "SELECT s.name, AVG(g.grade) FROM students s JOIN grades g ON s.id = g.student_id GROUP BY s.name HAVING AVG(g.grade) > 85 ORDER BY AVG(g.grade) DESC;"
    ]
    
    print("=" * 80)
    print("                    扩展语法分析器测试")
    print("=" * 80)
    
    for i, sql in enumerate(test_cases, 1):
        print(f"\\n\\n测试用例 {i}: {sql}")
        print("=" * 80)
        
        try:
            # 词法分析
            lexer = Lexer(sql)
            tokens = lexer.tokenize()
            
            print("Tokens:", [f"{token.type.value}('{token.value}')" 
                           for token in tokens if token.type != TokenType.EOF])
            
            # 扩展语法分析
            parser = ExtendedParser(tokens)
            ast = parser.parse()
            
            if ast:
                parser.print_ast()
            
        except Exception as e:
            print(f"❌ 错误: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    test_extended_parser()