"""
语法分析器实现
使用LL(1)预测分析法和栈进行语法分析
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from typing import List, Optional
from src.common.types import Token, TokenType, SyntaxError, ASTNode, ASTNodeType
from src.compiler.parser.grammar import SQLGrammar

class Parser:
    """LL(1)语法分析器"""
    
    def __init__(self, tokens: List[Token]):
        """
        初始化语法分析器
        
        Args:
            tokens: 词法分析器产生的Token列表
        """
        self.tokens = tokens
        self.position = 0
        self.current_token = self.tokens[0] if tokens else None
        self.grammar = SQLGrammar()
        self.parse_stack = []
        self.parse_steps = []  # 记录分析步骤
        self.ast_root = None   # 抽象语法树根节点
    
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
        执行语法分析
        
        Returns:
            抽象语法树的根节点，如果分析失败则返回None
        """
        try:
            # 初始化分析栈
            self.parse_stack = ['$', self.grammar.start_symbol]
            self.parse_steps = []
            
            print("\\n开始LL(1)语法分析:")
            print("-" * 80)
            print(f"{'步骤':<4} {'分析栈':<30} {'剩余输入':<25} {'动作':<20}")
            print("-" * 80)
            
            step = 1
            
            while len(self.parse_stack) > 1:  # 栈中还有$以外的符号
                stack_top = self.parse_stack[-1]
                current_input = self.current_token_type()
                
                # 记录当前状态
                stack_display = ' '.join(reversed(self.parse_stack))
                remaining_input = self._get_remaining_input()
                
                if stack_top in self.grammar.terminals:
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
                            f"期待的是 '{stack_top}', 发现的是 '{current_input}'",
                            self.current_token.line if self.current_token else 0,
                            self.current_token.column if self.current_token else 0
                        )
                else:
                    # 栈顶是非终结符
                    production = self.grammar.get_production(stack_top, current_input)
                    
                    if production is None:
                        raise SyntaxError(
                            f"对于非终结符 '{stack_top}' 和输入 '{current_input}'，找不到匹配的语法规则",
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
                    '步骤号': step,
                    '分析栈': stack_display,
                    '剩余输入': remaining_input,
                    '执行动作': action
                })
                
                print(f"{step:<4} {stack_display:<30} {remaining_input:<25} {action:<20}")
                step += 1
            
            # 检查是否成功分析
            if (len(self.parse_stack) == 1 and 
                self.parse_stack[0] == '$' and 
                self.current_token_type() == '$'):
                print("-" * 80)
                print("语法分析成功!")
                
                # 构建简单的AST
                self.ast_root = self._build_ast()
                return self.ast_root
            else:
                raise SyntaxError("超栈或者栈空")
                
        except SyntaxError as e:
            print("-" * 80)
            print(f"语法分析失败: {e}")
            return None
    
    def _get_remaining_input(self) -> str:
        """获取剩余输入串的显示"""
        remaining = []
        for i in range(self.position, len(self.tokens)):
            token = self.tokens[i]
            if token.type == TokenType.EOF:
                remaining.append('$')
                break
            else:
                remaining.append(token.value)
        return ' '.join(remaining)
    
    def _build_ast(self) -> ASTNode:
        """
        根据分析步骤构建抽象语法树
        这是一个简化版本，实际项目中会在分析过程中同步构建
        """
        # 这里是一个简化的AST构建
        # 实际实现中应该在语法分析过程中同步构建
        root = ASTNode(ASTNodeType.SELECT_STMT)
        
        # 重新解析tokens来构建AST（简化实现）
        pos = 0
        if pos < len(self.tokens) and self.tokens[pos].type == TokenType.SELECT:
            pos += 1
            
            # 解析列列表
            column_list = ASTNode(ASTNodeType.COLUMN_LIST)
            while (pos < len(self.tokens) and 
                   self.tokens[pos].type != TokenType.FROM):
                if self.tokens[pos].type == TokenType.IDENTIFIER:
                    col_node = ASTNode(ASTNodeType.IDENTIFIER, self.tokens[pos].value)
                    column_list.add_child(col_node)
                elif self.tokens[pos].type == TokenType.ASTERISK:
                    col_node = ASTNode(ASTNodeType.IDENTIFIER, "*")
                    column_list.add_child(col_node)
                pos += 1
            
            root.add_child(column_list)
            
            # 跳过FROM
            if pos < len(self.tokens) and self.tokens[pos].type == TokenType.FROM:
                pos += 1
                
                # 解析表名
                if (pos < len(self.tokens) and 
                    self.tokens[pos].type == TokenType.IDENTIFIER):
                    table_node = ASTNode(ASTNodeType.TABLE_NAME, self.tokens[pos].value)
                    root.add_child(table_node)
                    pos += 1
            
            # 解析WHERE子句（如果存在）
            if (pos < len(self.tokens) and 
                self.tokens[pos].type == TokenType.WHERE):
                pos += 1
                where_node = ASTNode(ASTNodeType.WHERE_CLAUSE)
                
                # 简化的条件解析
                if pos < len(self.tokens) - 2:  # 至少需要 operand op operand
                    condition = ASTNode(ASTNodeType.CONDITION)
                    
                    # 左操作数
                    if self.tokens[pos].type in [TokenType.IDENTIFIER, TokenType.NUMBER, TokenType.STRING]:
                        left_operand = ASTNode(ASTNodeType.LITERAL, self.tokens[pos].value)
                        condition.add_child(left_operand)
                        pos += 1
                    
                    # 运算符 (现在存储在AST中)
                    if pos < len(self.tokens):
                        operator_node = ASTNode(ASTNodeType.LITERAL, self.tokens[pos].value)
                        condition.add_child(operator_node)
                        pos += 1
                    
                    # 右操作数
                    if (pos < len(self.tokens) and 
                        self.tokens[pos].type in [TokenType.IDENTIFIER, TokenType.NUMBER, TokenType.STRING]):
                        right_operand = ASTNode(ASTNodeType.LITERAL, self.tokens[pos].value)
                        condition.add_child(right_operand)
                    
                    where_node.add_child(condition)
                
                root.add_child(where_node)
        
        return root
    
    def print_parse_steps(self):
        """打印分析步骤"""
        if not self.parse_steps:
            print("无分析步骤记录")
            return
        
        print("\\n详细分析步骤:")
        print("-" * 80)
        print(f"{'步骤':<4} {'分析栈':<30} {'剩余输入':<25} {'动作':<20}")
        print("-" * 80)
        
        for step_info in self.parse_steps:
            print(f"{step_info['step']:<4} {step_info['stack']:<30} "
                  f"{step_info['input']:<25} {step_info['action']:<20}")
    
    def print_ast(self):
        """打印抽象语法树"""
        if self.ast_root:
            print("\\n抽象语法树:")
            print("-" * 40)
            print(self.ast_root)
        else:
            print("\\n无抽象语法树")
