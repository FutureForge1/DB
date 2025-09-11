"""
简化SQL语法规则定义
LL(1)文法规则，消除左递归
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.common.types import TokenType

class SQLGrammar:
    """SQL语法规则定义"""
    
    def __init__(self):
        """初始化文法规则"""
        # 文法规则 (产生式规则)
        # 格式: 左部 -> [右部符号列表]
        self.productions = {
            # 开始符号
            'sql_statement': [
                ['select_statement']
            ],
            
            # SELECT语句
            'select_statement': [
                ['SELECT', 'column_list', 'FROM', 'table_name', 'where_clause', ';']
            ],
            
            # 列列表
            'column_list': [
                ['*'],
                ['column_spec', 'column_list_tail']
            ],
            
            'column_list_tail': [
                ['ε'],  # 空产生式
                [',', 'column_spec', 'column_list_tail']
            ],
            
            'column_spec': [
                ['IDENTIFIER']
            ],
            
            # 表名
            'table_name': [
                ['IDENTIFIER']
            ],
            
            # WHERE子句
            'where_clause': [
                ['ε'],  # WHERE子句是可选的
                ['WHERE', 'condition']
            ],
            
            # 条件表达式
            'condition': [
                ['simple_condition', 'condition_tail']
            ],
            
            'condition_tail': [
                ['ε'],
                ['AND', 'simple_condition', 'condition_tail'],
                ['OR', 'simple_condition', 'condition_tail']
            ],
            
            'simple_condition': [
                ['operand', 'comparison_op', 'operand'],
                ['(', 'condition', ')']
            ],
            
            # 操作数
            'operand': [
                ['IDENTIFIER'],
                ['NUMBER'],
                ['STRING']
            ],
            
            # 比较运算符
            'comparison_op': [
                ['='],
                ['<>'],
                ['<'],
                ['<='],
                ['>'],
                ['>=']
            ]
        }
        
        # 终结符集合
        self.terminals = {
            'SELECT', 'FROM', 'WHERE', 'AND', 'OR',
            'IDENTIFIER', 'NUMBER', 'STRING',
            '=', '<>', '<', '<=', '>', '>=',
            ',', ';', '(', ')', '*', 'ε'
        }
        
        # 非终结符集合
        self.non_terminals = set(self.productions.keys())
        
        # 开始符号
        self.start_symbol = 'sql_statement'
        
        # FIRST集合
        self.first_sets = {}
        
        # FOLLOW集合
        self.follow_sets = {}
        
        # 预测分析表
        self.parsing_table = {}
        
        # 计算FIRST和FOLLOW集合
        self._compute_first_sets()
        self._compute_follow_sets()
        
        # 构建预测分析表
        self._build_parsing_table()
    
    def _compute_first_sets(self):
        """计算所有符号的FIRST集合"""
        # 初始化FIRST集合
        for symbol in self.terminals | self.non_terminals:
            self.first_sets[symbol] = set()
        
        # 终结符的FIRST集合就是自身
        for terminal in self.terminals:
            self.first_sets[terminal] = {terminal}
        
        # 迭代计算非终结符的FIRST集合
        changed = True
        while changed:
            changed = False
            
            for non_terminal in self.non_terminals:
                old_first = self.first_sets[non_terminal].copy()
                
                for production in self.productions[non_terminal]:
                    # 对于产生式 A -> X1 X2 ... Xn
                    for i, symbol in enumerate(production):
                        first_symbol = self.first_sets[symbol].copy()
                        first_symbol.discard('ε')
                        
                        # 将FIRST(Xi) - {ε} 加入FIRST(A)
                        self.first_sets[non_terminal] |= first_symbol
                        
                        # 如果ε不在FIRST(Xi)中，则停止
                        if 'ε' not in self.first_sets[symbol]:
                            break
                        
                        # 如果是最后一个符号且ε在其FIRST中，将ε加入FIRST(A)
                        if i == len(production) - 1:
                            self.first_sets[non_terminal].add('ε')
                
                if old_first != self.first_sets[non_terminal]:
                    changed = True
    
    def _compute_follow_sets(self):
        """计算所有非终结符的FOLLOW集合"""
        # 初始化FOLLOW集合
        for non_terminal in self.non_terminals:
            self.follow_sets[non_terminal] = set()
        
        # 开始符号的FOLLOW集合包含$
        self.follow_sets[self.start_symbol].add('$')
        
        # 迭代计算FOLLOW集合
        changed = True
        while changed:
            changed = False
            
            for non_terminal in self.non_terminals:
                for production in self.productions[non_terminal]:
                    # 对于产生式 A -> αBβ
                    for i, symbol in enumerate(production):
                        if symbol in self.non_terminals:
                            old_follow = self.follow_sets[symbol].copy()
                            
                            # β是B后面的符号串
                            beta = production[i + 1:]
                            
                            if beta:
                                # 计算FIRST(β)
                                first_beta = self._compute_first_of_string(beta)
                                first_beta_no_epsilon = first_beta.copy()
                                first_beta_no_epsilon.discard('ε')
                                
                                # 将FIRST(β) - {ε} 加入FOLLOW(B)
                                self.follow_sets[symbol] |= first_beta_no_epsilon
                                
                                # 如果ε在FIRST(β)中，将FOLLOW(A)加入FOLLOW(B)
                                if 'ε' in first_beta:
                                    self.follow_sets[symbol] |= self.follow_sets[non_terminal]
                            else:
                                # β为空，将FOLLOW(A)加入FOLLOW(B)
                                self.follow_sets[symbol] |= self.follow_sets[non_terminal]
                            
                            if old_follow != self.follow_sets[symbol]:
                                changed = True
    
    def _compute_first_of_string(self, symbols):
        """计算符号串的FIRST集合"""
        if not symbols:
            return {'ε'}
        
        result = set()
        
        for i, symbol in enumerate(symbols):
            first_symbol = self.first_sets[symbol].copy()
            first_symbol.discard('ε')
            result |= first_symbol
            
            if 'ε' not in self.first_sets[symbol]:
                break
            
            # 如果是最后一个符号且ε在其FIRST中
            if i == len(symbols) - 1:
                result.add('ε')
        
        return result
    
    def _build_parsing_table(self):
        """构建LL(1)预测分析表"""
        # 初始化分析表
        for non_terminal in self.non_terminals:
            self.parsing_table[non_terminal] = {}
        
        # 为每个产生式构建表项
        for non_terminal in self.non_terminals:
            for i, production in enumerate(self.productions[non_terminal]):
                # 计算产生式右部的FIRST集合
                first_production = self._compute_first_of_string(production)
                
                # 对于FIRST(α)中的每个终结符a（除了ε）
                for terminal in first_production:
                    if terminal != 'ε':
                        self.parsing_table[non_terminal][terminal] = (i, production)
                
                # 如果ε在FIRST(α)中，对于FOLLOW(A)中的每个终结符b
                if 'ε' in first_production:
                    for terminal in self.follow_sets[non_terminal]:
                        self.parsing_table[non_terminal][terminal] = (i, production)
    
    def print_first_sets(self):
        """打印FIRST集合"""
        print("FIRST集合:")
        print("-" * 50)
        for symbol in sorted(self.first_sets.keys()):
            first_set = sorted(list(self.first_sets[symbol]))
            print(f"FIRST({symbol:15}) = {{{', '.join(first_set)}}}")
    
    def print_follow_sets(self):
        """打印FOLLOW集合"""
        print("\nFOLLOW集合:")
        print("-" * 50)
        for symbol in sorted(self.follow_sets.keys()):
            follow_set = sorted(list(self.follow_sets[symbol]))
            print(f"FOLLOW({symbol:15}) = {{{', '.join(follow_set)}}}")
    
    def print_parsing_table(self):
        """打印预测分析表"""
        print("\n预测分析表:")
        print("-" * 80)
        
        # 获取所有终结符（作为列标题）
        terminals_list = sorted([t for t in self.terminals if t != 'ε'])
        terminals_list.append('$')
        
        # 打印表头
        print(f"{'非终结符':<20}", end="")
        for terminal in terminals_list:
            print(f"{terminal:<12}", end="")
        print()
        print("-" * 80)
        
        # 打印表内容
        for non_terminal in sorted(self.non_terminals):
            print(f"{non_terminal:<20}", end="")
            for terminal in terminals_list:
                if terminal in self.parsing_table[non_terminal]:
                    prod_index, production = self.parsing_table[non_terminal][terminal]
                    prod_str = ' '.join(production)
                    if len(prod_str) > 10:
                        prod_str = prod_str[:7] + "..."
                    print(f"{prod_str:<12}", end="")
                else:
                    print(f"{'error':<12}", end="")
            print()
    
    def get_production(self, non_terminal, terminal):
        """获取预测分析表中的产生式"""
        if (non_terminal in self.parsing_table and 
            terminal in self.parsing_table[non_terminal]):
            return self.parsing_table[non_terminal][terminal][1]
        return None
