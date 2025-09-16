"""
扩展的SQL语法规则定义
支持JOIN、聚合函数、ORDER BY、GROUP BY等复杂查询语法
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.common.types import TokenType

class ExtendedSQLGrammar:
    """扩展的SQL语法规则"""
    
    def __init__(self):
        """初始化扩展语法规则"""
        self.start_symbol = "sql_statement"
        
        # 扩展的语法规则
        self.productions = {
            # 基本SQL语句
            "sql_statement": [
                ["select_statement"]
            ],
            
            # 扩展的SELECT语句（添加LIMIT/OFFSET支持）
            "select_statement": [
                ["SELECT", "select_list", "from_clause", "where_clause", "group_by_clause", "having_clause", "order_by_clause", "limit_clause", ";"]
            ],
            
            # SELECT列表（支持聚合函数）
            "select_list": [
                ["DISTINCT", "column_list"],
                ["column_list"]
            ],
            
            "column_list": [
                ["column_spec", "column_list_tail"]
            ],
            
            # 修改column_list_tail的定义，添加对列别名的支持
            "column_list_tail": [
                [",", "column_spec", "column_list_tail"],
                ["column_alias", "column_list_tail"],  # 添加对列别名的支持
                ["ε"]
            ],
            
            "column_spec": [
                ["aggregate_function"],
                ["table_column"],
                ["*"]
            ],
            
            # 聚合函数
            "aggregate_function": [
                ["COUNT", "(", "aggregate_arg", ")"],
                ["SUM", "(", "column_ref", ")"],
                ["AVG", "(", "column_ref", ")"],
                ["MAX", "(", "column_ref", ")"],
                ["MIN", "(", "column_ref", ")"]
            ],
            
            "aggregate_arg": [
                ["*"],
                ["DISTINCT", "column_ref"],
                ["column_ref"]
            ],
            
            # 表和列引用
            "table_column": [
                ["column_ref", "column_alias"]
            ],
            
            "column_ref": [
                ["table_ref", ".", "IDENTIFIER"],
                ["IDENTIFIER"]
            ],
            
            "column_alias": [
                ["AS", "IDENTIFIER"],
                ["IDENTIFIER"],
                ["ε"]
            ],
            
            "table_ref": [
                ["IDENTIFIER"]
            ],
            
            # FROM子句（支持JOIN）
            "from_clause": [
                ["FROM", "table_list"]
            ],
            
            "table_list": [
                ["table_spec", "join_list"]
            ],
            
            "table_spec": [
                ["table_name", "table_alias"]
            ],
            
            "table_name": [
                ["IDENTIFIER"]
            ],
            
            "table_alias": [
                ["AS", "IDENTIFIER"],
                ["IDENTIFIER"],
                ["ε"]
            ],
            
            # JOIN子句
            "join_list": [
                ["join_clause", "join_list"],
                ["ε"]
            ],
            
            "join_clause": [
                ["join_type", "JOIN", "table_spec", "on_clause"]
            ],
            
            "join_type": [
                ["INNER"],
                ["LEFT", "OUTER"],
                ["LEFT"],
                ["RIGHT", "OUTER"],
                ["RIGHT"],
                ["FULL", "OUTER"],
                ["FULL"],
                ["ε"]
            ],
            
            "on_clause": [
                ["ON", "join_condition"]
            ],
            
            "join_condition": [
                ["column_ref", "comparison_op", "column_ref"]
            ],
            
            # WHERE子句
            "where_clause": [
                ["WHERE", "condition"],
                ["ε"]
            ],
            
            "condition": [
                ["or_condition"]
            ],
            
            "or_condition": [
                ["and_condition", "or_condition_tail"]
            ],
            
            "or_condition_tail": [
                ["OR", "and_condition", "or_condition_tail"],
                ["ε"]
            ],
            
            "and_condition": [
                ["simple_condition", "and_condition_tail"]
            ],
            
            "and_condition_tail": [
                ["AND", "simple_condition", "and_condition_tail"],
                ["ε"]
            ],
            
            "simple_condition": [
                ["column_ref", "comparison_op", "operand"],
                ["aggregate_function", "comparison_op", "operand"],
                ["column_ref", "IN", "(", "value_list", ")"],
                ["EXISTS", "(", "subquery", ")"]
            ],
            
            "operand": [
                ["column_ref"],
                ["literal"],
                ["(", "subquery", ")"]
            ],
            
            "literal": [
                ["NUMBER"],
                ["STRING"]
            ],
            
            "comparison_op": [
                [">"],
                [">="],
                ["<"],
                ["<="],
                ["="],
                ["<>"]
            ],
            
            # 子查询
            "subquery": [
                ["select_statement"]
            ],
            
            "value_list": [
                ["literal", "value_list_tail"]
            ],
            
            "value_list_tail": [
                [",", "literal", "value_list_tail"],
                ["ε"]
            ],
            
            # GROUP BY子句
            "group_by_clause": [
                ["GROUP", "BY", "group_by_list"],
                ["ε"]
            ],
            
            "group_by_list": [
                ["column_ref", "group_by_list_tail"]
            ],
            
            "group_by_list_tail": [
                [",", "column_ref", "group_by_list_tail"],
                ["ε"]
            ],
            
            # HAVING子句
            "having_clause": [
                ["HAVING", "condition"],
                ["ε"]
            ],
            
            # ORDER BY子句
            "order_by_clause": [
                ["ORDER", "BY", "order_by_list"],
                ["ε"]
            ],
            
            "order_by_list": [
                ["order_by_spec", "order_by_list_tail"]
            ],
            
            "order_by_list_tail": [
                [",", "order_by_spec", "order_by_list_tail"],
                ["ε"]
            ],
            
            "order_by_spec": [
                ["column_ref", "order_direction"]
            ],
            
            "order_direction": [
                ["ASC"],
                ["DESC"],
                ["ε"]
            ],
            
            # LIMIT/OFFSET子句（修改为LL(1)兼容形式）
            "limit_clause": [
                ["limit_spec", "limit_clause_tail"],
                ["ε"]
            ],
            
            "limit_spec": [
                ["LIMIT", "NUMBER"]
            ],
            
            "limit_clause_tail": [
                ["OFFSET", "NUMBER"],
                ["ε"]
            ]
        }
        
        # 终结符集合
        self.terminals = {
            # SQL关键字
            "SELECT", "FROM", "WHERE", "AND", "OR", "NOT",
            "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "FULL", "ON",
            "GROUP", "BY", "HAVING", "ORDER", "ASC", "DESC",
            "COUNT", "SUM", "AVG", "MAX", "MIN",
            "DISTINCT", "AS", "IN", "EXISTS", "ALL", "ANY", "SOME",
            "UNION", "INTERSECT", "EXCEPT",
            
            # 标识符和字面值
            "IDENTIFIER", "NUMBER", "STRING",
            
            # 运算符
            ">", ">=", "<", "<=", "=", "<>",
            
            # 分隔符
            ",", ";", "(", ")", "*", ".",
            
            # 特殊符号
            "ε", "$"
        }
        
        # 更新终结符集合，添加LIMIT和OFFSET
        self.terminals.update({"LIMIT", "OFFSET"})
        
        # 非终结符集合
        self.nonterminals = set(self.productions.keys())
        
        # 构建预测分析表（简化版本）
        self._build_parsing_table()
        # 调用添加更多预测分析表项的方法
        self._add_more_parsing_entries()
    
    def _build_parsing_table(self):
        """构建预测分析表（简化实现）"""
        self.parsing_table = {}
        
        # 为每个非终结符和终结符组合定义产生式
        # 这里使用简化的手工构建方式
        
        # sql_statement的预测分析表项
        self.parsing_table[("sql_statement", "SELECT")] = ["select_statement"]
        
        # select_statement的预测分析表项
        self.parsing_table[("select_statement", "SELECT")] = [
            "SELECT", "select_list", "from_clause", "where_clause", 
            "group_by_clause", "having_clause", "order_by_clause", "limit_clause", ";"
        ]
        
        # select_list的预测分析表项
        self.parsing_table[("select_list", "DISTINCT")] = ["column_list"]
        self.parsing_table[("select_list", "*")] = ["column_list"]
        self.parsing_table[("select_list", "IDENTIFIER")] = ["column_list"]
        for func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("select_list", func)] = ["column_list"]
        
        # column_list的预测分析表项
        for token in ["*", "IDENTIFIER", "COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("column_list", token)] = ["column_spec", "column_list_tail"]
        
        # column_list_tail的预测分析表项
        self.parsing_table[("column_list_tail", ",")] = [",", "column_spec", "column_list_tail"]
        # 修复：当遇到AS时，应该处理column_alias，然后继续处理column_list_tail
        self.parsing_table[("column_list_tail", "AS")] = ["column_alias", "column_list_tail"]
        # 添加对点号的处理
        self.parsing_table[("column_list_tail", ".")] = ["ε"]
        for token in ["FROM", "WHERE", "GROUP", "HAVING", "ORDER", ";"]:
            self.parsing_table[("column_list_tail", token)] = ["ε"]
        
        # column_spec的预测分析表项
        self.parsing_table[("column_spec", "*")] = ["*"]
        self.parsing_table[("column_spec", "IDENTIFIER")] = ["table_column"]
        for func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("column_spec", func)] = ["aggregate_function"]
        
        # column_alias
        self.parsing_table[("column_alias", "AS")] = ["AS", "IDENTIFIER"]
        self.parsing_table[("column_alias", "IDENTIFIER")] = ["IDENTIFIER"]
        for token in [",", "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", ";"]:
            self.parsing_table[("column_alias", token)] = ["ε"]
        
        # table_column
        self.parsing_table[("table_column", "IDENTIFIER")] = ["column_ref", "column_alias"]
        
        # table_ref - 修复：处理表引用
        self.parsing_table[("table_ref", "IDENTIFIER")] = ["IDENTIFIER"]
        
        # column_ref - 修复：处理点号（表别名.列名）和简单标识符
        # 当前符号是IDENTIFIER时，我们需要特殊处理
        # 由于LL(1)的限制，我们不能前瞻多个符号
        # 所以我们需要修改语法结构来处理这种情况
        
        # 当遇到点号时，应该使用表别名.列名的形式
        self.parsing_table[("column_ref", ".")] = ["table_ref", ".", "IDENTIFIER"]
        
        # 当遇到其他分隔符时，使用简单标识符形式
        for token in [",", "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", ";", ")", "=", "ON"]:
            self.parsing_table[("column_ref", token)] = ["IDENTIFIER"]
            
        # 关键修改：当遇到IDENTIFIER时，默认假设是简单标识符
        # 但是在实际解析时，如果下一个符号是点号，我们需要特殊处理
        # 这需要在解析器中进行特殊处理，而不是在预测分析表中
        self.parsing_table[("column_ref", "IDENTIFIER")] = ["IDENTIFIER"]
        
        # aggregate_function
        for func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("aggregate_function", func)] = [func, "(", "aggregate_arg", ")"]
        
        # aggregate_arg
        self.parsing_table[("aggregate_arg", "*")] = ["*"]
        self.parsing_table[("aggregate_arg", "DISTINCT")] = ["DISTINCT", "column_ref"]
        self.parsing_table[("aggregate_arg", "IDENTIFIER")] = ["column_ref"]
        
        # join_list
        for token in ["INNER", "LEFT", "RIGHT", "FULL", "JOIN"]:
            self.parsing_table[("join_list", token)] = ["join_clause", "join_list"]
        for token in ["WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", ";"]:
            self.parsing_table[("join_list", token)] = ["ε"]
        
        # join_clause
        for token in ["INNER", "LEFT", "RIGHT", "FULL", "JOIN"]:
            self.parsing_table[("join_clause", token)] = ["join_type", "JOIN", "table_spec", "on_clause"]
        
        # join_type
        self.parsing_table[("join_type", "INNER")] = ["INNER"]
        self.parsing_table[("join_type", "LEFT")] = ["LEFT"]
        self.parsing_table[("join_type", "RIGHT")] = ["RIGHT"]
        self.parsing_table[("join_type", "FULL")] = ["FULL"]
        self.parsing_table[("join_type", "JOIN")] = ["ε"]
        
        # on_clause
        self.parsing_table[("on_clause", "ON")] = ["ON", "join_condition"]
        
        # join_condition
        self.parsing_table[("join_condition", "IDENTIFIER")] = ["column_ref", "comparison_op", "column_ref"]
        
        # comparison_op
        for op in [">", ">=", "<", "<=", "=", "<>"]:
            self.parsing_table[("comparison_op", op)] = [op]
        
        # where_clause
        self.parsing_table[("where_clause", "WHERE")] = ["WHERE", "condition"]
        for token in ["GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", ";"]:
            self.parsing_table[("where_clause", token)] = ["ε"]
        
        # condition
        self.parsing_table[("condition", "IDENTIFIER")] = ["or_condition"]
        for agg_func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("condition", agg_func)] = ["or_condition"]
        
        # or_condition
        self.parsing_table[("or_condition", "IDENTIFIER")] = ["and_condition", "or_condition_tail"]
        for agg_func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("or_condition", agg_func)] = ["and_condition", "or_condition_tail"]
        
        # or_condition_tail
        self.parsing_table[("or_condition_tail", "OR")] = ["OR", "and_condition", "or_condition_tail"]
        for token in ["GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", ";", ")"]:
            self.parsing_table[("or_condition_tail", token)] = ["ε"]
        
        # and_condition
        self.parsing_table[("and_condition", "IDENTIFIER")] = ["simple_condition", "and_condition_tail"]
        for agg_func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("and_condition", agg_func)] = ["simple_condition", "and_condition_tail"]
        
        # and_condition_tail
        self.parsing_table[("and_condition_tail", "AND")] = ["AND", "simple_condition", "and_condition_tail"]
        for token in ["OR", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", ";", ")"]:
            self.parsing_table[("and_condition_tail", token)] = ["ε"]
        
        # simple_condition
        self.parsing_table[("simple_condition", "IDENTIFIER")] = ["column_ref", "comparison_op", "operand"]
        for agg_func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("simple_condition", agg_func)] = ["aggregate_function", "comparison_op", "operand"]
        
        # operand
        self.parsing_table[("operand", "IDENTIFIER")] = ["column_ref"]
        self.parsing_table[("operand", "NUMBER")] = ["literal"]
        self.parsing_table[("operand", "STRING")] = ["literal"]
        
        # literal
        self.parsing_table[("literal", "NUMBER")] = ["NUMBER"]
        self.parsing_table[("literal", "STRING")] = ["STRING"]
        
        # group_by_clause
        self.parsing_table[("group_by_clause", "GROUP")] = ["GROUP", "BY", "group_by_list"]
        for token in ["HAVING", "ORDER", "LIMIT", "OFFSET", ";"]:
            self.parsing_table[("group_by_clause", token)] = ["ε"]
        
        # group_by_list
        self.parsing_table[("group_by_list", "IDENTIFIER")] = ["column_ref", "group_by_list_tail"]
        
        # group_by_list_tail
        self.parsing_table[("group_by_list_tail", ",")] = [",", "column_ref", "group_by_list_tail"]
        for token in ["HAVING", "ORDER", "LIMIT", "OFFSET", ";"]:
            self.parsing_table[("group_by_list_tail", token)] = ["ε"]
        
        # having_clause
        self.parsing_table[("having_clause", "HAVING")] = ["HAVING", "condition"]
        for token in ["ORDER", "LIMIT", "OFFSET", ";"]:
            self.parsing_table[("having_clause", token)] = ["ε"]
        
        # order_by_clause
        self.parsing_table[("order_by_clause", "ORDER")] = ["ORDER", "BY", "order_by_list"]
        for token in ["LIMIT", "OFFSET", ";"]:
            self.parsing_table[("order_by_clause", token)] = ["ε"]
        
        # order_by_list
        # 支持列引用和聚合函数
        self.parsing_table[("order_by_list", "IDENTIFIER")] = ["order_by_spec", "order_by_list_tail"]
        for func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("order_by_list", func)] = ["order_by_spec", "order_by_list_tail"]
        
        # order_by_list_tail
        self.parsing_table[("order_by_list_tail", ",")] = [",", "order_by_spec", "order_by_list_tail"]
        for token in ["LIMIT", "OFFSET", ";"]:
            self.parsing_table[("order_by_list_tail", token)] = ["ε"]
        
        # order_by_spec
        # 支持列引用和聚合函数
        self.parsing_table[("order_by_spec", "IDENTIFIER")] = ["column_ref", "order_direction"]
        for func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("order_by_spec", func)] = ["aggregate_function", "order_direction"]
        
        # order_direction
        self.parsing_table[("order_direction", "ASC")] = ["ASC"]
        self.parsing_table[("order_direction", "DESC")] = ["DESC"]
        for token in [",", "LIMIT", "OFFSET", ";"]:
            self.parsing_table[("order_direction", token)] = ["ε"]
        
        # limit_clause（更新为新的LL(1)兼容形式）
        self.parsing_table[("limit_clause", "LIMIT")] = ["limit_spec", "limit_clause_tail"]
        for token in ["OFFSET", ";"]:
            self.parsing_table[("limit_clause", token)] = ["ε"]
        
        # limit_spec
        self.parsing_table[("limit_spec", "LIMIT")] = ["LIMIT", "NUMBER"]
        
        # limit_clause_tail
        self.parsing_table[("limit_clause_tail", "OFFSET")] = ["OFFSET", "NUMBER"]
        for token in [";"]:
            self.parsing_table[("limit_clause_tail", token)] = ["ε"]
        
        # 添加对FROM子句的处理
        self.parsing_table[("from_clause", "FROM")] = ["FROM", "table_list"]
        
        # 添加对表列表的处理
        self.parsing_table[("table_list", "IDENTIFIER")] = ["table_spec", "join_list"]
        
        # 添加对表规范的处理
        self.parsing_table[("table_spec", "IDENTIFIER")] = ["table_name", "table_alias"]
        
        # 添加对表名的处理
        self.parsing_table[("table_name", "IDENTIFIER")] = ["IDENTIFIER"]
        
        # 添加对表别名的处理
        self.parsing_table[("table_alias", "AS")] = ["AS", "IDENTIFIER"]
        self.parsing_table[("table_alias", "IDENTIFIER")] = ["IDENTIFIER"]
        for token in ["WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", ";", "JOIN", "INNER", "LEFT", "RIGHT", "FULL"]:
            self.parsing_table[("table_alias", token)] = ["ε"]
    
    def _add_more_parsing_entries(self):
        """添加更多预测分析表项"""
        # from_clause
        self.parsing_table[("from_clause", "FROM")] = ["FROM", "table_list"]
        
        # table_list
        self.parsing_table[("table_list", "IDENTIFIER")] = ["table_spec", "join_list"]
        
        # table_spec
        self.parsing_table[("table_spec", "IDENTIFIER")] = ["table_name", "table_alias"]
        
        # table_name
        self.parsing_table[("table_name", "IDENTIFIER")] = ["IDENTIFIER"]
        
        # table_alias
        self.parsing_table[("table_alias", "AS")] = ["AS", "IDENTIFIER"]
        self.parsing_table[("table_alias", "IDENTIFIER")] = ["IDENTIFIER"]
        for token in ["WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", ";", "JOIN", "INNER", "LEFT", "RIGHT", "FULL"]:
            self.parsing_table[("table_alias", token)] = ["ε"]
        
        # table_ref - 修复：处理表引用
        self.parsing_table[("table_ref", "IDENTIFIER")] = ["IDENTIFIER"]
        
        # column_ref - 修复：处理点号（表别名.列名）和简单标识符
        # 当当前符号是IDENTIFIER时，我们需要查看下一个符号来决定使用哪种形式
        # 但在预测分析表中，我们只能基于当前符号决定
        # 所以我们需要为点号添加一个特殊的处理
        
        # 当遇到点号时，应该使用表别名.列名的形式
        self.parsing_table[("column_ref", ".")] = ["table_ref", ".", "IDENTIFIER"]
        
        # 当遇到其他分隔符时，使用简单标识符形式
        for token in [",", "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", ";", ")", "=", "ON"]:
            self.parsing_table[("column_ref", token)] = ["IDENTIFIER"]
            
        # 默认情况下，当遇到IDENTIFIER时，假设是简单标识符
        # 但在实际解析时，我们会检查下一个符号来决定是否是表别名.列名的形式
        self.parsing_table[("column_ref", "IDENTIFIER")] = ["IDENTIFIER"]
        
        # 添加对点号的处理，确保在各种上下文中都能正确处理
        # 注意：这里的问题是解析器在处理SELECT子句中的列引用时遇到点号
        # 但实际上点号不应该出现在FROM子句的上下文中
        # 我们需要确保column_ref能够正确处理点号
        
        # column_alias
        self.parsing_table[("column_alias", "AS")] = ["AS", "IDENTIFIER"]
        self.parsing_table[("column_alias", "IDENTIFIER")] = ["IDENTIFIER"]
        for token in [",", "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", ";"]:
            self.parsing_table[("column_alias", token)] = ["ε"]
        
        # column_list_tail
        self.parsing_table[("column_list_tail", ",")] = [",", "column_spec", "column_list_tail"]
        # 修复：当遇到AS时，应该处理column_alias，然后继续处理column_list_tail
        self.parsing_table[("column_list_tail", "AS")] = ["column_alias", "column_list_tail"]
        for token in ["FROM", "WHERE", "GROUP", "HAVING", "ORDER", ";"]:
            self.parsing_table[("column_list_tail", token)] = ["ε"]
        
        # column_spec
        self.parsing_table[("column_spec", "*")] = ["*"]
        self.parsing_table[("column_spec", "IDENTIFIER")] = ["table_column"]
        for func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("column_spec", func)] = ["aggregate_function"]
        
        # table_column
        self.parsing_table[("table_column", "IDENTIFIER")] = ["column_ref", "column_alias"]
        
        # aggregate_function
        for func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("aggregate_function", func)] = [func, "(", "aggregate_arg", ")"]
        
        # aggregate_arg
        self.parsing_table[("aggregate_arg", "*")] = ["*"]
        self.parsing_table[("aggregate_arg", "DISTINCT")] = ["DISTINCT", "column_ref"]
        self.parsing_table[("aggregate_arg", "IDENTIFIER")] = ["column_ref"]
        
        # join_list
        for token in ["INNER", "LEFT", "RIGHT", "FULL", "JOIN"]:
            self.parsing_table[("join_list", token)] = ["join_clause", "join_list"]
        for token in ["WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", ";"]:
            self.parsing_table[("join_list", token)] = ["ε"]
        
        # join_clause
        for token in ["INNER", "LEFT", "RIGHT", "FULL", "JOIN"]:
            self.parsing_table[("join_clause", token)] = ["join_type", "JOIN", "table_spec", "on_clause"]
        
        # join_type
        self.parsing_table[("join_type", "INNER")] = ["INNER"]
        self.parsing_table[("join_type", "LEFT")] = ["LEFT"]
        self.parsing_table[("join_type", "RIGHT")] = ["RIGHT"]
        self.parsing_table[("join_type", "FULL")] = ["FULL"]
        self.parsing_table[("join_type", "JOIN")] = ["ε"]
        
        # on_clause
        self.parsing_table[("on_clause", "ON")] = ["ON", "join_condition"]
        
        # join_condition
        self.parsing_table[("join_condition", "IDENTIFIER")] = ["column_ref", "comparison_op", "column_ref"]
        
        # comparison_op
        for op in [">", ">=", "<", "<=", "=", "<>"]:
            self.parsing_table[("comparison_op", op)] = [op]
        
        # where_clause
        self.parsing_table[("where_clause", "WHERE")] = ["WHERE", "condition"]
        for token in ["GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", ";"]:
            self.parsing_table[("where_clause", token)] = ["ε"]
        
        # condition
        self.parsing_table[("condition", "IDENTIFIER")] = ["or_condition"]
        for agg_func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("condition", agg_func)] = ["or_condition"]
        
        # or_condition
        self.parsing_table[("or_condition", "IDENTIFIER")] = ["and_condition", "or_condition_tail"]
        for agg_func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("or_condition", agg_func)] = ["and_condition", "or_condition_tail"]
        
        # or_condition_tail
        self.parsing_table[("or_condition_tail", "OR")] = ["OR", "and_condition", "or_condition_tail"]
        for token in ["GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", ";", ")"]:
            self.parsing_table[("or_condition_tail", token)] = ["ε"]
        
        # and_condition
        self.parsing_table[("and_condition", "IDENTIFIER")] = ["simple_condition", "and_condition_tail"]
        for agg_func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("and_condition", agg_func)] = ["simple_condition", "and_condition_tail"]
        
        # and_condition_tail
        self.parsing_table[("and_condition_tail", "AND")] = ["AND", "simple_condition", "and_condition_tail"]
        for token in ["OR", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", ";", ")"]:
            self.parsing_table[("and_condition_tail", token)] = ["ε"]
        
        # simple_condition
        self.parsing_table[("simple_condition", "IDENTIFIER")] = ["column_ref", "comparison_op", "operand"]
        for agg_func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("simple_condition", agg_func)] = ["aggregate_function", "comparison_op", "operand"]
        
        # operand
        self.parsing_table[("operand", "IDENTIFIER")] = ["column_ref"]
        self.parsing_table[("operand", "NUMBER")] = ["literal"]
        self.parsing_table[("operand", "STRING")] = ["literal"]
        
        # literal
        self.parsing_table[("literal", "NUMBER")] = ["NUMBER"]
        self.parsing_table[("literal", "STRING")] = ["STRING"]
        
        # group_by_clause
        self.parsing_table[("group_by_clause", "GROUP")] = ["GROUP", "BY", "group_by_list"]
        for token in ["HAVING", "ORDER", "LIMIT", "OFFSET", ";"]:
            self.parsing_table[("group_by_clause", token)] = ["ε"]
        
        # group_by_list
        self.parsing_table[("group_by_list", "IDENTIFIER")] = ["column_ref", "group_by_list_tail"]
        
        # group_by_list_tail
        self.parsing_table[("group_by_list_tail", ",")] = [",", "column_ref", "group_by_list_tail"]
        for token in ["HAVING", "ORDER", "LIMIT", "OFFSET", ";"]:
            self.parsing_table[("group_by_list_tail", token)] = ["ε"]
        
        # having_clause
        self.parsing_table[("having_clause", "HAVING")] = ["HAVING", "condition"]
        for token in ["ORDER", "LIMIT", "OFFSET", ";"]:
            self.parsing_table[("having_clause", token)] = ["ε"]
        
        # order_by_clause
        self.parsing_table[("order_by_clause", "ORDER")] = ["ORDER", "BY", "order_by_list"]
        for token in ["LIMIT", "OFFSET", ";"]:
            self.parsing_table[("order_by_clause", token)] = ["ε"]
        
        # order_by_list
        # 支持列引用和聚合函数
        self.parsing_table[("order_by_list", "IDENTIFIER")] = ["order_by_spec", "order_by_list_tail"]
        for func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("order_by_list", func)] = ["order_by_spec", "order_by_list_tail"]
        
        # order_by_list_tail
        self.parsing_table[("order_by_list_tail", ",")] = [",", "order_by_spec", "order_by_list_tail"]
        for token in ["LIMIT", "OFFSET", ";"]:
            self.parsing_table[("order_by_list_tail", token)] = ["ε"]
        
        # order_by_spec
        # 支持列引用和聚合函数
        self.parsing_table[("order_by_spec", "IDENTIFIER")] = ["column_ref", "order_direction"]
        for func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            self.parsing_table[("order_by_spec", func)] = ["aggregate_function", "order_direction"]
        
        # order_direction
        self.parsing_table[("order_direction", "ASC")] = ["ASC"]
        self.parsing_table[("order_direction", "DESC")] = ["DESC"]
        for token in [",", "LIMIT", "OFFSET", ";"]:
            self.parsing_table[("order_direction", token)] = ["ε"]
        
        # limit_clause（更新为新的LL(1)兼容形式）
        self.parsing_table[("limit_clause", "LIMIT")] = ["limit_spec", "limit_clause_tail"]
        for token in ["OFFSET", ";"]:
            self.parsing_table[("limit_clause", token)] = ["ε"]
        
        # limit_spec
        self.parsing_table[("limit_spec", "LIMIT")] = ["LIMIT", "NUMBER"]
        
        # limit_clause_tail
        self.parsing_table[("limit_clause_tail", "OFFSET")] = ["OFFSET", "NUMBER"]
        for token in [";"]:
            self.parsing_table[("limit_clause_tail", token)] = ["ε"]
    
    def get_production(self, nonterminal: str, terminal: str):
        """获取预测分析表中的产生式"""
        return self.parsing_table.get((nonterminal, terminal))
    
    def is_terminal(self, symbol: str) -> bool:
        """判断符号是否为终结符"""
        return symbol in self.terminals
    
    def is_nonterminal(self, symbol: str) -> bool:
        """判断符号是否为非终结符"""
        return symbol in self.nonterminals

