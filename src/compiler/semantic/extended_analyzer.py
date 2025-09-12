"""
扩展的语义分析器
支持复杂查询的语义分析和四元式生成，包括JOIN、聚合函数、ORDER BY等
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from typing import List, Optional, Dict, Any
from src.common.types import ASTNode, ASTNodeType, Quadruple, SymbolTable, Symbol, SemanticError

class ExtendedSemanticAnalyzer:
    """扩展的语义分析器"""
    
    def __init__(self):
        """初始化扩展语义分析器"""
        self.symbol_table = SymbolTable()
        self.quadruples: List[Quadruple] = []
        self.temp_counter = 0
        self.label_counter = 0
        
        # 扩展的操作符映射
        self.operators = {
            '>': 'GT',
            '>=': 'GE', 
            '<': 'LT',
            '<=': 'LE',
            '=': 'EQ',
            '<>': 'NE',
            '!=': 'NE'
        }
        
        # 聚合函数映射
        self.aggregate_functions = {
            'COUNT': 'COUNT',
            'SUM': 'SUM',
            'AVG': 'AVG',
            'MAX': 'MAX',
            'MIN': 'MIN'
        }
    
    def _generate_temp(self) -> str:
        """生成临时变量名"""
        self.temp_counter += 1
        return f"T{self.temp_counter}"
    
    def _emit(self, op: str, arg1: Optional[str], arg2: Optional[str], result: Optional[str]):
        """生成四元式"""
        quad = Quadruple(op, arg1, arg2, result)
        self.quadruples.append(quad)
        return quad
    
    def analyze(self, ast: ASTNode) -> List[Quadruple]:
        """
        分析AST并生成四元式
        
        Args:
            ast: 抽象语法树根节点
            
        Returns:
            四元式列表
        """
        self.quadruples = []
        self.temp_counter = 0
        self.label_counter = 0
        
        try:
            print("\\n开始扩展语义分析:")
            print("-" * 50)
            
            if ast.type == ASTNodeType.SELECT_STMT:
                self._analyze_select_statement(ast)
            else:
                raise SemanticError(f"Unsupported statement type: {ast.type}")
            
            print(f"\\n✅ 生成 {len(self.quadruples)} 个四元式")
            for i, quad in enumerate(self.quadruples, 1):
                print(f"  {i}: {quad}")
            
            return self.quadruples
            
        except Exception as e:
            print(f"❌ 语义错误: {e}")
            return []
    
    def _analyze_select_statement(self, node: ASTNode):
        """分析SELECT语句"""
        print("分析SELECT语句...")
        
        # 生成开始标记
        self._emit('BEGIN', None, None, None)
        
        # 处理FROM子句（必须先处理，确定表信息）
        table_info = self._analyze_from_clause(node)
        
        # 处理JOIN子句
        self._analyze_join_clauses(node, table_info)
        
        # 处理WHERE子句
        self._analyze_where_clause(node)
        
        # 处理GROUP BY子句
        group_info = self._analyze_group_by_clause(node)
        
        # 处理HAVING子句
        self._analyze_having_clause(node)
        
        # 处理ORDER BY子句
        self._analyze_order_by_clause(node)
        
        # 处理LIMIT/OFFSET子句
        self._analyze_limit_clause(node)
        
        # 处理SELECT列表（聚合函数需要在扫描表之后处理）
        select_info = self._analyze_select_list(node)
        
        # 生成最终的选择和输出操作
        result_temp = self._generate_temp()
        # 修复：正确传递列信息到SELECT四元式
        self._emit('SELECT', select_info.get('columns', '*'), table_info.get('main_table'), result_temp)
        self._emit('OUTPUT', result_temp, None, None)
        
        # 生成结束标记
        self._emit('END', None, None, None)
    
    def _analyze_from_clause(self, node: ASTNode) -> Dict[str, Any]:
        """分析FROM子句"""
        table_info = {'main_table': None, 'tables': [], 'aliases': {}}
        
        for child in node.children:
            if child.type == ASTNodeType.TABLE_NAME:
                table_name = child.value
                table_info['main_table'] = table_name
                table_info['tables'].append(table_name)
                # 设置当前表
                self.current_table = table_name
                
                print(f"  主表: {table_name}")
                
                # 添加表到符号表
                self.symbol_table.add_symbol(Symbol(
                    name=table_name,
                    type="TABLE",
                    scope="GLOBAL",
                    line=0,
                    column=0
                ))
            elif child.type == ASTNodeType.IDENTIFIER and child.value in ["INNER", "LEFT", "RIGHT", "FULL"]:
                # 处理JOIN类型
                print(f"  JOIN类型: {child.value}")
        
        return table_info
    
    def _analyze_select_list(self, node: ASTNode) -> Dict[str, Any]:
        """分析SELECT列表"""
        select_info = {'columns': [], 'aggregates': [], 'has_aggregates': False}
        
        for child in node.children:
            if child.type == ASTNodeType.COLUMN_LIST:
                print("  分析列列表...")
                self._analyze_column_list(child, select_info)
                break
        
        return select_info
    
    def _analyze_column_list(self, node: ASTNode, select_info: Dict[str, Any]):
        """分析列列表"""
        for child in node.children:
            if child.type == ASTNodeType.COLUMN_REF:  # 修复：处理COLUMN_REF节点
                # 获取列名，可能带表前缀
                column_name = child.value
                if isinstance(select_info['columns'], list):
                    select_info['columns'].append(column_name)
                else:
                    select_info['columns'] = [column_name]
                print(f"    选择列: {column_name}")
            elif child.type == ASTNodeType.IDENTIFIER:
                column_name = child.value
                if column_name == "*":
                    select_info['columns'] = "*"
                    print(f"    选择所有列: *")
                else:
                    if isinstance(select_info['columns'], list):
                        select_info['columns'].append(column_name)
                    else:
                        select_info['columns'] = [column_name]
                    print(f"    选择列: {column_name}")
                    
            elif child.type == ASTNodeType.AGGREGATE_FUNCTION:
                func_name = child.value
                select_info['has_aggregates'] = True
                
                # 生成聚合函数的四元式
                agg_temp = self._generate_temp()
                column_arg = self._get_aggregate_column(child)
                
                if func_name == 'COUNT':
                    # 对于COUNT(*)，参数为"*"
                    # 对于COUNT(column)，参数为列名
                    if column_arg == "*":
                        self._emit('COUNT', '*', None, agg_temp)
                    else:
                        self._emit('COUNT', column_arg, None, agg_temp)
                else:
                    # 对于其他聚合函数，需要确定列名
                    self._emit(func_name, '-', column_arg, agg_temp)
                
                select_info['aggregates'].append({
                    'function': func_name,
                    'temp': agg_temp
                })
                
                if isinstance(select_info['columns'], list):
                    select_info['columns'].append(agg_temp)
                else:
                    select_info['columns'] = [agg_temp]
                
                print(f"    聚合函数: {func_name}({column_arg}) -> {agg_temp}")
    
    def _get_aggregate_column(self, agg_node: ASTNode) -> str:
        """获取聚合函数的列参数"""
        # 查找聚合函数的参数
        for child in agg_node.children:
            if child.type == ASTNodeType.AGGREGATE_ARG:
                # 查找参数值
                for arg_child in child.children:
                    if arg_child.type == ASTNodeType.IDENTIFIER:
                        return arg_child.value
                    elif arg_child.type == ASTNodeType.ASTERISK:
                        return "*"
            elif child.type == ASTNodeType.AGGREGATE_ARG_LIST:
                # 处理参数列表
                for arg_child in child.children:
                    if arg_child.type == ASTNodeType.IDENTIFIER:
                        return arg_child.value
                    elif arg_child.type == ASTNodeType.ASTERISK:
                        return "*"
        
        # 默认返回"*"
        return "*"
    
    def _analyze_join_clauses(self, node: ASTNode, table_info: Dict[str, Any]):
        """分析JOIN子句"""
        for child in node.children:
            if child.type == ASTNodeType.JOIN_CLAUSE:
                print("  分析JOIN子句...")
                self._analyze_join_clause(child, table_info)
    
    def _analyze_join_clause(self, node: ASTNode, table_info: Dict[str, Any]):
        """分析单个JOIN子句"""
        join_table = None
        join_condition = None
        join_type = "INNER"  # 默认内连接
        
        # 查找JOIN类型 - 修复：不使用parent属性
        # 在JOIN子句中查找JOIN类型，而不是在父节点中查找
        # JOIN类型通常在JOIN关键字之前
        join_type = "INNER"  # 默认值
        
        for child in node.children:
            if child.type == ASTNodeType.TABLE_NAME:
                join_table = child.value
                table_info['tables'].append(join_table)
                print(f"    {join_type} JOIN表: {join_table}")
                
            elif child.type == ASTNodeType.ON_CLAUSE:
                # 查找条件节点
                for on_child in child.children:
                    if on_child.type == ASTNodeType.CONDITION:
                        join_condition = on_child.value
                        break
                
                print(f"    ON条件: {join_condition}")
        
        if join_table:
            # 生成JOIN操作的四元式
            join_temp = self._generate_temp()
            if join_type == "LEFT":
                self._emit('LEFT_JOIN', table_info['main_table'], f"{join_table} ON {join_condition}", join_temp)
            elif join_type == "RIGHT":
                self._emit('RIGHT_JOIN', table_info['main_table'], f"{join_table} ON {join_condition}", join_temp)
            elif join_type == "FULL":
                self._emit('FULL_JOIN', table_info['main_table'], f"{join_table} ON {join_condition}", join_temp)
            else:
                self._emit('INNER_JOIN', table_info['main_table'], f"{join_table} ON {join_condition}", join_temp)
    
    def _analyze_join_condition(self, node: ASTNode) -> Dict[str, str]:
        """分析JOIN条件"""
        # 简化实现，返回默认条件
        return {
            'left': 'table1.id',
            'right': 'table2.id', 
            'op': 'EQ'
        }
    
    def _analyze_where_clause(self, node: ASTNode):
        """分析WHERE子句"""
        for child in node.children:
            if child.type == ASTNodeType.WHERE_CLAUSE:
                print("  分析WHERE子句...")
                condition_temp = self._analyze_condition(child)
                if condition_temp:
                    self._emit('WHERE', condition_temp, None, None)
    
    def _analyze_condition(self, node: ASTNode) -> Optional[str]:
        """分析条件表达式"""
        # 简化的条件分析
        condition_temp = self._generate_temp()
        self._emit('CONDITION', 'column', '>', condition_temp)
        return condition_temp
    
    def _analyze_group_by_clause(self, node: ASTNode) -> Dict[str, Any]:
        """分析GROUP BY子句"""
        group_info = {'columns': [], 'has_group_by': False}
        
        for child in node.children:
            if child.type == ASTNodeType.GROUP_BY_CLAUSE:
                print("  分析GROUP BY子句...")
                group_info['has_group_by'] = True
                
                for grandchild in child.children:
                    if grandchild.type == ASTNodeType.IDENTIFIER:
                        column_name = grandchild.value
                        group_info['columns'].append(column_name)
                        print(f"    分组列: {column_name}")
                
                # 生成GROUP BY四元式
                if group_info['columns']:
                    group_temp = self._generate_temp()
                    self._emit('GROUP_BY', self.current_table, ','.join(group_info['columns']), group_temp)
        
        return group_info
    
    def _analyze_having_clause(self, node: ASTNode):
        """分析HAVING子句"""
        for child in node.children:
            if child.type == ASTNodeType.HAVING_CLAUSE:
                print("  分析HAVING子句...")
                
                # 查找条件表达式
                condition_str = ""
                for grandchild in child.children:
                    if grandchild.type == ASTNodeType.CONDITION:
                        condition_str = grandchild.value
                        break
                
                if condition_str:
                    having_temp = self._generate_temp()
                    self._emit('HAVING', condition_str, None, having_temp)
                    print(f"    HAVING条件: {condition_str}")
    
    def _analyze_order_by_clause(self, node: ASTNode):
        """分析ORDER BY子句"""
        for child in node.children:
            if child.type == ASTNodeType.ORDER_BY_CLAUSE:
                print("  分析ORDER BY子句...")
                
                # 查找ORDER BY列表
                order_list_node = None
                for order_child in child.children:
                    if order_child.type == ASTNodeType.ORDER_BY_LIST:
                        order_list_node = order_child
                        break
                
                if order_list_node:
                    # 分析排序规范
                    order_specs = []
                    self._analyze_order_list(order_list_node, order_specs)
                    
                    # 为每个排序规范生成四元式
                    for spec in order_specs:
                        column = spec['column']
                        direction = spec.get('direction', 'ASC')
                        order_spec = f"{column} {direction}"
                        temp = self._generate_temp()
                        self._emit('ORDER_BY', self.current_table, order_spec, temp)
                        print(f"    排序: {order_spec}")

    def _analyze_order_list(self, node: ASTNode, order_specs: List[Dict[str, str]]):
        """分析排序列表"""
        for child in node.children:
            if child.type == ASTNodeType.ORDER_BY_SPEC:
                # 分析排序规范
                spec = {}
                
                # 查找列引用或聚合函数
                for spec_child in child.children:
                    if spec_child.type == ASTNodeType.COLUMN_REF:
                        spec['column'] = spec_child.value
                        spec['type'] = 'column'
                        break
                    elif spec_child.type == ASTNodeType.AGGREGATE_FUNCTION:
                        # 处理聚合函数
                        func_name = spec_child.value
                        column_arg = self._get_aggregate_column(spec_child)
                        spec['column'] = f"{func_name}({column_arg})"
                        spec['type'] = 'aggregate'
                        break
                
                # 查找排序方向
                spec['direction'] = 'ASC'  # 默认升序
                for spec_child in child.children:
                    if spec_child.type == ASTNodeType.ORDER_SPEC:
                        if spec_child.value and spec_child.value.upper() in ['ASC', 'DESC']:
                            spec['direction'] = spec_child.value.upper()
                        break
                
                order_specs.append(spec)
            
            elif child.type == ASTNodeType.ORDER_BY_LIST_TAIL:
                # 递归处理排序列表的其余部分
                self._analyze_order_list(child, order_specs)

    def _analyze_limit_clause(self, node: ASTNode):
        """分析LIMIT/OFFSET子句"""
        for child in node.children:
            if child.type == ASTNodeType.LIMIT_CLAUSE:
                print("  分析LIMIT/OFFSET子句...")
                
                limit_value = None
                offset_value = None
                
                # 查找LIMIT和OFFSET值
                for grandchild in child.children:
                    if grandchild.type == ASTNodeType.LIMIT_VALUE:
                        limit_value = grandchild.value
                    elif grandchild.type == ASTNodeType.OFFSET_VALUE:
                        offset_value = grandchild.value
                
                # 生成LIMIT/OFFSET四元式
                if limit_value is not None:
                    self._emit('LIMIT', limit_value, None, None)
                    print(f"    LIMIT: {limit_value}")
                
                if offset_value is not None:
                    self._emit('OFFSET', offset_value, None, None)
                    print(f"    OFFSET: {offset_value}")

    def _generate_label(self) -> str:
        """生成标签"""
        self.label_counter += 1
        return f"L{self.label_counter}"

def test_extended_semantic_analyzer():
    """测试扩展语义分析器"""
    from src.compiler.lexer.lexer import Lexer
    from src.compiler.parser.extended_parser import ExtendedParser
    
    test_cases = [
        # 基本查询
        "SELECT name FROM students;",
        
        # 聚合函数
        "SELECT COUNT(*), AVG(grade) FROM students;", 
        
        # GROUP BY
        "SELECT major, COUNT(*) FROM students GROUP BY major;",
        
        # ORDER BY
        "SELECT name, grade FROM students ORDER BY grade DESC;",
    ]
    
    print("=" * 70)
    print("                 扩展语义分析器测试")
    print("=" * 70)
    
    for i, sql in enumerate(test_cases, 1):
        print(f"\n\n测试用例 {i}: {sql}")
        print("=" * 70)
        
        try:
            # 词法分析
            lexer = Lexer(sql)
            tokens = lexer.tokenize()
            
            # 语法分析
            parser = ExtendedParser(tokens)
            ast = parser.parse()
            
            if ast:
                # 扩展语义分析
                analyzer = ExtendedSemanticAnalyzer()
                quadruples = analyzer.analyze(ast)
                
                if quadruples:
                    print(f"\n生成四元式成功！共 {len(quadruples)} 条")
                else:
                    print("\n四元式生成失败")
            else:
                print("语法分析失败，跳过语义分析")
                
        except Exception as e:
            print(f"❌ 错误: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n✅ 扩展语义分析器测试完成!")

if __name__ == "__main__":
    test_extended_semantic_analyzer()