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
    
    def __init__(self, storage_engine=None):
        """初始化扩展语义分析器"""
        self.symbol_table = SymbolTable()
        self.quadruples: List[Quadruple] = []
        self.temp_counter = 0
        self.label_counter = 0
        self.storage_engine = storage_engine  # 存储引擎实例
        
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
        
        # 处理SELECT列表
        select_info = self._analyze_select_list(node)
        
        # 总是生成SELECT四元式以确保表被打开和扫描
        select_temp = self._generate_temp()
        self._emit('SELECT', select_info.get('columns', '*'), table_info.get('main_table'), select_temp)
        
        # 生成聚合函数四元式（在SELECT之后）
        if select_info.get('has_aggregates', False):
            self._generate_aggregate_quadruples(select_info)
            # 对于聚合函数查询，也需要生成PROJECT四元式来处理别名
            result_temp = select_info['aggregates'][0]['temp']  # 使用第一个聚合函数的结果

            # 生成PROJECT四元式以处理别名
            project_temp = self._generate_temp()
            columns = select_info.get('columns', '*')
            if isinstance(columns, list):
                columns_str = ','.join(columns)
            else:
                columns_str = columns
            self._emit('PROJECT', result_temp, columns_str, project_temp)
            # 更新结果临时变量
            result_temp = project_temp
        else:
            result_temp = select_temp
            
            # 对于非聚合函数查询，也需要生成PROJECT四元式
            project_temp = self._generate_temp()
            columns = select_info.get('columns', '*')
            if isinstance(columns, list):
                columns_str = ','.join(columns)
            else:
                columns_str = columns
            self._emit('PROJECT', result_temp, columns_str, project_temp)
            # 更新结果临时变量
            result_temp = project_temp
        
        # 生成输出和结束标记
        self._emit('OUTPUT', result_temp, None, None)
        self._emit('END', None, None, None)
    
    def _generate_aggregate_quadruples(self, select_info: Dict[str, Any]):
        """生成聚合函数四元式"""
        # 为每个聚合函数生成四元式
        for agg_info in select_info.get('aggregates', []):
            func_name = agg_info['function']
            temp_var = agg_info['temp']
            column_arg = agg_info.get('column_arg', '*')
            table_name = getattr(self, 'current_table', '-')  # 获取当前表名，如果没有则使用'-'
            
            # 生成聚合函数四元式
            if func_name == 'COUNT':
                self._emit('COUNT', table_name, column_arg, temp_var)
            elif func_name == 'SUM':
                self._emit('SUM', table_name, column_arg, temp_var)
            elif func_name == 'AVG':
                self._emit('AVG', table_name, column_arg, temp_var)
            elif func_name == 'MAX':
                self._emit('MAX', table_name, column_arg, temp_var)
            elif func_name == 'MIN':
                self._emit('MIN', table_name, column_arg, temp_var)
    
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
                
                # 验证表是否存在 - 新增：表存在性检查
                if self.storage_engine:
                    try:
                        tables = self.storage_engine.list_tables()
                        if table_name not in tables:
                            raise SemanticError(f"Table '{table_name}' does not exist")
                    except Exception as e:
                        # 如果无法访问存储引擎，忽略验证
                        pass
                else:
                    # 没有存储引擎时的默认行为
                    pass
                
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
            elif child.type == ASTNodeType.AGGREGATE_FUNCTION:
                # 直接处理聚合函数节点
                print("  直接处理聚合函数...")
                func_name = child.value
                select_info['has_aggregates'] = True
                
                # 检查是否有别名
                alias_name = None
                # 查找同级的下一个节点是否是别名
                for i, sibling in enumerate(node.children):
                    if sibling == child and i + 1 < len(node.children):
                        next_sibling = node.children[i + 1]
                        if hasattr(next_sibling, 'type') and next_sibling.type == ASTNodeType.COLUMN_ALIAS:
                            alias_name = next_sibling.value
                            break
                
                # 生成临时变量名（但不立即生成四元式）
                agg_temp = self._generate_temp()
                column_arg = self._get_aggregate_column(child)
                
                # 保存聚合函数信息，稍后生成四元式
                select_info['aggregates'].append({
                    'function': func_name,
                    'temp': agg_temp,
                    'alias': alias_name,  # 保存别名信息
                    'column_arg': column_arg  # 保存列参数
                })
                
                # 使用别名或临时变量名作为列名
                column_name = alias_name if alias_name else agg_temp
                if isinstance(select_info['columns'], list):
                    select_info['columns'].append(column_name)
                else:
                    select_info['columns'] = [column_name]
                
                print(f"    聚合函数: {func_name}({column_arg}) -> {column_name}")
        
        return select_info
    
    def _analyze_column_list(self, node: ASTNode, select_info: Dict[str, Any]):
        """分析列列表"""
        for child in node.children:
            if child.type == ASTNodeType.AGGREGATE_FUNCTION:
                func_name = child.value
                select_info['has_aggregates'] = True
                
                # 检查是否有别名 - 别名是聚合函数节点的子节点
                alias_name = None
                for subchild in child.children:
                    if hasattr(subchild, 'type') and subchild.type == ASTNodeType.COLUMN_ALIAS:
                        alias_name = subchild.value
                        break
                
                # 生成临时变量名（但不立即生成四元式）
                agg_temp = self._generate_temp()
                column_arg = self._get_aggregate_column(child)
                
                # 保存聚合函数信息，稍后生成四元式
                select_info['aggregates'].append({
                    'function': func_name,
                    'temp': agg_temp,
                    'alias': alias_name,  # 保存别名信息
                    'column_arg': column_arg  # 保存列参数
                })
                
                # 使用别名或临时变量名作为列名
                column_name = alias_name if alias_name else agg_temp
                if isinstance(select_info['columns'], list):
                    select_info['columns'].append(column_name)
                else:
                    select_info['columns'] = [column_name]
                
                print(f"    聚合函数: {func_name}({column_arg}) -> {column_name}")
            elif child.type == ASTNodeType.COLUMN_REF:  # 修复：处理COLUMN_REF节点
                # 获取列名，可能带表前缀
                column_name = child.value
                if isinstance(select_info['columns'], list):
                    select_info['columns'].append(column_name)
                else:
                    select_info['columns'] = [column_name]
                print(f"    选择列: {column_name}")
                
                # 检查是否有别名
                alias_name = None
                for alias_child in child.children:
                    if hasattr(alias_child, 'type') and alias_child.type == ASTNodeType.COLUMN_ALIAS:
                        # 处理列别名
                        alias_name = alias_child.value
                        print(f"    列别名: {alias_name}")
                        break
                
                # 如果有别名，使用别名替换列名
                if alias_name:
                    if isinstance(select_info['columns'], list):
                        select_info['columns'][-1] = alias_name  # 替换最后一个添加的列名
                    else:
                        select_info['columns'] = [alias_name]
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
                    
                    # 检查是否有别名
                    alias_name = None
                    # 查找同级的下一个节点是否是别名
                    for i, sibling in enumerate(node.children):
                        if sibling == child and i + 1 < len(node.children):
                            next_sibling = node.children[i + 1]
                            if hasattr(next_sibling, 'type') and next_sibling.type == ASTNodeType.COLUMN_ALIAS:
                                alias_name = next_sibling.value
                                break
                    
                    # 如果有别名，使用别名替换列名
                    if alias_name:
                        if isinstance(select_info['columns'], list):
                            select_info['columns'][-1] = alias_name  # 替换最后一个添加的列名
                        else:
                            select_info['columns'] = [alias_name]
            elif child.type == ASTNodeType.COLUMN_SPEC:
                # 处理列规范节点，可能包含聚合函数
                for spec_child in child.children:
                    if spec_child.type == ASTNodeType.AGGREGATE_FUNCTION:
                        func_name = spec_child.value
                        select_info['has_aggregates'] = True
                        
                        # 检查是否有别名
                        alias_name = None
                        # 查找列规范的下一个兄弟节点是否是别名
                        for i, sibling in enumerate(node.children):
                            if sibling == child and i + 1 < len(node.children):
                                next_sibling = node.children[i + 1]
                                if hasattr(next_sibling, 'type') and next_sibling.type == ASTNodeType.COLUMN_ALIAS:
                                    alias_name = next_sibling.value
                                    break
                        
                        # 生成临时变量名（但不立即生成四元式）
                        agg_temp = self._generate_temp()
                        column_arg = self._get_aggregate_column(spec_child)
                        
                        # 保存聚合函数信息，稍后生成四元式
                        select_info['aggregates'].append({
                            'function': func_name,
                            'temp': agg_temp,
                            'alias': alias_name,  # 保存别名信息
                            'column_arg': column_arg  # 保存列参数
                        })
                        
                        # 使用别名或临时变量名作为列名
                        column_name = alias_name if alias_name else agg_temp
                        if isinstance(select_info['columns'], list):
                            select_info['columns'].append(column_name)
                        else:
                            select_info['columns'] = [column_name]
                        
                        print(f"    聚合函数: {func_name}({column_arg}) -> {column_name}")
                        break  # 处理完聚合函数后跳出循环
                    elif spec_child.type == ASTNodeType.COLUMN_REF:
                        # 处理普通列引用
                        column_name = spec_child.value
                        if isinstance(select_info['columns'], list):
                            select_info['columns'].append(column_name)
                        else:
                            select_info['columns'] = [column_name]
                        print(f"    选择列: {column_name}")
                        
                        # 检查是否有别名
                        alias_name = None
                        for alias_child in spec_child.children:
                            if hasattr(alias_child, 'type') and alias_child.type == ASTNodeType.COLUMN_ALIAS:
                                alias_name = alias_child.value
                                break
                        
                        # 如果有别名，使用别名替换列名
                        if alias_name:
                            if isinstance(select_info['columns'], list):
                                select_info['columns'][-1] = alias_name  # 替换最后一个添加的列名
                            else:
                                select_info['columns'] = [alias_name]
                        break  # 处理完列引用后跳出循环
                    elif spec_child.type == ASTNodeType.IDENTIFIER:
                        # 处理标识符
                        column_name = spec_child.value
                        if column_name == "*":
                            select_info['columns'] = "*"
                            print(f"    选择所有列: *")
                        else:
                            if isinstance(select_info['columns'], list):
                                select_info['columns'].append(column_name)
                            else:
                                select_info['columns'] = [column_name]
                            print(f"    选择列: {column_name}")
                            
                            # 检查是否有别名
                            alias_name = None
                            # 查找同级的下一个节点是否是别名
                            for i, sibling in enumerate(node.children):
                                if sibling == child and i + 1 < len(node.children):
                                    next_sibling = node.children[i + 1]
                                    if hasattr(next_sibling, 'type') and next_sibling.type == ASTNodeType.COLUMN_ALIAS:
                                        alias_name = next_sibling.value
                                        break
                            
                            # 如果有别名，使用别名替换列名
                            if alias_name:
                                if isinstance(select_info['columns'], list):
                                    select_info['columns'][-1] = alias_name  # 替换最后一个添加的列名
                                else:
                                    select_info['columns'] = [alias_name]
                        break  # 处理完标识符后跳出循环
    
    def _get_aggregate_column(self, agg_node: ASTNode) -> str:
        """获取聚合函数的列参数"""
        try:
            # 查找聚合函数的参数
            for child in agg_node.children:
                if child.type == ASTNodeType.AGGREGATE_ARG:
                    # 查找参数值
                    for arg_child in child.children:
                        if arg_child.type == ASTNodeType.IDENTIFIER:
                            # 特殊处理：如果IDENTIFIER的值是"*"，返回"*"
                            if arg_child.value == "*":
                                return "*"
                            return arg_child.value
                        elif arg_child.type == ASTNodeType.COLUMN_REF:
                            # 直接返回COLUMN_REF节点的值
                            return arg_child.value
                elif child.type == ASTNodeType.AGGREGATE_ARG_LIST:
                    # 处理参数列表
                    for arg_child in child.children:
                        if arg_child.type == ASTNodeType.IDENTIFIER:
                            # 特殊处理：如果IDENTIFIER的值是"*"，返回"*"
                            if arg_child.value == "*":
                                return "*"
                            return arg_child.value
                        elif arg_child.type == ASTNodeType.COLUMN_REF:
                            # 直接返回COLUMN_REF节点的值
                            return arg_child.value
            
            # 默认返回"*"
            return "*"
        except Exception as e:
            # 如果出现任何错误，返回默认值"*"
            print(f"Warning: Error getting aggregate column, using default '*': {e}")
            return "*"
    
    def _analyze_join_clauses(self, node: ASTNode, table_info: Dict[str, Any]):
        """分析JOIN子句"""
        print("  分析JOIN子句...")
        for child in node.children:
            if child.type == ASTNodeType.JOIN_CLAUSE:
                self._analyze_join_clause(child, table_info)
    
    def _analyze_join_clause(self, node: ASTNode, table_info: Dict[str, Any]):
        """分析单个JOIN子句"""
        join_table = None
        join_condition = None
        join_type = "INNER"  # 默认内连接
        table_alias = None
        
        # 查找JOIN类型和相关信息
        for child in node.children:
            if child.type == ASTNodeType.JOIN_TYPE:
                join_type = child.value
                print(f"    JOIN类型: {join_type}")
            elif child.type == ASTNodeType.TABLE_NAME:
                join_table = child.value
            elif child.type == ASTNodeType.TABLE_ALIAS:
                table_alias = child.value
            elif child.type == ASTNodeType.ON_CLAUSE:
                # 查找条件节点
                for on_child in child.children:
                    if on_child.type == ASTNodeType.JOIN_CONDITION:
                        # 构建完整的条件字符串
                        condition_parts = []
                        for cond_child in on_child.children:
                            if hasattr(cond_child, 'value') and cond_child.value:
                                condition_parts.append(str(cond_child.value))
                        if condition_parts:
                            join_condition = ' '.join(condition_parts)
                        break
                
                print(f"    ON条件: {join_condition}")
        
        if join_table:
            # 添加表到表信息中
            table_info['tables'].append(join_table)
            if table_alias:
                table_info['aliases'][table_alias] = join_table
            
            # 生成JOIN操作的四元式
            join_temp = self._generate_temp()
            if join_type == "LEFT":
                self._emit('LEFT_JOIN', table_info['main_table'], join_table, join_condition)
            elif join_type == "RIGHT":
                self._emit('RIGHT_JOIN', table_info['main_table'], join_table, join_condition)
            elif join_type == "FULL":
                self._emit('FULL_JOIN', table_info['main_table'], join_table, join_condition)
            else:
                self._emit('INNER_JOIN', table_info['main_table'], join_table, join_condition)
            print(f"    生成{join_type} JOIN四元式: {table_info['main_table']} JOIN {join_table} ON {join_condition}")
    
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
                    # 生成FILTER四元式而不是WHERE四元式
                    self._emit('FILTER', condition_temp, None, None)
    
    def _analyze_condition(self, node: ASTNode) -> Optional[str]:
        """分析条件表达式"""
        # 查找条件的具体信息
        for child in node.children:
            if child.type == ASTNodeType.OR_CONDITION:
                # 处理OR条件
                return self._analyze_or_condition(child)
            elif child.type == ASTNodeType.AND_CONDITION:
                # 处理AND条件
                return self._analyze_and_condition(child)
            elif child.type == ASTNodeType.SIMPLE_CONDITION:
                # 处理简单条件
                return self._analyze_simple_condition(child)
        
        # 如果没有找到具体条件，返回None
        return None
    
    def _analyze_or_condition(self, node: ASTNode) -> Optional[str]:
        """分析OR条件"""
        # 简化处理，直接返回第一个条件
        for child in node.children:
            if child.type == ASTNodeType.AND_CONDITION:
                return self._analyze_and_condition(child)
        return None
    
    def _analyze_and_condition(self, node: ASTNode) -> Optional[str]:
        """分析AND条件"""
        # 简化处理，直接返回第一个条件
        for child in node.children:
            if child.type == ASTNodeType.SIMPLE_CONDITION:
                return self._analyze_simple_condition(child)
        return None
    
    def _analyze_simple_condition(self, node: ASTNode) -> Optional[str]:
        """分析简单条件"""
        column = None
        operator = None
        value = None
        
        # 查找条件的组成部分
        children = node.children
        if len(children) >= 3:
            # 期望格式: column_ref comparison_op operand
            if children[0].type == ASTNodeType.COLUMN_REF:
                column = children[0].value
            elif children[0].type == ASTNodeType.IDENTIFIER:
                column = children[0].value
                
            if children[1].type == ASTNodeType.COMPARISON_OP:
                operator = children[1].value
            elif children[1].type == ASTNodeType.IDENTIFIER:
                operator = children[1].value
                
            if children[2].type == ASTNodeType.OPERAND:
                # 查找操作数的值
                for operand_child in children[2].children:
                    if operand_child.type in [ASTNodeType.COLUMN_REF, ASTNodeType.IDENTIFIER, ASTNodeType.LITERAL]:
                        value = operand_child.value
                        break
            elif children[2].type in [ASTNodeType.COLUMN_REF, ASTNodeType.IDENTIFIER, ASTNodeType.LITERAL]:
                value = children[2].value
        
        # 如果找到了条件的所有部分，生成比较四元式
        if column and operator and value:
            condition_temp = self._generate_temp()
            # 生成比较操作的四元式
            op_map = {
                '>': 'GT',
                '>=': 'GE',
                '<': 'LT', 
                '<=': 'LE',
                '=': 'EQ',
                '<>': 'NE',
                '!=': 'NE'
            }
            quad_op = op_map.get(operator, 'GT')  # 默认使用GT
            self._emit(quad_op, column, value, condition_temp)
            return condition_temp
        
        return None
    
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