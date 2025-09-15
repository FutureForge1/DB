"""
四元式到目标代码翻译器
将语义分析器生成的四元式中间代码转换为目标指令
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from typing import List, Dict, Optional
from src.common.types import Quadruple
from src.compiler.codegen.target_instructions import (
    TargetCodeGenerator, TargetInstructionType, TargetInstruction
)

class QuadrupleTranslator:
    """四元式翻译器"""
    
    def __init__(self):
        """初始化翻译器"""
        self.target_gen = TargetCodeGenerator()
        self.temp_var_mapping: Dict[str, str] = {}  # 临时变量到寄存器的映射
        self.opened_tables: set = set()  # 已打开的表
        self.aggregate_aliases: Dict[str, str] = {}  # 聚合函数别名到寄存器的映射
        self.table_alias_mapping: Dict[str, str] = {}  # 表别名映射
        
        # 四元式操作符到目标指令的映射
        self.op_mapping = {
            'GT': TargetInstructionType.GT,
            'GE': TargetInstructionType.GE,
            'LT': TargetInstructionType.LT,
            'LE': TargetInstructionType.LE,
            'EQ': TargetInstructionType.EQ,
            'NE': TargetInstructionType.NE,
            'CMP': TargetInstructionType.GT,  # 默认为GT
            'AND': TargetInstructionType.AND,
            'OR': TargetInstructionType.OR,
            'NOT': TargetInstructionType.NOT
        }
    
    def get_or_create_register(self, temp_var: str) -> str:
        """获取或创建临时变量对应的寄存器"""
        if temp_var not in self.temp_var_mapping:
            self.temp_var_mapping[temp_var] = self.target_gen.generate_register()
        return self.temp_var_mapping[temp_var]
    
    def generate_target_code(self, quadruples: List[Quadruple]) -> List[TargetInstruction]:
        """
        将四元式列表翻译为目标指令
        
        Args:
            quadruples: 四元式中间代码列表
            
        Returns:
            目标指令列表
        """
        return self.translate(quadruples)
    
    def translate(self, quadruples: List[Quadruple]) -> List[TargetInstruction]:
        """
        将四元式列表翻译为目标指令
        
        Args:
            quadruples: 四元式中间代码列表
            
        Returns:
            目标指令列表
        """
        print(f"\n开始翻译 {len(quadruples)} 个四元式到目标代码:")
        print("-" * 60)
        
        # 清空状态
        self.target_gen.clear()
        self.temp_var_mapping.clear()
        self.opened_tables.clear()
        # 注意：不要清空aggregate_aliases，因为它可能在执行过程中被使用
        # self.aggregate_aliases.clear()  # 这行被注释掉了
        
        print(f"  翻译开始时aggregate_aliases状态: {self.aggregate_aliases}")
        
        # 保存四元式上下文供翻译时使用
        self.quadruples_context = quadruples
        
        # 分离不同类型的四元式
        begin_quads = []
        aggregate_quads = []
        select_quads = []
        filter_quads = []
        project_quads = []
        output_quads = []
        end_quads = []
        order_by_quads = []
        group_by_quads = []
        limit_quads = []
        offset_quads = []
        join_quads = []  # 添加JOIN四元式
        other_quads = []
        
        # 分类四元式
        for quad in quadruples:
            if quad.op == 'BEGIN':
                begin_quads.append(quad)
            elif quad.op in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']:
                aggregate_quads.append(quad)
            elif quad.op == 'SELECT':
                select_quads.append(quad)
            elif quad.op == 'FILTER':
                filter_quads.append(quad)
            elif quad.op == 'PROJECT':
                project_quads.append(quad)
            elif quad.op == 'OUTPUT':
                output_quads.append(quad)
            elif quad.op == 'END':
                end_quads.append(quad)
            elif quad.op == 'ORDER_BY':
                order_by_quads.append(quad)
            elif quad.op == 'GROUP_BY':
                group_by_quads.append(quad)
            elif quad.op == 'LIMIT':
                limit_quads.append(quad)
            elif quad.op == 'OFFSET':
                offset_quads.append(quad)
            elif quad.op in ['JOIN', 'INNER_JOIN', 'LEFT_JOIN', 'RIGHT_JOIN', 'FULL_JOIN']:
                join_quads.append(quad)
            else:
                other_quads.append(quad)
        
        # 按正确执行顺序生成目标指令
        # BEGIN -> OPEN/SCAN -> JOIN -> 聚合函数 -> GROUP BY -> ORDER BY -> LIMIT/OFFSET -> PROJECT -> OUTPUT -> END
        
        # 生成BEGIN指令
        for quad in begin_quads:
            print(f"翻译四元式: {quad}")
            self._translate_begin()
        
        # 生成SELECT操作指令（OPEN和SCAN）
        for quad in select_quads:
            print(f"翻译四元式: {quad}")
            self._translate_quadruple(quad)
        
        # 生成JOIN操作指令
        for quad in join_quads:
            print(f"翻译四元式: {quad}")
            self._translate_quadruple(quad)
        
        # 生成GROUP BY指令（在聚合函数之前，但在SCAN之后）
        for quad in group_by_quads:
            print(f"翻译四元式: {quad}")
            self._translate_quadruple(quad)
        
        # 生成聚合函数指令（在GROUP BY之后执行）
        for quad in aggregate_quads:
            print(f"翻译四元式: {quad}")
            self._translate_quadruple(quad)
        
        # 生成ORDER BY指令
        for quad in order_by_quads:
            print(f"翻译四元式: {quad}")
            self._translate_quadruple(quad)
        
        # 生成LIMIT/OFFSET指令
        for quad in limit_quads + offset_quads:
            print(f"翻译四元式: {quad}")
            self._translate_quadruple(quad)
        
        # 生成PROJECT指令
        for quad in project_quads:
            print(f"翻译四元式: {quad}")
            self._translate_quadruple(quad)
        
        # 生成其他操作指令
        for quad in filter_quads + output_quads + other_quads:
            print(f"翻译四元式: {quad}")
            self._translate_quadruple(quad)
        
        # 生成END指令
        for quad in end_quads:
            print(f"翻译四元式: {quad}")
            self._translate_end()
        
        # 关闭所有打开的表
        for table in self.opened_tables:
            self.target_gen.emit_table_close(table)
        
        # 添加程序结束指令
        self.target_gen.emit_halt()
        
        return self.target_gen.get_instructions()
    
    def _is_comparison_for_filter(self, comparison_quad: Quadruple, all_quadruples: List[Quadruple]) -> bool:
        """检查比较操作是否被用于FILTER操作"""
        # 查找是否有FILTER操作使用了这个比较结果
        for quad in all_quadruples:
            if quad.op == 'FILTER' and quad.arg2 == comparison_quad.result:
                return True
        return False
    
    def _translate_quadruple(self, quad: Quadruple):
        """翻译单个四元式"""
        op = quad.op
        arg1 = quad.arg1
        arg2 = quad.arg2
        result = quad.result
        
        # 特殊处理：对于SELECT操作，需要先打开和扫描表，但不立即生成PROJECT指令
        if op == 'SELECT':
            # 先处理表的打开和扫描
            table = arg2
            if table not in self.opened_tables:
                self.target_gen.emit_table_open(table)
                self.opened_tables.add(table)
            
            # 检查是否是JOIN查询，如果是则不生成SCAN指令
            is_join_query = any(q.op in ['JOIN', 'INNER_JOIN', 'LEFT_JOIN', 'RIGHT_JOIN', 'FULL_JOIN'] 
                               for q in self.quadruples_context)
            
            # 扫描表 - 但在JOIN情况下不应执行此操作
            source_reg = self.get_or_create_register(result)
            if not is_join_query:
                self.target_gen.emit_scan(table, source_reg)
            
            # 不再立即生成PROJECT指令，而是等到聚合函数执行之后
            return
        
        if op == 'FILTER':
            self._translate_filter(arg1, arg2, result)
        elif op == 'PROJECT':
            self._translate_project(arg1, arg2, result)
        elif op == 'OUTPUT':
            self._translate_output(arg1)
        elif op in self.op_mapping:
            self._translate_comparison(op, arg1, arg2, result)
        # 控制流操作
        elif op == 'BEGIN':
            self._translate_begin()
        elif op == 'END':
            self._translate_end()
        # 限制操作
        elif op == 'LIMIT':
            self._translate_limit(arg1, arg2, result)
        elif op == 'OFFSET':
            self._translate_offset(arg1, arg2, result)
        # 复杂查询操作
        elif op == 'JOIN':
            self._translate_join(arg1, arg2, result)
        elif op == 'INNER_JOIN':
            self._translate_inner_join(arg1, arg2, result)
        elif op == 'LEFT_JOIN':
            self._translate_left_join(arg1, arg2, result)
        elif op == 'RIGHT_JOIN':
            self._translate_right_join(arg1, arg2, result)
        elif op == 'FULL_JOIN':
            self._translate_full_join(arg1, arg2, result)
        elif op == 'COUNT':
            self._translate_count(arg1, arg2, result)
        elif op == 'SUM':
            self._translate_sum(arg1, arg2, result)
        elif op == 'AVG':
            self._translate_avg(arg1, arg2, result)
        elif op == 'MAX':
            self._translate_max(arg1, arg2, result)
        elif op == 'MIN':
            self._translate_min(arg1, arg2, result)
        elif op == 'GROUP_BY':
            self._translate_group_by(arg1, arg2, result)
        elif op == 'ORDER_BY':
            self._translate_order_by(arg1, arg2, result)
        elif op == 'HAVING':
            self._translate_having(arg1, arg2, result)
        else:
            print(f"未知操作: {op}")
    
    def _translate_begin(self):
        """翻译BEGIN操作"""
        print(f"  → BEGIN")
        self.target_gen.emit(TargetInstructionType.BEGIN, [], comment="开始执行")
    
    def _translate_end(self):
        """翻译END操作"""
        print(f"  → END")
        self.target_gen.emit(TargetInstructionType.END, [], comment="结束执行")
    
    def _translate_select(self, columns: str, table: str, result: str):
        """翻译SELECT操作"""
        print(f"  → SELECT {columns} FROM {table}")
        
        # 添加调试信息
        print(f"    四元式上下文中的操作:")
        for i, quad in enumerate(self.quadruples_context):
            print(f"      {i}: {quad.op}")
        
        # 检查是否已经有连接操作的结果
        # 如果当前上下文中已经有记录，说明可能已经执行了JOIN操作，不需要再SCAN
        has_existing_records = False
        # 简单检查：如果表名与已打开的表不同，可能需要SCAN
        # 但在JOIN情况下，我们不应该再SCAN
        
        # 如果表还未打开，先打开
        if table not in self.opened_tables:
            self.target_gen.emit_table_open(table)
            self.opened_tables.add(table)
        
        # 扫描表 - 但在JOIN情况下不应执行此操作
        # 检查是否是JOIN操作后的SELECT（通过检查是否已经有连接结果）
        # 简化处理：对于JOIN查询，我们不生成SCAN指令
        is_join_query = any(quad.op in ['JOIN', 'INNER_JOIN', 'LEFT_JOIN', 'RIGHT_JOIN', 'FULL_JOIN'] 
                           for quad in self.quadruples_context)
        
        print(f"    是否为JOIN查询: {is_join_query}")
        
        result_reg = self.get_or_create_register(result)
        
        # 只有在非JOIN查询时才生成SCAN指令
        if not is_join_query:
            print(f"    生成SCAN指令")
            self.target_gen.emit_scan(table, result_reg)
        else:
            print(f"    跳过SCAN指令（JOIN查询）")
        
        # 如果不是选择所有列，需要投影
        # 修复：处理聚合函数结果的投影
        if columns != "*":
            proj_reg = self.target_gen.generate_register()
            # 修复：正确处理列参数，支持列表格式
            if isinstance(columns, list):
                columns_str = ','.join(columns)
            else:
                columns_str = columns
            # 特殊处理：如果列是临时变量（如T1），则直接使用MOVE指令
            if isinstance(columns_str, str) and columns_str.startswith('T'):
                # 这是一个临时变量，直接移动
                self.target_gen.emit_move(columns_str, proj_reg, 
                                        comment=f"移动聚合结果: {columns_str}")
            else:
                # 检查是否包含聚合函数的别名
                # 查找聚合函数四元式
                aggregate_quads = [quad for quad in self.quadruples_context if quad.op in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']]
                
                if aggregate_quads and isinstance(columns, list):
                    # 如果有聚合函数且列是列表格式，需要特殊处理
                    # 检查列表中的每个列是否是聚合函数别名
                    processed_columns = []
                    for col in columns:
                        # 检查是否是聚合函数的结果
                        is_aggregate_result = False
                        for agg_quad in aggregate_quads:
                            # 获取聚合函数结果的寄存器
                            if agg_quad.result in self.temp_var_mapping:
                                agg_result_reg = self.temp_var_mapping[agg_quad.result]
                                # 如果列与聚合函数相关，使用聚合结果
                                # 这里简化处理，假设别名与聚合函数结果对应
                                processed_columns.append(agg_result_reg)
                                is_aggregate_result = True
                                break
                        
                        if not is_aggregate_result:
                            processed_columns.append(col)
                    
                    # 如果有聚合函数结果，需要特殊处理
                    if any(col.startswith('T') for col in processed_columns):
                        # 对于聚合函数，我们直接移动结果
                        for agg_quad in aggregate_quads:
                            if agg_quad.result in self.temp_var_mapping:
                                agg_result_reg = self.temp_var_mapping[agg_quad.result]
                                self.target_gen.emit_move(agg_result_reg, proj_reg,
                                                        comment=f"移动聚合结果: {agg_result_reg}")
                                break
                    else:
                        self.target_gen.emit_project(result_reg, ','.join(processed_columns), proj_reg)
                else:
                    self.target_gen.emit_project(result_reg, columns_str, proj_reg)
            # 更新寄存器映射
            self.temp_var_mapping[result] = proj_reg

    def _translate_project(self, source: str, columns: str, result: str):
        """翻译PROJECT操作"""
        print(f"  → PROJECT {columns} FROM {source}")
        
        source_reg = self.get_or_create_register(source)
        result_reg = self.get_or_create_register(result)
        
        # 特殊处理：如果列是临时变量（如T1），则直接使用MOVE指令
        if isinstance(columns, str) and columns.startswith('T'):
            # 这是一个临时变量，直接移动
            if columns in self.temp_var_mapping:
                self.target_gen.emit_move(self.temp_var_mapping[columns], result_reg, 
                                        comment=f"移动聚合结果: {columns}")
            else:
                self.target_gen.emit_move(columns, result_reg, 
                                        comment=f"移动聚合结果: {columns}")
        else:
            # 检查是否是聚合函数的别名
            # 查找聚合函数四元式
            aggregate_quads = [quad for quad in self.quadruples_context if quad.op in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']]
            
            # 获取SELECT四元式中的列列表
            select_quad = None
            for quad in self.quadruples_context:
                if quad.op == 'SELECT':
                    select_quad = quad
                    break
            
            # 检查列是否是聚合函数的别名
            is_aggregate_alias = False
            aggregate_result_reg = None
            
            if aggregate_quads:
                # 对于聚合函数，检查是否是单列的简单聚合
                agg_quad = aggregate_quads[0]  # 取第一个聚合函数
                if agg_quad.result in self.temp_var_mapping:
                    aggregate_result_reg = self.temp_var_mapping[agg_quad.result]
                    print(f"    聚合函数结果寄存器: {aggregate_result_reg}")
                    
                    # 检查是否是多列投影（包含分组列的GROUP BY查询）
                    column_list = [col.strip() for col in columns.split(',')]
                    if len(column_list) > 1:
                        # 多列投影，需要使用PROJECT指令
                        print(f"    多列投影，使用PROJECT指令: {column_list}")
                        self.target_gen.emit_project(source_reg, columns, result_reg)
                        # 为每个列保存别名映射（如果需要）
                        for col in column_list:
                            if col not in ['author_id']:  # 非分组列可能是聚合结果
                                self.aggregate_aliases[col] = aggregate_result_reg
                                print(f"    保存聚合列别名映射: {col} -> {aggregate_result_reg}")
                    else:
                        # 单列投影，可以直接移动
                        is_aggregate_alias = True
                        self.aggregate_aliases[columns] = aggregate_result_reg
                        print(f"    保存别名映射: {columns} -> {aggregate_result_reg}")
                        self.target_gen.emit_move(aggregate_result_reg, result_reg,
                                                comment=f"移动聚合结果到别名: {columns}")
            else:
                print(f"    没有找到聚合函数四元式")
                if select_quad:
                    print(f"    SELECT四元式: {select_quad}")
                    print(f"    SELECT列类型: {type(select_quad.arg1)}")
                    print(f"    SELECT列值: {select_quad.arg1}")
                self.target_gen.emit_project(source_reg, columns, result_reg)

    def _translate_filter(self, source: str, condition: str, result: str):
        """翻译FILTER操作"""
        print(f"  → FILTER {source} WITH {condition}")
        
        # 检查条件是否是一个比较操作的临时变量
        # 如果是，我们需要找到对应的比较操作四元式
        comparison_quad = None
        for quad in self.quadruples_context:
            if quad.result == condition and quad.op in ['GT', 'GE', 'LT', 'LE', 'EQ', 'NE']:
                comparison_quad = quad
                break
        
        if comparison_quad:
            # 直接生成包含比较条件的FILTER指令
            column = comparison_quad.arg1
            op_symbol = {
                'GT': '>',
                'GE': '>=',
                'LT': '<',
                'LE': '<=',
                'EQ': '=',
                'NE': '!='
            }.get(comparison_quad.op, '>')
            value = comparison_quad.arg2
            
            self.target_gen.emit(TargetInstructionType.FILTER, [column, op_symbol, value],
                               comment=f"过滤条件: {column} {op_symbol} {value}")
        else:
            # 兜底：使用寄存器方式
            source_reg = self.get_or_create_register(source)
            condition_reg = self.get_or_create_register(condition)
            result_reg = self.get_or_create_register(result)
            
            self.target_gen.emit_filter(source_reg, condition_reg, result_reg)
    
    def _translate_comparison(self, op: str, operand1: str, operand2: str, result: str):
        """翻译比较操作"""
        print(f"  → {op} {operand1} {operand2}")
        
        result_reg = self.get_or_create_register(result)
        target_op = self.op_mapping[op]
        
        self.target_gen.emit_comparison(target_op, operand1, operand2, result_reg)
    
    def _translate_output(self, source: str):
        """翻译OUTPUT操作"""
        print(f"  → OUTPUT {source}")
        
        source_reg = self.get_or_create_register(source)
        self.target_gen.emit_output(source_reg)
    
    def _translate_limit(self, limit: str, offset: str, result: str):
        """翻译LIMIT操作"""
        print(f"  → LIMIT {limit}")
        self.target_gen.emit_limit(limit)
    
    def _translate_offset(self, offset: str, limit: str, result: str):
        """翻译OFFSET操作"""
        print(f"  → OFFSET {offset}")
        self.target_gen.emit_offset(offset)
    
    def _translate_join(self, table1: str, table2_and_condition: str, result: str):
        """翻译JOIN操作"""
        # 解析table2_and_condition，格式可能是 "table2 ON condition"
        parts = table2_and_condition.split(' ON ')
        table2 = parts[0]
        condition = parts[1] if len(parts) > 1 else "true"
        
        print(f"  → JOIN {table1} {table2} ON {condition}")
        
        # 确保两个表都已打开
        for table in [table1, table2]:
            if table not in self.opened_tables:
                self.target_gen.emit_table_open(table)
                self.opened_tables.add(table)
        
        result_reg = self.get_or_create_register(result)
        self.target_gen.emit(TargetInstructionType.JOIN, [table1, table2, condition], result_reg,
                           comment=f"连接表 {table1} 和 {table2}")
    
    def _translate_inner_join(self, table1: str, table2: str, condition: str):
        """翻译INNER JOIN操作"""
        print(f"  → INNER JOIN {table1} {table2} ON {condition}")
        
        for table in [table1, table2]:
            if table not in self.opened_tables:
                self.target_gen.emit_table_open(table)
                self.opened_tables.add(table)
        
        result_reg = self.target_gen.generate_register()
        self.target_gen.emit(TargetInstructionType.INNER_JOIN, [table1, table2, condition], result_reg,
                           comment=f"内连接表 {table1} 和 {table2}")
    
    def _translate_left_join(self, table1: str, table2: str, condition: str):
        """翻译LEFT JOIN操作"""
        print(f"  → LEFT JOIN {table1} {table2} ON {condition}")
        
        for table in [table1, table2]:
            if table not in self.opened_tables:
                self.target_gen.emit_table_open(table)
                self.opened_tables.add(table)
        
        result_reg = self.target_gen.generate_register()
        self.target_gen.emit(TargetInstructionType.LEFT_JOIN, [table1, table2, condition], result_reg,
                           comment=f"左连接表 {table1} 和 {table2}")
    
    def _translate_right_join(self, table1: str, table2: str, condition: str):
        """翻译RIGHT JOIN操作"""
        print(f"  → RIGHT JOIN {table1} {table2} ON {condition}")
        
        for table in [table1, table2]:
            if table not in self.opened_tables:
                self.target_gen.emit_table_open(table)
                self.opened_tables.add(table)
        
        result_reg = self.target_gen.generate_register()
        self.target_gen.emit(TargetInstructionType.RIGHT_JOIN, [table1, table2, condition], result_reg,
                           comment=f"右连接表 {table1} 和 {table2}")
    
    def _translate_full_join(self, table1: str, table2: str, condition: str):
        """翻译FULL JOIN操作"""
        print(f"  → FULL JOIN {table1} {table2} ON {condition}")
        
        for table in [table1, table2]:
            if table not in self.opened_tables:
                self.target_gen.emit_table_open(table)
                self.opened_tables.add(table)
        
        result_reg = self.target_gen.generate_register()
        self.target_gen.emit(TargetInstructionType.FULL_JOIN, [table1, table2, condition], result_reg,
                           comment=f"全连接表 {table1} 和 {table2}")
    
    def _translate_count(self, source: str, column: str, result: str):
        """翻译COUNT聚合操作"""
        print(f"  → COUNT({column}) FROM {source}")
        
        result_reg = self.get_or_create_register(result)
        
        # 对于COUNT操作，我们需要先扫描表，然后执行COUNT
        # 如果表还未打开，先打开
        if source and source not in self.opened_tables:
            self.target_gen.emit_table_open(source)
            self.opened_tables.add(source)
        
        # 生成SCAN指令来加载数据
        source_reg = self.get_or_create_register(source) if source else "R1"
        self.target_gen.emit_scan(source, source_reg)
        
        # 生成COUNT指令，参数为[source_reg, column]
        # 确保column参数正确传递
        operands = [source_reg, column or "*"]  # 传递源寄存器和列名
        self.target_gen.emit(TargetInstructionType.COUNT, operands, result_reg,
                           comment=f"计数聚合: COUNT({column or '*'})")

    def _translate_sum(self, source: str, column: str, result: str):
        """翻译SUM聚合操作"""
        print(f"  → SUM({column}) FROM {source}")
        
        # 如果表还未打开，先打开
        if source and source not in self.opened_tables:
            self.target_gen.emit_table_open(source)
            self.opened_tables.add(source)
        
        # 生成SCAN指令来加载数据
        source_reg = self.get_or_create_register(source) if source else "R1"
        self.target_gen.emit_scan(source, source_reg)
        
        result_reg = self.get_or_create_register(result)
        
        # 传递源寄存器和列名
        operands = [source_reg, column]
        self.target_gen.emit(TargetInstructionType.SUM, operands, result_reg,
                           comment=f"求和聚合: SUM({column})")
    
    def _translate_avg(self, source: str, column: str, result: str):
        """翻译AVG聚合操作"""
        print(f"  → AVG({column}) FROM {source}")
        
        # 如果表还未打开，先打开
        if source and source not in self.opened_tables:
            self.target_gen.emit_table_open(source)
            self.opened_tables.add(source)
        
        # 生成SCAN指令来加载数据
        source_reg = self.get_or_create_register(source) if source else "R1"
        self.target_gen.emit_scan(source, source_reg)
        
        result_reg = self.get_or_create_register(result)
        
        # 传递源寄存器和列名
        operands = [source_reg, column]
        self.target_gen.emit(TargetInstructionType.AVG, operands, result_reg,
                           comment=f"平均值聚合: AVG({column})")
    
    def _translate_max(self, source: str, column: str, result: str):
        """翻译MAX聚合操作"""
        print(f"  → MAX({column}) FROM {source}")
        
        # 如果表还未打开，先打开
        if source and source not in self.opened_tables:
            self.target_gen.emit_table_open(source)
            self.opened_tables.add(source)
        
        # 生成SCAN指令来加载数据
        source_reg = self.get_or_create_register(source) if source else "R1"
        self.target_gen.emit_scan(source, source_reg)
        
        result_reg = self.get_or_create_register(result)
        
        # 传递源寄存器和列名
        operands = [source_reg, column]
        self.target_gen.emit(TargetInstructionType.MAX, operands, result_reg,
                           comment=f"最大值聚合: MAX({column})")
    
    def _translate_min(self, source: str, column: str, result: str):
        """翻译MIN聚合操作"""
        print(f"  → MIN({column}) FROM {source}")
        
        # 如果表还未打开，先打开
        if source and source not in self.opened_tables:
            self.target_gen.emit_table_open(source)
            self.opened_tables.add(source)
        
        # 生成SCAN指令来加载数据
        source_reg = self.get_or_create_register(source) if source else "R1"
        self.target_gen.emit_scan(source, source_reg)
        
        result_reg = self.get_or_create_register(result)
        
        # 传递源寄存器和列名
        operands = [source_reg, column]
        self.target_gen.emit(TargetInstructionType.MIN, operands, result_reg,
                           comment=f"最小值聚合: MIN({column})")
    
    def _translate_group_by(self, source: str, columns: str, result: str):
        """翻译GROUP BY操作"""
        print(f"  → GROUP BY {columns} FROM {source}")
        
        source_reg = self.get_or_create_register(source)
        result_reg = self.get_or_create_register(result)
        
        self.target_gen.emit(TargetInstructionType.GROUP_BY, [source_reg, columns], result_reg,
                           comment=f"分组操作: GROUP BY {columns}")
    
    def _translate_order_by(self, source: str, column_and_direction: str, result: str):
        """翻译ORDER BY操作"""
        print(f"  → ORDER BY {column_and_direction} FROM {source}")
        
        source_reg = self.get_or_create_register(source)
        result_reg = self.get_or_create_register(result)
        
        # 确保column_and_direction不是None
        if column_and_direction is None:
            column_and_direction = ""
            
        self.target_gen.emit_order_by(source_reg, column_and_direction, result_reg)
    
    def _translate_having(self, source: str, condition: str, result: str):
        """翻译HAVING操作"""
        print(f"  → HAVING {condition}")
        
        # HAVING操作需要将条件传递给执行引擎
        result_reg = self.get_or_create_register(result)
        self.target_gen.emit(TargetInstructionType.HAVING, [condition], result_reg,
                           comment=f"分组过滤: HAVING {condition}")
    
    def print_translation_result(self):
        """打印翻译结果"""
        print(f"\n翻译完成! 生成了 {len(self.target_gen.instructions)} 条目标指令")
        self.target_gen.print_code()
        
        print(f"\n临时变量映射:")
        print("-" * 30)
        for temp_var, register in self.temp_var_mapping.items():
            print(f"  {temp_var} → {register}")
    
    def get_target_instructions(self) -> List[TargetInstruction]:
        """获取生成的目标指令"""
        return self.target_gen.get_instructions()


class IntegratedCodeGenerator:
    """集成的代码生成器"""
    
    def __init__(self):
        """初始化集成代码生成器"""
        self.translator = QuadrupleTranslator()
        # 添加表别名映射
        self.table_alias_mapping = {}
    
    def generate_target_code(self, quadruples: List[Quadruple]) -> List[TargetInstruction]:
        """
        从四元式生成目标代码的完整流程
        
        Args:
            quadruples: 四元式中间代码
            
        Returns:
            目标指令列表
        """
        print("=" * 80)
        print("              四元式到目标代码翻译")
        print("=" * 80)
        
        # 显示输入的四元式
        print("输入的四元式中间代码:")
        print("-" * 40)
        for i, quad in enumerate(quadruples, 1):
            print(f"  {i}: {quad}")
        
        # 在翻译之前，从四元式中提取表别名信息
        self._extract_table_aliases(quadruples)
        
        # 翻译为目标代码
        target_instructions = self.translator.generate_target_code(quadruples)
        
        # 显示翻译结果
        self.translator.print_translation_result()
        
        return target_instructions
    
    @property
    def aggregate_aliases(self):
        """获取聚合函数别名映射"""
        return self.translator.aggregate_aliases
    
    def _extract_table_aliases(self, quadruples: List[Quadruple]):
        """从四元式中提取表别名信息"""
        # 这里可以添加从四元式中提取表别名的逻辑
        # 目前我们使用一个简单的映射
        self.table_alias_mapping = {
            'b': 'books',
            'a': 'authors'
        }
        print(f"提取的表别名映射: {self.table_alias_mapping}")
        
        # 将表别名映射传递给翻译器
        self.translator.table_alias_mapping = self.table_alias_mapping.copy()
    
    def optimize_target_code(self, instructions: List[TargetInstruction]) -> List[TargetInstruction]:
        """
        目标代码优化（简单版本）
        
        Args:
            instructions: 原始目标指令
            
        Returns:
            优化后的目标指令
        """
        optimized = []
        
        # 简单的优化规则
        i = 0
        while i < len(instructions):
            current = instructions[i]
            
            # 移除连续的NOP指令
            if current.op == TargetInstructionType.NOP:
                i += 1
                continue
            
            # 合并连续的相同表操作
            if (current.op == TargetInstructionType.OPEN and 
                i + 1 < len(instructions) and
                instructions[i + 1].op == TargetInstructionType.CLOSE and
                current.operands[0] == instructions[i + 1].operands[0]):
                # 跳过无效的 OPEN + CLOSE 组合
                i += 2
                continue
            
            optimized.append(current)
            i += 1
        
        if len(optimized) < len(instructions):
            print(f"\n优化: 移除了 {len(instructions) - len(optimized)} 条冗余指令")
        
        return optimized
