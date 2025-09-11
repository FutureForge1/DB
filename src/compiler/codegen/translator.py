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
            else:
                other_quads.append(quad)
        
        # 按正确顺序翻译四元式
        # BEGIN -> SELECT(OPEN/SCAN) -> 聚合函数 -> PROJECT -> OUTPUT -> END
        
        # 翻译BEGIN
        for quad in begin_quads:
            print(f"翻译四元式: {quad}")
            self._translate_begin()
        
        # 翻译SELECT（这会生成OPEN和SCAN指令）
        for quad in select_quads:
            print(f"翻译四元式: {quad}")
            self._translate_quadruple(quad)
        
        # 翻译聚合函数
        for quad in aggregate_quads:
            print(f"翻译四元式: {quad}")
            self._translate_quadruple(quad)
        
        # 翻译PROJECT
        for quad in project_quads:
            print(f"翻译四元式: {quad}")
            self._translate_quadruple(quad)
        
        # 翻译其他操作
        for quad in filter_quads + output_quads + other_quads:
            print(f"翻译四元式: {quad}")
            self._translate_quadruple(quad)
        
        # 翻译END
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
        
        # 特殊处理：对于SELECT操作，需要先打开和扫描表
        if op == 'SELECT':
            # 先处理表的打开和扫描
            table = arg2
            if table not in self.opened_tables:
                self.target_gen.emit_table_open(table)
                self.opened_tables.add(table)
            
            # 扫描表
            source_reg = self.get_or_create_register(result)
            self.target_gen.emit_scan(table, source_reg)
            
            # 然后处理投影
            if arg1 != "*":
                proj_reg = self.target_gen.generate_register()
                self.target_gen.emit_project(source_reg, arg1, proj_reg)
                # 更新寄存器映射
                self.temp_var_mapping[result] = proj_reg
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
        # 复杂查询操作
        elif op == 'JOIN':
            self._translate_join(arg1, arg2, result)
        elif op == 'INNER_JOIN':
            self._translate_inner_join(arg1, arg2, result)
        elif op == 'LEFT_JOIN':
            self._translate_left_join(arg1, arg2, result)
        elif op == 'RIGHT_JOIN':
            self._translate_right_join(arg1, arg2, result)
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
        
        # 如果表还未打开，先打开
        if table not in self.opened_tables:
            self.target_gen.emit_table_open(table)
            self.opened_tables.add(table)
        
        # 扫描表
        result_reg = self.get_or_create_register(result)
        self.target_gen.emit_scan(table, result_reg)
        
        # 如果不是选择所有列，需要投影
        if columns != "*":
            proj_reg = self.target_gen.generate_register()
            self.target_gen.emit_project(result_reg, columns, proj_reg)
            # 更新寄存器映射
            self.temp_var_mapping[result] = proj_reg
    
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
    
    def _translate_project(self, source: str, columns: str, result: str):
        """翻译PROJECT操作"""
        print(f"  → PROJECT {columns} FROM {source}")
        
        source_reg = self.get_or_create_register(source)
        result_reg = self.get_or_create_register(result)
        
        self.target_gen.emit_project(source_reg, columns, result_reg)
    
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
    
    def _translate_inner_join(self, table1: str, table2_and_condition: str, result: str):
        """翻译INNER JOIN操作"""
        parts = table2_and_condition.split(' ON ')
        table2 = parts[0]
        condition = parts[1] if len(parts) > 1 else "true"
        
        print(f"  → INNER JOIN {table1} {table2} ON {condition}")
        
        for table in [table1, table2]:
            if table not in self.opened_tables:
                self.target_gen.emit_table_open(table)
                self.opened_tables.add(table)
        
        result_reg = self.get_or_create_register(result)
        self.target_gen.emit(TargetInstructionType.INNER_JOIN, [table1, table2, condition], result_reg,
                           comment=f"内连接表 {table1} 和 {table2}")
    
    def _translate_left_join(self, table1: str, table2_and_condition: str, result: str):
        """翻译LEFT JOIN操作"""
        parts = table2_and_condition.split(' ON ')
        table2 = parts[0]
        condition = parts[1] if len(parts) > 1 else "true"
        
        print(f"  → LEFT JOIN {table1} {table2} ON {condition}")
        
        for table in [table1, table2]:
            if table not in self.opened_tables:
                self.target_gen.emit_table_open(table)
                self.opened_tables.add(table)
        
        result_reg = self.get_or_create_register(result)
        self.target_gen.emit(TargetInstructionType.LEFT_JOIN, [table1, table2, condition], result_reg,
                           comment=f"左连接表 {table1} 和 {table2}")
    
    def _translate_right_join(self, table1: str, table2_and_condition: str, result: str):
        """翻译RIGHT JOIN操作"""
        parts = table2_and_condition.split(' ON ')
        table2 = parts[0]
        condition = parts[1] if len(parts) > 1 else "true"
        
        print(f"  → RIGHT JOIN {table1} {table2} ON {condition}")
        
        for table in [table1, table2]:
            if table not in self.opened_tables:
                self.target_gen.emit_table_open(table)
                self.opened_tables.add(table)
        
        result_reg = self.get_or_create_register(result)
        self.target_gen.emit(TargetInstructionType.RIGHT_JOIN, [table1, table2, condition], result_reg,
                           comment=f"右连接表 {table1} 和 {table2}")
    
    def _translate_count(self, source: str, column: str, result: str):
        """翻译COUNT聚合操作"""
        print(f"  → COUNT({column}) FROM {source}")
        
        result_reg = self.get_or_create_register(result)
        
        # 对于COUNT操作，我们需要先扫描表，然后执行COUNT
        # 生成COUNT指令，参数为[source_reg, column]
        source_reg = self.get_or_create_register(source) if source else "R1"
        # 确保column参数正确传递
        operands = [source_reg, column or "*"]  # 传递源寄存器和列名
        self.target_gen.emit(TargetInstructionType.COUNT, operands, result_reg,
                           comment=f"计数聚合: COUNT({column})")
    
    def _translate_sum(self, source: str, column: str, result: str):
        """翻译SUM聚合操作"""
        print(f"  → SUM({column}) FROM {source}")
        
        source_reg = self.get_or_create_register(source) if source else "R1"
        result_reg = self.get_or_create_register(result)
        
        # 传递源寄存器和列名
        operands = [source_reg, column]
        self.target_gen.emit(TargetInstructionType.SUM, operands, result_reg,
                           comment=f"求和聚合: SUM({column})")
    
    def _translate_avg(self, source: str, column: str, result: str):
        """翻译AVG聚合操作"""
        print(f"  → AVG({column}) FROM {source}")
        
        source_reg = self.get_or_create_register(source) if source else "R1"
        result_reg = self.get_or_create_register(result)
        
        # 传递源寄存器和列名
        operands = [source_reg, column]
        self.target_gen.emit(TargetInstructionType.AVG, operands, result_reg,
                           comment=f"平均值聚合: AVG({column})")
    
    def _translate_max(self, source: str, column: str, result: str):
        """翻译MAX聚合操作"""
        print(f"  → MAX({column}) FROM {source}")
        
        source_reg = self.get_or_create_register(source) if source else "R1"
        result_reg = self.get_or_create_register(result)
        
        # 传递源寄存器和列名
        operands = [source_reg, column]
        self.target_gen.emit(TargetInstructionType.MAX, operands, result_reg,
                           comment=f"最大值聚合: MAX({column})")
    
    def _translate_min(self, source: str, column: str, result: str):
        """翻译MIN聚合操作"""
        print(f"  → MIN({column}) FROM {source}")
        
        source_reg = self.get_or_create_register(source) if source else "R1"
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
        
        self.target_gen.emit(TargetInstructionType.ORDER_BY, [source_reg, column_and_direction], result_reg,
                           comment=f"排序操作: ORDER BY {column_and_direction}")
    
    def _translate_having(self, source: str, condition: str, result: str):
        """翻译HAVING操作"""
        print(f"  → HAVING {condition} FROM {source}")
        
        source_reg = self.get_or_create_register(source)
        result_reg = self.get_or_create_register(result)
        
        self.target_gen.emit(TargetInstructionType.HAVING, [source_reg, condition], result_reg,
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
        
        # 翻译为目标代码
        target_instructions = self.translator.translate(quadruples)
        
        # 显示翻译结果
        self.translator.print_translation_result()
        
        return target_instructions
    
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


def test_translator():
    """测试翻译器"""
    from src.common.types import Quadruple
    
    print("=" * 80)
    print("              四元式翻译器测试")
    print("=" * 80)
    
    # 创建测试用的四元式序列
    # 模拟 SELECT name FROM students WHERE age > 18;
    test_quadruples = [
        Quadruple("GT", "age", "18", "T1"),
        Quadruple("SELECT", "name", "students", "T2"), 
        Quadruple("FILTER", "T2", "T1", "T3"),
        Quadruple("OUTPUT", "T3", None, "RESULT")
    ]
    
    # 使用集成代码生成器
    code_gen = IntegratedCodeGenerator()
    target_instructions = code_gen.generate_target_code(test_quadruples)
    
    # 测试优化
    print("\n" + "=" * 80)
    print("              目标代码优化测试")
    print("=" * 80)
    
    optimized_instructions = code_gen.optimize_target_code(target_instructions)
    
    print("优化后的目标代码:")
    print("-" * 40)
    for i, instruction in enumerate(optimized_instructions, 1):
        print(f"  {i}: {instruction}")
    
    return target_instructions

if __name__ == "__main__":
    test_translator()