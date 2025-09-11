"""
目标代码指令集定义
将四元式中间代码转换为可执行的目标指令
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from enum import Enum
from dataclasses import dataclass
from typing import List, Optional, Any

class TargetInstructionType(Enum):
    """目标指令类型"""
    # 表操作指令
    OPEN = "OPEN"           # 打开表 - OPEN table_name
    CLOSE = "CLOSE"         # 关闭表 - CLOSE table_name
    
    # 数据访问指令
    SCAN = "SCAN"           # 扫描表 - SCAN table_name -> result_set
    FILTER = "FILTER"       # 过滤数据 - FILTER source condition -> result_set
    PROJECT = "PROJECT"     # 投影列 - PROJECT source columns -> result_set
    
    # JOIN指令
    JOIN = "JOIN"           # 连接表 - JOIN table1 table2 condition -> result_set
    INNER_JOIN = "INNER_JOIN" # 内连接
    LEFT_JOIN = "LEFT_JOIN"   # 左连接
    RIGHT_JOIN = "RIGHT_JOIN" # 右连接
    FULL_JOIN = "FULL_JOIN"   # 全外连接
    ON = "ON"               # JOIN条件
    
    # 聚合指令
    COUNT = "COUNT"         # 计数 - COUNT column -> register
    SUM = "SUM"             # 求和 - SUM column -> register
    AVG = "AVG"             # 平均值 - AVG column -> register
    MAX = "MAX"             # 最大值 - MAX column -> register
    MIN = "MIN"             # 最小值 - MIN column -> register
    
    # 分组和排序指令
    GROUP_BY = "GROUP_BY"   # 分组 - GROUP_BY columns -> register
    ORDER_BY = "ORDER_BY"   # 排序 - ORDER_BY column direction -> register
    HAVING = "HAVING"       # 分组过滤 - HAVING condition
    
    # 窗口函数指令
    ROW_NUMBER = "ROW_NUMBER" # 行号
    RANK = "RANK"           # 排名
    DENSE_RANK = "DENSE_RANK" # 密集排名
    
    # 控制流增强指令
    BEGIN = "BEGIN"         # 开始执行
    END = "END"             # 结束执行
    WHERE = "WHERE"         # WHERE条件
    
    # 条件操作指令
    GT = "GT"               # 大于比较 - GT operand1 operand2 -> bool_result
    GE = "GE"               # 大于等于 - GE operand1 operand2 -> bool_result  
    LT = "LT"               # 小于比较 - LT operand1 operand2 -> bool_result
    LE = "LE"               # 小于等于 - LE operand1 operand2 -> bool_result
    EQ = "EQ"               # 等于比较 - EQ operand1 operand2 -> bool_result
    NE = "NE"               # 不等于比较 - NE operand1 operand2 -> bool_result
    
    # 逻辑操作指令
    AND = "AND"             # 逻辑与 - AND condition1 condition2 -> bool_result
    OR = "OR"               # 逻辑或 - OR condition1 condition2 -> bool_result
    NOT = "NOT"             # 逻辑非 - NOT condition -> bool_result
    
    # 输出指令
    OUTPUT = "OUTPUT"       # 输出结果 - OUTPUT result_set
    
    # 内存操作指令
    LOAD = "LOAD"           # 加载数据 - LOAD source -> register
    STORE = "STORE"         # 存储数据 - STORE register -> destination
    MOVE = "MOVE"           # 移动数据 - MOVE source -> destination
    
    # 控制流指令
    JUMP = "JUMP"           # 无条件跳转 - JUMP label
    JUMP_IF = "JUMP_IF"     # 条件跳转 - JUMP_IF condition label
    LABEL = "LABEL"         # 标签定义 - LABEL label_name
    
    # 特殊指令
    NOP = "NOP"             # 空操作 - NOP
    HALT = "HALT"           # 停机 - HALT

@dataclass
class TargetInstruction:
    """目标指令"""
    op: TargetInstructionType   # 操作类型
    operands: List[str]         # 操作数列表
    result: Optional[str] = None # 结果存储位置
    comment: Optional[str] = None # 注释
    
    def __str__(self) -> str:
        """格式化输出指令"""
        operands_str = " ".join(self.operands) if self.operands else ""
        result_str = f" -> {self.result}" if self.result else ""
        comment_str = f"  # {self.comment}" if self.comment else ""
        
        return f"{self.op.value} {operands_str}{result_str}{comment_str}".strip()

class TargetCodeGenerator:
    """目标代码生成器"""
    
    def __init__(self):
        """初始化目标代码生成器"""
        self.instructions: List[TargetInstruction] = []
        self.label_counter = 0
        self.register_counter = 0
    
    def generate_label(self) -> str:
        """生成标签"""
        self.label_counter += 1
        return f"L{self.label_counter}"
    
    def generate_register(self) -> str:
        """生成寄存器名"""
        self.register_counter += 1
        return f"R{self.register_counter}"
    
    def emit(self, op: TargetInstructionType, operands: List[str] = None, 
             result: str = None, comment: str = None) -> TargetInstruction:
        """生成目标指令"""
        if operands is None:
            operands = []
        
        instruction = TargetInstruction(op, operands, result, comment)
        self.instructions.append(instruction)
        return instruction
    
    def emit_table_open(self, table_name: str) -> TargetInstruction:
        """生成打开表指令"""
        return self.emit(TargetInstructionType.OPEN, [table_name], 
                        comment=f"打开表 {table_name}")
    
    def emit_table_close(self, table_name: str) -> TargetInstruction:
        """生成关闭表指令"""
        return self.emit(TargetInstructionType.CLOSE, [table_name],
                        comment=f"关闭表 {table_name}")
    
    def emit_scan(self, table_name: str, result_reg: str) -> TargetInstruction:
        """生成扫描表指令"""
        return self.emit(TargetInstructionType.SCAN, [table_name], result_reg,
                        comment=f"扫描表 {table_name}")
    
    def emit_filter(self, source_reg: str, condition_reg: str, 
                   result_reg: str) -> TargetInstruction:
        """生成过滤指令"""
        return self.emit(TargetInstructionType.FILTER, 
                        [source_reg, condition_reg], result_reg,
                        comment="应用过滤条件")
    
    def emit_project(self, source_reg: str, columns: str, 
                    result_reg: str) -> TargetInstruction:
        """生成投影指令"""
        return self.emit(TargetInstructionType.PROJECT,
                        [source_reg, columns], result_reg,
                        comment=f"投影列: {columns}")
    
    def emit_comparison(self, op: TargetInstructionType, operand1: str, 
                       operand2: str, result_reg: str) -> TargetInstruction:
        """生成比较指令"""
        op_name = {
            TargetInstructionType.GT: "大于",
            TargetInstructionType.GE: "大于等于", 
            TargetInstructionType.LT: "小于",
            TargetInstructionType.LE: "小于等于",
            TargetInstructionType.EQ: "等于",
            TargetInstructionType.NE: "不等于"
        }.get(op, str(op.value))
        
        return self.emit(op, [operand1, operand2], result_reg,
                        comment=f"{op_name}比较: {operand1} {op.value} {operand2}")
    
    def emit_output(self, source_reg: str) -> TargetInstruction:
        """生成输出指令"""
        return self.emit(TargetInstructionType.OUTPUT, [source_reg],
                        comment="输出查询结果")
    
    def emit_halt(self) -> TargetInstruction:
        """生成停机指令"""
        return self.emit(TargetInstructionType.HALT, [],
                        comment="程序结束")
    
    def emit_join(self, table1: str, table2: str, condition: str, 
                 result_reg: str, join_type: TargetInstructionType = TargetInstructionType.JOIN) -> TargetInstruction:
        """生成JOIN指令"""
        join_name = {
            TargetInstructionType.JOIN: "连接",
            TargetInstructionType.INNER_JOIN: "内连接",
            TargetInstructionType.LEFT_JOIN: "左连接",
            TargetInstructionType.RIGHT_JOIN: "右连接",
            TargetInstructionType.FULL_JOIN: "全外连接"
        }.get(join_type, "连接")
        
        return self.emit(join_type, [table1, table2, condition], result_reg,
                        comment=f"{join_name}: {table1} {join_type.value} {table2} ON {condition}")
    
    def emit_aggregate(self, source_reg: str, column: str, result_reg: str,
                      agg_type: TargetInstructionType) -> TargetInstruction:
        """生成聚合函数指令"""
        agg_name = {
            TargetInstructionType.COUNT: "计数",
            TargetInstructionType.SUM: "求和",
            TargetInstructionType.AVG: "平均值",
            TargetInstructionType.MAX: "最大值",
            TargetInstructionType.MIN: "最小值"
        }.get(agg_type, "聚合")
        
        return self.emit(agg_type, [source_reg, column], result_reg,
                        comment=f"{agg_name}聚合: {agg_type.value}({column})")
    
    def emit_group_by(self, source_reg: str, columns: str, result_reg: str) -> TargetInstruction:
        """生成GROUP BY指令"""
        return self.emit(TargetInstructionType.GROUP_BY, [source_reg, columns], result_reg,
                        comment=f"分组操作: GROUP BY {columns}")
    
    def emit_order_by(self, source_reg: str, column_and_direction: str, 
                     result_reg: str) -> TargetInstruction:
        """生成ORDER BY指令"""
        return self.emit(TargetInstructionType.ORDER_BY, [source_reg, column_and_direction], result_reg,
                        comment=f"排序操作: ORDER BY {column_and_direction}")
    
    def emit_having(self, source_reg: str, condition: str, result_reg: str) -> TargetInstruction:
        """生成HAVING指令"""
        return self.emit(TargetInstructionType.HAVING, [source_reg, condition], result_reg,
                        comment=f"分组过滤: HAVING {condition}")
    
    def get_instructions(self) -> List[TargetInstruction]:
        """获取生成的指令列表"""
        return self.instructions.copy()
    
    def print_code(self):
        """打印生成的目标代码"""
        print("\n生成的目标代码:")
        print("-" * 60)
        if not self.instructions:
            print("无目标代码生成")
            return
        
        for i, instruction in enumerate(self.instructions, 1):
            print(f"{i:3d}: {instruction}")
    
    def clear(self):
        """清空生成器状态"""
        self.instructions = []
        self.label_counter = 0
        self.register_counter = 0

# 指令集说明文档
INSTRUCTION_SET_DOC = """
目标指令集说明
=============

1. 表操作指令:
   OPEN table_name          # 打开表，准备访问
   CLOSE table_name         # 关闭表，释放资源

2. 数据访问指令:
   SCAN table_name -> reg   # 扫描表中所有记录到寄存器
   FILTER reg1 reg2 -> reg3 # 根据条件reg2过滤reg1中的数据
   PROJECT reg columns -> reg2 # 从reg中投影指定列到reg2

3. 比较指令:
   GT/GE/LT/LE/EQ/NE operand1 operand2 -> reg # 比较操作

4. 输出指令:
   OUTPUT reg               # 输出寄存器中的数据

5. 控制指令:
   HALT                     # 程序结束

指令执行流程示例:
================
SELECT name FROM students WHERE age > 18;

转换为目标代码:
1: OPEN students             # 打开表 students
2: SCAN students -> R1       # 扫描表 students
3: GT age 18 -> R2          # 大于比较: age GT 18  
4: FILTER R1 R2 -> R3       # 应用过滤条件
5: PROJECT R3 name -> R4    # 投影列: name
6: OUTPUT R4                # 输出查询结果
7: CLOSE students           # 关闭表 students
8: HALT                     # 程序结束
"""

def test_target_instructions():
    """测试目标指令集"""
    print("=" * 60)
    print("          目标指令集测试")
    print("=" * 60)
    
    # 创建代码生成器
    generator = TargetCodeGenerator()
    
    print("基本查询示例:")
    print("SQL: SELECT name FROM students WHERE age > 18;")
    print()
    
    # 生成指令序列
    generator.emit_table_open("students")
    reg1 = generator.generate_register()
    generator.emit_scan("students", reg1)
    
    reg2 = generator.generate_register() 
    generator.emit_comparison(TargetInstructionType.GT, "age", "18", reg2)
    
    reg3 = generator.generate_register()
    generator.emit_filter(reg1, reg2, reg3)
    
    reg4 = generator.generate_register()
    generator.emit_project(reg3, "name", reg4)
    
    generator.emit_output(reg4)
    generator.emit_table_close("students")
    generator.emit_halt()
    
    # 打印生成的代码
    generator.print_code()
    
    print(f"\n生成了 {len(generator.instructions)} 条目标指令")
    
    # 清空生成器并测试复杂查询
    generator.clear()
    
    print("\n" + "=" * 60)
    print("复杂查询示例:")
    print("SQL: SELECT s.name, COUNT(*) FROM students s INNER JOIN courses c ON s.id = c.student_id GROUP BY s.name ORDER BY COUNT(*) DESC;")
    print()
    
    # 生成复杂查询指令
    generator.emit_table_open("students")
    generator.emit_table_open("courses")
    
    reg1 = generator.generate_register()
    generator.emit_join("students", "courses", "students.id = courses.student_id", reg1, TargetInstructionType.INNER_JOIN)
    
    reg2 = generator.generate_register()
    generator.emit_group_by(reg1, "students.name", reg2)
    
    reg3 = generator.generate_register()
    generator.emit_aggregate(reg2, "*", reg3, TargetInstructionType.COUNT)
    
    reg4 = generator.generate_register()
    generator.emit_project(reg3, "students.name, COUNT(*)", reg4)
    
    reg5 = generator.generate_register()
    generator.emit_order_by(reg4, "COUNT(*) DESC", reg5)
    
    generator.emit_output(reg5)
    generator.emit_table_close("students")
    generator.emit_table_close("courses")
    generator.emit_halt()
    
    generator.print_code()
    
    print(f"\n生成了 {len(generator.instructions)} 条目标指令")
    
    # 打印指令集说明
    print(INSTRUCTION_SET_DOC)

if __name__ == "__main__":
    test_target_instructions()