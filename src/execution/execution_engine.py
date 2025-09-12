"""
目标代码执行引擎
执行编译器生成的目标指令，与存储引擎交互完成SQL查询处理
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import time
from typing import Dict, List, Optional, Any, Tuple, Iterator
from dataclasses import dataclass
from src.compiler.codegen.target_instructions import TargetInstruction, TargetInstructionType
from src.storage.storage_engine import StorageEngine
from src.storage.table.table_manager import ColumnType

@dataclass
class ExecutionContext:
    """执行上下文"""
    current_table: Optional[str] = None
    current_records: List[Dict[str, Any]] = None
    filtered_records: List[Dict[str, Any]] = None
    projected_columns: Optional[List[str]] = None
    comparison_result: Optional[bool] = None
    registers: Dict[str, Any] = None
    # 复杂查询相关
    groups: Dict[tuple, List[Dict[str, Any]]] = None  # 分组数据
    group_columns: List[str] = None  # 分组列
    join_tables: List[str] = None  # 参与连接的表
    
    def __post_init__(self):
        if self.current_records is None:
            self.current_records = []
        if self.filtered_records is None:
            self.filtered_records = []
        if self.registers is None:
            self.registers = {}
        if self.groups is None:
            self.groups = {}
        if self.group_columns is None:
            self.group_columns = []
        if self.join_tables is None:
            self.join_tables = []

class ExecutionEngine:
    """执行引擎"""
    
    def __init__(self, storage_engine: Optional[StorageEngine] = None):
        """
        初始化执行引擎
        
        Args:
            storage_engine: 存储引擎实例
        """
        self.storage_engine = storage_engine or StorageEngine()
        self.context = ExecutionContext()
        
        # 执行统计
        self.stats = {
            'instructions_executed': 0,
            'execution_time': 0.0,
            'tables_opened': 0,
            'records_scanned': 0,
            'records_filtered': 0,
            'records_output': 0
        }
        
        # 添加LIMIT和OFFSET相关属性
        self.limit_count = None
        self.offset_count = 0
        
        # 指令处理映射
        self.instruction_handlers = {
            TargetInstructionType.OPEN: self._execute_open,
            TargetInstructionType.CLOSE: self._execute_close,
            TargetInstructionType.SCAN: self._execute_scan,
            TargetInstructionType.FILTER: self._execute_filter,
            TargetInstructionType.PROJECT: self._execute_project,
            TargetInstructionType.OUTPUT: self._execute_output,
            TargetInstructionType.GT: self._execute_gt,
            TargetInstructionType.GE: self._execute_ge,
            TargetInstructionType.LT: self._execute_lt,
            TargetInstructionType.LE: self._execute_le,
            TargetInstructionType.EQ: self._execute_eq,
            TargetInstructionType.NE: self._execute_ne,
            TargetInstructionType.LOAD: self._execute_load,
            TargetInstructionType.STORE: self._execute_store,
            TargetInstructionType.JUMP: self._execute_jump,
            TargetInstructionType.JUMP_IF: self._execute_jump_if,
            TargetInstructionType.HALT: self._execute_halt,
            # 复杂查询指令
            TargetInstructionType.JOIN: self._execute_join,
            TargetInstructionType.INNER_JOIN: self._execute_inner_join,
            TargetInstructionType.LEFT_JOIN: self._execute_left_join,
            TargetInstructionType.RIGHT_JOIN: self._execute_right_join,
            TargetInstructionType.FULL_JOIN: self._execute_full_join,
            TargetInstructionType.COUNT: self._execute_count,
            TargetInstructionType.SUM: self._execute_sum,
            TargetInstructionType.AVG: self._execute_avg,
            TargetInstructionType.MAX: self._execute_max,
            TargetInstructionType.MIN: self._execute_min,
            TargetInstructionType.GROUP_BY: self._execute_group_by,
            TargetInstructionType.ORDER_BY: self._execute_order_by,
            TargetInstructionType.HAVING: self._execute_having,
            # 控制流指令
            TargetInstructionType.BEGIN: self._execute_begin,
            TargetInstructionType.END: self._execute_end,
            # 限制指令
            TargetInstructionType.LIMIT: self._execute_limit,
            TargetInstructionType.OFFSET: self._execute_offset,
        }
    
    def execute(self, instructions: List[TargetInstruction]) -> List[Dict[str, Any]]:
        """
        执行目标指令序列
        
        Args:
            instructions: 目标指令列表
            
        Returns:
            查询结果
        """
        start_time = time.time()
        results = []
        
        try:
            # 重置执行上下文
            self.context = ExecutionContext()
            instruction_pointer = 0
            
            print(f"\n开始执行 {len(instructions)} 条目标指令:")
            print("-" * 60)
            
            while instruction_pointer < len(instructions):
                instruction = instructions[instruction_pointer]
                
                # 过滤掉None值
                operands_str = ' '.join(str(op) for op in instruction.operands if op is not None)
                print(f"[{instruction_pointer:2d}] {instruction.op.value:<8} {operands_str}")
                
                # 执行指令
                handler = self.instruction_handlers.get(instruction.op)
                if handler:
                    result = handler(instruction)
                    
                    # 处理跳转指令
                    if isinstance(result, int):
                        instruction_pointer = result
                        continue
                    elif result == "HALT":
                        break
                    elif isinstance(result, list):
                        results.extend(result)
                else:
                    print(f" 未知指令类型: {instruction.op}")
                
                self.stats['instructions_executed'] += 1
                instruction_pointer += 1
            
            # 如果有输出结果，返回最终结果
            if hasattr(self.context, 'output_results'):
                results = self.context.output_results
            
        except Exception as e:
            print(f"执行错误: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            self.stats['execution_time'] = time.time() - start_time
        
        return results
    
    def _execute_open(self, instruction: TargetInstruction) -> None:
        """执行OPEN指令 - 打开表"""
        table_name = instruction.operands[0]
        print(f"  → 打开表: {table_name}")
        
        self.context.current_table = table_name
        self.stats['tables_opened'] += 1
    
    def _execute_close(self, instruction: TargetInstruction) -> None:
        """执行CLOSE指令 - 关闭表"""
        print(f"  → 关闭表: {self.context.current_table}")
        self.context.current_table = None
        self.context.current_records = []
    
    def _execute_scan(self, instruction: TargetInstruction) -> None:
        """执行SCAN指令 - 扫描表记录"""
        if not self.context.current_table:
            raise RuntimeError("No table opened for SCAN operation")
        
        # 从存储引擎获取所有记录
        records = self.storage_engine.select(self.context.current_table)
        self.context.current_records = records
        self.stats['records_scanned'] += len(records)
        
        print(f"  → 扫描到 {len(records)} 条记录")
    
    def _execute_filter(self, instruction: TargetInstruction) -> None:
        """执行FILTER指令 - 过滤记录"""
        if len(instruction.operands) < 2:
            print("  → 使用比较结果进行过滤")
            # 使用上一次比较的结果进行过滤
            if self.context.comparison_result is not None:
                if self.context.comparison_result:
                    self.context.filtered_records = self.context.current_records.copy()
                else:
                    self.context.filtered_records = []
            else:
                self.context.filtered_records = self.context.current_records.copy()
            return
        
        source_reg = instruction.operands[0]
        condition_reg = instruction.operands[1]
        
        # 检查是否有三个操作数（直接比较模式）
        if len(instruction.operands) >= 3:
            column = instruction.operands[0]
            operator = instruction.operands[1]
            value = instruction.operands[2]
            
            # 直接进行记录过滤
            self._filter_records_directly(column, operator, value)
            return
        
        # 如果没有比较结果，尝试从寄存器获取比较条件
        if condition_reg in self.context.registers:
            comparison_result = self.context.registers[condition_reg]
            if isinstance(comparison_result, bool):
                if comparison_result:
                    self.context.filtered_records = self.context.current_records.copy()
                else:
                    self.context.filtered_records = []
                print(f"  → 使用寄存器 {condition_reg} 的比较结果进行过滤: {len(self.context.filtered_records)} 条记录")
                return
        
        # 兜底：不过滤
        self.context.filtered_records = self.context.current_records.copy()
        print(f"  → 无有效过滤条件，保持所有记录: {len(self.context.filtered_records)} 条")
    
    def _execute_project(self, instruction: TargetInstruction) -> None:
        """执行PROJECT指令 - 投影列"""
        if len(instruction.operands) < 2:
            print("PROJECT指令参数不足")
            return
        
        source_reg = instruction.operands[0]
        columns = instruction.operands[1]
        
        # 处理列参数，确保它是字符串而不是列表
        if isinstance(columns, list):
            if len(columns) == 1:
                columns = columns[0]
            else:
                columns = ','.join(str(col) for col in columns)
        
        print(f"  → 投影列: {columns}")
        
        # 获取要投影的记录
        records = self.context.filtered_records or self.context.current_records
        
        # 保存原始记录以供聚合函数使用
        original_records = records
        
        if columns == '*':
            # 选择所有列，保持原有数据不变
            pass
        else:
            # 选择指定列
            column_list = [col.strip() for col in columns.split(',')]
            projected_records = []
            
            # 打印第一条记录的键，用于调试
            if records:
                print(f"    记录中的键: {list(records[0].keys())}")
            
            for record in records:
                projected_record = {}
                for col in column_list:
                    print(f"    尝试匹配列: '{col}'")
                    # 直接匹配列名
                    if col in record:
                        projected_record[col] = record[col]
                        print(f"    直接匹配列 '{col}' -> {record[col]}")
                    else:
                        # 处理带前缀的列名（如 "students.name"）
                        # 如果直接匹配失败，尝试匹配带前缀的列名
                        found = False
                        for key, value in record.items():
                            print(f"      检查键 '{key}'")
                            # 精确匹配
                            if key == col:
                                projected_record[col] = value
                                found = True
                                print(f"    精确匹配列 '{col}' -> {value}")
                                break
                            # 处理带表前缀的列名，如 "students.name" 匹配记录中的 "students.name"
                            elif '.' in col and key == col:
                                projected_record[col] = value
                                found = True
                                print(f"    带前缀匹配列 '{col}' -> {value}")
                                break
                            # 处理不带表前缀的列名，如 "name" 匹配记录中的 "students.name"
                            elif '.' not in col and key.endswith('.' + col):
                                projected_record[col] = value
                                found = True
                                print(f"    后缀匹配列 '{col}' -> {value} (来自 {key})")
                                break
                            # 处理表别名形式的列名，如 "s.name" 匹配记录中的 "students.name"
                            # 需要根据JOIN操作中表名和别名的映射关系来处理
                            elif '.' in col:
                                # 分解列名 "s.name" 为表别名 "s" 和列名 "name"
                                alias_parts = col.split('.')
                                if len(alias_parts) == 2:
                                    alias, column_name = alias_parts
                                    print(f"      尝试别名匹配: alias='{alias}', column_name='{column_name}'")
                                    # 根据JOIN操作的特性，我们需要匹配完整的表名前缀
                                    # 例如，"s.name" 应该匹配 "students.name"
                                    # 我们可以通过检查键是否以完整表名开头来实现
                                    if alias == 's' and key.startswith('students.') and key.endswith('.' + column_name):
                                        projected_record[col] = value
                                        found = True
                                        print(f"    别名匹配列 '{col}' -> {value} (来自 {key})")
                                        break
                                    elif alias == 'c' and key.startswith('courses.') and key.endswith('.' + column_name):
                                        projected_record[col] = value
                                        found = True
                                        print(f"    别名匹配列 '{col}' -> {value} (来自 {key})")
                                        break
                        
                        # 如果还是没找到，检查是否是聚合结果
                        if not found:
                            if col in self.context.registers:
                                projected_record[col] = self.context.registers[col]
                                print(f"    寄存器匹配列 '{col}' -> {self.context.registers[col]}")
                            else:
                                projected_record[col] = None
                                print(f"    未找到列 '{col}'，设置为 None")
                # 只有当projected_record非空时才添加
                if projected_record:
                    projected_records.append(projected_record)
            
            # 更新当前记录，但保留原始记录供聚合函数使用
            if projected_records:
                self.context.current_records = projected_records
        
        # 保存原始记录供聚合函数使用
        self.context.original_records = original_records
    
    def _execute_output(self, instruction: TargetInstruction) -> List[Dict[str, Any]]:
        """执行OUTPUT指令 - 输出结果"""
        # 确定要输出的记录
        records_to_output = self.context.filtered_records or self.context.current_records
        
        # 应用OFFSET
        if self.offset_count > 0:
            records_to_output = records_to_output[self.offset_count:]
            print(f"  → 应用OFFSET {self.offset_count}，剩余 {len(records_to_output)} 条记录")
        
        # 应用LIMIT
        if self.limit_count is not None:
            records_to_output = records_to_output[:self.limit_count]
            print(f"  → 应用LIMIT {self.limit_count}，最终输出 {len(records_to_output)} 条记录")
        
        # 应用列投影
        if self.context.projected_columns:
            projected_records = []
            for record in records_to_output:
                projected_record = {}
                for col in self.context.projected_columns:
                    if col == '*':
                        projected_record = record.copy()
                        break
                    elif col in record:
                        projected_record[col] = record[col]
                    else:
                        # 如果列不存在，设置为None或跳过
                        print(f"    警告: 列 '{col}' 在记录中不存在")
                # 只有当projected_record非空时才添加
                if projected_record:
                    projected_records.append(projected_record)
            records_to_output = projected_records
        
        self.stats['records_output'] += len(records_to_output)
        
        print(f"  → 输出 {len(records_to_output)} 条记录")
        
        # 保存输出结果
        if not hasattr(self.context, 'output_results'):
            self.context.output_results = []
        self.context.output_results.extend(records_to_output)
        
        return records_to_output
    
    def _execute_gt(self, instruction: TargetInstruction) -> None:
        """执行GT指令 - 大于比较"""
        left = self._get_value(instruction.operands[0])
        right = self._get_value(instruction.operands[1])
        self.context.comparison_result = left > right
        print(f"  → {left} > {right} = {self.context.comparison_result}")
    
    def _execute_ge(self, instruction: TargetInstruction) -> None:
        """执行GE指令 - 大于等于比较"""
        left = self._get_value(instruction.operands[0])
        right = self._get_value(instruction.operands[1])
        self.context.comparison_result = left >= right
        print(f"  → {left} >= {right} = {self.context.comparison_result}")
    
    def _execute_lt(self, instruction: TargetInstruction) -> None:
        """执行LT指令 - 小于比较"""
        left = self._get_value(instruction.operands[0])
        right = self._get_value(instruction.operands[1])
        self.context.comparison_result = left < right
        print(f"  → {left} < {right} = {self.context.comparison_result}")
    
    def _execute_le(self, instruction: TargetInstruction) -> None:
        """执行LE指令 - 小于等于比较"""
        left = self._get_value(instruction.operands[0])
        right = self._get_value(instruction.operands[1])
        self.context.comparison_result = left <= right
        print(f"  → {left} <= {right} = {self.context.comparison_result}")
    
    def _execute_eq(self, instruction: TargetInstruction) -> None:
        """执行EQ指令 - 等于比较"""
        left = self._get_value(instruction.operands[0])
        right = self._get_value(instruction.operands[1])
        self.context.comparison_result = left == right
        print(f"  → {left} == {right} = {self.context.comparison_result}")
    
    def _execute_ne(self, instruction: TargetInstruction) -> None:
        """执行NE指令 - 不等于比较"""
        left = self._get_value(instruction.operands[0])
        right = self._get_value(instruction.operands[1])
        self.context.comparison_result = left != right
        print(f"  → {left} != {right} = {self.context.comparison_result}")
    
    def _execute_load(self, instruction: TargetInstruction) -> None:
        """执行LOAD指令 - 加载值到寄存器"""
        register = instruction.operands[0]
        value = self._get_value(instruction.operands[1])
        self.context.registers[register] = value
        print(f"  → 加载 {value} 到寄存器 {register}")
    
    def _execute_store(self, instruction: TargetInstruction) -> None:
        """执行STORE指令 - 存储寄存器值"""
        register = instruction.operands[0]
        target = instruction.operands[1]
        value = self.context.registers.get(register)
        print(f"  → 存储寄存器 {register} 的值 {value} 到 {target}")
    
    def _execute_jump(self, instruction: TargetInstruction) -> int:
        """执行JUMP指令 - 无条件跳转"""
        target = int(instruction.operands[0])
        print(f"  → 跳转到指令 {target}")
        return target
    
    def _execute_jump_if(self, instruction: TargetInstruction) -> Optional[int]:
        """执行JUMP_IF指令 - 条件跳转"""
        target = int(instruction.operands[0])
        condition = instruction.operands[1] if len(instruction.operands) > 1 else "true"
        
        should_jump = False
        if condition == "true":
            should_jump = self.context.comparison_result if self.context.comparison_result is not None else True
        elif condition == "false":
            should_jump = not (self.context.comparison_result if self.context.comparison_result is not None else False)
        
        if should_jump:
            print(f"  → 条件跳转到指令 {target}")
            return target
        else:
            print(f"  → 条件跳转失败，继续执行")
            return None
    
    def _execute_halt(self, instruction: TargetInstruction) -> str:
        """执行HALT指令 - 停机"""
        print("  → 程序终止")
        return "HALT"
    
    def _execute_join(self, instruction: TargetInstruction) -> None:
        """执行JOIN指令 - 表连接"""
        if len(instruction.operands) < 3:
            print("JOIN指令参数不足")
            return
        
        table1 = instruction.operands[0]
        table2 = instruction.operands[1] 
        condition = instruction.operands[2]
        
        print(f"  → 连接表: {table1} JOIN {table2} ON {condition}")
        
        # 获取两个表的数据
        records1 = self.storage_engine.select(table1)
        records2 = self.storage_engine.select(table2)
        
        # 执行笛卡尔积（简化实现）
        joined_records = []
        for r1 in records1:
            for r2 in records2:
                # 创建合并记录
                merged_record = {}
                # 添加table1的字段（加前缀）
                for key, value in r1.items():
                    merged_record[f"{table1}.{key}"] = value
                # 添加table2的字段（加前缀）
                for key, value in r2.items():
                    merged_record[f"{table2}.{key}"] = value
                
                joined_records.append(merged_record)
        
        self.context.current_records = joined_records
        print(f"    连接结果: {len(joined_records)} 条记录")
    
    def _execute_inner_join(self, instruction: TargetInstruction) -> None:
        """执行INNER JOIN指令 - 内连接"""
        if len(instruction.operands) < 3:
            print("INNER JOIN指令参数不足")
            return
        
        table1 = instruction.operands[0]
        table2 = instruction.operands[1]
        condition = instruction.operands[2]
        
        print(f"  → 内连接: {table1} INNER JOIN {table2} ON {condition}")
        
        # 获取两个表的数据
        records1 = self.storage_engine.select(table1)
        records2 = self.storage_engine.select(table2)
        
        # 执行内连接
        joined_records = []
        for r1 in records1:
            for r2 in records2:
                # 简化的条件检查（实际应该解析condition）
                if self._evaluate_join_condition(r1, r2, condition, table1, table2):
                    # 创建合并记录
                    merged_record = {}
                    # 使用表名作为前缀
                    for key, value in r1.items():
                        merged_record[f"{table1}.{key}"] = value
                    for key, value in r2.items():
                        merged_record[f"{table2}.{key}"] = value
                    joined_records.append(merged_record)
        
        self.context.current_records = joined_records
        print(f"    内连接结果: {len(joined_records)} 条记录")
        # 调试输出第一条记录
        if joined_records:
            print(f"    第一条记录: {joined_records[0]}")
    
    def _execute_left_join(self, instruction: TargetInstruction) -> None:
        """执行LEFT JOIN指令 - 左连接"""
        if len(instruction.operands) < 3:
            print("LEFT JOIN指令参数不足")
            return
        
        table1 = instruction.operands[0]
        table2 = instruction.operands[1]
        condition = instruction.operands[2]
        
        print(f"  → 左连接: {table1} LEFT JOIN {table2} ON {condition}")
        
        records1 = self.storage_engine.select(table1)
        records2 = self.storage_engine.select(table2)
        
        joined_records = []
        for r1 in records1:
            matched = False
            for r2 in records2:
                if self._evaluate_join_condition(r1, r2, condition, table1, table2):
                    merged_record = {}
                    for key, value in r1.items():
                        merged_record[f"{table1}.{key}"] = value
                    for key, value in r2.items():
                        merged_record[f"{table2}.{key}"] = value
                    joined_records.append(merged_record)
                    matched = True
            
            # 如果左表记录没有匹配，仍然保留（右表字段为NULL）
            if not matched:
                merged_record = {}
                for key, value in r1.items():
                    merged_record[f"{table1}.{key}"] = value
                # 为右表字段设置NULL
                if records2:
                    for key in records2[0].keys():
                        merged_record[f"{table2}.{key}"] = None
                joined_records.append(merged_record)
        
        self.context.current_records = joined_records
        print(f"    左连接结果: {len(joined_records)} 条记录")
    
    def _execute_right_join(self, instruction: TargetInstruction) -> None:
        """执行RIGHT JOIN指令 - 右连接"""
        if len(instruction.operands) < 3:
            print("  ⚠️  RIGHT JOIN指令参数不足")
            return
        
        table1 = instruction.operands[0]
        table2 = instruction.operands[1]
        condition = instruction.operands[2]
        
        print(f"  → 右连接: {table1} RIGHT JOIN {table2} ON {condition}")
        
        # 右连接等价于交换表顺序的左连接
        records1 = self.storage_engine.select(table1)
        records2 = self.storage_engine.select(table2)
        
        joined_records = []
        for r2 in records2:
            matched = False
            for r1 in records1:
                if self._evaluate_join_condition(r1, r2, condition, table1, table2):
                    merged_record = {}
                    for key, value in r1.items():
                        merged_record[f"{table1}.{key}"] = value
                    for key, value in r2.items():
                        merged_record[f"{table2}.{key}"] = value
                    joined_records.append(merged_record)
                    matched = True
            
            if not matched:
                merged_record = {}
                # 为左表字段设置NULL
                if records1:
                    for key in records1[0].keys():
                        merged_record[f"{table1}.{key}"] = None
                for key, value in r2.items():
                    merged_record[f"{table2}.{key}"] = value
                joined_records.append(merged_record)
        
        self.context.current_records = joined_records
        print(f"右连接结果: {len(joined_records)} 条记录")
    
    def _execute_full_join(self, instruction: TargetInstruction) -> None:
        """执行FULL JOIN指令 - 全外连接"""
        if len(instruction.operands) < 3:
            print("  ⚠️  FULL JOIN指令参数不足")
            return
        
        table1 = instruction.operands[0]
        table2 = instruction.operands[1]
        condition = instruction.operands[2]
        
        print(f"  → 全外连接: {table1} FULL JOIN {table2} ON {condition}")
        
        # 获取两个表的数据
        records1 = self.storage_engine.select(table1)
        records2 = self.storage_engine.select(table2)
        
        # 执行全外连接
        joined_records = []
        matched_records1 = set()  # 记录表1中已匹配的记录索引
        matched_records2 = set()  # 记录表2中已匹配的记录索引
        
        # 内连接部分
        for i, r1 in enumerate(records1):
            for j, r2 in enumerate(records2):
                if self._evaluate_join_condition(r1, r2, condition, table1, table2):
                    # 创建合并记录
                    merged_record = {}
                    for key, value in r1.items():
                        merged_record[f"{table1}.{key}"] = value
                    for key, value in r2.items():
                        merged_record[f"{table2}.{key}"] = value
                    joined_records.append(merged_record)
                    matched_records1.add(i)
                    matched_records2.add(j)
        
        # 左外连接部分（表1中未匹配的记录）
        for i, r1 in enumerate(records1):
            if i not in matched_records1:
                merged_record = {}
                for key, value in r1.items():
                    merged_record[f"{table1}.{key}"] = value
                # 为右表字段设置NULL
                if records2:
                    for key in records2[0].keys():
                        merged_record[f"{table2}.{key}"] = None
                joined_records.append(merged_record)
        
        # 右外连接部分（表2中未匹配的记录）
        for j, r2 in enumerate(records2):
            if j not in matched_records2:
                merged_record = {}
                # 为左表字段设置NULL
                if records1:
                    for key in records1[0].keys():
                        merged_record[f"{table1}.{key}"] = None
                for key, value in r2.items():
                    merged_record[f"{table2}.{key}"] = value
                joined_records.append(merged_record)
        
        self.context.current_records = joined_records
        print(f"    全外连接结果: {len(joined_records)} 条记录")
    
    def _evaluate_join_condition(self, record1: Dict[str, Any], record2: Dict[str, Any], 
                               condition: str, table1: str, table2: str) -> bool:
        """评估连接条件"""
        # 简化的条件评估，支持多种格式的条件
        try:
            # 处理格式 "s . id = c . student_id" 或 "table1.col1 = table2.col2"
            if ' = ' in condition:
                left_part, right_part = condition.split(' = ')
                left_part = left_part.strip()
                right_part = right_part.strip()
                
                # 移除空格
                left_part = left_part.replace(' ', '')
                right_part = right_part.replace(' ', '')
                
                # 解析左边 - 支持 "table.column" 和 "alias.column" 格式
                left_value = None
                if '.' in left_part:
                    left_alias, left_col = left_part.split('.', 1)
                    # 根据别名查找对应表的值
                    if left_alias == table1 or (hasattr(self, '_get_table_alias') and self._get_table_alias(table1) == left_alias):
                        left_value = record1.get(left_col)
                    elif left_alias == table2 or (hasattr(self, '_get_table_alias') and self._get_table_alias(table2) == left_alias):
                        left_value = record2.get(left_col)
                    else:
                        # 尝试直接查找
                        left_value = record1.get(left_col, record2.get(left_col))
                else:
                    # 直接列名
                    left_value = record1.get(left_part, record2.get(left_part))
                
                # 解析右边 - 支持 "table.column" 和 "alias.column" 格式
                right_value = None
                if '.' in right_part:
                    right_alias, right_col = right_part.split('.', 1)
                    # 根据别名查找对应表的值
                    if right_alias == table1 or (hasattr(self, '_get_table_alias') and self._get_table_alias(table1) == right_alias):
                        right_value = record1.get(right_col)
                    elif right_alias == table2 or (hasattr(self, '_get_table_alias') and self._get_table_alias(table2) == right_alias):
                        right_value = record2.get(right_col)
                    else:
                        # 尝试直接查找
                        right_value = record1.get(right_col, record2.get(right_col))
                else:
                    # 直接列名
                    right_value = record1.get(right_part, record2.get(right_part))
                
                return left_value == right_value
            elif ' = ' in condition.replace(' ', ''):  # 处理带空格的条件
                # 移除所有空格后处理
                clean_condition = condition.replace(' ', '')
                left_part, right_part = clean_condition.split('=')
                
                # 解析左边
                left_value = None
                if '.' in left_part:
                    left_alias, left_col = left_part.split('.', 1)
                    if left_alias == table1:
                        left_value = record1.get(left_col)
                    elif left_alias == table2:
                        left_value = record2.get(left_col)
                    else:
                        left_value = record1.get(left_col, record2.get(left_col))
                else:
                    left_value = record1.get(left_part, record2.get(left_part))
                
                # 解析右边
                right_value = None
                if '.' in right_part:
                    right_alias, right_col = right_part.split('.', 1)
                    if right_alias == table1:
                        right_value = record1.get(right_col)
                    elif right_alias == table2:
                        right_value = record2.get(right_col)
                    else:
                        right_value = record1.get(right_col, record2.get(right_col))
                else:
                    right_value = record1.get(right_part, record2.get(right_part))
                
                return left_value == right_value
        except Exception as e:
            print(f"    条件评估错误: '{condition}' - {e}")
        
        return False  # 默认不匹配
    
    def _execute_count(self, instruction: TargetInstruction) -> None:
        """执行COUNT聚合指令"""
        # COUNT指令需要至少1个参数（源寄存器）
        if len(instruction.operands) < 1:
            print("COUNT指令参数不足")
            return
        
        # 获取源寄存器和列名
        source_reg = instruction.operands[0] if instruction.operands[0] is not None else "R1"
        column = instruction.operands[1] if len(instruction.operands) > 1 and instruction.operands[1] is not None else "*"
        
        # 获取记录数据 - 确保在扫描表之后执行
        # 首先尝试从原始记录获取数据（PROJECT指令之前的数据）
        records = getattr(self.context, 'original_records', None) or self.context.current_records
        print(f"  → COUNT指令获取记录: {len(records) if records else 0} 条")
        
        # 如果当前记录为空，尝试从寄存器获取数据
        if not records and source_reg in self.context.registers:
            reg_data = self.context.registers[source_reg]
            print(f"  → 从寄存器 {source_reg} 获取数据: {type(reg_data)}")
            if isinstance(reg_data, list):
                records = reg_data
        
        # 计算COUNT值
        if column == '*' or column is None:
            count = len(records) if records else 0
        else:
            count = sum(1 for record in records if column in record and record[column] is not None) if records else 0
        
        # 将结果存储在寄存器中
        result_reg = instruction.result if instruction.result is not None else source_reg
        self.context.registers[result_reg] = count
        
        print(f"  → COUNT({column or '*'}) = {count}")
        
        # 创建聚合结果记录
        self.context.current_records = [{'COUNT': count}]
    
    def _execute_sum(self, instruction: TargetInstruction) -> None:
        """执行SUM聚合指令"""
        # SUM指令需要至少1个参数（源寄存器）和1个参数（列名）
        if len(instruction.operands) < 1:
            print("SUM指令参数不足")
            return
        
        source_reg = instruction.operands[0] if len(instruction.operands) > 0 and instruction.operands[0] is not None else "R1"
        column = instruction.operands[1] if len(instruction.operands) > 1 and instruction.operands[1] is not None else "grade"
        
        # 获取记录数据 - 确保在扫描表之后执行
        # 首先尝试从原始记录获取数据（PROJECT指令之前的数据）
        records = getattr(self.context, 'original_records', None) or self.context.current_records
        print(f"  → SUM指令获取记录: {len(records) if records else 0} 条")
        
        # 如果当前记录为空，尝试从寄存器获取数据
        if not records and source_reg in self.context.registers:
            reg_data = self.context.registers[source_reg]
            print(f"  → 从寄存器 {source_reg} 获取数据: {type(reg_data)}")
            if isinstance(reg_data, list):
                records = reg_data
        
        total = 0
        count = 0
        
        if records:
            for record in records:
                if column in record and record[column] is not None:
                    try:
                        total += float(record[column])
                        count += 1
                    except (ValueError, TypeError):
                        continue
        
        # 将结果存储在寄存器中
        result_reg = instruction.result if instruction.result is not None else source_reg
        self.context.registers[result_reg] = total
        
        print(f"  → SUM({column}) = {total} (基于 {count} 条记录)")
        
        self.context.current_records = [{'SUM': total}]
    
    def _execute_avg(self, instruction: TargetInstruction) -> None:
        """执行AVG聚合指令"""
        # AVG指令需要至少1个参数（源寄存器）和1个参数（列名）
        if len(instruction.operands) < 1:
            print("AVG指令参数不足")
            return
        
        source_reg = instruction.operands[0] if len(instruction.operands) > 0 and instruction.operands[0] is not None else "R1"
        column = instruction.operands[1] if len(instruction.operands) > 1 and instruction.operands[1] is not None else "grade"
        
        # 获取记录数据 - 确保在扫描表之后执行
        # 首先尝试从原始记录获取数据（PROJECT指令之前的数据）
        records = getattr(self.context, 'original_records', None) or self.context.current_records
        print(f"  → AVG指令获取记录: {len(records) if records else 0} 条")
        
        # 如果当前记录为空，尝试从寄存器获取数据
        if not records and source_reg in self.context.registers:
            reg_data = self.context.registers[source_reg]
            print(f"  → 从寄存器 {source_reg} 获取数据: {type(reg_data)}")
            if isinstance(reg_data, list):
                records = reg_data
        
        total = 0
        count = 0
        
        if records:
            for record in records:
                if column in record and record[column] is not None:
                    try:
                        total += float(record[column])
                        count += 1
                    except (ValueError, TypeError):
                        continue
        
        avg = total / count if count > 0 else 0
        
        # 将结果存储在寄存器中
        result_reg = instruction.result if instruction.result is not None else source_reg
        self.context.registers[result_reg] = avg
        
        print(f"  → AVG({column}) = {avg:.2f} (基于 {count} 条记录)")
        
        self.context.current_records = [{'AVG': avg}]
    
    def _execute_max(self, instruction: TargetInstruction) -> None:
        """执行MAX聚合指令"""
        # MAX指令需要至少1个参数（源寄存器）和1个参数（列名）
        if len(instruction.operands) < 1:
            print("MAX指令参数不足")
            return
        
        source_reg = instruction.operands[0] if len(instruction.operands) > 0 and instruction.operands[0] is not None else "R1"
        column = instruction.operands[1] if len(instruction.operands) > 1 and instruction.operands[1] is not None else "grade"
        
        # 获取记录数据 - 确保在扫描表之后执行
        # 首先尝试从原始记录获取数据（PROJECT指令之前的数据）
        records = getattr(self.context, 'original_records', None) or self.context.current_records
        print(f"  → MAX指令获取记录: {len(records) if records else 0} 条")
        
        # 如果当前记录为空，尝试从寄存器获取数据
        if not records and source_reg in self.context.registers:
            reg_data = self.context.registers[source_reg]
            print(f"  → 从寄存器 {source_reg} 获取数据: {type(reg_data)}")
            if isinstance(reg_data, list):
                records = reg_data
        
        max_value = None
        
        if records:
            for record in records:
                if column in record and record[column] is not None:
                    value = record[column]
                    if max_value is None or value > max_value:
                        max_value = value
        
        # 将结果存储在寄存器中
        result_reg = instruction.result if instruction.result is not None else source_reg
        self.context.registers[result_reg] = max_value
        
        print(f"  → MAX({column}) = {max_value}")
        
        self.context.current_records = [{'MAX': max_value}]
    
    def _execute_min(self, instruction: TargetInstruction) -> None:
        """执行MIN聚合指令"""
        # MIN指令需要至少1个参数（源寄存器）和1个参数（列名）
        if len(instruction.operands) < 1:
            print("MIN指令参数不足")
            return
        
        source_reg = instruction.operands[0] if len(instruction.operands) > 0 and instruction.operands[0] is not None else "R1"
        column = instruction.operands[1] if len(instruction.operands) > 1 and instruction.operands[1] is not None else "grade"
        
        # 获取记录数据 - 确保在扫描表之后执行
        # 首先尝试从原始记录获取数据（PROJECT指令之前的数据）
        records = getattr(self.context, 'original_records', None) or self.context.current_records
        print(f"  → MIN指令获取记录: {len(records) if records else 0} 条")
        
        # 如果当前记录为空，尝试从寄存器获取数据
        if not records and source_reg in self.context.registers:
            reg_data = self.context.registers[source_reg]
            print(f"  → 从寄存器 {source_reg} 获取数据: {type(reg_data)}")
            if isinstance(reg_data, list):
                records = reg_data
        
        min_value = None
        
        if records:
            for record in records:
                if column in record and record[column] is not None:
                    value = record[column]
                    if min_value is None or value < min_value:
                        min_value = value
        
        # 将结果存储在寄存器中
        result_reg = instruction.result if instruction.result is not None else source_reg
        self.context.registers[result_reg] = min_value
        
        print(f"  → MIN({column}) = {min_value}")
        
        self.context.current_records = [{'MIN': min_value}]
    
    def _execute_group_by(self, instruction: TargetInstruction) -> None:
        """执行GROUP BY指令"""
        if len(instruction.operands) < 2:
            print("GROUP BY指令参数不足")
            return
        
        source_reg = instruction.operands[0]
        columns = instruction.operands[1]
        
        # 处理None值
        if columns is None:
            print("GROUP BY指令列参数为None")
            return
            
        records = self.context.current_records
        column_list = [col.strip() for col in columns.split(',')]
        
        # 按指定列分组
        groups = {}
        for record in records:
            # 创建分组键
            group_key = tuple(record.get(col, None) for col in column_list)
            
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(record)
        
        print(f"  → GROUP BY {columns}: 分成 {len(groups)} 个组")
        
        # 保存分组信息到上下文
        self.context.groups = groups
        self.context.group_columns = column_list
    
    def _execute_order_by(self, instruction: TargetInstruction) -> None:
        """执行ORDER BY指令"""
        if len(instruction.operands) < 2:
            print("ORDER BY指令参数不足")
            return
        
        source_reg = instruction.operands[0]
        order_spec = instruction.operands[1]
        
        # 检查order_spec是否为None或空字符串
        if order_spec is None or order_spec == "":
            print("ORDER BY指令排序规范为空")
            return
            
        records = self.context.current_records
        
        # 解析排序规范（如 "column ASC" 或 "column DESC"）
        parts = order_spec.strip().split()
        if not parts:
            print("ORDER BY指令排序规范格式错误")
            return
            
        column = parts[0]
        direction = parts[1].upper() if len(parts) > 1 else 'ASC'
        
        # 执行排序
        reverse = (direction == 'DESC')
        
        try:
            sorted_records = sorted(records, 
                                  key=lambda x: x.get(column, 0) if x.get(column) is not None else 0,
                                  reverse=reverse)
            self.context.current_records = sorted_records
            print(f"  → ORDER BY {column} {direction}: 排序 {len(sorted_records)} 条记录")
        except Exception as e:
            print(f"排序失败: {e}")
    
    def _execute_having(self, instruction: TargetInstruction) -> None:
        """执行HAVING指令"""
        if len(instruction.operands) < 1:
            print("HAVING指令参数不足")
            return
        
        condition = instruction.operands[0]
        print(f"  → HAVING {condition}: 对分组结果进行过滤")
        
        # HAVING用于对分组后的结果进行过滤
        # 这里实现一个简化的HAVING条件处理逻辑
        filtered_groups = {}
        
        # 简化的条件解析，支持 COUNT(*) > 1 这样的条件
        if '>' in condition:
            parts = condition.split('>')
            left_part = parts[0].strip()
            right_part = parts[1].strip()
            
            # 尝试解析右值为数字
            try:
                threshold = float(right_part)
            except ValueError:
                print(f"    无法解析阈值: {right_part}")
                return
            
            # 处理聚合函数条件
            if 'COUNT' in left_part:
                # 对每个分组检查COUNT是否满足条件
                for group_key, records in self.context.groups.items():
                    count = len(records)
                    if count > threshold:
                        filtered_groups[group_key] = records
            elif 'SUM' in left_part:
                # 处理SUM条件
                column = left_part.replace('SUM(', '').replace(')', '').strip()
                for group_key, records in self.context.groups.items():
                    total = sum(record.get(column, 0) for record in records if record.get(column) is not None)
                    if total > threshold:
                        filtered_groups[group_key] = records
            elif 'AVG' in left_part:
                # 处理AVG条件
                column = left_part.replace('AVG(', '').replace(')', '').replace('(', '').strip()
                for group_key, records in self.context.groups.items():
                    total = sum(record.get(column, 0) for record in records if record.get(column) is not None)
                    count = sum(1 for record in records if record.get(column) is not None)
                    avg = total / count if count > 0 else 0
                    if avg > threshold:
                        filtered_groups[group_key] = records
        else:
            # 默认保持所有分组
            filtered_groups = self.context.groups
        
        # 更新分组信息
        self.context.groups = filtered_groups
        
        # 将分组结果转换为记录列表
        filtered_records = []
        for group_key, records in filtered_groups.items():
            # 取第一个记录作为代表
            if records:
                filtered_records.extend(records)
        
        self.context.current_records = filtered_records
        print(f"    过滤后保留 {len(filtered_groups)} 个分组，共 {len(filtered_records)} 条记录")
    
    def _execute_begin(self, instruction: TargetInstruction) -> None:
        """执行BEGIN指令 - 开始执行"""
        print(f"  → 开始执行")
    
    def _execute_end(self, instruction: TargetInstruction) -> None:
        """执行END指令 - 结束执行"""
        print(f"  → 结束执行")
    
    def _filter_records_directly(self, column: str, operator: str, value: str):
        """直接按条件过滤记录"""
        # 尝试转换值的类型
        try:
            if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
                value = int(value)
            elif '.' in value:
                value = float(value)
            elif value.lower() in ['true', 'false']:
                value = value.lower() == 'true'
            elif value.startswith("'") and value.endswith("'"):
                value = value[1:-1]  # 去除引号
        except:
            pass  # 保持原始字符串
        
        filtered = []
        for record in self.context.current_records:
            if column in record:
                record_value = record[column]
                
                # 执行比较操作
                if operator == '>':
                    if record_value > value:
                        filtered.append(record)
                elif operator == '>=':
                    if record_value >= value:
                        filtered.append(record)
                elif operator == '<':
                    if record_value < value:
                        filtered.append(record)
                elif operator == '<=':
                    if record_value <= value:
                        filtered.append(record)
                elif operator == '=':
                    if record_value == value:
                        filtered.append(record)
                elif operator == '!=':
                    if record_value != value:
                        filtered.append(record)
        
        self.context.filtered_records = filtered
        self.context.current_records = filtered  # 更新当前记录
        self.stats['records_filtered'] += len(filtered)
        
        print(f"  → 过滤条件: {column} {operator} {value}, 结果: {len(filtered)} 条记录")
    
    def _get_value(self, operand: str) -> Any:
        """获取操作数的值"""
        # 处理None值
        if operand is None:
            return None
            
        # 检查是否是寄存器
        if operand in self.context.registers:
            return self.context.registers[operand]
        
        # 尝试转换为数值
        try:
            if operand.isdigit() or (operand.startswith('-') and operand[1:].isdigit()):
                return int(operand)
            elif '.' in operand:
                return float(operand)
        except:
            pass
        
        # 处理字符串字面量
        if isinstance(operand, str) and operand.startswith("'") and operand.endswith("'"):
            return operand[1:-1]
        
        # 处理布尔值
        if isinstance(operand, str) and operand.lower() in ['true', 'false']:
            return operand.lower() == 'true'
        
        # 默认返回字符串
        return operand
    
    def _execute_limit(self, instruction: TargetInstruction) -> None:
        """执行LIMIT指令"""
        if len(instruction.operands) < 1:
            print("LIMIT指令参数不足")
            return
        
        limit_value = instruction.operands[0]
        try:
            self.limit_count = int(limit_value)
            print(f"  → LIMIT {self.limit_count}")
        except ValueError:
            print(f"  → LIMIT 值无效: {limit_value}")
    
    def _execute_offset(self, instruction: TargetInstruction) -> None:
        """执行OFFSET指令"""
        if len(instruction.operands) < 1:
            print("OFFSET指令参数不足")
            return
        
        offset_value = instruction.operands[0]
        try:
            self.offset_count = int(offset_value)
            print(f"  → OFFSET {self.offset_count}")
        except ValueError:
            print(f"  → OFFSET 值无效: {offset_value}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取执行统计信息"""
        return self.stats.copy()
    
    def print_stats(self):
        """打印执行统计信息"""
        stats = self.get_stats()
        
        print("\\n" + "=" * 50)
        print("           执行引擎统计")
        print("=" * 50)
        
        print(f"执行指令数: {stats['instructions_executed']}")
        print(f"执行时间: {stats['execution_time']:.4f} 秒")
        print(f"打开表数: {stats['tables_opened']}")
        print(f"扫描记录数: {stats['records_scanned']}")
        print(f"过滤记录数: {stats['records_filtered']}")
        print(f"输出记录数: {stats['records_output']}")

def test_execution_engine():
    """测试执行引擎"""
    print("=" * 60)
    print("              执行引擎测试")
    print("=" * 60)
    
    # 创建存储引擎和测试数据
    storage = StorageEngine("test_execution")
    
    # 创建测试表
    print("\\n1. 创建测试表和数据...")
    
    students_columns = [
        {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
        {'name': 'name', 'type': 'STRING', 'max_length': 50},
        {'name': 'age', 'type': 'INTEGER'},
        {'name': 'grade', 'type': 'FLOAT'}
    ]
    
    storage.create_table("students", students_columns)
    
    # 插入测试数据
    test_students = [
        {'id': 1, 'name': 'Alice', 'age': 20, 'grade': 85.5},
        {'id': 2, 'name': 'Bob', 'age': 22, 'grade': 92.0},
        {'id': 3, 'name': 'Charlie', 'age': 19, 'grade': 78.5},
        {'id': 4, 'name': 'Diana', 'age': 21, 'grade': 96.0}
    ]
    
    for student in test_students:
        storage.insert("students", student)
    
    print(f"插入了 {len(test_students)} 条学生记录")
    
    # 创建执行引擎
    engine = ExecutionEngine(storage)
    
    print("\\n2. 测试简单查询: SELECT * FROM students")
    
    # 构造目标指令：SELECT * FROM students
    instructions1 = [
        TargetInstruction(TargetInstructionType.OPEN, ["students"]),
        TargetInstruction(TargetInstructionType.SCAN, []),
        TargetInstruction(TargetInstructionType.PROJECT, ["*"]),
        TargetInstruction(TargetInstructionType.OUTPUT, []),
        TargetInstruction(TargetInstructionType.CLOSE, [])
    ]
    
    results1 = engine.execute(instructions1)
    print("\\n查询结果:")
    for i, record in enumerate(results1, 1):
        print(f"  {i}. {record}")
    
    print("\\n3. 测试条件查询: SELECT name, grade FROM students WHERE age > 20")
    
    # 构造目标指令：SELECT name, grade FROM students WHERE age > 20
    instructions2 = [
        TargetInstruction(TargetInstructionType.OPEN, ["students"]),
        TargetInstruction(TargetInstructionType.SCAN, []),
        TargetInstruction(TargetInstructionType.FILTER, ["age", ">", "20"]),
        TargetInstruction(TargetInstructionType.PROJECT, ["name", "grade"]),
        TargetInstruction(TargetInstructionType.OUTPUT, []),
        TargetInstruction(TargetInstructionType.CLOSE, [])
    ]
    
    results2 = engine.execute(instructions2)
    print("\\n查询结果:")
    for i, record in enumerate(results2, 1):
        print(f"  {i}. {record}")
    
    print("\\n4. 测试多条件查询: SELECT * FROM students WHERE grade >= 90")
    
    instructions3 = [
        TargetInstruction(TargetInstructionType.OPEN, ["students"]),
        TargetInstruction(TargetInstructionType.SCAN, []),
        TargetInstruction(TargetInstructionType.FILTER, ["grade", ">=", "90"]),
        TargetInstruction(TargetInstructionType.PROJECT, ["*"]),
        TargetInstruction(TargetInstructionType.OUTPUT, []),
        TargetInstruction(TargetInstructionType.CLOSE, [])
    ]
    
    results3 = engine.execute(instructions3)
    print("\\n查询结果:")
    for i, record in enumerate(results3, 1):
        print(f"  {i}. {record}")
    
    # 打印统计信息
    engine.print_stats()
    
    # 关闭存储引擎
    storage.shutdown()
    
    print("\\n执行引擎测试完成!")

if __name__ == "__main__":
    test_execution_engine()
