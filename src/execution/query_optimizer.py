"""
查询优化器
实现各种查询优化策略以提高SQL查询性能
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import time
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass
from src.compiler.codegen.target_instructions import TargetInstruction, TargetInstructionType
from src.storage.storage_engine import StorageEngine

@dataclass
class OptimizationStats:
    """优化统计信息"""
    original_instructions: int = 0
    optimized_instructions: int = 0
    optimization_time: float = 0.0
    optimizations_applied: List[str] = None
    estimated_cost_reduction: float = 0.0
    
    def __post_init__(self):
        if self.optimizations_applied is None:
            self.optimizations_applied = []

@dataclass
class TableStats:
    """表统计信息"""
    table_name: str
    record_count: int = 0
    has_index: bool = False
    indexed_columns: List[str] = None
    selectivity: Dict[str, float] = None  # 列的选择性
    
    def __post_init__(self):
        if self.indexed_columns is None:
            self.indexed_columns = []
        if self.selectivity is None:
            self.selectivity = {}

class QueryOptimizer:
    """查询优化器"""
    
    def __init__(self, storage_engine: Optional[StorageEngine] = None):
        self.storage_engine = storage_engine
        self.table_stats: Dict[str, TableStats] = {}
        self.optimization_enabled = True
        
        # 优化策略开关
        self.enable_predicate_pushdown = True
        self.enable_projection_pushdown = True
        self.enable_index_optimization = True
        self.enable_join_optimization = True
        self.enable_constant_folding = True
        
    def optimize(self, instructions: List[TargetInstruction]) -> Tuple[List[TargetInstruction], OptimizationStats]:
        """
        优化指令序列
        
        Args:
            instructions: 原始指令序列
            
        Returns:
            (优化后的指令序列, 优化统计信息)
        """
        if not self.optimization_enabled:
            return instructions, OptimizationStats(
                original_instructions=len(instructions),
                optimized_instructions=len(instructions)
            )
        
        start_time = time.time()
        stats = OptimizationStats(original_instructions=len(instructions))
        
        # 收集表统计信息
        self._collect_table_stats(instructions)
        
        # 应用各种优化策略
        optimized = instructions.copy()
        
        # 1. 谓词下推优化
        if self.enable_predicate_pushdown:
            optimized, applied = self._apply_predicate_pushdown(optimized)
            if applied:
                stats.optimizations_applied.append("谓词下推")
        
        # 2. 投影下推优化  
        if self.enable_projection_pushdown:
            optimized, applied = self._apply_projection_pushdown(optimized)
            if applied:
                stats.optimizations_applied.append("投影下推")
        
        # 3. 索引优化
        if self.enable_index_optimization:
            optimized, applied = self._apply_index_optimization(optimized)
            if applied:
                stats.optimizations_applied.append("索引优化")
        
        # 4. JOIN优化
        if self.enable_join_optimization:
            optimized, applied = self._apply_join_optimization(optimized)
            if applied:
                stats.optimizations_applied.append("JOIN优化")
        
        # 5. 常量折叠
        if self.enable_constant_folding:
            optimized, applied = self._apply_constant_folding(optimized)
            if applied:
                stats.optimizations_applied.append("常量折叠")
        
        # 6. 死代码消除
        optimized, applied = self._remove_dead_code(optimized)
        if applied:
            stats.optimizations_applied.append("死代码消除")
        
        # 完成优化统计
        stats.optimized_instructions = len(optimized)
        stats.optimization_time = time.time() - start_time
        stats.estimated_cost_reduction = self._estimate_cost_reduction(instructions, optimized)
        
        return optimized, stats
    
    def _collect_table_stats(self, instructions: List[TargetInstruction]) -> None:
        """收集表统计信息"""
        if not self.storage_engine:
            return
            
        # 找出查询中涉及的所有表
        tables = set()
        for instr in instructions:
            if instr.op == TargetInstructionType.OPEN:
                tables.add(instr.operands[0])
        
        # 收集每个表的统计信息
        for table_name in tables:
            if table_name not in self.table_stats:
                try:
                    # 获取表记录数
                    records = self.storage_engine.select(table_name)
                    record_count = len(records) if records else 0
                    
                    # 检查索引信息
                    has_index = self.storage_engine.has_index(table_name) if hasattr(self.storage_engine, 'has_index') else False
                    indexed_columns = []
                    if has_index:
                        # 获取索引列信息
                        indexed_columns = self.storage_engine.get_indexed_columns(table_name) if hasattr(self.storage_engine, 'get_indexed_columns') else []
                    
                    self.table_stats[table_name] = TableStats(
                        table_name=table_name,
                        record_count=record_count,
                        has_index=has_index,
                        indexed_columns=indexed_columns
                    )
                except Exception as e:
                    print(f"收集表 {table_name} 统计信息失败: {e}")
    
    def _apply_predicate_pushdown(self, instructions: List[TargetInstruction]) -> Tuple[List[TargetInstruction], bool]:
        """
        谓词下推优化
        将过滤条件尽可能早地应用，减少后续操作的数据量
        """
        optimized = []
        applied = False
        
        # 查找过滤条件和扫描操作
        filter_conditions = []
        scan_index = -1
        
        for i, instr in enumerate(instructions):
            if instr.op == TargetInstructionType.SCAN:
                scan_index = i
            elif instr.op in [TargetInstructionType.EQ, TargetInstructionType.GT, 
                               TargetInstructionType.LT, TargetInstructionType.GE, 
                               TargetInstructionType.LE, TargetInstructionType.NE]:
                filter_conditions.append((i, instr))
        
        # 如果找到了扫描和过滤条件，尝试下推
        if scan_index >= 0 and filter_conditions:
            # 将可以下推的过滤条件移动到扫描之后
            for i, instr in enumerate(instructions):
                optimized.append(instr)
                
                # 在SCAN指令后立即添加过滤条件
                if i == scan_index:
                    for _, filter_instr in filter_conditions:
                        if self._can_pushdown_predicate(filter_instr):
                            optimized.append(filter_instr)
                            applied = True
            
            # 移除原来位置的过滤条件
            if applied:
                final_optimized = []
                filter_indices = {idx for idx, _ in filter_conditions}
                for i, instr in enumerate(optimized):
                    if i not in filter_indices or i == scan_index:
                        final_optimized.append(instr)
                optimized = final_optimized
        else:
            optimized = instructions.copy()
        
        return optimized, applied
    
    def _can_pushdown_predicate(self, instruction: TargetInstruction) -> bool:
        """检查谓词是否可以下推"""
        # 简单的启发式：单表条件可以下推
        # 更复杂的逻辑可以检查是否涉及多表
        return True
    
    def _apply_projection_pushdown(self, instructions: List[TargetInstruction]) -> Tuple[List[TargetInstruction], bool]:
        """
        投影下推优化
        尽早进行列投影，减少数据传输量
        """
        optimized = []
        applied = False
        
        # 查找投影操作
        project_columns = set()
        project_index = -1
        
        for i, instr in enumerate(instructions):
            if instr.op == TargetInstructionType.PROJECT:
                project_index = i
                # 收集需要投影的列
                if len(instr.operands) >= 2:
                    columns = instr.operands[1:]
                    project_columns.update(columns)
        
        # 如果有投影操作，尝试下推
        if project_index >= 0 and project_columns:
            # 在数据加载后尽早应用投影
            for i, instr in enumerate(instructions):
                optimized.append(instr)
                
                # 在SCAN后立即进行投影
                if instr.op == TargetInstructionType.SCAN:
                    # 创建早期投影指令
                    early_project = TargetInstruction(
                        op=TargetInstructionType.PROJECT,
                        operands=["temp_reg"] + list(project_columns)
                    )
                    optimized.append(early_project)
                    applied = True
        else:
            optimized = instructions.copy()
        
        return optimized, applied
    
    def _apply_index_optimization(self, instructions: List[TargetInstruction]) -> Tuple[List[TargetInstruction], bool]:
        """
        索引优化
        根据查询条件选择最优的索引访问路径
        """
        optimized = instructions.copy()
        applied = False
        
        # 分析查询模式，选择是否使用索引
        for i, instr in enumerate(instructions):
            if instr.op == TargetInstructionType.OPEN:
                table_name = instr.operands[0]
                
                # 检查该表是否有索引且适合使用
                if table_name in self.table_stats:
                    stats = self.table_stats[table_name]
                    
                    # 如果表很小，可能全表扫描更快
                    if stats.record_count < 100:
                        # 添加禁用索引的指令
                        disable_index = TargetInstruction(
                            op=TargetInstructionType.LOAD,
                            operands=["use_index", "false"]
                        )
                        optimized.insert(i + 1, disable_index)
                        applied = True
                    elif stats.has_index and self._should_use_index(instructions, table_name):
                        # 确保使用索引
                        enable_index = TargetInstruction(
                            op=TargetInstructionType.LOAD,
                            operands=["use_index", "true"]
                        )
                        optimized.insert(i + 1, enable_index)
                        applied = True
        
        return optimized, applied
    
    def _should_use_index(self, instructions: List[TargetInstruction], table_name: str) -> bool:
        """判断是否应该使用索引"""
        # 查找等值查询条件
        has_equality_condition = False
        for instr in instructions:
            if instr.op == TargetInstructionType.EQ:
                has_equality_condition = True
                break
        
        # 如果有等值条件且表较大，使用索引
        if table_name in self.table_stats:
            stats = self.table_stats[table_name]
            return has_equality_condition and stats.record_count > 100
        
        return has_equality_condition
    
    def _apply_join_optimization(self, instructions: List[TargetInstruction]) -> Tuple[List[TargetInstruction], bool]:
        """
        JOIN优化
        选择最优的JOIN顺序和算法
        """
        optimized = instructions.copy()
        applied = False
        
        # 查找JOIN操作
        join_instructions = []
        for i, instr in enumerate(instructions):
            if instr.op in [TargetInstructionType.JOIN, TargetInstructionType.INNER_JOIN,
                             TargetInstructionType.LEFT_JOIN, TargetInstructionType.RIGHT_JOIN]:
                join_instructions.append((i, instr))
        
        # 如果有多个JOIN，优化JOIN顺序
        if len(join_instructions) > 1:
            # 根据表大小重排JOIN顺序（小表在前）
            reordered = self._reorder_joins(join_instructions)
            
            # 应用重排后的JOIN顺序
            for i, (orig_idx, new_instr) in enumerate(reordered):
                if orig_idx < len(optimized):
                    optimized[orig_idx] = new_instr
                    applied = True
        
        return optimized, applied
    
    def _reorder_joins(self, join_instructions: List[Tuple[int, TargetInstruction]]) -> List[Tuple[int, TargetInstruction]]:
        """重排JOIN顺序，小表在前"""
        def get_table_size(instr: TargetInstruction) -> int:
            # 从JOIN指令中提取表名并获取大小
            if len(instr.operands) >= 2:
                table1, table2 = instr.operands[0], instr.operands[1]
                size1 = self.table_stats.get(table1, TableStats(table1)).record_count
                size2 = self.table_stats.get(table2, TableStats(table2)).record_count
                return min(size1, size2)
            return 0
        
        # 按表大小排序
        sorted_joins = sorted(join_instructions, key=lambda x: get_table_size(x[1]))
        return sorted_joins
    
    def _apply_constant_folding(self, instructions: List[TargetInstruction]) -> Tuple[List[TargetInstruction], bool]:
        """
        常量折叠优化
        在编译时计算常量表达式
        """
        optimized = []
        applied = False
        
        for instr in instructions:
            # 查找可以在编译时计算的表达式
            if instr.op in [TargetInstructionType.EQ, TargetInstructionType.GT, TargetInstructionType.LT]:
                if len(instr.operands) >= 2:
                    left, right = instr.operands[0], instr.operands[1]
                    
                    # 如果两个操作数都是常量，直接计算结果
                    if self._is_constant(left) and self._is_constant(right):
                        result = self._evaluate_constant_expression(instr.op, left, right)
                        
                        # 替换为LOAD指令加载计算结果
                        load_instr = TargetInstruction(
                            op=TargetInstructionType.LOAD,
                            operands=["comparison_result", str(result)]
                        )
                        optimized.append(load_instr)
                        applied = True
                        continue
            
            optimized.append(instr)
        
        return optimized, applied
    
    def _is_constant(self, value: str) -> bool:
        """检查值是否为常量"""
        try:
            # 尝试转换为数字
            float(value)
            return True
        except ValueError:
            # 检查是否为字符串常量
            return value.startswith('"') and value.endswith('"')
    
    def _evaluate_constant_expression(self, op_type: TargetInstructionType, left: str, right: str) -> bool:
        """计算常量表达式"""
        try:
            # 转换操作数
            left_val = float(left) if not left.startswith('"') else left.strip('"')
            right_val = float(right) if not right.startswith('"') else right.strip('"')
            
            # 执行比较
            if op_type == TargetInstructionType.EQ:
                return left_val == right_val
            elif op_type == TargetInstructionType.GT:
                return left_val > right_val
            elif op_type == TargetInstructionType.LT:
                return left_val < right_val
            elif op_type == TargetInstructionType.GE:
                return left_val >= right_val
            elif op_type == TargetInstructionType.LE:
                return left_val <= right_val
            elif op_type == TargetInstructionType.NE:
                return left_val != right_val
            
        except (ValueError, TypeError):
            pass
        
        return False
    
    def _remove_dead_code(self, instructions: List[TargetInstruction]) -> Tuple[List[TargetInstruction], bool]:
        """
        死代码消除
        移除不会被执行的指令
        """
        optimized = []
        applied = False
        
        # 简单的死代码检测：移除重复的LOAD指令
        loaded_registers = set()
        
        for instr in instructions:
            if instr.op == TargetInstructionType.LOAD:
                register = instr.operands[0] if instr.operands else None
                
                # 如果寄存器已经加载了相同的值，跳过
                register_value = f"{register}:{instr.operands[1] if len(instr.operands) > 1 else ''}"
                if register_value in loaded_registers:
                    applied = True
                    continue
                
                loaded_registers.add(register_value)
            
            optimized.append(instr)
        
        return optimized, applied
    
    def _estimate_cost_reduction(self, original: List[TargetInstruction], optimized: List[TargetInstruction]) -> float:
        """估算成本降低百分比"""
        if not original:
            return 0.0
        
        # 简单的启发式成本模型
        original_cost = self._calculate_instruction_cost(original)
        optimized_cost = self._calculate_instruction_cost(optimized)
        
        if original_cost == 0:
            return 0.0
        
        reduction = (original_cost - optimized_cost) / original_cost * 100
        return max(0.0, reduction)
    
    def _calculate_instruction_cost(self, instructions: List[TargetInstruction]) -> float:
        """计算指令序列的估算成本"""
        cost = 0.0
        
        # 不同指令类型的相对成本
        cost_weights = {
            TargetInstructionType.SCAN: 10.0,  # 扫描成本高
            TargetInstructionType.JOIN: 20.0,  # JOIN成本更高
            TargetInstructionType.FILTER: 2.0,
            TargetInstructionType.PROJECT: 1.0,
            TargetInstructionType.LOAD: 0.1,
            TargetInstructionType.EQ: 1.0,
            TargetInstructionType.GT: 1.0,
            TargetInstructionType.LT: 1.0,
        }
        
        for instr in instructions:
            weight = cost_weights.get(instr.op, 1.0)
            cost += weight
        
        return cost
    
    def get_optimization_report(self, stats: OptimizationStats) -> str:
        """生成优化报告"""
        report = "=== 查询优化报告 ===\n"
        report += f"原始指令数: {stats.original_instructions}\n"
        report += f"优化后指令数: {stats.optimized_instructions}\n"
        report += f"指令减少: {stats.original_instructions - stats.optimized_instructions}\n"
        report += f"优化时间: {stats.optimization_time:.4f}秒\n"
        report += f"估算成本降低: {stats.estimated_cost_reduction:.1f}%\n"
        
        if stats.optimizations_applied:
            report += f"应用的优化策略: {', '.join(stats.optimizations_applied)}\n"
        else:
            report += "未应用任何优化策略\n"
        
        return report
