"""
分布式查询处理模块

实现分布式查询计划生成、跨节点数据合并和分布式聚合计算
"""

import threading
import time
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue

class QueryType(Enum):
    """查询类型枚举"""
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    AGGREGATE = "AGGREGATE"

class MergeOperation(Enum):
    """合并操作类型"""
    UNION = "UNION"          # 联合
    INTERSECT = "INTERSECT"  # 交集
    SORT_MERGE = "SORT_MERGE"  # 排序合并
    HASH_JOIN = "HASH_JOIN"    # 哈希连接
    AGGREGATE = "AGGREGATE"    # 聚合

@dataclass
class QueryFragment:
    """查询片段"""
    fragment_id: str
    sql: str
    target_shard: str
    node_id: str
    dependencies: List[str] = None  # 依赖的其他片段
    estimated_cost: float = 0.0
    estimated_rows: int = 0
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []

@dataclass
class QueryResult:
    """查询结果"""
    fragment_id: str
    shard_id: str
    node_id: str
    data: List[Dict[str, Any]]
    columns: List[str]
    execution_time: float
    rows_affected: int = 0
    error: Optional[str] = None
    
    @property
    def success(self) -> bool:
        return self.error is None

@dataclass
class DistributedQueryPlan:
    """分布式查询计划"""
    query_id: str
    original_sql: str
    query_type: QueryType
    fragments: List[QueryFragment]
    merge_operations: List[Tuple[MergeOperation, List[str], str]]  # (操作类型, 输入片段, 输出片段)
    estimated_total_cost: float = 0.0
    created_time: float = None
    
    def __post_init__(self):
        if self.created_time is None:
            self.created_time = time.time()

class DistributedQueryOptimizer:
    """分布式查询优化器"""
    
    def __init__(self, shard_manager):
        self.shard_manager = shard_manager
        self.cost_model = DistributedCostModel()
    
    def optimize_query(self, sql: str, table_name: str) -> DistributedQueryPlan:
        """优化分布式查询"""
        query_id = f"query_{int(time.time() * 1000000)}"
        query_type = self._determine_query_type(sql)
        
        if query_type == QueryType.SELECT:
            return self._optimize_select_query(query_id, sql, table_name)
        elif query_type == QueryType.INSERT:
            return self._optimize_insert_query(query_id, sql, table_name)
        elif query_type == QueryType.UPDATE:
            return self._optimize_update_query(query_id, sql, table_name)
        elif query_type == QueryType.DELETE:
            return self._optimize_delete_query(query_id, sql, table_name)
        else:
            raise ValueError(f"Unsupported query type: {query_type}")
    
    def _determine_query_type(self, sql: str) -> QueryType:
        """确定查询类型"""
        sql_upper = sql.strip().upper()
        if sql_upper.startswith('SELECT'):
            # 检查是否包含聚合函数
            if any(func in sql_upper for func in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(', 'GROUP BY']):
                return QueryType.AGGREGATE
            return QueryType.SELECT
        elif sql_upper.startswith('INSERT'):
            return QueryType.INSERT
        elif sql_upper.startswith('UPDATE'):
            return QueryType.UPDATE
        elif sql_upper.startswith('DELETE'):
            return QueryType.DELETE
        else:
            return QueryType.SELECT
    
    def _optimize_select_query(self, query_id: str, sql: str, table_name: str) -> DistributedQueryPlan:
        """优化SELECT查询"""
        # 解析WHERE条件
        conditions = self._parse_where_conditions(sql)
        
        # 获取需要查询的分片
        target_shards = self.shard_manager.get_shards_for_query(table_name, conditions)
        
        if not target_shards:
            # 非分片表，创建单个片段
            fragment = QueryFragment(
                fragment_id=f"{query_id}_f0",
                sql=sql,
                target_shard="local",
                node_id="local"
            )
            return DistributedQueryPlan(
                query_id=query_id,
                original_sql=sql,
                query_type=QueryType.SELECT,
                fragments=[fragment],
                merge_operations=[]
            )
        
        # 创建查询片段
        fragments = []
        for i, shard_id in enumerate(target_shards):
            shard_info = self.shard_manager.get_shard_info(shard_id)
            fragment = QueryFragment(
                fragment_id=f"{query_id}_f{i}",
                sql=sql,  # 在实际实现中，这里应该修改SQL以适应分片
                target_shard=shard_id,
                node_id=shard_info.node_id if shard_info else "unknown",
                estimated_cost=self.cost_model.estimate_fragment_cost(sql, shard_id),
                estimated_rows=self.cost_model.estimate_result_rows(sql, shard_id)
            )
            fragments.append(fragment)
        
        # 创建合并操作
        merge_operations = []
        if len(fragments) > 1:
            # 需要合并多个分片的结果
            if 'GROUP BY' in sql.upper() or any(func in sql.upper() for func in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']):
                merge_operations.append((MergeOperation.AGGREGATE, [f.fragment_id for f in fragments], f"{query_id}_merge"))
            elif 'ORDER BY' in sql.upper():
                merge_operations.append((MergeOperation.SORT_MERGE, [f.fragment_id for f in fragments], f"{query_id}_merge"))
            else:
                merge_operations.append((MergeOperation.UNION, [f.fragment_id for f in fragments], f"{query_id}_merge"))
        
        return DistributedQueryPlan(
            query_id=query_id,
            original_sql=sql,
            query_type=QueryType.SELECT,
            fragments=fragments,
            merge_operations=merge_operations,
            estimated_total_cost=sum(f.estimated_cost for f in fragments)
        )
    
    def _optimize_insert_query(self, query_id: str, sql: str, table_name: str) -> DistributedQueryPlan:
        """优化INSERT查询"""
        # 解析INSERT语句中的数据
        data = self._parse_insert_data(sql)
        
        if not data:
            raise ValueError("Could not parse INSERT data")
        
        # 为每条记录确定目标分片
        fragments = []
        shard_fragments = {}  # shard_id -> fragment
        
        for i, row_data in enumerate(data):
            try:
                shard_id = self.shard_manager.get_shard_for_insert(table_name, row_data)
                
                if shard_id not in shard_fragments:
                    shard_info = self.shard_manager.get_shard_info(shard_id)
                    fragment = QueryFragment(
                        fragment_id=f"{query_id}_f{len(shard_fragments)}",
                        sql="",  # 将在后面构建
                        target_shard=shard_id,
                        node_id=shard_info.node_id if shard_info else "unknown"
                    )
                    shard_fragments[shard_id] = fragment
                    fragments.append(fragment)
            except ValueError:
                # 非分片表或找不到分片，使用本地
                if "local" not in shard_fragments:
                    fragment = QueryFragment(
                        fragment_id=f"{query_id}_f{len(shard_fragments)}",
                        sql=sql,
                        target_shard="local",
                        node_id="local"
                    )
                    shard_fragments["local"] = fragment
                    fragments.append(fragment)
        
        # 为每个分片构建INSERT SQL
        for fragment in fragments:
            if fragment.target_shard == "local":
                fragment.sql = sql
            else:
                # 构建该分片的INSERT语句
                fragment.sql = self._build_shard_insert_sql(sql, fragment.target_shard, data)
        
        return DistributedQueryPlan(
            query_id=query_id,
            original_sql=sql,
            query_type=QueryType.INSERT,
            fragments=fragments,
            merge_operations=[]  # INSERT通常不需要合并
        )
    
    def _optimize_update_query(self, query_id: str, sql: str, table_name: str) -> DistributedQueryPlan:
        """优化UPDATE查询"""
        conditions = self._parse_where_conditions(sql)
        target_shards = self.shard_manager.get_shards_for_query(table_name, conditions)
        
        if not target_shards:
            fragment = QueryFragment(
                fragment_id=f"{query_id}_f0",
                sql=sql,
                target_shard="local",
                node_id="local"
            )
            return DistributedQueryPlan(
                query_id=query_id,
                original_sql=sql,
                query_type=QueryType.UPDATE,
                fragments=[fragment],
                merge_operations=[]
            )
        
        fragments = []
        for i, shard_id in enumerate(target_shards):
            shard_info = self.shard_manager.get_shard_info(shard_id)
            fragment = QueryFragment(
                fragment_id=f"{query_id}_f{i}",
                sql=sql,
                target_shard=shard_id,
                node_id=shard_info.node_id if shard_info else "unknown"
            )
            fragments.append(fragment)
        
        return DistributedQueryPlan(
            query_id=query_id,
            original_sql=sql,
            query_type=QueryType.UPDATE,
            fragments=fragments,
            merge_operations=[]
        )
    
    def _optimize_delete_query(self, query_id: str, sql: str, table_name: str) -> DistributedQueryPlan:
        """优化DELETE查询"""
        conditions = self._parse_where_conditions(sql)
        target_shards = self.shard_manager.get_shards_for_query(table_name, conditions)
        
        if not target_shards:
            fragment = QueryFragment(
                fragment_id=f"{query_id}_f0",
                sql=sql,
                target_shard="local",
                node_id="local"
            )
            return DistributedQueryPlan(
                query_id=query_id,
                original_sql=sql,
                query_type=QueryType.DELETE,
                fragments=[fragment],
                merge_operations=[]
            )
        
        fragments = []
        for i, shard_id in enumerate(target_shards):
            shard_info = self.shard_manager.get_shard_info(shard_id)
            fragment = QueryFragment(
                fragment_id=f"{query_id}_f{i}",
                sql=sql,
                target_shard=shard_id,
                node_id=shard_info.node_id if shard_info else "unknown"
            )
            fragments.append(fragment)
        
        return DistributedQueryPlan(
            query_id=query_id,
            original_sql=sql,
            query_type=QueryType.DELETE,
            fragments=fragments,
            merge_operations=[]
        )
    
    def _parse_where_conditions(self, sql: str) -> Dict[str, Any]:
        """解析WHERE条件（简化实现）"""
        # 这里是一个简化的实现，实际应该使用SQL解析器
        conditions = {}
        sql_upper = sql.upper()
        
        if 'WHERE' in sql_upper:
            where_part = sql[sql_upper.find('WHERE') + 5:].strip()
            # 简单的等值条件解析
            if '=' in where_part:
                parts = where_part.split('=')
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip().strip("'\"")
                    try:
                        value = int(value)
                    except ValueError:
                        try:
                            value = float(value)
                        except ValueError:
                            pass
                    conditions[key] = value
        
        return conditions
    
    def _parse_insert_data(self, sql: str) -> List[Dict[str, Any]]:
        """解析INSERT语句中的数据（简化实现）"""
        # 这是一个非常简化的实现
        # 实际应该使用完整的SQL解析器
        data = []
        
        try:
            sql_upper = sql.upper()
            if 'VALUES' in sql_upper:
                values_part = sql[sql_upper.find('VALUES') + 6:].strip()
                # 简单解析单行数据
                if values_part.startswith('(') and values_part.endswith(')'):
                    values_str = values_part[1:-1]
                    values = [v.strip().strip("'\"") for v in values_str.split(',')]
                    
                    # 假设有一个简单的表结构
                    columns = ['id', 'name', 'value']  # 这应该从表schema获取
                    if len(values) == len(columns):
                        row_data = {}
                        for i, col in enumerate(columns):
                            try:
                                row_data[col] = int(values[i])
                            except ValueError:
                                try:
                                    row_data[col] = float(values[i])
                                except ValueError:
                                    row_data[col] = values[i]
                        data.append(row_data)
        except Exception:
            pass
        
        return data
    
    def _build_shard_insert_sql(self, original_sql: str, shard_id: str, data: List[Dict[str, Any]]) -> str:
        """为特定分片构建INSERT SQL"""
        # 这里应该根据分片ID和数据构建适合该分片的INSERT语句
        # 简化实现，直接返回原SQL
        return original_sql

class DistributedCostModel:
    """分布式查询成本模型"""
    
    def __init__(self):
        self.base_cost = 1.0
        self.network_cost_factor = 0.1
        self.cpu_cost_factor = 0.05
        self.io_cost_factor = 0.2
    
    def estimate_fragment_cost(self, sql: str, shard_id: str) -> float:
        """估算查询片段的执行成本"""
        cost = self.base_cost
        
        # 基于SQL复杂度的成本估算
        sql_upper = sql.upper()
        
        if 'JOIN' in sql_upper:
            cost += 5.0
        if 'GROUP BY' in sql_upper:
            cost += 3.0
        if 'ORDER BY' in sql_upper:
            cost += 2.0
        if 'DISTINCT' in sql_upper:
            cost += 1.5
        
        # 网络成本（如果是远程分片）
        if shard_id != "local":
            cost += self.network_cost_factor * cost
        
        return cost
    
    def estimate_result_rows(self, sql: str, shard_id: str) -> int:
        """估算查询结果行数"""
        # 简化的行数估算
        base_rows = 1000
        
        sql_upper = sql.upper()
        if 'WHERE' in sql_upper:
            base_rows = int(base_rows * 0.1)  # WHERE条件减少结果
        if 'GROUP BY' in sql_upper:
            base_rows = int(base_rows * 0.05)  # GROUP BY进一步减少
        
        return max(1, base_rows)
    
    def estimate_merge_cost(self, operation: MergeOperation, input_fragments: List[str]) -> float:
        """估算合并操作成本"""
        base_cost = len(input_fragments) * 0.5
        
        if operation == MergeOperation.SORT_MERGE:
            return base_cost * 2.0
        elif operation == MergeOperation.HASH_JOIN:
            return base_cost * 1.5
        elif operation == MergeOperation.AGGREGATE:
            return base_cost * 1.2
        else:
            return base_cost

class DistributedQueryExecutor:
    """分布式查询执行器"""
    
    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.active_queries: Dict[str, DistributedQueryPlan] = {}
        self.lock = threading.RLock()
    
    def execute_query(self, plan: DistributedQueryPlan, 
                     fragment_executor_func) -> List[QueryResult]:
        """执行分布式查询计划"""
        with self.lock:
            self.active_queries[plan.query_id] = plan
        
        try:
            # 执行查询片段
            fragment_results = self._execute_fragments(plan, fragment_executor_func)
            
            # 执行合并操作
            if plan.merge_operations:
                merged_results = self._execute_merge_operations(
                    plan, fragment_results, fragment_executor_func
                )
                return merged_results
            else:
                return fragment_results
        
        finally:
            with self.lock:
                if plan.query_id in self.active_queries:
                    del self.active_queries[plan.query_id]
    
    def _execute_fragments(self, plan: DistributedQueryPlan, 
                          fragment_executor_func) -> Dict[str, QueryResult]:
        """执行查询片段"""
        future_to_fragment = {}
        
        for fragment in plan.fragments:
            future = self.executor.submit(
                self._execute_single_fragment, 
                fragment, 
                fragment_executor_func
            )
            future_to_fragment[future] = fragment
        
        results = {}
        for future in as_completed(future_to_fragment):
            fragment = future_to_fragment[future]
            try:
                result = future.result()
                results[fragment.fragment_id] = result
            except Exception as e:
                error_result = QueryResult(
                    fragment_id=fragment.fragment_id,
                    shard_id=fragment.target_shard,
                    node_id=fragment.node_id,
                    data=[],
                    columns=[],
                    execution_time=0.0,
                    error=str(e)
                )
                results[fragment.fragment_id] = error_result
        
        return results
    
    def _execute_single_fragment(self, fragment: QueryFragment, 
                                fragment_executor_func) -> QueryResult:
        """执行单个查询片段"""
        start_time = time.time()
        
        try:
            # 调用外部提供的片段执行函数
            data, columns, rows_affected = fragment_executor_func(
                fragment.sql, 
                fragment.target_shard, 
                fragment.node_id
            )
            
            execution_time = time.time() - start_time
            
            return QueryResult(
                fragment_id=fragment.fragment_id,
                shard_id=fragment.target_shard,
                node_id=fragment.node_id,
                data=data,
                columns=columns,
                execution_time=execution_time,
                rows_affected=rows_affected
            )
        
        except Exception as e:
            execution_time = time.time() - start_time
            return QueryResult(
                fragment_id=fragment.fragment_id,
                shard_id=fragment.target_shard,
                node_id=fragment.node_id,
                data=[],
                columns=[],
                execution_time=execution_time,
                error=str(e)
            )
    
    def _execute_merge_operations(self, plan: DistributedQueryPlan, 
                                 fragment_results: Dict[str, QueryResult],
                                 fragment_executor_func) -> List[QueryResult]:
        """执行合并操作"""
        current_results = fragment_results.copy()
        
        for operation, input_fragments, output_id in plan.merge_operations:
            input_results = [current_results[fid] for fid in input_fragments 
                           if fid in current_results and current_results[fid].success]
            
            if not input_results:
                continue
            
            merged_result = self._merge_results(operation, input_results, output_id)
            current_results[output_id] = merged_result
        
        # 返回最终结果
        final_results = []
        for operation, input_fragments, output_id in plan.merge_operations:
            if output_id in current_results:
                final_results.append(current_results[output_id])
        
        return final_results if final_results else list(current_results.values())
    
    def _merge_results(self, operation: MergeOperation, 
                      results: List[QueryResult], 
                      output_id: str) -> QueryResult:
        """合并查询结果"""
        start_time = time.time()
        
        try:
            if operation == MergeOperation.UNION:
                return self._union_merge(results, output_id)
            elif operation == MergeOperation.SORT_MERGE:
                return self._sort_merge(results, output_id)
            elif operation == MergeOperation.AGGREGATE:
                return self._aggregate_merge(results, output_id)
            else:
                # 默认使用UNION合并
                return self._union_merge(results, output_id)
        
        except Exception as e:
            execution_time = time.time() - start_time
            return QueryResult(
                fragment_id=output_id,
                shard_id="merged",
                node_id="local",
                data=[],
                columns=[],
                execution_time=execution_time,
                error=str(e)
            )
    
    def _union_merge(self, results: List[QueryResult], output_id: str) -> QueryResult:
        """联合合并"""
        if not results:
            return QueryResult(
                fragment_id=output_id,
                shard_id="merged",
                node_id="local",
                data=[],
                columns=[],
                execution_time=0.0
            )
        
        merged_data = []
        columns = results[0].columns
        total_rows = 0
        
        for result in results:
            merged_data.extend(result.data)
            total_rows += result.rows_affected
        
        execution_time = sum(r.execution_time for r in results)
        
        return QueryResult(
            fragment_id=output_id,
            shard_id="merged",
            node_id="local",
            data=merged_data,
            columns=columns,
            execution_time=execution_time,
            rows_affected=total_rows
        )
    
    def _sort_merge(self, results: List[QueryResult], output_id: str) -> QueryResult:
        """排序合并"""
        union_result = self._union_merge(results, output_id)
        
        # 简化的排序实现，假设按第一列排序
        if union_result.data and union_result.columns:
            first_col = union_result.columns[0]
            try:
                union_result.data.sort(key=lambda x: x.get(first_col, 0))
            except (TypeError, KeyError):
                pass  # 排序失败，保持原顺序
        
        return union_result
    
    def _aggregate_merge(self, results: List[QueryResult], output_id: str) -> QueryResult:
        """聚合合并"""
        if not results:
            return QueryResult(
                fragment_id=output_id,
                shard_id="merged",
                node_id="local",
                data=[],
                columns=[],
                execution_time=0.0
            )
        
        # 简化的聚合实现
        # 实际实现应该根据具体的聚合函数进行处理
        columns = results[0].columns
        aggregated_data = []
        
        if columns and results[0].data:
            # 假设是简单的COUNT聚合
            total_count = sum(len(result.data) for result in results)
            aggregated_data = [{"count": total_count}]
            columns = ["count"]
        
        execution_time = sum(r.execution_time for r in results)
        
        return QueryResult(
            fragment_id=output_id,
            shard_id="merged",
            node_id="local",
            data=aggregated_data,
            columns=columns,
            execution_time=execution_time,
            rows_affected=len(aggregated_data)
        )
    
    def get_active_queries(self) -> List[str]:
        """获取活跃查询列表"""
        with self.lock:
            return list(self.active_queries.keys())
    
    def cancel_query(self, query_id: str) -> bool:
        """取消查询"""
        with self.lock:
            if query_id in self.active_queries:
                # 实际实现中应该取消相关的Future任务
                del self.active_queries[query_id]
                return True
            return False
    
    def shutdown(self):
        """关闭执行器"""
        self.executor.shutdown(wait=True)

class DistributedQueryProcessor:
    """分布式查询处理器主类"""
    
    def __init__(self, shard_manager, max_workers: int = 10):
        self.shard_manager = shard_manager
        self.optimizer = DistributedQueryOptimizer(shard_manager)
        self.executor = DistributedQueryExecutor(max_workers)
        self.query_cache: Dict[str, DistributedQueryPlan] = {}
        self.cache_lock = threading.RLock()
    
    def process_query(self, sql: str, table_name: str, 
                     fragment_executor_func) -> List[QueryResult]:
        """处理分布式查询"""
        # 查询计划缓存
        cache_key = f"{table_name}:{hash(sql)}"
        
        with self.cache_lock:
            if cache_key in self.query_cache:
                plan = self.query_cache[cache_key]
            else:
                plan = self.optimizer.optimize_query(sql, table_name)
                self.query_cache[cache_key] = plan
        
        # 执行查询
        return self.executor.execute_query(plan, fragment_executor_func)
    
    def get_query_plan(self, sql: str, table_name: str) -> DistributedQueryPlan:
        """获取查询计划（不执行）"""
        return self.optimizer.optimize_query(sql, table_name)
    
    def clear_cache(self):
        """清空查询计划缓存"""
        with self.cache_lock:
            self.query_cache.clear()
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取查询处理器统计信息"""
        return {
            'cached_plans': len(self.query_cache),
            'active_queries': len(self.executor.get_active_queries()),
            'max_workers': self.executor.max_workers
        }
    
    def shutdown(self):
        """关闭查询处理器"""
        self.executor.shutdown()
