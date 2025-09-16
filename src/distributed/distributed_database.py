"""
分布式数据库主控制器

整合所有分布式功能模块，提供统一的分布式数据库接口
"""

import threading
import time
import uuid
from typing import Dict, List, Any, Optional, Tuple
import logging

from .sharding import ShardManager, ShardingType
from .query_processor import DistributedQueryProcessor
from .replication import ReplicationManager, ConsistencyLevel, ReplicationMode
from .transaction import DistributedTransactionManager, IsolationLevel
from .fault_tolerance import FaultToleranceManager, NodeInfo, NodeStatus
from .coordination import ClusterCoordinator, NodeRole
from .monitoring import DistributedMonitor

class DistributedDatabase:
    """分布式数据库主控制器"""
    
    def __init__(self, node_id: str, initial_cluster_members: List[str] = None):
        self.node_id = node_id
        self.is_distributed_mode = bool(initial_cluster_members)
        
        # 核心组件
        self.shard_manager = ShardManager()
        self.query_processor = DistributedQueryProcessor(self.shard_manager)
        self.replication_manager = ReplicationManager(node_id)
        self.transaction_manager = DistributedTransactionManager(node_id)
        self.fault_tolerance = FaultToleranceManager(node_id)
        self.coordinator = ClusterCoordinator(node_id, initial_cluster_members) if self.is_distributed_mode else None
        self.monitor = DistributedMonitor(node_id)
        
        # 状态管理
        self.running = False
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        
        # 设置组件间的回调
        self._setup_callbacks()
    
    def _setup_callbacks(self):
        """设置组件间的回调关系"""
        if self.coordinator:
            # 集群成员变化回调
            self.coordinator.add_member_change_callback(self._on_member_change)
            self.coordinator.add_leader_change_callback(self._on_leader_change)
            self.coordinator.add_config_change_callback(self._on_config_change)
        
        # 故障检测回调
        self.fault_tolerance.failure_detector.add_failure_callback(self._on_node_failure)
        self.fault_tolerance.failure_detector.add_recovery_callback(self._on_node_recovery)
        
        # 监控告警回调
        self.monitor.add_alert_callback(self._on_alert)
    
    def start(self):
        """启动分布式数据库"""
        with self.lock:
            if self.running:
                return
            
            self.logger.info(f"Starting distributed database node {self.node_id}")
            
            try:
                # 启动核心组件
                if self.coordinator:
                    self.coordinator.start()
                
                self.replication_manager.start()
                self.fault_tolerance.start()
                self.monitor.start()
                
                # 注册自己到故障检测器
                self_node = NodeInfo(
                    node_id=self.node_id,
                    endpoint=f"node_{self.node_id}",
                    status=NodeStatus.HEALTHY
                )
                self.fault_tolerance.register_node(self_node)
                
                self.running = True
                self.logger.info(f"Distributed database node {self.node_id} started successfully")
                
            except Exception as e:
                self.logger.error(f"Failed to start distributed database: {e}")
                self.stop()
                raise
    
    def stop(self):
        """停止分布式数据库"""
        with self.lock:
            if not self.running:
                return
            
            self.logger.info(f"Stopping distributed database node {self.node_id}")
            
            try:
                # 停止所有组件
                self.monitor.stop()
                self.fault_tolerance.stop()
                self.replication_manager.stop()
                self.transaction_manager.shutdown()
                self.query_processor.shutdown()
                
                if self.coordinator:
                    self.coordinator.stop()
                
                self.running = False
                self.logger.info(f"Distributed database node {self.node_id} stopped")
                
            except Exception as e:
                self.logger.error(f"Error during shutdown: {e}")
    
    # 分片管理接口
    def create_sharded_table(self, table_name: str, shard_key: str, 
                           shard_type: ShardingType, shard_count: int,
                           nodes: List[str]) -> bool:
        """创建分片表"""
        try:
            metadata = self.shard_manager.create_sharded_table(
                table_name, shard_key, shard_type, shard_count, nodes
            )
            
            self.logger.info(f"Created sharded table {table_name} with {shard_count} shards")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create sharded table {table_name}: {e}")
            return False
    
    def get_shard_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取表的分片信息"""
        metadata = self.shard_manager.get_table_metadata(table_name)
        if metadata:
            return {
                'table_name': metadata.table_name,
                'shard_key': metadata.shard_key,
                'shard_type': metadata.shard_type.value,
                'total_shards': metadata.total_shards,
                'shards': [
                    {
                        'shard_id': shard.shard_id,
                        'node_id': shard.node_id,
                        'status': shard.status,
                        'key_range': shard.key_range,
                        'hash_range': shard.hash_range
                    }
                    for shard in metadata.shards
                ]
            }
        return None
    
    # 查询处理接口
    def execute_query(self, sql: str, table_name: str = None) -> Tuple[List[Dict[str, Any]], List[str]]:
        """执行分布式查询"""
        query_id = f"query_{uuid.uuid4().hex[:8]}"
        
        # 开始监控
        query_metrics = self.monitor.record_query(query_id, sql)
        
        try:
            # 如果指定了表名，使用分布式查询处理
            if table_name:
                results = self.query_processor.process_query(
                    sql, table_name, self._execute_fragment
                )
                
                if results:
                    # 合并结果
                    all_data = []
                    columns = []
                    
                    for result in results:
                        if result.success:
                            all_data.extend(result.data)
                            if not columns and result.columns:
                                columns = result.columns
                    
                    # 完成监控
                    self.monitor.complete_query(
                        query_id,
                        rows_returned=len(all_data),
                        rows_examined=len(all_data)
                    )
                    
                    return all_data, columns
            
            # 单节点查询
            data, columns = self._execute_local_query(sql)
            
            self.monitor.complete_query(
                query_id,
                rows_returned=len(data),
                rows_examined=len(data)
            )
            
            return data, columns
            
        except Exception as e:
            self.monitor.complete_query(query_id, error_message=str(e))
            raise
    
    def _execute_fragment(self, sql: str, shard_id: str, node_id: str) -> Tuple[List[Dict[str, Any]], List[str], int]:
        """执行查询片段（供分布式查询处理器调用）"""
        # 这里应该根据shard_id和node_id路由到相应的节点执行
        # 简化实现，直接在本地执行
        data, columns = self._execute_local_query(sql)
        return data, columns, len(data)
    
    def _execute_local_query(self, sql: str) -> Tuple[List[Dict[str, Any]], List[str]]:
        """执行本地查询"""
        # 这里应该调用实际的存储引擎
        # 简化实现，返回模拟数据
        return [{'id': 1, 'name': 'test', 'value': 100}], ['id', 'name', 'value']
    
    # 事务管理接口
    def begin_transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED) -> str:
        """开始分布式事务"""
        return self.transaction_manager.begin_transaction(isolation_level)
    
    def commit_transaction(self, transaction_id: str) -> bool:
        """提交事务"""
        return self.transaction_manager.commit_transaction(transaction_id)
    
    def abort_transaction(self, transaction_id: str) -> bool:
        """中止事务"""
        return self.transaction_manager.abort_transaction(transaction_id)
    
    # 复制管理接口
    def create_replication_group(self, group_id: str, 
                                consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL) -> bool:
        """创建复制组"""
        try:
            self.replication_manager.create_replication_group(group_id, consistency_level)
            return True
        except Exception as e:
            self.logger.error(f"Failed to create replication group {group_id}: {e}")
            return False
    
    def join_replication_group(self, group_id: str, role: str = "slave") -> bool:
        """加入复制组"""
        from .replication import ReplicaRole
        replica_role = ReplicaRole.MASTER if role == "master" else ReplicaRole.SLAVE
        
        return self.replication_manager.join_replication_group(
            group_id, replica_role, f"node_{self.node_id}"
        )
    
    def set_replication_mode(self, mode: str):
        """设置复制模式"""
        from .replication import ReplicationMode
        replication_mode = ReplicationMode(mode)
        self.replication_manager.set_replication_mode(replication_mode)
    
    # 集群管理接口
    def join_cluster(self, member_id: str, endpoint: str) -> bool:
        """节点加入集群"""
        if not self.coordinator:
            return False
        
        success = self.coordinator.join_cluster(member_id, endpoint)
        
        if success:
            # 注册到故障检测器
            node_info = NodeInfo(
                node_id=member_id,
                endpoint=endpoint,
                status=NodeStatus.HEALTHY
            )
            self.fault_tolerance.register_node(node_info)
        
        return success
    
    def leave_cluster(self, member_id: str) -> bool:
        """节点离开集群"""
        if not self.coordinator:
            return False
        
        success = self.coordinator.leave_cluster(member_id)
        
        if success:
            self.fault_tolerance.unregister_node(member_id)
        
        return success
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """获取集群状态"""
        if not self.coordinator:
            return {'distributed_mode': False, 'node_id': self.node_id}
        
        cluster_status = self.coordinator.get_cluster_status()
        health_status = self.fault_tolerance.get_cluster_health()
        
        return {
            'distributed_mode': True,
            'cluster': cluster_status,
            'health': health_status
        }
    
    # 配置管理接口
    def set_config(self, key: str, value: Any) -> bool:
        """设置配置"""
        if not self.coordinator:
            return False
        return self.coordinator.set_config(key, value, self.node_id)
    
    def get_config(self, key: str) -> Optional[Any]:
        """获取配置"""
        if not self.coordinator:
            return None
        return self.coordinator.get_config(key)
    
    # 监控接口
    def get_performance_metrics(self) -> Dict[str, Any]:
        """获取性能指标"""
        return self.monitor.get_dashboard_data()
    
    def get_slow_queries(self, limit: int = 50) -> List[Dict[str, Any]]:
        """获取慢查询"""
        return self.monitor.get_slow_queries(limit)
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        return {
            'node_id': self.node_id,
            'running': self.running,
            'distributed_mode': self.is_distributed_mode,
            'components': {
                'shard_manager': self.shard_manager.get_statistics(),
                'replication_manager': self.replication_manager.get_statistics(),
                'transaction_manager': self.transaction_manager.get_statistics(),
                'query_processor': self.query_processor.get_statistics(),
                'fault_tolerance': self.fault_tolerance.get_cluster_health(),
                'monitor': self.monitor.get_dashboard_data()
            }
        }
    
    # 回调处理方法
    def _on_member_change(self, change_type: str, member_id: str):
        """处理集群成员变化"""
        self.logger.info(f"Cluster member {change_type}: {member_id}")
        
        if change_type == "join":
            # 新节点加入，可能需要重新平衡分片
            pass
        elif change_type == "leave":
            # 节点离开，可能需要故障转移
            pass
    
    def _on_leader_change(self, new_leader_id: str):
        """处理领导者变化"""
        self.logger.info(f"New cluster leader: {new_leader_id}")
        
        # 如果自己成为领导者，可能需要承担额外职责
        if new_leader_id == self.node_id:
            self.logger.info("This node is now the cluster leader")
    
    def _on_config_change(self, change_type: str, key: str, value: Any):
        """处理配置变化"""
        self.logger.info(f"Config {change_type}: {key} = {value}")
        
        # 根据配置变化调整系统行为
        if key == "replication_mode":
            self.set_replication_mode(value)
        elif key == "load_balancing_strategy":
            self.fault_tolerance.set_load_balancing_strategy(value)
    
    def _on_node_failure(self, node_id: str, failure_type):
        """处理节点故障"""
        self.logger.warning(f"Node {node_id} failed: {failure_type}")
        
        # 记录故障事件
        self.fault_tolerance.record_failure_event(
            node_id, failure_type, f"Node {node_id} failed with {failure_type}"
        )
        
        # 可能需要触发故障转移
        if self.coordinator:
            self.coordinator.leave_cluster(node_id)
    
    def _on_node_recovery(self, node_id: str):
        """处理节点恢复"""
        self.logger.info(f"Node {node_id} recovered")
        
        # 节点恢复后可能需要重新加入集群
        if self.coordinator:
            self.coordinator.join_cluster(node_id, f"node_{node_id}")
    
    def _on_alert(self, alert):
        """处理监控告警"""
        self.logger.warning(f"Alert: {alert.message} (Level: {alert.level.value})")
        
        # 根据告警级别采取行动
        if alert.level.value == "critical":
            # 关键告警，可能需要立即处理
            pass
    
    # 工具方法
    def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        return {
            'node_id': self.node_id,
            'status': 'healthy' if self.running else 'stopped',
            'timestamp': time.time(),
            'components': {
                'coordinator': self.coordinator.get_cluster_status() if self.coordinator else None,
                'replication': self.replication_manager.get_statistics(),
                'transactions': self.transaction_manager.get_statistics(),
                'fault_tolerance': self.fault_tolerance.get_cluster_health(),
                'monitoring': self.monitor.get_dashboard_data()
            }
        }
    
    def export_configuration(self) -> Dict[str, Any]:
        """导出配置"""
        config = {
            'node_id': self.node_id,
            'distributed_mode': self.is_distributed_mode,
            'shard_metadata': self.shard_manager.export_metadata(),
            'replication_status': self.replication_manager.get_all_groups_status()
        }
        
        if self.coordinator:
            config['cluster_config'] = self.coordinator.get_config_snapshot()
        
        return config
    
    def import_configuration(self, config: Dict[str, Any]) -> bool:
        """导入配置"""
        try:
            if 'shard_metadata' in config:
                self.shard_manager.import_metadata(config['shard_metadata'])
            
            # 其他配置导入...
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to import configuration: {e}")
            return False
