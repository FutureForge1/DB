"""
数据复制与一致性模块

实现主从复制机制、最终一致性模型和读写分离
"""

import threading
import time
import queue
import json
from typing import Dict, List, Any, Optional, Set, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

class ReplicaRole(Enum):
    """副本角色"""
    MASTER = "master"
    SLAVE = "slave"
    CANDIDATE = "candidate"  # 候选主节点

class ConsistencyLevel(Enum):
    """一致性级别"""
    STRONG = "strong"        # 强一致性
    EVENTUAL = "eventual"    # 最终一致性
    WEAK = "weak"           # 弱一致性

class ReplicationMode(Enum):
    """复制模式"""
    SYNC = "synchronous"     # 同步复制
    ASYNC = "asynchronous"   # 异步复制
    SEMI_SYNC = "semi_synchronous"  # 半同步复制

@dataclass
class ReplicationLog:
    """复制日志条目"""
    log_id: str
    sequence_number: int
    timestamp: float
    operation_type: str  # INSERT, UPDATE, DELETE, DDL
    table_name: str
    data: Dict[str, Any]
    sql: Optional[str] = None
    checksum: Optional[str] = None
    
    def __post_init__(self):
        if self.checksum is None:
            self.checksum = self._calculate_checksum()
    
    def _calculate_checksum(self) -> str:
        """计算日志条目的校验和"""
        import hashlib
        content = f"{self.sequence_number}{self.timestamp}{self.operation_type}{self.table_name}{json.dumps(self.data, sort_keys=True)}"
        return hashlib.md5(content.encode()).hexdigest()

@dataclass
class ReplicaInfo:
    """副本节点信息"""
    node_id: str
    role: ReplicaRole
    endpoint: str  # 网络地址
    last_heartbeat: float
    lag_sequence: int = 0  # 复制延迟（序列号差）
    status: str = "active"  # active, inactive, failed
    priority: int = 1  # 选主优先级
    
    def __post_init__(self):
        if self.last_heartbeat == 0:
            self.last_heartbeat = time.time()
    
    @property
    def is_healthy(self) -> bool:
        """检查节点是否健康"""
        return (time.time() - self.last_heartbeat) < 30.0 and self.status == "active"

class ReplicationGroup:
    """复制组"""
    
    def __init__(self, group_id: str, consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL):
        self.group_id = group_id
        self.consistency_level = consistency_level
        self.replicas: Dict[str, ReplicaInfo] = {}
        self.master_id: Optional[str] = None
        self.replication_logs: List[ReplicationLog] = []
        self.current_sequence = 0
        self.lock = threading.RLock()
    
    def add_replica(self, replica: ReplicaInfo) -> bool:
        """添加副本节点"""
        with self.lock:
            if replica.role == ReplicaRole.MASTER:
                if self.master_id is not None:
                    return False  # 已有主节点
                self.master_id = replica.node_id
            
            self.replicas[replica.node_id] = replica
            return True
    
    def remove_replica(self, node_id: str) -> bool:
        """移除副本节点"""
        with self.lock:
            if node_id not in self.replicas:
                return False
            
            if self.master_id == node_id:
                self.master_id = None
                # 触发选主
                self._elect_new_master()
            
            del self.replicas[node_id]
            return True
    
    def get_master(self) -> Optional[ReplicaInfo]:
        """获取主节点"""
        with self.lock:
            if self.master_id and self.master_id in self.replicas:
                return self.replicas[self.master_id]
            return None
    
    def get_slaves(self) -> List[ReplicaInfo]:
        """获取从节点列表"""
        with self.lock:
            return [replica for replica in self.replicas.values() 
                   if replica.role == ReplicaRole.SLAVE and replica.is_healthy]
    
    def _elect_new_master(self):
        """选举新的主节点"""
        candidates = [replica for replica in self.replicas.values() 
                     if replica.is_healthy and replica.role != ReplicaRole.MASTER]
        
        if candidates:
            # 按优先级和序列号选择
            new_master = max(candidates, key=lambda r: (r.priority, -r.lag_sequence))
            new_master.role = ReplicaRole.MASTER
            self.master_id = new_master.node_id
            
            # 将其他候选者设为从节点
            for replica in candidates:
                if replica.node_id != new_master.node_id:
                    replica.role = ReplicaRole.SLAVE

class ReplicationManager:
    """复制管理器"""
    
    def __init__(self, node_id: str, max_workers: int = 5):
        self.node_id = node_id
        self.groups: Dict[str, ReplicationGroup] = {}
        self.replication_mode = ReplicationMode.ASYNC
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # 复制相关队列和线程
        self.replication_queue = queue.Queue()
        self.heartbeat_queue = queue.Queue()
        
        # 回调函数
        self.data_change_callbacks: List[Callable] = []
        self.failure_callbacks: List[Callable] = []
        
        # 控制线程
        self.running = False
        self.replication_thread = None
        self.heartbeat_thread = None
        
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
    
    def start(self):
        """启动复制管理器"""
        with self.lock:
            if self.running:
                return
            
            self.running = True
            self.replication_thread = threading.Thread(target=self._replication_worker, daemon=True)
            self.heartbeat_thread = threading.Thread(target=self._heartbeat_worker, daemon=True)
            
            self.replication_thread.start()
            self.heartbeat_thread.start()
            
            self.logger.info(f"Replication manager started for node {self.node_id}")
    
    def stop(self):
        """停止复制管理器"""
        with self.lock:
            if not self.running:
                return
            
            self.running = False
            
            # 等待线程结束
            if self.replication_thread:
                self.replication_thread.join(timeout=5.0)
            if self.heartbeat_thread:
                self.heartbeat_thread.join(timeout=5.0)
            
            self.executor.shutdown(wait=True)
            self.logger.info(f"Replication manager stopped for node {self.node_id}")
    
    def create_replication_group(self, group_id: str, 
                                consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL) -> ReplicationGroup:
        """创建复制组"""
        with self.lock:
            if group_id in self.groups:
                raise ValueError(f"Replication group {group_id} already exists")
            
            group = ReplicationGroup(group_id, consistency_level)
            self.groups[group_id] = group
            return group
    
    def join_replication_group(self, group_id: str, role: ReplicaRole = ReplicaRole.SLAVE,
                              endpoint: str = "", priority: int = 1) -> bool:
        """加入复制组"""
        with self.lock:
            if group_id not in self.groups:
                return False
            
            group = self.groups[group_id]
            replica = ReplicaInfo(
                node_id=self.node_id,
                role=role,
                endpoint=endpoint,
                last_heartbeat=time.time(),
                priority=priority
            )
            
            return group.add_replica(replica)
    
    def leave_replication_group(self, group_id: str) -> bool:
        """离开复制组"""
        with self.lock:
            if group_id not in self.groups:
                return False
            
            group = self.groups[group_id]
            success = group.remove_replica(self.node_id)
            
            if success and len(group.replicas) == 0:
                del self.groups[group_id]
            
            return success
    
    def replicate_operation(self, group_id: str, operation_type: str, 
                           table_name: str, data: Dict[str, Any], 
                           sql: Optional[str] = None) -> bool:
        """复制操作到其他节点"""
        with self.lock:
            if group_id not in self.groups:
                return False
            
            group = self.groups[group_id]
            master = group.get_master()
            
            if not master or master.node_id != self.node_id:
                return False  # 只有主节点可以发起复制
            
            # 创建复制日志
            group.current_sequence += 1
            log_entry = ReplicationLog(
                log_id=f"{group_id}_{group.current_sequence}",
                sequence_number=group.current_sequence,
                timestamp=time.time(),
                operation_type=operation_type,
                table_name=table_name,
                data=data,
                sql=sql
            )
            
            group.replication_logs.append(log_entry)
            
            # 添加到复制队列
            self.replication_queue.put((group_id, log_entry))
            
            return True
    
    def _replication_worker(self):
        """复制工作线程"""
        while self.running:
            try:
                # 从队列获取复制任务
                group_id, log_entry = self.replication_queue.get(timeout=1.0)
                
                if group_id not in self.groups:
                    continue
                
                group = self.groups[group_id]
                slaves = group.get_slaves()
                
                if not slaves:
                    continue
                
                if self.replication_mode == ReplicationMode.SYNC:
                    self._sync_replicate(group_id, log_entry, slaves)
                elif self.replication_mode == ReplicationMode.SEMI_SYNC:
                    self._semi_sync_replicate(group_id, log_entry, slaves)
                else:  # ASYNC
                    self._async_replicate(group_id, log_entry, slaves)
                
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Replication worker error: {e}")
    
    def _sync_replicate(self, group_id: str, log_entry: ReplicationLog, slaves: List[ReplicaInfo]):
        """同步复制"""
        futures = []
        
        for slave in slaves:
            future = self.executor.submit(self._replicate_to_slave, slave, log_entry)
            futures.append((future, slave))
        
        # 等待所有从节点完成
        success_count = 0
        for future, slave in futures:
            try:
                success = future.result(timeout=10.0)  # 10秒超时
                if success:
                    success_count += 1
                else:
                    self._handle_replication_failure(slave, log_entry)
            except Exception as e:
                self.logger.error(f"Sync replication to {slave.node_id} failed: {e}")
                self._handle_replication_failure(slave, log_entry)
        
        # 强一致性要求所有节点都成功
        if self.groups[group_id].consistency_level == ConsistencyLevel.STRONG:
            if success_count < len(slaves):
                self.logger.warning(f"Strong consistency violated: only {success_count}/{len(slaves)} replicas succeeded")
    
    def _semi_sync_replicate(self, group_id: str, log_entry: ReplicationLog, slaves: List[ReplicaInfo]):
        """半同步复制"""
        if not slaves:
            return
        
        # 半同步：等待至少一个从节点确认
        futures = []
        for slave in slaves:
            future = self.executor.submit(self._replicate_to_slave, slave, log_entry)
            futures.append((future, slave))
        
        success_count = 0
        required_acks = max(1, len(slaves) // 2)  # 至少半数确认
        
        for future, slave in as_completed([f for f, s in futures], timeout=5.0):
            try:
                success = future.result()
                if success:
                    success_count += 1
                    if success_count >= required_acks:
                        break  # 达到要求的确认数
                else:
                    self._handle_replication_failure(slave, log_entry)
            except Exception as e:
                self.logger.error(f"Semi-sync replication to {slave.node_id} failed: {e}")
                self._handle_replication_failure(slave, log_entry)
    
    def _async_replicate(self, group_id: str, log_entry: ReplicationLog, slaves: List[ReplicaInfo]):
        """异步复制"""
        for slave in slaves:
            self.executor.submit(self._replicate_to_slave_async, slave, log_entry)
    
    def _replicate_to_slave(self, slave: ReplicaInfo, log_entry: ReplicationLog) -> bool:
        """复制到单个从节点"""
        try:
            # 这里应该实现实际的网络通信
            # 模拟复制操作
            time.sleep(0.01)  # 模拟网络延迟
            
            # 更新从节点的复制延迟
            slave.lag_sequence = log_entry.sequence_number
            
            # 触发数据变更回调
            for callback in self.data_change_callbacks:
                try:
                    callback(slave.node_id, log_entry)
                except Exception as e:
                    self.logger.error(f"Data change callback error: {e}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to replicate to {slave.node_id}: {e}")
            return False
    
    def _replicate_to_slave_async(self, slave: ReplicaInfo, log_entry: ReplicationLog):
        """异步复制到从节点"""
        success = self._replicate_to_slave(slave, log_entry)
        if not success:
            self._handle_replication_failure(slave, log_entry)
    
    def _handle_replication_failure(self, slave: ReplicaInfo, log_entry: ReplicationLog):
        """处理复制失败"""
        slave.status = "failed"
        
        # 触发失败回调
        for callback in self.failure_callbacks:
            try:
                callback(slave.node_id, log_entry)
            except Exception as e:
                self.logger.error(f"Failure callback error: {e}")
    
    def _heartbeat_worker(self):
        """心跳工作线程"""
        while self.running:
            try:
                self._send_heartbeats()
                self._check_replica_health()
                time.sleep(5.0)  # 每5秒发送心跳
            except Exception as e:
                self.logger.error(f"Heartbeat worker error: {e}")
    
    def _send_heartbeats(self):
        """发送心跳"""
        current_time = time.time()
        
        for group in self.groups.values():
            master = group.get_master()
            if master and master.node_id == self.node_id:
                # 主节点发送心跳给从节点
                for slave in group.get_slaves():
                    self.heartbeat_queue.put(("heartbeat", slave.node_id, current_time))
    
    def _check_replica_health(self):
        """检查副本健康状态"""
        current_time = time.time()
        
        for group in self.groups.values():
            unhealthy_replicas = []
            
            for replica in group.replicas.values():
                if not replica.is_healthy:
                    unhealthy_replicas.append(replica)
            
            # 处理不健康的副本
            for replica in unhealthy_replicas:
                if replica.role == ReplicaRole.MASTER:
                    # 主节点故障，触发选主
                    group._elect_new_master()
                else:
                    # 从节点故障，标记为失效
                    replica.status = "failed"
    
    def get_replication_status(self, group_id: str) -> Dict[str, Any]:
        """获取复制状态"""
        with self.lock:
            if group_id not in self.groups:
                return {}
            
            group = self.groups[group_id]
            master = group.get_master()
            slaves = group.get_slaves()
            
            return {
                'group_id': group_id,
                'consistency_level': group.consistency_level.value,
                'master': {
                    'node_id': master.node_id,
                    'endpoint': master.endpoint,
                    'status': master.status
                } if master else None,
                'slaves': [
                    {
                        'node_id': slave.node_id,
                        'endpoint': slave.endpoint,
                        'status': slave.status,
                        'lag_sequence': slave.lag_sequence,
                        'last_heartbeat': slave.last_heartbeat
                    } for slave in slaves
                ],
                'current_sequence': group.current_sequence,
                'log_count': len(group.replication_logs),
                'replication_mode': self.replication_mode.value
            }
    
    def get_all_groups_status(self) -> Dict[str, Any]:
        """获取所有复制组状态"""
        with self.lock:
            return {
                group_id: self.get_replication_status(group_id)
                for group_id in self.groups.keys()
            }
    
    def set_replication_mode(self, mode: ReplicationMode):
        """设置复制模式"""
        with self.lock:
            self.replication_mode = mode
            self.logger.info(f"Replication mode changed to {mode.value}")
    
    def add_data_change_callback(self, callback: Callable):
        """添加数据变更回调"""
        self.data_change_callbacks.append(callback)
    
    def add_failure_callback(self, callback: Callable):
        """添加失败回调"""
        self.failure_callbacks.append(callback)
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self.lock:
            total_replicas = sum(len(group.replicas) for group in self.groups.values())
            total_logs = sum(len(group.replication_logs) for group in self.groups.values())
            
            return {
                'node_id': self.node_id,
                'total_groups': len(self.groups),
                'total_replicas': total_replicas,
                'total_logs': total_logs,
                'replication_mode': self.replication_mode.value,
                'queue_size': self.replication_queue.qsize(),
                'running': self.running
            }

class ReadWriteSeparator:
    """读写分离器"""
    
    def __init__(self, replication_manager: ReplicationManager):
        self.replication_manager = replication_manager
        self.read_preference = "slave_first"  # slave_first, master_only, slave_only
        self.load_balancer = RoundRobinLoadBalancer()
        self.lock = threading.RLock()
    
    def route_query(self, group_id: str, sql: str, is_write_operation: bool = None) -> Optional[str]:
        """路由查询到合适的节点"""
        with self.lock:
            if group_id not in self.replication_manager.groups:
                return None
            
            group = self.replication_manager.groups[group_id]
            
            if is_write_operation is None:
                is_write_operation = self._is_write_operation(sql)
            
            if is_write_operation:
                # 写操作只能路由到主节点
                master = group.get_master()
                return master.node_id if master else None
            else:
                # 读操作根据策略路由
                return self._route_read_query(group)
    
    def _is_write_operation(self, sql: str) -> bool:
        """判断是否为写操作"""
        sql_upper = sql.strip().upper()
        write_keywords = ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER']
        return any(sql_upper.startswith(keyword) for keyword in write_keywords)
    
    def _route_read_query(self, group: ReplicationGroup) -> Optional[str]:
        """路由读查询"""
        master = group.get_master()
        slaves = group.get_slaves()
        
        if self.read_preference == "master_only":
            return master.node_id if master else None
        
        elif self.read_preference == "slave_only":
            if slaves:
                return self.load_balancer.select_node([s.node_id for s in slaves])
            return None
        
        else:  # slave_first
            if slaves:
                return self.load_balancer.select_node([s.node_id for s in slaves])
            return master.node_id if master else None
    
    def set_read_preference(self, preference: str):
        """设置读偏好"""
        valid_preferences = ["slave_first", "master_only", "slave_only"]
        if preference in valid_preferences:
            with self.lock:
                self.read_preference = preference

class RoundRobinLoadBalancer:
    """轮询负载均衡器"""
    
    def __init__(self):
        self.counters: Dict[str, int] = {}
        self.lock = threading.RLock()
    
    def select_node(self, nodes: List[str]) -> Optional[str]:
        """选择节点"""
        if not nodes:
            return None
        
        with self.lock:
            nodes_key = "|".join(sorted(nodes))
            
            if nodes_key not in self.counters:
                self.counters[nodes_key] = 0
            
            selected_index = self.counters[nodes_key] % len(nodes)
            self.counters[nodes_key] = (self.counters[nodes_key] + 1) % len(nodes)
            
            return nodes[selected_index]
