"""
容错与高可用模块

实现节点故障检测、数据副本自动恢复和负载均衡
"""

import threading
import time
import random
from typing import Dict, List, Any, Optional, Set, Callable
from dataclasses import dataclass, field
from enum import Enum
import json
import logging
from concurrent.futures import ThreadPoolExecutor
import queue

class NodeStatus(Enum):
    """节点状态"""
    HEALTHY = "healthy"
    SUSPECT = "suspect"
    FAILED = "failed"
    RECOVERING = "recovering"
    MAINTENANCE = "maintenance"

class FailureType(Enum):
    """故障类型"""
    NETWORK_PARTITION = "network_partition"
    NODE_CRASH = "node_crash"
    DISK_FAILURE = "disk_failure"
    HIGH_LOAD = "high_load"
    TIMEOUT = "timeout"

class RecoveryStrategy(Enum):
    """恢复策略"""
    IMMEDIATE = "immediate"
    DELAYED = "delayed"
    MANUAL = "manual"

@dataclass
class NodeInfo:
    """节点信息"""
    node_id: str
    endpoint: str
    status: NodeStatus = NodeStatus.HEALTHY
    last_heartbeat: float = field(default_factory=time.time)
    failure_count: int = 0
    load_average: float = 0.0
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    disk_usage: float = 0.0
    network_latency: float = 0.0
    priority: int = 1
    
    @property
    def is_healthy(self) -> bool:
        """检查节点是否健康"""
        return (self.status == NodeStatus.HEALTHY and 
                time.time() - self.last_heartbeat < 30.0)
    
    @property
    def health_score(self) -> float:
        """计算节点健康分数"""
        if self.status != NodeStatus.HEALTHY:
            return 0.0
        
        # 基于负载、延迟等计算健康分数
        load_score = max(0, 1.0 - self.load_average / 10.0)
        cpu_score = max(0, 1.0 - self.cpu_usage / 100.0)
        memory_score = max(0, 1.0 - self.memory_usage / 100.0)
        latency_score = max(0, 1.0 - self.network_latency / 1000.0)
        
        return (load_score + cpu_score + memory_score + latency_score) / 4.0

@dataclass
class FailureEvent:
    """故障事件"""
    event_id: str
    node_id: str
    failure_type: FailureType
    timestamp: float
    description: str
    severity: str = "medium"  # low, medium, high, critical
    resolved: bool = False
    resolution_time: Optional[float] = None

class FailureDetector:
    """故障检测器"""
    
    def __init__(self, heartbeat_interval: float = 5.0, failure_threshold: int = 3):
        self.heartbeat_interval = heartbeat_interval
        self.failure_threshold = failure_threshold
        self.nodes: Dict[str, NodeInfo] = {}
        self.failure_callbacks: List[Callable] = []
        self.recovery_callbacks: List[Callable] = []
        
        self.running = False
        self.detector_thread = None
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
    
    def start(self):
        """启动故障检测器"""
        with self.lock:
            if self.running:
                return
            
            self.running = True
            self.detector_thread = threading.Thread(target=self._detection_worker, daemon=True)
            self.detector_thread.start()
            
            self.logger.info("Failure detector started")
    
    def stop(self):
        """停止故障检测器"""
        with self.lock:
            if not self.running:
                return
            
            self.running = False
            if self.detector_thread:
                self.detector_thread.join(timeout=5.0)
            
            self.logger.info("Failure detector stopped")
    
    def register_node(self, node: NodeInfo):
        """注册节点"""
        with self.lock:
            self.nodes[node.node_id] = node
            self.logger.info(f"Node {node.node_id} registered")
    
    def unregister_node(self, node_id: str):
        """注销节点"""
        with self.lock:
            if node_id in self.nodes:
                del self.nodes[node_id]
                self.logger.info(f"Node {node_id} unregistered")
    
    def update_heartbeat(self, node_id: str, metrics: Dict[str, Any] = None):
        """更新心跳"""
        with self.lock:
            if node_id not in self.nodes:
                return
            
            node = self.nodes[node_id]
            node.last_heartbeat = time.time()
            
            # 更新节点指标
            if metrics:
                node.load_average = metrics.get('load_average', node.load_average)
                node.cpu_usage = metrics.get('cpu_usage', node.cpu_usage)
                node.memory_usage = metrics.get('memory_usage', node.memory_usage)
                node.disk_usage = metrics.get('disk_usage', node.disk_usage)
                node.network_latency = metrics.get('network_latency', node.network_latency)
            
            # 如果节点之前是失败状态，现在恢复了
            if node.status == NodeStatus.FAILED:
                node.status = NodeStatus.RECOVERING
                node.failure_count = 0
                self._trigger_recovery_callbacks(node_id)
    
    def _detection_worker(self):
        """检测工作线程"""
        while self.running:
            try:
                self._check_node_health()
                time.sleep(self.heartbeat_interval)
            except Exception as e:
                self.logger.error(f"Detection worker error: {e}")
    
    def _check_node_health(self):
        """检查节点健康状态"""
        current_time = time.time()
        
        with self.lock:
            for node_id, node in self.nodes.items():
                time_since_heartbeat = current_time - node.last_heartbeat
                
                if node.status == NodeStatus.HEALTHY:
                    if time_since_heartbeat > self.heartbeat_interval * 2:
                        node.status = NodeStatus.SUSPECT
                        self.logger.warning(f"Node {node_id} is suspect (no heartbeat for {time_since_heartbeat:.1f}s)")
                
                elif node.status == NodeStatus.SUSPECT:
                    if time_since_heartbeat > self.heartbeat_interval * self.failure_threshold:
                        node.status = NodeStatus.FAILED
                        node.failure_count += 1
                        self.logger.error(f"Node {node_id} failed (no heartbeat for {time_since_heartbeat:.1f}s)")
                        self._trigger_failure_callbacks(node_id, FailureType.TIMEOUT)
                    elif time_since_heartbeat <= self.heartbeat_interval * 2:
                        node.status = NodeStatus.HEALTHY
                        self.logger.info(f"Node {node_id} recovered from suspect state")
                
                elif node.status == NodeStatus.RECOVERING:
                    if time_since_heartbeat <= self.heartbeat_interval * 2:
                        node.status = NodeStatus.HEALTHY
                        self.logger.info(f"Node {node_id} fully recovered")
                    elif time_since_heartbeat > self.heartbeat_interval * self.failure_threshold:
                        node.status = NodeStatus.FAILED
                        node.failure_count += 1
                        self.logger.error(f"Node {node_id} failed again during recovery")
                        self._trigger_failure_callbacks(node_id, FailureType.TIMEOUT)
    
    def _trigger_failure_callbacks(self, node_id: str, failure_type: FailureType):
        """触发故障回调"""
        for callback in self.failure_callbacks:
            try:
                callback(node_id, failure_type)
            except Exception as e:
                self.logger.error(f"Failure callback error: {e}")
    
    def _trigger_recovery_callbacks(self, node_id: str):
        """触发恢复回调"""
        for callback in self.recovery_callbacks:
            try:
                callback(node_id)
            except Exception as e:
                self.logger.error(f"Recovery callback error: {e}")
    
    def add_failure_callback(self, callback: Callable):
        """添加故障回调"""
        self.failure_callbacks.append(callback)
    
    def add_recovery_callback(self, callback: Callable):
        """添加恢复回调"""
        self.recovery_callbacks.append(callback)
    
    def get_healthy_nodes(self) -> List[NodeInfo]:
        """获取健康节点列表"""
        with self.lock:
            return [node for node in self.nodes.values() if node.is_healthy]
    
    def get_failed_nodes(self) -> List[NodeInfo]:
        """获取失败节点列表"""
        with self.lock:
            return [node for node in self.nodes.values() if node.status == NodeStatus.FAILED]
    
    def get_node_status(self, node_id: str) -> Optional[NodeStatus]:
        """获取节点状态"""
        with self.lock:
            if node_id in self.nodes:
                return self.nodes[node_id].status
            return None

class LoadBalancer:
    """负载均衡器"""
    
    def __init__(self, strategy: str = "round_robin"):
        self.strategy = strategy
        self.node_counters: Dict[str, int] = {}
        self.lock = threading.RLock()
    
    def select_node(self, nodes: List[NodeInfo], exclude: Set[str] = None) -> Optional[NodeInfo]:
        """选择节点"""
        if not nodes:
            return None
        
        if exclude:
            nodes = [node for node in nodes if node.node_id not in exclude]
        
        if not nodes:
            return None
        
        healthy_nodes = [node for node in nodes if node.is_healthy]
        if not healthy_nodes:
            return None
        
        if self.strategy == "round_robin":
            return self._round_robin_select(healthy_nodes)
        elif self.strategy == "weighted_round_robin":
            return self._weighted_round_robin_select(healthy_nodes)
        elif self.strategy == "least_connections":
            return self._least_connections_select(healthy_nodes)
        elif self.strategy == "health_based":
            return self._health_based_select(healthy_nodes)
        else:
            return random.choice(healthy_nodes)
    
    def _round_robin_select(self, nodes: List[NodeInfo]) -> NodeInfo:
        """轮询选择"""
        with self.lock:
            nodes_key = "|".join(sorted(node.node_id for node in nodes))
            
            if nodes_key not in self.node_counters:
                self.node_counters[nodes_key] = 0
            
            selected_index = self.node_counters[nodes_key] % len(nodes)
            self.node_counters[nodes_key] = (self.node_counters[nodes_key] + 1) % len(nodes)
            
            return nodes[selected_index]
    
    def _weighted_round_robin_select(self, nodes: List[NodeInfo]) -> NodeInfo:
        """加权轮询选择"""
        # 基于优先级的加权选择
        weighted_nodes = []
        for node in nodes:
            weighted_nodes.extend([node] * node.priority)
        
        return self._round_robin_select(weighted_nodes)
    
    def _least_connections_select(self, nodes: List[NodeInfo]) -> NodeInfo:
        """最少连接选择"""
        # 简化实现，基于负载平均值
        return min(nodes, key=lambda n: n.load_average)
    
    def _health_based_select(self, nodes: List[NodeInfo]) -> NodeInfo:
        """基于健康分数选择"""
        return max(nodes, key=lambda n: n.health_score)

class AutoRecoveryManager:
    """自动恢复管理器"""
    
    def __init__(self, failure_detector: FailureDetector):
        self.failure_detector = failure_detector
        self.recovery_strategies: Dict[str, RecoveryStrategy] = {}
        self.recovery_queue = queue.Queue()
        self.recovery_tasks: Dict[str, threading.Thread] = {}
        
        self.running = False
        self.recovery_thread = None
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        
        # 注册故障回调
        self.failure_detector.add_failure_callback(self._on_node_failure)
        self.failure_detector.add_recovery_callback(self._on_node_recovery)
    
    def start(self):
        """启动自动恢复管理器"""
        with self.lock:
            if self.running:
                return
            
            self.running = True
            self.recovery_thread = threading.Thread(target=self._recovery_worker, daemon=True)
            self.recovery_thread.start()
            
            self.logger.info("Auto recovery manager started")
    
    def stop(self):
        """停止自动恢复管理器"""
        with self.lock:
            if not self.running:
                return
            
            self.running = False
            if self.recovery_thread:
                self.recovery_thread.join(timeout=5.0)
            
            # 停止所有恢复任务
            for task_thread in self.recovery_tasks.values():
                if task_thread.is_alive():
                    task_thread.join(timeout=1.0)
            
            self.logger.info("Auto recovery manager stopped")
    
    def set_recovery_strategy(self, node_id: str, strategy: RecoveryStrategy):
        """设置节点的恢复策略"""
        with self.lock:
            self.recovery_strategies[node_id] = strategy
    
    def _on_node_failure(self, node_id: str, failure_type: FailureType):
        """节点故障处理"""
        self.logger.info(f"Handling failure of node {node_id}: {failure_type}")
        
        strategy = self.recovery_strategies.get(node_id, RecoveryStrategy.DELAYED)
        
        if strategy == RecoveryStrategy.IMMEDIATE:
            self.recovery_queue.put(("recover", node_id, failure_type))
        elif strategy == RecoveryStrategy.DELAYED:
            # 延迟恢复，添加到队列但等待一段时间
            self.recovery_queue.put(("delayed_recover", node_id, failure_type))
        # MANUAL策略不自动恢复
    
    def _on_node_recovery(self, node_id: str):
        """节点恢复处理"""
        self.logger.info(f"Node {node_id} is recovering")
        
        # 可以在这里执行数据同步等恢复操作
        self.recovery_queue.put(("sync", node_id))
    
    def _recovery_worker(self):
        """恢复工作线程"""
        while self.running:
            try:
                action, node_id, *args = self.recovery_queue.get(timeout=1.0)
                
                if action == "recover":
                    self._start_recovery_task(node_id, args[0])
                elif action == "delayed_recover":
                    # 延迟5分钟后恢复
                    threading.Timer(300.0, lambda: self._start_recovery_task(node_id, args[0])).start()
                elif action == "sync":
                    self._start_sync_task(node_id)
                
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Recovery worker error: {e}")
    
    def _start_recovery_task(self, node_id: str, failure_type: FailureType):
        """启动恢复任务"""
        with self.lock:
            if node_id in self.recovery_tasks and self.recovery_tasks[node_id].is_alive():
                return  # 已有恢复任务在进行
            
            task_thread = threading.Thread(
                target=self._execute_recovery,
                args=(node_id, failure_type),
                daemon=True
            )
            self.recovery_tasks[node_id] = task_thread
            task_thread.start()
    
    def _start_sync_task(self, node_id: str):
        """启动同步任务"""
        with self.lock:
            sync_thread = threading.Thread(
                target=self._execute_sync,
                args=(node_id,),
                daemon=True
            )
            sync_thread.start()
    
    def _execute_recovery(self, node_id: str, failure_type: FailureType):
        """执行恢复操作"""
        try:
            self.logger.info(f"Starting recovery for node {node_id}")
            
            # 根据故障类型执行不同的恢复策略
            if failure_type == FailureType.NODE_CRASH:
                self._recover_from_crash(node_id)
            elif failure_type == FailureType.NETWORK_PARTITION:
                self._recover_from_partition(node_id)
            elif failure_type == FailureType.DISK_FAILURE:
                self._recover_from_disk_failure(node_id)
            else:
                self._generic_recovery(node_id)
            
            self.logger.info(f"Recovery completed for node {node_id}")
            
        except Exception as e:
            self.logger.error(f"Recovery failed for node {node_id}: {e}")
        finally:
            with self.lock:
                if node_id in self.recovery_tasks:
                    del self.recovery_tasks[node_id]
    
    def _recover_from_crash(self, node_id: str):
        """从崩溃中恢复"""
        # 1. 尝试重启节点服务
        # 2. 检查数据完整性
        # 3. 从副本恢复数据
        # 4. 重新加入集群
        time.sleep(10)  # 模拟恢复过程
    
    def _recover_from_partition(self, node_id: str):
        """从网络分区中恢复"""
        # 1. 等待网络恢复
        # 2. 重新建立连接
        # 3. 同步数据
        time.sleep(5)  # 模拟恢复过程
    
    def _recover_from_disk_failure(self, node_id: str):
        """从磁盘故障中恢复"""
        # 1. 替换故障磁盘
        # 2. 从备份恢复数据
        # 3. 重建索引
        time.sleep(30)  # 模拟恢复过程
    
    def _generic_recovery(self, node_id: str):
        """通用恢复"""
        # 通用的恢复步骤
        time.sleep(5)  # 模拟恢复过程
    
    def _execute_sync(self, node_id: str):
        """执行数据同步"""
        try:
            self.logger.info(f"Starting data sync for node {node_id}")
            
            # 1. 获取需要同步的数据
            # 2. 计算数据差异
            # 3. 传输增量数据
            # 4. 验证数据一致性
            
            time.sleep(10)  # 模拟同步过程
            
            self.logger.info(f"Data sync completed for node {node_id}")
            
        except Exception as e:
            self.logger.error(f"Data sync failed for node {node_id}: {e}")

class FaultToleranceManager:
    """容错管理器主类"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.failure_detector = FailureDetector()
        self.load_balancer = LoadBalancer("health_based")
        self.auto_recovery = AutoRecoveryManager(self.failure_detector)
        self.failure_events: List[FailureEvent] = []
        
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
    
    def start(self):
        """启动容错管理器"""
        self.failure_detector.start()
        self.auto_recovery.start()
        self.logger.info(f"Fault tolerance manager started for node {self.node_id}")
    
    def stop(self):
        """停止容错管理器"""
        self.auto_recovery.stop()
        self.failure_detector.stop()
        self.logger.info(f"Fault tolerance manager stopped for node {self.node_id}")
    
    def register_node(self, node_info: NodeInfo):
        """注册节点"""
        self.failure_detector.register_node(node_info)
    
    def unregister_node(self, node_id: str):
        """注销节点"""
        self.failure_detector.unregister_node(node_id)
    
    def update_node_metrics(self, node_id: str, metrics: Dict[str, Any]):
        """更新节点指标"""
        self.failure_detector.update_heartbeat(node_id, metrics)
    
    def select_best_node(self, exclude: Set[str] = None) -> Optional[NodeInfo]:
        """选择最佳节点"""
        healthy_nodes = self.failure_detector.get_healthy_nodes()
        return self.load_balancer.select_node(healthy_nodes, exclude)
    
    def set_load_balancing_strategy(self, strategy: str):
        """设置负载均衡策略"""
        self.load_balancer.strategy = strategy
    
    def set_recovery_strategy(self, node_id: str, strategy: RecoveryStrategy):
        """设置恢复策略"""
        self.auto_recovery.set_recovery_strategy(node_id, strategy)
    
    def record_failure_event(self, node_id: str, failure_type: FailureType, 
                           description: str, severity: str = "medium"):
        """记录故障事件"""
        event = FailureEvent(
            event_id=f"failure_{int(time.time() * 1000000)}",
            node_id=node_id,
            failure_type=failure_type,
            timestamp=time.time(),
            description=description,
            severity=severity
        )
        
        with self.lock:
            self.failure_events.append(event)
            # 只保留最近1000个事件
            if len(self.failure_events) > 1000:
                self.failure_events = self.failure_events[-1000:]
    
    def get_cluster_health(self) -> Dict[str, Any]:
        """获取集群健康状态"""
        healthy_nodes = self.failure_detector.get_healthy_nodes()
        failed_nodes = self.failure_detector.get_failed_nodes()
        
        with self.lock:
            recent_failures = [
                event for event in self.failure_events
                if time.time() - event.timestamp < 3600  # 最近1小时
            ]
        
        return {
            'total_nodes': len(self.failure_detector.nodes),
            'healthy_nodes': len(healthy_nodes),
            'failed_nodes': len(failed_nodes),
            'cluster_health_percentage': len(healthy_nodes) / len(self.failure_detector.nodes) * 100 if self.failure_detector.nodes else 0,
            'recent_failures': len(recent_failures),
            'load_balancing_strategy': self.load_balancer.strategy
        }
    
    def get_node_details(self) -> List[Dict[str, Any]]:
        """获取节点详细信息"""
        with self.failure_detector.lock:
            return [
                {
                    'node_id': node.node_id,
                    'status': node.status.value,
                    'health_score': node.health_score,
                    'load_average': node.load_average,
                    'cpu_usage': node.cpu_usage,
                    'memory_usage': node.memory_usage,
                    'disk_usage': node.disk_usage,
                    'network_latency': node.network_latency,
                    'failure_count': node.failure_count,
                    'last_heartbeat': node.last_heartbeat
                }
                for node in self.failure_detector.nodes.values()
            ]
    
    def get_recent_failure_events(self, hours: int = 24) -> List[Dict[str, Any]]:
        """获取最近的故障事件"""
        cutoff_time = time.time() - hours * 3600
        
        with self.lock:
            recent_events = [
                {
                    'event_id': event.event_id,
                    'node_id': event.node_id,
                    'failure_type': event.failure_type.value,
                    'timestamp': event.timestamp,
                    'description': event.description,
                    'severity': event.severity,
                    'resolved': event.resolved,
                    'resolution_time': event.resolution_time
                }
                for event in self.failure_events
                if event.timestamp >= cutoff_time
            ]
        
        return sorted(recent_events, key=lambda x: x['timestamp'], reverse=True)
