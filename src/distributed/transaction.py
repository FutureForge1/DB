"""
分布式事务模块

实现两阶段提交（2PC）协议、分布式死锁检测和事务隔离级别支持
"""

import threading
import time
import uuid
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, Future

class TransactionState(Enum):
    """事务状态"""
    ACTIVE = "active"
    PREPARING = "preparing"
    PREPARED = "prepared"
    COMMITTING = "committing"
    COMMITTED = "committed"
    ABORTING = "aborting"
    ABORTED = "aborted"
    TIMEOUT = "timeout"

class IsolationLevel(Enum):
    """事务隔离级别"""
    READ_UNCOMMITTED = "read_uncommitted"
    READ_COMMITTED = "read_committed"
    REPEATABLE_READ = "repeatable_read"
    SERIALIZABLE = "serializable"

class VoteResult(Enum):
    """投票结果"""
    YES = "yes"
    NO = "no"
    TIMEOUT = "timeout"

@dataclass
class TransactionOperation:
    """事务操作"""
    operation_id: str
    operation_type: str  # SELECT, INSERT, UPDATE, DELETE
    table_name: str
    sql: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    node_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

@dataclass
class Lock:
    """锁信息"""
    lock_id: str
    resource_id: str  # 资源标识符
    lock_type: str   # SHARED, EXCLUSIVE
    transaction_id: str
    node_id: str
    acquired_time: float = field(default_factory=time.time)
    
    def is_compatible(self, other: 'Lock') -> bool:
        """检查锁兼容性"""
        if self.resource_id != other.resource_id:
            return True
        if self.transaction_id == other.transaction_id:
            return True
        if self.lock_type == "SHARED" and other.lock_type == "SHARED":
            return True
        return False

@dataclass
class DistributedTransaction:
    """分布式事务"""
    transaction_id: str
    coordinator_id: str
    participants: Set[str] = field(default_factory=set)
    operations: List[TransactionOperation] = field(default_factory=list)
    state: TransactionState = TransactionState.ACTIVE
    isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED
    start_time: float = field(default_factory=time.time)
    timeout: float = 300.0  # 5分钟超时
    locks: List[Lock] = field(default_factory=list)
    
    @property
    def is_expired(self) -> bool:
        """检查事务是否超时"""
        return time.time() - self.start_time > self.timeout
    
    def add_participant(self, node_id: str):
        """添加参与者"""
        self.participants.add(node_id)
    
    def add_operation(self, operation: TransactionOperation):
        """添加操作"""
        self.operations.append(operation)
        if operation.node_id:
            self.add_participant(operation.node_id)

class DeadlockDetector:
    """死锁检测器"""
    
    def __init__(self):
        self.wait_for_graph: Dict[str, Set[str]] = {}  # transaction_id -> waiting_for_transactions
        self.lock = threading.RLock()
    
    def add_wait_edge(self, waiting_tx: str, holding_tx: str):
        """添加等待边"""
        with self.lock:
            if waiting_tx not in self.wait_for_graph:
                self.wait_for_graph[waiting_tx] = set()
            self.wait_for_graph[waiting_tx].add(holding_tx)
    
    def remove_wait_edge(self, waiting_tx: str, holding_tx: str):
        """移除等待边"""
        with self.lock:
            if waiting_tx in self.wait_for_graph:
                self.wait_for_graph[waiting_tx].discard(holding_tx)
                if not self.wait_for_graph[waiting_tx]:
                    del self.wait_for_graph[waiting_tx]
    
    def remove_transaction(self, tx_id: str):
        """移除事务的所有边"""
        with self.lock:
            # 移除该事务作为等待者的边
            if tx_id in self.wait_for_graph:
                del self.wait_for_graph[tx_id]
            
            # 移除该事务作为被等待者的边
            for waiting_tx in list(self.wait_for_graph.keys()):
                self.wait_for_graph[waiting_tx].discard(tx_id)
                if not self.wait_for_graph[waiting_tx]:
                    del self.wait_for_graph[waiting_tx]
    
    def detect_deadlock(self) -> Optional[List[str]]:
        """检测死锁，返回死锁环"""
        with self.lock:
            visited = set()
            rec_stack = set()
            
            for tx_id in self.wait_for_graph:
                if tx_id not in visited:
                    cycle = self._dfs_detect_cycle(tx_id, visited, rec_stack, [])
                    if cycle:
                        return cycle
            
            return None
    
    def _dfs_detect_cycle(self, tx_id: str, visited: Set[str], 
                         rec_stack: Set[str], path: List[str]) -> Optional[List[str]]:
        """DFS检测环"""
        visited.add(tx_id)
        rec_stack.add(tx_id)
        path.append(tx_id)
        
        if tx_id in self.wait_for_graph:
            for neighbor in self.wait_for_graph[tx_id]:
                if neighbor not in visited:
                    cycle = self._dfs_detect_cycle(neighbor, visited, rec_stack, path.copy())
                    if cycle:
                        return cycle
                elif neighbor in rec_stack:
                    # 找到环
                    cycle_start = path.index(neighbor)
                    return path[cycle_start:] + [neighbor]
        
        rec_stack.remove(tx_id)
        return None

class LockManager:
    """锁管理器"""
    
    def __init__(self):
        self.locks: Dict[str, List[Lock]] = {}  # resource_id -> locks
        self.transaction_locks: Dict[str, List[Lock]] = {}  # transaction_id -> locks
        self.waiting_queue: Dict[str, List[Tuple[Lock, Future]]] = {}  # resource_id -> waiting locks
        self.deadlock_detector = DeadlockDetector()
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
    
    def acquire_lock(self, transaction_id: str, resource_id: str, 
                    lock_type: str, node_id: str, timeout: float = 30.0) -> bool:
        """获取锁"""
        lock = Lock(
            lock_id=f"{transaction_id}_{resource_id}_{lock_type}",
            resource_id=resource_id,
            lock_type=lock_type,
            transaction_id=transaction_id,
            node_id=node_id
        )
        
        with self.lock:
            if self._can_acquire_lock(lock):
                self._grant_lock(lock)
                return True
            else:
                # 需要等待
                return self._wait_for_lock(lock, timeout)
    
    def _can_acquire_lock(self, new_lock: Lock) -> bool:
        """检查是否可以获取锁"""
        resource_id = new_lock.resource_id
        
        if resource_id not in self.locks:
            return True
        
        existing_locks = self.locks[resource_id]
        return all(new_lock.is_compatible(existing_lock) for existing_lock in existing_locks)
    
    def _grant_lock(self, lock: Lock):
        """授予锁"""
        resource_id = lock.resource_id
        transaction_id = lock.transaction_id
        
        if resource_id not in self.locks:
            self.locks[resource_id] = []
        self.locks[resource_id].append(lock)
        
        if transaction_id not in self.transaction_locks:
            self.transaction_locks[transaction_id] = []
        self.transaction_locks[transaction_id].append(lock)
    
    def _wait_for_lock(self, lock: Lock, timeout: float) -> bool:
        """等待锁"""
        resource_id = lock.resource_id
        transaction_id = lock.transaction_id
        
        # 添加到等待队列
        if resource_id not in self.waiting_queue:
            self.waiting_queue[resource_id] = []
        
        future = Future()
        self.waiting_queue[resource_id].append((lock, future))
        
        # 更新死锁检测图
        if resource_id in self.locks:
            for existing_lock in self.locks[resource_id]:
                if not lock.is_compatible(existing_lock):
                    self.deadlock_detector.add_wait_edge(transaction_id, existing_lock.transaction_id)
        
        # 释放锁后等待
        self.lock.release()
        
        try:
            # 等待锁被授予
            result = future.result(timeout=timeout)
            return result
        except Exception as e:
            self.logger.error(f"Lock wait failed: {e}")
            return False
        finally:
            self.lock.acquire()
            # 清理等待队列
            if resource_id in self.waiting_queue:
                self.waiting_queue[resource_id] = [
                    (l, f) for l, f in self.waiting_queue[resource_id] 
                    if l.lock_id != lock.lock_id
                ]
    
    def release_locks(self, transaction_id: str):
        """释放事务的所有锁"""
        with self.lock:
            if transaction_id not in self.transaction_locks:
                return
            
            locks_to_release = self.transaction_locks[transaction_id].copy()
            
            for lock in locks_to_release:
                self._release_single_lock(lock)
            
            del self.transaction_locks[transaction_id]
            self.deadlock_detector.remove_transaction(transaction_id)
            
            # 尝试授予等待中的锁
            self._process_waiting_locks()
    
    def _release_single_lock(self, lock: Lock):
        """释放单个锁"""
        resource_id = lock.resource_id
        
        if resource_id in self.locks:
            self.locks[resource_id] = [
                l for l in self.locks[resource_id] 
                if l.lock_id != lock.lock_id
            ]
            
            if not self.locks[resource_id]:
                del self.locks[resource_id]
    
    def _process_waiting_locks(self):
        """处理等待中的锁"""
        for resource_id in list(self.waiting_queue.keys()):
            if resource_id not in self.waiting_queue:
                continue
                
            waiting_locks = self.waiting_queue[resource_id].copy()
            
            for lock, future in waiting_locks:
                if self._can_acquire_lock(lock):
                    self._grant_lock(lock)
                    future.set_result(True)
                    
                    # 从等待队列移除
                    self.waiting_queue[resource_id].remove((lock, future))
                    
                    # 更新死锁检测图
                    self.deadlock_detector.remove_transaction(lock.transaction_id)
            
            if not self.waiting_queue[resource_id]:
                del self.waiting_queue[resource_id]
    
    def detect_and_resolve_deadlock(self) -> bool:
        """检测并解决死锁"""
        with self.lock:
            cycle = self.deadlock_detector.detect_deadlock()
            
            if cycle:
                self.logger.warning(f"Deadlock detected: {cycle}")
                
                # 选择牺牲者（选择最年轻的事务）
                victim = min(cycle, key=lambda tx_id: self._get_transaction_start_time(tx_id))
                
                # 中止牺牲者事务
                self.release_locks(victim)
                
                self.logger.info(f"Deadlock resolved by aborting transaction {victim}")
                return True
            
            return False
    
    def _get_transaction_start_time(self, transaction_id: str) -> float:
        """获取事务开始时间（用于死锁解决）"""
        # 简化实现，实际应该从事务管理器获取
        return time.time()
    
    def get_lock_statistics(self) -> Dict[str, Any]:
        """获取锁统计信息"""
        with self.lock:
            return {
                'total_locks': sum(len(locks) for locks in self.locks.values()),
                'locked_resources': len(self.locks),
                'waiting_requests': sum(len(waiting) for waiting in self.waiting_queue.values()),
                'transactions_with_locks': len(self.transaction_locks)
            }

class TwoPhaseCommitCoordinator:
    """两阶段提交协调器"""
    
    def __init__(self, node_id: str, timeout: float = 30.0):
        self.node_id = node_id
        self.timeout = timeout
        self.active_transactions: Dict[str, DistributedTransaction] = {}
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
    
    def begin_transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED,
                         timeout: float = 300.0) -> str:
        """开始分布式事务"""
        transaction_id = f"tx_{self.node_id}_{uuid.uuid4().hex[:8]}"
        
        transaction = DistributedTransaction(
            transaction_id=transaction_id,
            coordinator_id=self.node_id,
            isolation_level=isolation_level,
            timeout=timeout
        )
        
        with self.lock:
            self.active_transactions[transaction_id] = transaction
        
        self.logger.info(f"Started distributed transaction {transaction_id}")
        return transaction_id
    
    def add_operation(self, transaction_id: str, operation: TransactionOperation):
        """添加事务操作"""
        with self.lock:
            if transaction_id not in self.active_transactions:
                raise ValueError(f"Transaction {transaction_id} not found")
            
            transaction = self.active_transactions[transaction_id]
            if transaction.state != TransactionState.ACTIVE:
                raise ValueError(f"Transaction {transaction_id} is not active")
            
            transaction.add_operation(operation)
    
    def prepare_transaction(self, transaction_id: str, 
                          participant_prepare_func) -> Dict[str, VoteResult]:
        """准备阶段"""
        with self.lock:
            if transaction_id not in self.active_transactions:
                raise ValueError(f"Transaction {transaction_id} not found")
            
            transaction = self.active_transactions[transaction_id]
            transaction.state = TransactionState.PREPARING
        
        self.logger.info(f"Preparing transaction {transaction_id}")
        
        # 向所有参与者发送准备请求
        votes = {}
        futures = {}
        
        with ThreadPoolExecutor(max_workers=len(transaction.participants)) as executor:
            for participant in transaction.participants:
                future = executor.submit(
                    self._send_prepare_request, 
                    participant, 
                    transaction_id, 
                    participant_prepare_func
                )
                futures[participant] = future
            
            # 收集投票结果
            for participant, future in futures.items():
                try:
                    vote = future.result(timeout=self.timeout)
                    votes[participant] = vote
                except Exception as e:
                    self.logger.error(f"Prepare request to {participant} failed: {e}")
                    votes[participant] = VoteResult.TIMEOUT
        
        # 更新事务状态
        with self.lock:
            if all(vote == VoteResult.YES for vote in votes.values()):
                transaction.state = TransactionState.PREPARED
            else:
                transaction.state = TransactionState.ABORTING
        
        return votes
    
    def commit_transaction(self, transaction_id: str, 
                          participant_commit_func) -> bool:
        """提交事务"""
        with self.lock:
            if transaction_id not in self.active_transactions:
                raise ValueError(f"Transaction {transaction_id} not found")
            
            transaction = self.active_transactions[transaction_id]
            if transaction.state != TransactionState.PREPARED:
                raise ValueError(f"Transaction {transaction_id} is not prepared")
            
            transaction.state = TransactionState.COMMITTING
        
        self.logger.info(f"Committing transaction {transaction_id}")
        
        # 向所有参与者发送提交请求
        success = True
        futures = {}
        
        with ThreadPoolExecutor(max_workers=len(transaction.participants)) as executor:
            for participant in transaction.participants:
                future = executor.submit(
                    self._send_commit_request,
                    participant,
                    transaction_id,
                    participant_commit_func
                )
                futures[participant] = future
            
            # 等待所有参与者完成提交
            for participant, future in futures.items():
                try:
                    result = future.result(timeout=self.timeout)
                    if not result:
                        success = False
                        self.logger.error(f"Commit failed on participant {participant}")
                except Exception as e:
                    success = False
                    self.logger.error(f"Commit request to {participant} failed: {e}")
        
        # 更新事务状态
        with self.lock:
            if success:
                transaction.state = TransactionState.COMMITTED
            else:
                transaction.state = TransactionState.ABORTED
            
            # 清理事务
            del self.active_transactions[transaction_id]
        
        return success
    
    def abort_transaction(self, transaction_id: str, 
                         participant_abort_func) -> bool:
        """中止事务"""
        with self.lock:
            if transaction_id not in self.active_transactions:
                return True  # 事务不存在，认为已中止
            
            transaction = self.active_transactions[transaction_id]
            transaction.state = TransactionState.ABORTING
        
        self.logger.info(f"Aborting transaction {transaction_id}")
        
        # 向所有参与者发送中止请求
        futures = {}
        
        with ThreadPoolExecutor(max_workers=len(transaction.participants)) as executor:
            for participant in transaction.participants:
                future = executor.submit(
                    self._send_abort_request,
                    participant,
                    transaction_id,
                    participant_abort_func
                )
                futures[participant] = future
            
            # 等待所有参与者完成中止
            for participant, future in futures.items():
                try:
                    future.result(timeout=self.timeout)
                except Exception as e:
                    self.logger.error(f"Abort request to {participant} failed: {e}")
        
        # 更新事务状态
        with self.lock:
            transaction.state = TransactionState.ABORTED
            del self.active_transactions[transaction_id]
        
        return True
    
    def _send_prepare_request(self, participant: str, transaction_id: str,
                             participant_prepare_func) -> VoteResult:
        """发送准备请求"""
        try:
            return participant_prepare_func(participant, transaction_id)
        except Exception as e:
            self.logger.error(f"Prepare request failed: {e}")
            return VoteResult.NO
    
    def _send_commit_request(self, participant: str, transaction_id: str,
                            participant_commit_func) -> bool:
        """发送提交请求"""
        try:
            return participant_commit_func(participant, transaction_id)
        except Exception as e:
            self.logger.error(f"Commit request failed: {e}")
            return False
    
    def _send_abort_request(self, participant: str, transaction_id: str,
                           participant_abort_func) -> bool:
        """发送中止请求"""
        try:
            return participant_abort_func(participant, transaction_id)
        except Exception as e:
            self.logger.error(f"Abort request failed: {e}")
            return False
    
    def get_transaction_status(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """获取事务状态"""
        with self.lock:
            if transaction_id not in self.active_transactions:
                return None
            
            transaction = self.active_transactions[transaction_id]
            return {
                'transaction_id': transaction.transaction_id,
                'state': transaction.state.value,
                'coordinator_id': transaction.coordinator_id,
                'participants': list(transaction.participants),
                'operation_count': len(transaction.operations),
                'isolation_level': transaction.isolation_level.value,
                'start_time': transaction.start_time,
                'timeout': transaction.timeout,
                'is_expired': transaction.is_expired
            }
    
    def cleanup_expired_transactions(self, participant_abort_func):
        """清理超时事务"""
        with self.lock:
            expired_transactions = [
                tx_id for tx_id, tx in self.active_transactions.items()
                if tx.is_expired
            ]
        
        for tx_id in expired_transactions:
            self.logger.warning(f"Transaction {tx_id} expired, aborting")
            self.abort_transaction(tx_id, participant_abort_func)

class DistributedTransactionManager:
    """分布式事务管理器"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.coordinator = TwoPhaseCommitCoordinator(node_id)
        self.lock_manager = LockManager()
        
        # 作为参与者的事务
        self.participant_transactions: Dict[str, DistributedTransaction] = {}
        
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        
        # 启动清理线程
        self.cleanup_thread = threading.Thread(target=self._cleanup_worker, daemon=True)
        self.running = True
        self.cleanup_thread.start()
    
    def begin_transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED,
                         timeout: float = 300.0) -> str:
        """开始分布式事务"""
        return self.coordinator.begin_transaction(isolation_level, timeout)
    
    def execute_operation(self, transaction_id: str, operation: TransactionOperation) -> bool:
        """执行事务操作"""
        # 添加操作到事务
        self.coordinator.add_operation(transaction_id, operation)
        
        # 获取锁
        if operation.operation_type in ['UPDATE', 'DELETE']:
            lock_type = "EXCLUSIVE"
        else:
            lock_type = "SHARED"
        
        resource_id = f"{operation.table_name}:{operation.parameters.get('id', 'all')}"
        
        success = self.lock_manager.acquire_lock(
            transaction_id, 
            resource_id, 
            lock_type, 
            operation.node_id or self.node_id
        )
        
        if not success:
            self.logger.error(f"Failed to acquire lock for operation {operation.operation_id}")
            return False
        
        # 检测死锁
        if self.lock_manager.detect_and_resolve_deadlock():
            return False
        
        return True
    
    def commit_transaction(self, transaction_id: str) -> bool:
        """提交事务"""
        try:
            # 准备阶段
            votes = self.coordinator.prepare_transaction(
                transaction_id, 
                self._participant_prepare
            )
            
            # 检查投票结果
            if all(vote == VoteResult.YES for vote in votes.values()):
                # 提交阶段
                success = self.coordinator.commit_transaction(
                    transaction_id,
                    self._participant_commit
                )
            else:
                # 中止事务
                success = False
                self.coordinator.abort_transaction(
                    transaction_id,
                    self._participant_abort
                )
            
            # 释放锁
            self.lock_manager.release_locks(transaction_id)
            
            return success
            
        except Exception as e:
            self.logger.error(f"Transaction commit failed: {e}")
            self.abort_transaction(transaction_id)
            return False
    
    def abort_transaction(self, transaction_id: str) -> bool:
        """中止事务"""
        try:
            success = self.coordinator.abort_transaction(
                transaction_id,
                self._participant_abort
            )
            
            # 释放锁
            self.lock_manager.release_locks(transaction_id)
            
            return success
            
        except Exception as e:
            self.logger.error(f"Transaction abort failed: {e}")
            return False
    
    def _participant_prepare(self, participant_id: str, transaction_id: str) -> VoteResult:
        """参与者准备阶段"""
        try:
            # 模拟参与者准备逻辑
            # 实际实现中应该检查资源状态、锁等
            
            with self.lock:
                if transaction_id in self.participant_transactions:
                    transaction = self.participant_transactions[transaction_id]
                    if transaction.state == TransactionState.ACTIVE:
                        transaction.state = TransactionState.PREPARED
                        return VoteResult.YES
            
            return VoteResult.NO
            
        except Exception as e:
            self.logger.error(f"Participant prepare failed: {e}")
            return VoteResult.NO
    
    def _participant_commit(self, participant_id: str, transaction_id: str) -> bool:
        """参与者提交阶段"""
        try:
            with self.lock:
                if transaction_id in self.participant_transactions:
                    transaction = self.participant_transactions[transaction_id]
                    transaction.state = TransactionState.COMMITTED
                    
                    # 执行实际的数据修改
                    # 这里应该调用存储引擎的提交操作
                    
                    del self.participant_transactions[transaction_id]
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Participant commit failed: {e}")
            return False
    
    def _participant_abort(self, participant_id: str, transaction_id: str) -> bool:
        """参与者中止阶段"""
        try:
            with self.lock:
                if transaction_id in self.participant_transactions:
                    transaction = self.participant_transactions[transaction_id]
                    transaction.state = TransactionState.ABORTED
                    
                    # 回滚数据修改
                    # 这里应该调用存储引擎的回滚操作
                    
                    del self.participant_transactions[transaction_id]
            
            return True
            
        except Exception as e:
            self.logger.error(f"Participant abort failed: {e}")
            return False
    
    def _cleanup_worker(self):
        """清理工作线程"""
        while self.running:
            try:
                # 清理超时事务
                self.coordinator.cleanup_expired_transactions(self._participant_abort)
                
                # 清理超时的参与者事务
                with self.lock:
                    expired_participants = [
                        tx_id for tx_id, tx in self.participant_transactions.items()
                        if tx.is_expired
                    ]
                
                for tx_id in expired_participants:
                    self._participant_abort(self.node_id, tx_id)
                
                time.sleep(30)  # 每30秒清理一次
                
            except Exception as e:
                self.logger.error(f"Cleanup worker error: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取事务统计信息"""
        with self.lock:
            coordinator_stats = {
                'active_transactions': len(self.coordinator.active_transactions),
                'node_id': self.node_id
            }
            
            participant_stats = {
                'participant_transactions': len(self.participant_transactions)
            }
            
            lock_stats = self.lock_manager.get_lock_statistics()
            
            return {
                'coordinator': coordinator_stats,
                'participant': participant_stats,
                'locks': lock_stats
            }
    
    def shutdown(self):
        """关闭事务管理器"""
        self.running = False
        if self.cleanup_thread.is_alive():
            self.cleanup_thread.join(timeout=5.0)
