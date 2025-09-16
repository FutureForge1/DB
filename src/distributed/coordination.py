"""
分布式系统协调模块

集成分布式协调服务（模拟ZooKeeper功能）、集群成员管理和配置信息同步
"""

import threading
import time
import json
import uuid
from typing import Dict, List, Any, Optional, Set, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
import logging
from concurrent.futures import ThreadPoolExecutor
import queue

class NodeRole(Enum):
    """节点角色"""
    LEADER = "leader"
    FOLLOWER = "follower"
    CANDIDATE = "candidate"

class ConfigChangeType(Enum):
    """配置变更类型"""
    SET = "set"
    DELETE = "delete"
    UPDATE = "update"

class ElectionState(Enum):
    """选举状态"""
    IDLE = "idle"
    VOTING = "voting"
    ELECTED = "elected"

@dataclass
class ClusterMember:
    """集群成员"""
    node_id: str
    endpoint: str
    role: NodeRole = NodeRole.FOLLOWER
    term: int = 0
    last_heartbeat: float = field(default_factory=time.time)
    vote_count: int = 0
    is_active: bool = True
    join_time: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_alive(self) -> bool:
        """检查成员是否存活"""
        return self.is_active and (time.time() - self.last_heartbeat) < 30.0

@dataclass
class ConfigEntry:
    """配置条目"""
    key: str
    value: Any
    version: int
    timestamp: float
    change_type: ConfigChangeType
    node_id: str  # 发起变更的节点
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class LeaderElection:
    """领导者选举"""
    term: int
    candidate_id: str
    votes: Dict[str, bool] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)
    state: ElectionState = ElectionState.VOTING
    
    @property
    def is_expired(self) -> bool:
        return time.time() - self.start_time > 30.0  # 30秒选举超时

class DistributedLock:
    """分布式锁"""
    
    def __init__(self, lock_id: str, owner_id: str, timeout: float = 300.0):
        self.lock_id = lock_id
        self.owner_id = owner_id
        self.acquired_time = time.time()
        self.timeout = timeout
        self.is_acquired = True
    
    @property
    def is_expired(self) -> bool:
        """检查锁是否过期"""
        return time.time() - self.acquired_time > self.timeout
    
    def extend_timeout(self, additional_time: float = 60.0):
        """延长锁超时时间"""
        self.timeout += additional_time

class ClusterCoordinator:
    """集群协调器（模拟ZooKeeper）"""
    
    def __init__(self, node_id: str, initial_members: List[str] = None):
        self.node_id = node_id
        self.current_term = 0
        self.voted_for: Optional[str] = None
        
        # 集群成员管理
        self.members: Dict[str, ClusterMember] = {}
        self.leader_id: Optional[str] = None
        self.my_role = NodeRole.FOLLOWER
        
        # 配置管理
        self.config_store: Dict[str, ConfigEntry] = {}
        self.config_version = 0
        
        # 分布式锁
        self.locks: Dict[str, DistributedLock] = {}
        
        # 选举管理
        self.current_election: Optional[LeaderElection] = None
        self.election_timeout = 15.0 + random.uniform(0, 5.0)  # 随机化避免同时选举
        self.last_heartbeat_received = time.time()
        
        # 事件回调
        self.leader_change_callbacks: List[Callable] = []
        self.config_change_callbacks: List[Callable] = []
        self.member_change_callbacks: List[Callable] = []
        
        # 线程控制
        self.running = False
        self.coordinator_thread = None
        self.heartbeat_thread = None
        
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        
        # 初始化成员
        if initial_members:
            for member_id in initial_members:
                if member_id != node_id:
                    self.members[member_id] = ClusterMember(
                        node_id=member_id,
                        endpoint=f"node_{member_id}",
                        is_active=False
                    )
        
        # 添加自己
        self.members[node_id] = ClusterMember(
            node_id=node_id,
            endpoint=f"node_{node_id}",
            is_active=True
        )
    
    def start(self):
        """启动协调器"""
        with self.lock:
            if self.running:
                return
            
            self.running = True
            self.coordinator_thread = threading.Thread(target=self._coordination_worker, daemon=True)
            self.heartbeat_thread = threading.Thread(target=self._heartbeat_worker, daemon=True)
            
            self.coordinator_thread.start()
            self.heartbeat_thread.start()
            
            self.logger.info(f"Cluster coordinator started for node {self.node_id}")
    
    def stop(self):
        """停止协调器"""
        with self.lock:
            if not self.running:
                return
            
            self.running = False
            
            if self.coordinator_thread:
                self.coordinator_thread.join(timeout=5.0)
            if self.heartbeat_thread:
                self.heartbeat_thread.join(timeout=5.0)
            
            self.logger.info(f"Cluster coordinator stopped for node {self.node_id}")
    
    def join_cluster(self, member_id: str, endpoint: str) -> bool:
        """加入集群"""
        with self.lock:
            if member_id in self.members:
                # 更新现有成员
                member = self.members[member_id]
                member.endpoint = endpoint
                member.is_active = True
                member.last_heartbeat = time.time()
            else:
                # 添加新成员
                member = ClusterMember(
                    node_id=member_id,
                    endpoint=endpoint,
                    is_active=True
                )
                self.members[member_id] = member
            
            self._trigger_member_change_callbacks("join", member_id)
            self.logger.info(f"Node {member_id} joined cluster")
            return True
    
    def leave_cluster(self, member_id: str) -> bool:
        """离开集群"""
        with self.lock:
            if member_id not in self.members:
                return False
            
            member = self.members[member_id]
            member.is_active = False
            
            # 如果离开的是领导者，触发新的选举
            if self.leader_id == member_id:
                self.leader_id = None
                self.my_role = NodeRole.FOLLOWER
                self._start_election()
            
            self._trigger_member_change_callbacks("leave", member_id)
            self.logger.info(f"Node {member_id} left cluster")
            return True
    
    def update_heartbeat(self, member_id: str, term: int = None):
        """更新心跳"""
        with self.lock:
            if member_id not in self.members:
                return
            
            member = self.members[member_id]
            member.last_heartbeat = time.time()
            
            if term is not None:
                member.term = term
            
            # 如果收到来自领导者的心跳
            if member_id == self.leader_id:
                self.last_heartbeat_received = time.time()
    
    def _coordination_worker(self):
        """协调工作线程"""
        while self.running:
            try:
                self._check_leader_status()
                self._check_election_timeout()
                self._cleanup_expired_locks()
                time.sleep(1.0)
            except Exception as e:
                self.logger.error(f"Coordination worker error: {e}")
    
    def _heartbeat_worker(self):
        """心跳工作线程"""
        while self.running:
            try:
                if self.my_role == NodeRole.LEADER:
                    self._send_heartbeats()
                time.sleep(5.0)  # 每5秒发送心跳
            except Exception as e:
                self.logger.error(f"Heartbeat worker error: {e}")
    
    def _check_leader_status(self):
        """检查领导者状态"""
        with self.lock:
            current_time = time.time()
            
            # 检查当前领导者是否还活着
            if self.leader_id and self.leader_id in self.members:
                leader = self.members[self.leader_id]
                if not leader.is_alive:
                    self.logger.warning(f"Leader {self.leader_id} appears to be dead")
                    self.leader_id = None
                    self.my_role = NodeRole.FOLLOWER
                    self._start_election()
            
            # 如果没有领导者且不在选举中，开始选举
            if not self.leader_id and not self.current_election:
                self._start_election()
    
    def _check_election_timeout(self):
        """检查选举超时"""
        with self.lock:
            current_time = time.time()
            
            # 如果是跟随者且长时间没收到领导者心跳，开始选举
            if (self.my_role == NodeRole.FOLLOWER and 
                not self.current_election and 
                current_time - self.last_heartbeat_received > self.election_timeout):
                self.logger.info("Election timeout, starting new election")
                self._start_election()
            
            # 检查当前选举是否超时
            if self.current_election and self.current_election.is_expired:
                self.logger.warning("Election expired, starting new election")
                self._start_election()
    
    def _start_election(self):
        """开始选举"""
        with self.lock:
            self.current_term += 1
            self.my_role = NodeRole.CANDIDATE
            self.voted_for = self.node_id
            
            # 创建新的选举
            self.current_election = LeaderElection(
                term=self.current_term,
                candidate_id=self.node_id
            )
            
            # 给自己投票
            self.current_election.votes[self.node_id] = True
            
            self.logger.info(f"Starting election for term {self.current_term}")
            
            # 向其他节点请求投票
            self._request_votes()
    
    def _request_votes(self):
        """请求投票"""
        active_members = [m for m in self.members.values() if m.is_alive and m.node_id != self.node_id]
        
        for member in active_members:
            # 在实际实现中，这里应该发送网络请求
            # 这里模拟投票过程
            self._simulate_vote_request(member.node_id)
    
    def _simulate_vote_request(self, member_id: str):
        """模拟投票请求"""
        # 简化的投票逻辑：50%概率同意投票
        import random
        if random.random() < 0.7:  # 70%概率同意
            self.receive_vote(member_id, self.current_term, True)
        else:
            self.receive_vote(member_id, self.current_term, False)
    
    def receive_vote(self, voter_id: str, term: int, granted: bool):
        """接收投票"""
        with self.lock:
            if not self.current_election or self.current_election.term != term:
                return
            
            self.current_election.votes[voter_id] = granted
            
            if granted:
                vote_count = sum(1 for vote in self.current_election.votes.values() if vote)
                total_members = len([m for m in self.members.values() if m.is_alive])
                majority = total_members // 2 + 1
                
                if vote_count >= majority:
                    self._become_leader()
    
    def _become_leader(self):
        """成为领导者"""
        with self.lock:
            self.my_role = NodeRole.LEADER
            self.leader_id = self.node_id
            self.current_election = None
            
            # 更新自己的角色
            if self.node_id in self.members:
                self.members[self.node_id].role = NodeRole.LEADER
                self.members[self.node_id].term = self.current_term
            
            self.logger.info(f"Became leader for term {self.current_term}")
            self._trigger_leader_change_callbacks(self.node_id)
            
            # 立即发送心跳确立领导地位
            self._send_heartbeats()
    
    def _send_heartbeats(self):
        """发送心跳"""
        with self.lock:
            for member in self.members.values():
                if member.node_id != self.node_id and member.is_alive:
                    # 在实际实现中，这里应该发送网络心跳
                    # 这里模拟心跳接收
                    member.last_heartbeat = time.time()
                    member.role = NodeRole.FOLLOWER
                    member.term = self.current_term
    
    def set_config(self, key: str, value: Any, node_id: str = None) -> bool:
        """设置配置"""
        with self.lock:
            if self.my_role != NodeRole.LEADER:
                return False  # 只有领导者可以修改配置
            
            self.config_version += 1
            entry = ConfigEntry(
                key=key,
                value=value,
                version=self.config_version,
                timestamp=time.time(),
                change_type=ConfigChangeType.SET,
                node_id=node_id or self.node_id
            )
            
            self.config_store[key] = entry
            self._trigger_config_change_callbacks("set", key, value)
            
            self.logger.info(f"Config set: {key} = {value}")
            return True
    
    def get_config(self, key: str) -> Optional[Any]:
        """获取配置"""
        with self.lock:
            if key in self.config_store:
                return self.config_store[key].value
            return None
    
    def delete_config(self, key: str, node_id: str = None) -> bool:
        """删除配置"""
        with self.lock:
            if self.my_role != NodeRole.LEADER:
                return False
            
            if key not in self.config_store:
                return False
            
            self.config_version += 1
            entry = ConfigEntry(
                key=key,
                value=None,
                version=self.config_version,
                timestamp=time.time(),
                change_type=ConfigChangeType.DELETE,
                node_id=node_id or self.node_id
            )
            
            del self.config_store[key]
            self._trigger_config_change_callbacks("delete", key, None)
            
            self.logger.info(f"Config deleted: {key}")
            return True
    
    def acquire_lock(self, lock_id: str, timeout: float = 300.0) -> bool:
        """获取分布式锁"""
        with self.lock:
            if self.my_role != NodeRole.LEADER:
                return False  # 只有领导者可以管理锁
            
            if lock_id in self.locks and not self.locks[lock_id].is_expired:
                return False  # 锁已被占用
            
            # 创建新锁
            lock = DistributedLock(lock_id, self.node_id, timeout)
            self.locks[lock_id] = lock
            
            self.logger.info(f"Lock acquired: {lock_id} by {self.node_id}")
            return True
    
    def release_lock(self, lock_id: str) -> bool:
        """释放分布式锁"""
        with self.lock:
            if lock_id not in self.locks:
                return False
            
            lock = self.locks[lock_id]
            if lock.owner_id != self.node_id and self.my_role != NodeRole.LEADER:
                return False  # 只有锁的拥有者或领导者可以释放锁
            
            del self.locks[lock_id]
            self.logger.info(f"Lock released: {lock_id}")
            return True
    
    def extend_lock(self, lock_id: str, additional_time: float = 60.0) -> bool:
        """延长锁时间"""
        with self.lock:
            if lock_id not in self.locks:
                return False
            
            lock = self.locks[lock_id]
            if lock.owner_id != self.node_id:
                return False
            
            lock.extend_timeout(additional_time)
            return True
    
    def _cleanup_expired_locks(self):
        """清理过期锁"""
        with self.lock:
            expired_locks = [
                lock_id for lock_id, lock in self.locks.items()
                if lock.is_expired
            ]
            
            for lock_id in expired_locks:
                del self.locks[lock_id]
                self.logger.info(f"Expired lock cleaned up: {lock_id}")
    
    def _trigger_leader_change_callbacks(self, new_leader_id: str):
        """触发领导者变更回调"""
        for callback in self.leader_change_callbacks:
            try:
                callback(new_leader_id)
            except Exception as e:
                self.logger.error(f"Leader change callback error: {e}")
    
    def _trigger_config_change_callbacks(self, change_type: str, key: str, value: Any):
        """触发配置变更回调"""
        for callback in self.config_change_callbacks:
            try:
                callback(change_type, key, value)
            except Exception as e:
                self.logger.error(f"Config change callback error: {e}")
    
    def _trigger_member_change_callbacks(self, change_type: str, member_id: str):
        """触发成员变更回调"""
        for callback in self.member_change_callbacks:
            try:
                callback(change_type, member_id)
            except Exception as e:
                self.logger.error(f"Member change callback error: {e}")
    
    def add_leader_change_callback(self, callback: Callable):
        """添加领导者变更回调"""
        self.leader_change_callbacks.append(callback)
    
    def add_config_change_callback(self, callback: Callable):
        """添加配置变更回调"""
        self.config_change_callbacks.append(callback)
    
    def add_member_change_callback(self, callback: Callable):
        """添加成员变更回调"""
        self.member_change_callbacks.append(callback)
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """获取集群状态"""
        with self.lock:
            active_members = [m for m in self.members.values() if m.is_alive]
            
            return {
                'node_id': self.node_id,
                'role': self.my_role.value,
                'term': self.current_term,
                'leader_id': self.leader_id,
                'total_members': len(self.members),
                'active_members': len(active_members),
                'config_version': self.config_version,
                'active_locks': len(self.locks),
                'election_in_progress': self.current_election is not None
            }
    
    def get_member_list(self) -> List[Dict[str, Any]]:
        """获取成员列表"""
        with self.lock:
            return [
                {
                    'node_id': member.node_id,
                    'endpoint': member.endpoint,
                    'role': member.role.value,
                    'term': member.term,
                    'is_active': member.is_active,
                    'is_alive': member.is_alive,
                    'join_time': member.join_time,
                    'last_heartbeat': member.last_heartbeat
                }
                for member in self.members.values()
            ]
    
    def get_config_snapshot(self) -> Dict[str, Any]:
        """获取配置快照"""
        with self.lock:
            return {
                'version': self.config_version,
                'configs': {
                    key: {
                        'value': entry.value,
                        'version': entry.version,
                        'timestamp': entry.timestamp,
                        'change_type': entry.change_type.value,
                        'node_id': entry.node_id
                    }
                    for key, entry in self.config_store.items()
                }
            }
    
    def get_lock_status(self) -> List[Dict[str, Any]]:
        """获取锁状态"""
        with self.lock:
            return [
                {
                    'lock_id': lock.lock_id,
                    'owner_id': lock.owner_id,
                    'acquired_time': lock.acquired_time,
                    'timeout': lock.timeout,
                    'is_expired': lock.is_expired,
                    'remaining_time': max(0, lock.timeout - (time.time() - lock.acquired_time))
                }
                for lock in self.locks.values()
            ]

# 为了避免循环导入，在这里添加random导入
import random
