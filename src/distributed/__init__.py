"""
分布式数据库扩展模块

提供数据分片、分布式查询、数据复制、分布式事务等功能
"""

from .sharding import ShardManager, ShardingStrategy
from .query_processor import DistributedQueryProcessor
from .replication import ReplicationManager
from .transaction import DistributedTransactionManager
from .coordination import ClusterCoordinator
from .monitoring import DistributedMonitor

__all__ = [
    'ShardManager',
    'ShardingStrategy', 
    'DistributedQueryProcessor',
    'ReplicationManager',
    'DistributedTransactionManager',
    'ClusterCoordinator',
    'DistributedMonitor'
]
