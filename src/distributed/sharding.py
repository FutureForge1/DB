"""
数据分片管理模块

实现数据分片策略、分片元数据管理和跨分片数据定位
"""

import hashlib
import json
import threading
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum
from dataclasses import dataclass, asdict
import time

class ShardingType(Enum):
    """分片类型枚举"""
    RANGE = "range"      # 范围分片
    HASH = "hash"        # 哈希分片
    DIRECTORY = "directory"  # 目录分片

@dataclass
class ShardInfo:
    """分片信息"""
    shard_id: str
    node_id: str
    shard_type: ShardingType
    key_range: Optional[Tuple[Any, Any]] = None  # 范围分片的键范围
    hash_range: Optional[Tuple[int, int]] = None  # 哈希分片的哈希范围
    status: str = "active"  # active, inactive, migrating
    created_time: float = None
    last_updated: float = None
    
    def __post_init__(self):
        if self.created_time is None:
            self.created_time = time.time()
        if self.last_updated is None:
            self.last_updated = time.time()

@dataclass
class ShardMetadata:
    """分片元数据"""
    table_name: str
    shard_key: str  # 分片键
    shard_type: ShardingType
    shards: List[ShardInfo]
    total_shards: int
    created_time: float = None
    
    def __post_init__(self):
        if self.created_time is None:
            self.created_time = time.time()

class ShardingStrategy(ABC):
    """分片策略抽象基类"""
    
    @abstractmethod
    def determine_shard(self, shard_key_value: Any, metadata: ShardMetadata) -> str:
        """确定数据应该存储在哪个分片"""
        pass
    
    @abstractmethod
    def get_query_shards(self, conditions: Dict[str, Any], metadata: ShardMetadata) -> List[str]:
        """根据查询条件确定需要查询的分片"""
        pass

class RangeShardingStrategy(ShardingStrategy):
    """范围分片策略"""
    
    def determine_shard(self, shard_key_value: Any, metadata: ShardMetadata) -> str:
        """根据范围确定分片"""
        for shard in metadata.shards:
            if shard.key_range and shard.status == "active":
                min_val, max_val = shard.key_range
                if min_val <= shard_key_value < max_val:
                    return shard.shard_id
        
        # 如果没有找到合适的分片，返回第一个活跃分片
        for shard in metadata.shards:
            if shard.status == "active":
                return shard.shard_id
        
        raise ValueError(f"No active shard found for key value: {shard_key_value}")
    
    def get_query_shards(self, conditions: Dict[str, Any], metadata: ShardMetadata) -> List[str]:
        """根据范围条件确定查询分片"""
        shard_key = metadata.shard_key
        
        if shard_key not in conditions:
            # 如果查询条件中没有分片键，需要查询所有分片
            return [shard.shard_id for shard in metadata.shards if shard.status == "active"]
        
        condition = conditions[shard_key]
        target_shards = []
        
        for shard in metadata.shards:
            if shard.status != "active" or not shard.key_range:
                continue
                
            min_val, max_val = shard.key_range
            
            # 根据不同的条件类型判断
            if isinstance(condition, dict):
                # 处理范围查询 {'>': 100, '<': 200}
                if self._range_overlaps(condition, min_val, max_val):
                    target_shards.append(shard.shard_id)
            else:
                # 处理等值查询
                if min_val <= condition < max_val:
                    target_shards.append(shard.shard_id)
        
        return target_shards if target_shards else [shard.shard_id for shard in metadata.shards if shard.status == "active"]
    
    def _range_overlaps(self, condition: Dict[str, Any], min_val: Any, max_val: Any) -> bool:
        """检查范围条件是否与分片范围重叠"""
        # 简化的范围重叠检查
        for op, value in condition.items():
            if op == '>' or op == '>=':
                if value >= max_val:
                    return False
            elif op == '<' or op == '<=':
                if value <= min_val:
                    return False
        return True

class HashShardingStrategy(ShardingStrategy):
    """哈希分片策略"""
    
    def __init__(self, hash_function: str = "md5"):
        self.hash_function = hash_function
    
    def determine_shard(self, shard_key_value: Any, metadata: ShardMetadata) -> str:
        """根据哈希值确定分片"""
        hash_value = self._hash_key(shard_key_value)
        
        for shard in metadata.shards:
            if shard.hash_range and shard.status == "active":
                min_hash, max_hash = shard.hash_range
                if min_hash <= hash_value < max_hash:
                    return shard.shard_id
        
        # 如果没有找到合适的分片，使用模运算
        active_shards = [s for s in metadata.shards if s.status == "active"]
        if active_shards:
            shard_index = hash_value % len(active_shards)
            return active_shards[shard_index].shard_id
        
        raise ValueError(f"No active shard found for key value: {shard_key_value}")
    
    def get_query_shards(self, conditions: Dict[str, Any], metadata: ShardMetadata) -> List[str]:
        """哈希分片的查询分片确定"""
        shard_key = metadata.shard_key
        
        if shard_key not in conditions:
            # 没有分片键条件，需要查询所有分片
            return [shard.shard_id for shard in metadata.shards if shard.status == "active"]
        
        condition = conditions[shard_key]
        
        if isinstance(condition, dict):
            # 范围查询在哈希分片中需要查询所有分片
            return [shard.shard_id for shard in metadata.shards if shard.status == "active"]
        else:
            # 等值查询可以精确定位到一个分片
            try:
                target_shard = self.determine_shard(condition, metadata)
                return [target_shard]
            except ValueError:
                return [shard.shard_id for shard in metadata.shards if shard.status == "active"]
    
    def _hash_key(self, key_value: Any) -> int:
        """计算键值的哈希值"""
        key_str = str(key_value)
        if self.hash_function == "md5":
            return int(hashlib.md5(key_str.encode()).hexdigest(), 16) % (2**32)
        else:
            return hash(key_str) % (2**32)

class DirectoryShardingStrategy(ShardingStrategy):
    """目录分片策略"""
    
    def __init__(self):
        self.directory: Dict[Any, str] = {}
    
    def determine_shard(self, shard_key_value: Any, metadata: ShardMetadata) -> str:
        """根据目录映射确定分片"""
        if shard_key_value in self.directory:
            return self.directory[shard_key_value]
        
        # 如果目录中没有映射，选择负载最轻的分片
        active_shards = [s for s in metadata.shards if s.status == "active"]
        if active_shards:
            # 简单地选择第一个活跃分片
            selected_shard = active_shards[0].shard_id
            self.directory[shard_key_value] = selected_shard
            return selected_shard
        
        raise ValueError(f"No active shard found for key value: {shard_key_value}")
    
    def get_query_shards(self, conditions: Dict[str, Any], metadata: ShardMetadata) -> List[str]:
        """根据目录映射确定查询分片"""
        shard_key = metadata.shard_key
        
        if shard_key not in conditions:
            return [shard.shard_id for shard in metadata.shards if shard.status == "active"]
        
        condition = conditions[shard_key]
        
        if isinstance(condition, dict):
            # 范围查询需要查询所有可能的分片
            return [shard.shard_id for shard in metadata.shards if shard.status == "active"]
        else:
            # 等值查询
            if condition in self.directory:
                return [self.directory[condition]]
            else:
                return [shard.shard_id for shard in metadata.shards if shard.status == "active"]

class ShardManager:
    """分片管理器"""
    
    def __init__(self):
        self.metadata_store: Dict[str, ShardMetadata] = {}
        self.strategies: Dict[ShardingType, ShardingStrategy] = {
            ShardingType.RANGE: RangeShardingStrategy(),
            ShardingType.HASH: HashShardingStrategy(),
            ShardingType.DIRECTORY: DirectoryShardingStrategy()
        }
        self.lock = threading.RLock()
    
    def create_sharded_table(self, table_name: str, shard_key: str, 
                           shard_type: ShardingType, shard_count: int,
                           nodes: List[str]) -> ShardMetadata:
        """创建分片表"""
        with self.lock:
            if table_name in self.metadata_store:
                raise ValueError(f"Sharded table {table_name} already exists")
            
            shards = []
            
            if shard_type == ShardingType.HASH:
                # 哈希分片：平均分配哈希空间
                hash_space = 2**32
                hash_per_shard = hash_space // shard_count
                
                for i in range(shard_count):
                    shard_id = f"{table_name}_shard_{i}"
                    node_id = nodes[i % len(nodes)]
                    min_hash = i * hash_per_shard
                    max_hash = (i + 1) * hash_per_shard if i < shard_count - 1 else hash_space
                    
                    shard = ShardInfo(
                        shard_id=shard_id,
                        node_id=node_id,
                        shard_type=shard_type,
                        hash_range=(min_hash, max_hash)
                    )
                    shards.append(shard)
            
            elif shard_type == ShardingType.RANGE:
                # 范围分片：需要后续指定范围
                for i in range(shard_count):
                    shard_id = f"{table_name}_shard_{i}"
                    node_id = nodes[i % len(nodes)]
                    
                    shard = ShardInfo(
                        shard_id=shard_id,
                        node_id=node_id,
                        shard_type=shard_type
                    )
                    shards.append(shard)
            
            else:  # DIRECTORY
                for i in range(shard_count):
                    shard_id = f"{table_name}_shard_{i}"
                    node_id = nodes[i % len(nodes)]
                    
                    shard = ShardInfo(
                        shard_id=shard_id,
                        node_id=node_id,
                        shard_type=shard_type
                    )
                    shards.append(shard)
            
            metadata = ShardMetadata(
                table_name=table_name,
                shard_key=shard_key,
                shard_type=shard_type,
                shards=shards,
                total_shards=shard_count
            )
            
            self.metadata_store[table_name] = metadata
            return metadata
    
    def get_shard_for_insert(self, table_name: str, data: Dict[str, Any]) -> str:
        """获取插入数据应该使用的分片"""
        with self.lock:
            if table_name not in self.metadata_store:
                raise ValueError(f"Table {table_name} is not sharded")
            
            metadata = self.metadata_store[table_name]
            shard_key_value = data.get(metadata.shard_key)
            
            if shard_key_value is None:
                raise ValueError(f"Shard key {metadata.shard_key} not found in data")
            
            strategy = self.strategies[metadata.shard_type]
            return strategy.determine_shard(shard_key_value, metadata)
    
    def get_shards_for_query(self, table_name: str, conditions: Dict[str, Any]) -> List[str]:
        """获取查询需要访问的分片列表"""
        with self.lock:
            if table_name not in self.metadata_store:
                # 非分片表，返回空列表表示查询本地
                return []
            
            metadata = self.metadata_store[table_name]
            strategy = self.strategies[metadata.shard_type]
            return strategy.get_query_shards(conditions, metadata)
    
    def get_all_shards(self, table_name: str) -> List[str]:
        """获取表的所有活跃分片"""
        with self.lock:
            if table_name not in self.metadata_store:
                return []
            
            metadata = self.metadata_store[table_name]
            return [shard.shard_id for shard in metadata.shards if shard.status == "active"]
    
    def get_shard_info(self, shard_id: str) -> Optional[ShardInfo]:
        """获取分片信息"""
        with self.lock:
            for metadata in self.metadata_store.values():
                for shard in metadata.shards:
                    if shard.shard_id == shard_id:
                        return shard
            return None
    
    def get_table_metadata(self, table_name: str) -> Optional[ShardMetadata]:
        """获取表的分片元数据"""
        with self.lock:
            return self.metadata_store.get(table_name)
    
    def update_shard_range(self, table_name: str, shard_id: str, 
                          key_range: Tuple[Any, Any]) -> bool:
        """更新范围分片的键范围"""
        with self.lock:
            if table_name not in self.metadata_store:
                return False
            
            metadata = self.metadata_store[table_name]
            for shard in metadata.shards:
                if shard.shard_id == shard_id:
                    shard.key_range = key_range
                    shard.last_updated = time.time()
                    return True
            return False
    
    def set_shard_status(self, shard_id: str, status: str) -> bool:
        """设置分片状态"""
        with self.lock:
            for metadata in self.metadata_store.values():
                for shard in metadata.shards:
                    if shard.shard_id == shard_id:
                        shard.status = status
                        shard.last_updated = time.time()
                        return True
            return False
    
    def add_shard(self, table_name: str, node_id: str) -> str:
        """添加新分片"""
        with self.lock:
            if table_name not in self.metadata_store:
                raise ValueError(f"Table {table_name} is not sharded")
            
            metadata = self.metadata_store[table_name]
            shard_id = f"{table_name}_shard_{len(metadata.shards)}"
            
            new_shard = ShardInfo(
                shard_id=shard_id,
                node_id=node_id,
                shard_type=metadata.shard_type
            )
            
            metadata.shards.append(new_shard)
            metadata.total_shards += 1
            
            return shard_id
    
    def remove_shard(self, table_name: str, shard_id: str) -> bool:
        """移除分片"""
        with self.lock:
            if table_name not in self.metadata_store:
                return False
            
            metadata = self.metadata_store[table_name]
            for i, shard in enumerate(metadata.shards):
                if shard.shard_id == shard_id:
                    metadata.shards.pop(i)
                    metadata.total_shards -= 1
                    return True
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取分片统计信息"""
        with self.lock:
            stats = {
                'total_sharded_tables': len(self.metadata_store),
                'tables': {}
            }
            
            for table_name, metadata in self.metadata_store.items():
                active_shards = sum(1 for shard in metadata.shards if shard.status == "active")
                stats['tables'][table_name] = {
                    'shard_type': metadata.shard_type.value,
                    'shard_key': metadata.shard_key,
                    'total_shards': metadata.total_shards,
                    'active_shards': active_shards,
                    'created_time': metadata.created_time
                }
            
            return stats
    
    def export_metadata(self) -> str:
        """导出分片元数据为JSON"""
        with self.lock:
            export_data = {}
            for table_name, metadata in self.metadata_store.items():
                export_data[table_name] = {
                    'table_name': metadata.table_name,
                    'shard_key': metadata.shard_key,
                    'shard_type': metadata.shard_type.value,
                    'total_shards': metadata.total_shards,
                    'created_time': metadata.created_time,
                    'shards': [asdict(shard) for shard in metadata.shards]
                }
            return json.dumps(export_data, indent=2, default=str)
    
    def import_metadata(self, metadata_json: str) -> bool:
        """从JSON导入分片元数据"""
        try:
            with self.lock:
                import_data = json.loads(metadata_json)
                
                for table_name, table_data in import_data.items():
                    shards = []
                    for shard_data in table_data['shards']:
                        shard_data['shard_type'] = ShardingType(shard_data['shard_type'])
                        shard = ShardInfo(**shard_data)
                        shards.append(shard)
                    
                    metadata = ShardMetadata(
                        table_name=table_data['table_name'],
                        shard_key=table_data['shard_key'],
                        shard_type=ShardingType(table_data['shard_type']),
                        shards=shards,
                        total_shards=table_data['total_shards'],
                        created_time=table_data['created_time']
                    )
                    
                    self.metadata_store[table_name] = metadata
                
                return True
        except Exception as e:
            print(f"Error importing metadata: {e}")
            return False
