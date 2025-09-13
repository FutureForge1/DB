"""
B+树索引实现
提供高效的范围查询和等值查询支持
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import json
import math
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, field
from src.storage.page.page import Page, PageManager, PageType
from src.storage.buffer.buffer_manager import BufferManager

@dataclass
class BPTreeNode:
    """B+树节点"""
    is_leaf: bool = False
    keys: List[Any] = field(default_factory=list)
    children: List[int] = field(default_factory=list)  # 子节点页ID或记录指针
    next_leaf: Optional[int] = None  # 叶子节点链表指针
    page_id: int = -1  # 对应的页ID

def _compare_keys(key1: Any, key2: Any) -> int:
    """
    比较两个键值
    
    Args:
        key1: 第一个键
        key2: 第二个键
        
    Returns:
        -1 if key1 < key2, 0 if key1 == key2, 1 if key1 > key2
    """
    # 处理元组键和列表键（复合键）
    if (isinstance(key1, (tuple, list)) and isinstance(key2, (tuple, list))):
        # 转换为相同类型进行比较
        key1_tuple = tuple(key1) if isinstance(key1, list) else key1
        key2_tuple = tuple(key2) if isinstance(key2, list) else key2
        if key1_tuple < key2_tuple:
            return -1
        elif key1_tuple > key2_tuple:
            return 1
        else:
            return 0
    # 处理普通键
    elif key1 < key2:
        return -1
    elif key1 > key2:
        return 1
    else:
        return 0

class BPTreeIndex:
    """B+树索引实现"""
    
    def __init__(self, 
                 index_name: str,
                 table_name: str,
                 columns: List[str],
                 buffer_manager: BufferManager,
                 order: int = 4,
                 is_unique: bool = False):
        """
        初始化B+树索引
        
        Args:
            index_name: 索引名称
            table_name: 表名
            columns: 索引列
            buffer_manager: 缓存管理器
            order: B+树的阶数
            is_unique: 是否唯一索引
        """
        self.index_name = index_name
        self.table_name = table_name
        self.columns = columns
        self.buffer_manager = buffer_manager
        self.order = order
        self.is_unique = is_unique  # 唯一性约束
        self.root_page_id: Optional[int] = None
        self.leaf_head_page_id: Optional[int] = None  # 叶子节点链表头部
        
        # 创建根节点
        self._create_root()
    
    def _create_root(self) -> None:
        """创建根节点"""
        root_page = self.buffer_manager.create_page(PageType.INDEX_PAGE)
        if root_page:
            # 初始化根节点为叶子节点
            root_node = BPTreeNode(is_leaf=True, page_id=root_page.header.page_id)
            self._save_node(root_page, root_node)
            self.root_page_id = root_page.header.page_id
            self.leaf_head_page_id = root_page.header.page_id
            self.buffer_manager.unpin_page(root_page.header.page_id, is_dirty=True)
    
    def _load_node(self, page_id: int) -> Optional[BPTreeNode]:
        """从页加载节点"""
        page = self.buffer_manager.get_page(page_id)
        if not page:
            return None
        
        try:
            # 解析节点数据
            records = page.get_records()
            if not records:
                self.buffer_manager.unpin_page(page_id)
                return None
                
            node_data = json.loads(records[0]['node_data'])
            node = BPTreeNode(
                is_leaf=node_data['is_leaf'],
                keys=node_data['keys'],
                children=node_data['children'],
                next_leaf=node_data.get('next_leaf'),
                page_id=page_id
            )
            self.buffer_manager.unpin_page(page_id)
            return node
        except Exception as e:
            print(f"Error loading node {page_id}: {e}")
            self.buffer_manager.unpin_page(page_id)
            return None
    
    def _save_node(self, page: Page, node: BPTreeNode) -> bool:
        """将节点保存到页"""
        try:
            # 序列化节点数据
            node_data = {
                'is_leaf': node.is_leaf,
                'keys': node.keys,
                'children': node.children,
                'next_leaf': node.next_leaf
            }
            
            # 清空页面并添加记录
            page.data = bytearray(page.data.__class__(b'\x00' * len(page.data)))
            page.header.record_count = 0
            page.header.free_space = len(page.data)
            page.records.clear()
            
            page.add_record({'node_type': 'BPTREE_NODE', 'node_data': json.dumps(node_data, ensure_ascii=False)})
            return True
        except Exception as e:
            print(f"Error saving node {node.page_id}: {e}")
            return False
    
    def insert(self, key: Any, record_id: int) -> bool:
        """
        插入键值对到索引
        
        Args:
            key: 索引键
            record_id: 记录ID
            
        Returns:
            是否插入成功
        """
        # 检查唯一性约束（仅对唯一索引）
        if self.is_unique and self.search(key):
            print(f"Unique constraint violation: key {key} already exists")
            return False
        
        if self.root_page_id is None:
            self._create_root()
        
        root_node = self._load_node(self.root_page_id)
        if not root_node:
            return False
        
        # 插入键值
        new_root_page_id = self._insert_recursive(root_node, key, record_id)
        
        # 如果返回了新的根节点ID，创建新的根节点
        if new_root_page_id is not None:
            # 创建新的根节点
            new_root_page = self.buffer_manager.create_page(PageType.INDEX_PAGE)
            if new_root_page:
                # 加载新分裂的节点以获取其第一个键
                new_node = self._load_node(new_root_page_id)
                separator_key = None
                if new_node and new_node.keys:
                    separator_key = new_node.keys[0]
                
                # 新根节点包含两个子节点：原根节点和新分裂的节点
                new_root_node = BPTreeNode(
                    is_leaf=False,
                    keys=[separator_key] if separator_key is not None else [],
                    children=[self.root_page_id, new_root_page_id],
                    page_id=new_root_page.header.page_id
                )
                
                # 保存新根节点
                self._save_node(new_root_page, new_root_node)
                self.buffer_manager.unpin_page(new_root_page.header.page_id, is_dirty=True)
                
                # 更新根节点ID
                self.root_page_id = new_root_page.header.page_id
        
        return True
    
    def _insert_recursive(self, node: BPTreeNode, key: Any, record_id: int) -> Optional[int]:
        """递归插入实现"""
        if node.is_leaf:
            return self._insert_into_leaf(node, key, record_id)
        else:
            # 找到合适的子节点
            child_index = self._find_child_index(node, key)
            child_page_id = node.children[child_index]
            child_node = self._load_node(child_page_id)
            
            if not child_node:
                return None
            
            # 递归插入到子节点
            new_child_page_id = self._insert_recursive(child_node, key, record_id)
            
            # 如果子节点分裂，需要更新当前节点
            if new_child_page_id is not None:
                # 重新加载当前节点（可能已被替换）
                current_node = self._load_node(node.page_id)
                if not current_node:
                    return None
                
                return self._insert_into_node(current_node, new_child_page_id)
            
            return None
    
    def _insert_into_leaf(self, leaf_node: BPTreeNode, key: Any, record_id: int) -> Optional[int]:
        """在叶子节点中插入键值"""
        # 找到插入位置
        insert_pos = 0
        while insert_pos < len(leaf_node.keys) and _compare_keys(leaf_node.keys[insert_pos], key) < 0:
            insert_pos += 1
        
        # 检查唯一性约束（仅对唯一索引）
        if self.is_unique:
            # 对于唯一索引，检查是否已存在相同的键
            if insert_pos < len(leaf_node.keys) and _compare_keys(leaf_node.keys[insert_pos], key) == 0:
                print(f"Unique constraint violation: key {key} already exists")
                return None
            # 插入新键值对
            leaf_node.keys.insert(insert_pos, key)
            leaf_node.children.insert(insert_pos, record_id)
        else:
            # 对于非唯一索引，允许重复键，直接插入
            leaf_node.keys.insert(insert_pos, key)
            leaf_node.children.insert(insert_pos, record_id)
        
        # 检查是否需要分裂
        if len(leaf_node.keys) > self.order:
            return self._split_leaf_node(leaf_node)
        
        # 保存节点
        page = self.buffer_manager.get_page(leaf_node.page_id)
        if page:
            self._save_node(page, leaf_node)
            self.buffer_manager.unpin_page(leaf_node.page_id, is_dirty=True)
        
        return None
    
    def _insert_into_node(self, node: BPTreeNode, new_child_page_id: int) -> Optional[int]:
        """在内部节点中插入子节点"""
        # 加载新子节点以获取其第一个键
        new_child_node = self._load_node(new_child_page_id)
        if not new_child_node or not new_child_node.keys:
            return None
        
        # 获取新子节点的第一个键作为分隔键
        separator_key = new_child_node.keys[0]
        
        # 找到插入位置
        insert_pos = 0
        while insert_pos < len(node.keys) and _compare_keys(node.keys[insert_pos], separator_key) < 0:
            insert_pos += 1
        
        # 插入分隔键和子节点引用
        node.keys.insert(insert_pos, separator_key)
        node.children.insert(insert_pos + 1, new_child_page_id)
        
        # 检查是否需要分裂
        if len(node.keys) > self.order:
            return self._split_internal_node(node)
        
        # 保存节点
        page = self.buffer_manager.get_page(node.page_id)
        if page:
            self._save_node(page, node)
            self.buffer_manager.unpin_page(node.page_id, is_dirty=True)
        
        return None
    
    def _split_leaf_node(self, leaf_node: BPTreeNode) -> Optional[int]:
        """分裂叶子节点"""
        # 计算分裂点
        split_index = math.ceil(self.order / 2)
        
        # 创建新节点
        new_page = self.buffer_manager.create_page(PageType.INDEX_PAGE)
        if not new_page:
            return None
        
        new_node = BPTreeNode(
            is_leaf=True,
            keys=leaf_node.keys[split_index:],
            children=leaf_node.children[split_index:],
            next_leaf=leaf_node.next_leaf,
            page_id=new_page.header.page_id
        )
        
        # 更新原节点
        leaf_node.keys = leaf_node.keys[:split_index]
        leaf_node.children = leaf_node.children[:split_index]
        leaf_node.next_leaf = new_page.header.page_id
        
        # 保存两个节点
        page1 = self.buffer_manager.get_page(leaf_node.page_id)
        if page1:
            self._save_node(page1, leaf_node)
            self.buffer_manager.unpin_page(leaf_node.page_id, is_dirty=True)
        
        self._save_node(new_page, new_node)
        self.buffer_manager.unpin_page(new_page.header.page_id, is_dirty=True)
        
        # 返回新节点的页ID，用于父节点更新
        return new_page.header.page_id
    
    def _split_internal_node(self, node: BPTreeNode) -> Optional[int]:
        """分裂内部节点"""
        # 计算分裂点
        split_index = math.ceil(self.order / 2)
        
        # 创建新节点
        new_page = self.buffer_manager.create_page(PageType.INDEX_PAGE)
        if not new_page:
            return None
        
        # 新节点获得后半部分的键和子节点
        new_node = BPTreeNode(
            is_leaf=False,
            keys=node.keys[split_index + 1:],
            children=node.children[split_index + 1:],
            page_id=new_page.header.page_id
        )
        
        # 更新原节点
        node.keys = node.keys[:split_index]
        node.children = node.children[:split_index + 1]
        
        # 保存两个节点
        page1 = self.buffer_manager.get_page(node.page_id)
        if page1:
            self._save_node(page1, node)
            self.buffer_manager.unpin_page(node.page_id, is_dirty=True)
        
        self._save_node(new_page, new_node)
        self.buffer_manager.unpin_page(new_page.header.page_id, is_dirty=True)
        
        # 返回新节点的页ID，用于父节点更新
        return new_page.header.page_id
    
    def _find_child_index(self, node: BPTreeNode, key: Any) -> int:
        """找到键值应该插入的子节点索引"""
        for i, node_key in enumerate(node.keys):
            if _compare_keys(key, node_key) <= 0:
                return i
        return len(node.keys)
    
    def search(self, key: Any) -> List[int]:
        """
        在索引中搜索键值
        
        Args:
            key: 搜索键
            
        Returns:
            记录ID列表
        """
        if self.root_page_id is None:
            return []
        
        node = self._find_leaf_node(self.root_page_id, key)
        if not node:
            return []
        
        # 在叶子节点中查找
        result = []
        for i, node_key in enumerate(node.keys):
            comparison = _compare_keys(node_key, key)
            if comparison == 0:
                result.append(node.children[i])
            elif comparison > 0:
                break
        
        return result
    
    def range_search(self, start_key: Any, end_key: Any) -> List[int]:
        """
        范围搜索
        
        Args:
            start_key: 起始键
            end_key: 结束键
            
        Returns:
            记录ID列表
        """
        if self.root_page_id is None:
            return []
        
        # 找到起始叶子节点
        node = self._find_leaf_node(self.root_page_id, start_key)
        if not node:
            return []
        
        result = []
        current_node = node
        
        # 遍历叶子节点链表直到超出范围
        while current_node:
            for i, key in enumerate(current_node.keys):
                # 检查是否在范围内
                if _compare_keys(key, start_key) >= 0 and _compare_keys(key, end_key) <= 0:
                    result.append(current_node.children[i])
                elif _compare_keys(key, end_key) > 0:
                    return result
            
            # 移动到下一个叶子节点
            if current_node.next_leaf:
                current_node = self._load_node(current_node.next_leaf)
            else:
                break
        
        return result
    
    def delete(self, key: Any, record_id: Optional[int] = None) -> bool:
        """
        从索引中删除键值对
        
        Args:
            key: 索引键
            record_id: 记录ID（如果为None，删除所有匹配的键值对）
            
        Returns:
            是否删除成功
        """
        if self.root_page_id is None:
            return False
        
        # 查找包含键的叶子节点
        leaf_node = self._find_leaf_node(self.root_page_id, key)
        if not leaf_node:
            return False
        
        # 查找键的位置
        key_indices = []
        for i, node_key in enumerate(leaf_node.keys):
            if _compare_keys(node_key, key) == 0:
                if record_id is None or leaf_node.children[i] == record_id:
                    key_indices.append(i)
        
        if not key_indices:
            return False
        
        # 从后往前删除，避免索引变化影响
        for i in reversed(key_indices):
            leaf_node.keys.pop(i)
            leaf_node.children.pop(i)
        
        # 保存更新后的节点
        page = self.buffer_manager.get_page(leaf_node.page_id)
        if page:
            self._save_node(page, leaf_node)
            self.buffer_manager.unpin_page(leaf_node.page_id, is_dirty=True)
        
        # TODO: 实现节点合并逻辑以保持B+树平衡
        return True
    
    def _find_leaf_node(self, page_id: int, key: Any) -> Optional[BPTreeNode]:
        """查找包含键值的叶子节点"""
        node = self._load_node(page_id)
        if not node:
            return None
        
        if node.is_leaf:
            return node
        
        # 递归查找子节点
        child_index = self._find_child_index(node, key)
        if child_index < len(node.children):
            return self._find_leaf_node(node.children[child_index], key)
        
        return None
    
    def update(self, old_key: Any, new_key: Any, record_id: int) -> bool:
        """
        更新索引中的键值
        
        Args:
            old_key: 旧键值
            new_key: 新键值
            record_id: 记录ID
            
        Returns:
            是否更新成功
        """
        # 先删除旧键值
        if not self.delete(old_key, record_id):
            return False
        
        # 再插入新键值
        return self.insert(new_key, record_id)

class BPTreeIndexManager:
    """B+树索引管理器"""
    
    def __init__(self, buffer_manager: BufferManager):
        """初始化索引管理器"""
        self.buffer_manager = buffer_manager
        self.indexes: Dict[str, BPTreeIndex] = {}
        self._load_indexes()
    
    def _load_indexes(self):
        """加载已存在的索引"""
        # 简化实现，实际应该从磁盘加载索引元数据
        pass
    
    def create_index(self, index_name: str, table_name: str, columns: List[str], is_unique: bool = False) -> bool:
        """
        创建B+树索引
        
        Args:
            index_name: 索引名称
            table_name: 表名
            columns: 索引列
            is_unique: 是否唯一索引
            
        Returns:
            是否创建成功
        """
        if index_name in self.indexes:
            return False
        
        index = BPTreeIndex(index_name, table_name, columns, self.buffer_manager, is_unique=is_unique)
        self.indexes[index_name] = index
        return True
    
    def get_index(self, index_name: str) -> Optional[BPTreeIndex]:
        """获取索引实例"""
        return self.indexes.get(index_name)
    
    def drop_index(self, index_name: str) -> bool:
        """删除索引"""
        if index_name in self.indexes:
            del self.indexes[index_name]
            return True
        return False
    
    def list_indexes(self) -> List[str]:
        """列出所有索引"""
        return list(self.indexes.keys())