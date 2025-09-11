"""
页存储系统实现
基于页的数据存储管理，每页固定大小，支持数据的读取和写入
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import os
import struct
import json
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum

# 页大小常量
PAGE_SIZE = 4096  # 4KB per page
HEADER_SIZE = 64  # 页头大小
DATA_SIZE = PAGE_SIZE - HEADER_SIZE  # 实际数据区大小

class PageType(Enum):
    """页类型枚举"""
    DATA_PAGE = "DATA_PAGE"       # 数据页
    INDEX_PAGE = "INDEX_PAGE"     # 索引页  
    HEADER_PAGE = "HEADER_PAGE"   # 表头页
    FREE_PAGE = "FREE_PAGE"       # 空闲页

@dataclass
class PageHeader:
    """页头结构"""
    page_id: int = 0              # 页ID
    page_type: PageType = PageType.DATA_PAGE  # 页类型
    record_count: int = 0         # 记录数量
    free_space: int = DATA_SIZE   # 剩余空间
    next_page_id: int = -1        # 下一页ID
    prev_page_id: int = -1        # 上一页ID
    checksum: int = 0             # 校验和
    timestamp: int = 0            # 时间戳
    
    def to_bytes(self) -> bytes:
        """转换为字节序列"""
        return struct.pack(
            'IIII III I',  # 8个整数字段
            self.page_id,
            hash(self.page_type.value) & 0xFFFFFFFF,  # 将枚举转换为hash值并限制范围
            self.record_count,
            self.free_space,
            self.next_page_id if self.next_page_id >= 0 else 0xFFFFFFFF,
            self.prev_page_id if self.prev_page_id >= 0 else 0xFFFFFFFF,
            self.checksum,
            self.timestamp
        ).ljust(HEADER_SIZE, b'\x00')  # 填充到指定大小
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'PageHeader':
        """从字节序列创建"""
        values = struct.unpack('IIII III I', data[:32])
        
        # 通过hash值反推页类型
        page_type_hash = values[1]
        page_type = PageType.DATA_PAGE
        for pt in PageType:
            if (hash(pt.value) & 0xFFFFFFFF) == page_type_hash:
                page_type = pt
                break
        
        return cls(
            page_id=values[0],
            page_type=page_type,
            record_count=values[2],
            free_space=values[3],
            next_page_id=values[4] if values[4] != 0xFFFFFFFF else -1,
            prev_page_id=values[5] if values[5] != 0xFFFFFFFF else -1,
            checksum=values[6],
            timestamp=values[7]
        )

class Page:
    """页对象"""
    
    def __init__(self, page_id: int, page_type: PageType = PageType.DATA_PAGE):
        """初始化页对象"""
        self.header = PageHeader(
            page_id=page_id,
            page_type=page_type,
            free_space=DATA_SIZE
        )
        self.data = bytearray(DATA_SIZE)  # 数据区
        self.is_dirty = False  # 脏页标记
        self.records: List[Dict[str, Any]] = []  # 记录列表
    
    def add_record(self, record: Dict[str, Any]) -> bool:
        """添加记录到页中"""
        # 序列化记录
        record_bytes = json.dumps(record, ensure_ascii=False).encode('utf-8')
        record_size = len(record_bytes) + 4  # 包含长度字段
        
        # 检查空间是否足够
        if record_size > self.header.free_space:
            return False
        
        # 添加记录长度信息
        length_bytes = struct.pack('I', len(record_bytes))
        
        # 计算写入位置
        used_space = DATA_SIZE - self.header.free_space
        
        # 写入数据
        self.data[used_space:used_space + 4] = length_bytes
        self.data[used_space + 4:used_space + record_size] = record_bytes
        
        # 更新头信息
        self.header.record_count += 1
        self.header.free_space -= record_size
        self.is_dirty = True
        self.records.append(record)
        
        return True
    
    def get_records(self) -> List[Dict[str, Any]]:
        """获取页中所有记录"""
        if self.records:
            return self.records.copy()
        
        # 从数据区解析记录
        records = []
        offset = 0
        
        for _ in range(self.header.record_count):
            if offset + 4 > len(self.data):
                break
                
            # 读取记录长度
            record_length = struct.unpack('I', self.data[offset:offset + 4])[0]
            offset += 4
            
            if offset + record_length > len(self.data):
                break
            
            # 读取记录数据
            record_bytes = self.data[offset:offset + record_length]
            offset += record_length
            
            try:
                record = json.loads(record_bytes.decode('utf-8'))
                records.append(record)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
        
        self.records = records
        return records.copy()
    
    def delete_record(self, index: int) -> bool:
        """删除指定索引的记录"""
        if index < 0 or index >= self.header.record_count:
            return False
        
        # 重新构建数据区
        records = self.get_records()
        if index >= len(records):
            return False
        
        records.pop(index)
        
        # 清空并重新写入
        self.data = bytearray(DATA_SIZE)
        self.header.record_count = 0
        self.header.free_space = DATA_SIZE
        self.records.clear()
        
        # 重新添加记录
        for record in records:
            self.add_record(record)
        
        self.is_dirty = True
        return True
    
    def to_bytes(self) -> bytes:
        """转换为字节序列用于存储"""
        header_bytes = self.header.to_bytes()
        return header_bytes + bytes(self.data)
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Page':
        """从字节序列创建页对象"""
        if len(data) != PAGE_SIZE:
            raise ValueError(f"Invalid page size: {len(data)}")
        
        # 解析页头
        header_data = data[:HEADER_SIZE]
        header = PageHeader.from_bytes(header_data)
        
        # 创建页对象
        page = cls(header.page_id, header.page_type)
        page.header = header
        page.data = bytearray(data[HEADER_SIZE:])
        
        return page
    
    def calculate_checksum(self) -> int:
        """计算页的校验和"""
        return hash(bytes(self.data)) & 0xFFFFFFFF
    
    def verify_checksum(self) -> bool:
        """验证页的校验和"""
        return self.header.checksum == self.calculate_checksum()
    
    def update_checksum(self):
        """更新页的校验和"""
        self.header.checksum = self.calculate_checksum()
    
    def __str__(self) -> str:
        """字符串表示"""
        return (f"Page(id={self.header.page_id}, "
                f"type={self.header.page_type.value}, "
                f"records={self.header.record_count}, "
                f"free_space={self.header.free_space})")

class PageManager:
    """页管理器"""
    
    def __init__(self, data_dir: str = "data"):
        """初始化页管理器"""
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.pages: Dict[int, Page] = {}  # 内存中的页缓存
        self.next_page_id = 1
        self._load_metadata()
    
    def _get_page_file_path(self, page_id: int) -> Path:
        """获取页文件路径"""
        return self.data_dir / f"page_{page_id:06d}.dat"
    
    def _get_metadata_path(self) -> Path:
        """获取元数据文件路径"""
        return self.data_dir / "metadata.json"
    
    def _load_metadata(self):
        """加载元数据"""
        metadata_path = self._get_metadata_path()
        if metadata_path.exists():
            try:
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                    self.next_page_id = metadata.get('next_page_id', 1)
            except:
                pass
    
    def _save_metadata(self):
        """保存元数据"""
        metadata = {
            'next_page_id': self.next_page_id
        }
        with open(self._get_metadata_path(), 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
    
    def create_page(self, page_type: PageType = PageType.DATA_PAGE) -> Page:
        """创建新页"""
        page_id = self.next_page_id
        self.next_page_id += 1
        
        page = Page(page_id, page_type)
        self.pages[page_id] = page
        
        self._save_metadata()
        return page
    
    def load_page(self, page_id: int) -> Optional[Page]:
        """加载页"""
        # 先检查内存缓存
        if page_id in self.pages:
            return self.pages[page_id]
        
        # 从磁盘加载
        page_file = self._get_page_file_path(page_id)
        if not page_file.exists():
            return None
        
        try:
            with open(page_file, 'rb') as f:
                data = f.read()
                if len(data) == PAGE_SIZE:
                    page = Page.from_bytes(data)
                    self.pages[page_id] = page
                    return page
        except Exception as e:
            print(f"Error loading page {page_id}: {e}")
        
        return None
    
    def save_page(self, page: Page) -> bool:
        """保存页到磁盘"""
        try:
            page.update_checksum()
            page_file = self._get_page_file_path(page.header.page_id)
            
            with open(page_file, 'wb') as f:
                f.write(page.to_bytes())
            
            page.is_dirty = False
            return True
        except Exception as e:
            print(f"Error saving page {page.header.page_id}: {e}")
            return False
    
    def save_all_dirty_pages(self) -> int:
        """保存所有脏页"""
        saved_count = 0
        for page in self.pages.values():
            if page.is_dirty:
                if self.save_page(page):
                    saved_count += 1
        return saved_count
    
    def get_page_stats(self) -> Dict[str, Any]:
        """获取页统计信息"""
        total_pages = len(self.pages)
        dirty_pages = sum(1 for page in self.pages.values() if page.is_dirty)
        total_records = sum(page.header.record_count for page in self.pages.values())
        
        page_types = {}
        for page in self.pages.values():
            page_type = page.header.page_type.value
            page_types[page_type] = page_types.get(page_type, 0) + 1
        
        return {
            'total_pages': total_pages,
            'dirty_pages': dirty_pages,
            'total_records': total_records,
            'page_types': page_types,
            'next_page_id': self.next_page_id
        }

def test_page_system():
    """测试页存储系统"""
    print("=" * 60)
    print("              页存储系统测试")
    print("=" * 60)
    
    # 创建页管理器
    page_manager = PageManager("test_data")
    
    # 创建数据页
    print("\n1. 创建数据页...")
    page = page_manager.create_page(PageType.DATA_PAGE)
    print(f"创建页: {page}")
    
    # 添加记录
    print("\n2. 添加记录...")
    test_records = [
        {"id": 1, "name": "张三", "age": 25, "grade": 85.5},
        {"id": 2, "name": "李四", "age": 23, "grade": 92.0},
        {"id": 3, "name": "王五", "age": 26, "grade": 78.5}
    ]
    
    for record in test_records:
        if page.add_record(record):
            print(f"  添加记录: {record}")
        else:
            print(f"  添加失败: {record}")
    
    print(f"\n页状态: {page}")
    
    # 保存页
    print("\n3. 保存页到磁盘...")
    if page_manager.save_page(page):
        print("页保存成功")
    else:
        print("页保存失败")
    
    # 重新加载页
    print("\n4. 从磁盘加载页...")
    loaded_page = page_manager.load_page(page.header.page_id)
    if loaded_page:
        print(f"加载页: {loaded_page}")
        records = loaded_page.get_records()
        print(f"记录数量: {len(records)}")
        for i, record in enumerate(records):
            print(f"  记录 {i+1}: {record}")
    else:
        print("页加载失败")
    
    # 页统计信息
    print("\n5. 页统计信息...")
    stats = page_manager.get_page_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n✅ 页存储系统测试完成!")

if __name__ == "__main__":
    test_page_system()