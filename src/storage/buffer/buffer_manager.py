"""
缓存管理器(Buffer Manager)实现
管理页面的内存缓存，实现页面替换策略（LRU、FIFO等）
提供页面的读取、写入和缓存管理功能
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import time
from typing import Dict, List, Optional, Any, Set
from collections import OrderedDict
from enum import Enum
from dataclasses import dataclass, field
from threading import RLock
from src.storage.page.page import Page, PageManager, PageType

class ReplacementPolicy(Enum):
    """页面替换策略"""
    LRU = "LRU"     # Least Recently Used
    FIFO = "FIFO"   # First In First Out
    CLOCK = "CLOCK" # Clock Algorithm

@dataclass
class BufferFrame:
    """缓存帧结构"""
    page_id: int = -1
    page: Optional[Page] = None
    is_dirty: bool = False
    pin_count: int = 0      # 引用计数
    last_access_time: float = field(default_factory=time.time)
    access_count: int = 0   # 访问次数
    reference_bit: bool = False  # 用于Clock算法的引用位

    def __post_init__(self):
        """初始化后处理"""
        if self.page_id == -1:
            self.last_access_time = time.time()

class BufferManager:
    """缓存管理器"""
    
    def __init__(self, 
                 buffer_size: int = 100,
                 page_manager: Optional[PageManager] = None,
                 replacement_policy: ReplacementPolicy = ReplacementPolicy.LRU):
        """
        初始化缓存管理器
        
        Args:
            buffer_size: 缓存大小（页数）
            page_manager: 页管理器
            replacement_policy: 页面替换策略
        """
        self.buffer_size = buffer_size
        self.page_manager = page_manager or PageManager()
        self.replacement_policy = replacement_policy
        
        # 缓存结构
        self.buffer_frames: List[BufferFrame] = [BufferFrame() for _ in range(buffer_size)]
        self.page_to_frame: Dict[int, int] = {}  # 页ID到缓存帧的映射
        self.free_frames: Set[int] = set(range(buffer_size))  # 空闲帧集合
        
        # LRU相关
        self.lru_list: OrderedDict[int, int] = OrderedDict()  # page_id -> frame_index
        
        # FIFO相关
        self.fifo_queue: List[int] = []  # frame_index队列
        
        # Clock相关
        self.clock_hand: int = 0  # 时钟指针
        
        # 统计信息
        self.stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'page_reads': 0,
            'page_writes': 0,
            'evictions': 0
        }
        
        # 线程安全
        self._lock = RLock()
    
    def get_page(self, page_id: int) -> Optional[Page]:
        """
        获取页面（从缓存或磁盘加载）
        
        Args:
            page_id: 页ID
            
        Returns:
            页面对象，如果不存在则返回None
        """
        with self._lock:
            # 检查页面是否在缓存中
            if page_id in self.page_to_frame:
                frame_index = self.page_to_frame[page_id]
                frame = self.buffer_frames[frame_index]
                
                # 更新访问信息
                self._update_access_info(frame_index)
                self.stats['cache_hits'] += 1
                
                # 增加引用计数
                frame.pin_count += 1
                
                return frame.page
            
            # 缓存未命中，从磁盘加载
            self.stats['cache_misses'] += 1
            page = self.page_manager.load_page(page_id)
            
            if page is None:
                return None
            
            self.stats['page_reads'] += 1
            
            # 将页面加载到缓存
            frame_index = self._allocate_frame()
            if frame_index is None:
                # 缓存已满，需要替换
                frame_index = self._evict_page()
            
            # 设置缓存帧
            frame = self.buffer_frames[frame_index]
            frame.page_id = page_id
            frame.page = page
            frame.is_dirty = False
            frame.pin_count = 1
            self._update_access_info(frame_index)
            
            # 更新映射
            self.page_to_frame[page_id] = frame_index
            self.free_frames.discard(frame_index)
            
            return page
    
    def pin_page(self, page_id: int) -> Optional[Page]:
        """
        固定页面（增加引用计数）
        
        Args:
            page_id: 页ID
            
        Returns:
            页面对象
        """
        with self._lock:
            page = self.get_page(page_id)
            if page and page_id in self.page_to_frame:
                frame_index = self.page_to_frame[page_id]
                self.buffer_frames[frame_index].pin_count += 1
            return page
    
    def unpin_page(self, page_id: int, is_dirty: bool = False) -> bool:
        """
        取消固定页面（减少引用计数）
        
        Args:
            page_id: 页ID
            is_dirty: 是否为脏页
            
        Returns:
            是否成功
        """
        with self._lock:
            if page_id not in self.page_to_frame:
                return False
            
            frame_index = self.page_to_frame[page_id]
            frame = self.buffer_frames[frame_index]
            
            if frame.pin_count > 0:
                frame.pin_count -= 1
            
            if is_dirty:
                frame.is_dirty = True
            
            return True
    
    def flush_page(self, page_id: int) -> bool:
        """
        将指定页面写入磁盘
        
        Args:
            page_id: 页ID
            
        Returns:
            是否成功
        """
        with self._lock:
            if page_id not in self.page_to_frame:
                return False
            
            frame_index = self.page_to_frame[page_id]
            frame = self.buffer_frames[frame_index]
            
            if frame.is_dirty and frame.page:
                if self.page_manager.save_page(frame.page):
                    frame.is_dirty = False
                    self.stats['page_writes'] += 1
                    return True
            
            return False
    
    def flush_all_pages(self) -> int:
        """
        将所有脏页写入磁盘
        
        Returns:
            写入的页面数
        """
        with self._lock:
            flushed_count = 0
            
            for frame in self.buffer_frames:
                if frame.is_dirty and frame.page:
                    if self.page_manager.save_page(frame.page):
                        frame.is_dirty = False
                        self.stats['page_writes'] += 1
                        flushed_count += 1
            
            return flushed_count
    
    def create_page(self, page_type: PageType = PageType.DATA_PAGE) -> Optional[Page]:
        """
        创建新页面并加载到缓存
        
        Args:
            page_type: 页面类型
            
        Returns:
            新创建的页面
        """
        with self._lock:
            # 创建新页面
            page = self.page_manager.create_page(page_type)
            
            # 分配缓存帧
            frame_index = self._allocate_frame()
            if frame_index is None:
                frame_index = self._evict_page()
            
            # 设置缓存帧
            frame = self.buffer_frames[frame_index]
            frame.page_id = page.header.page_id
            frame.page = page
            frame.is_dirty = True  # 新页面默认为脏页
            frame.pin_count = 1
            self._update_access_info(frame_index)
            
            # 更新映射
            self.page_to_frame[page.header.page_id] = frame_index
            self.free_frames.discard(frame_index)
            
            return page
    
    def _allocate_frame(self) -> Optional[int]:
        """分配空闲缓存帧"""
        if self.free_frames:
            return self.free_frames.pop()
        return None
    
    def _evict_page(self) -> int:
        """根据替换策略驱逐页面"""
        if self.replacement_policy == ReplacementPolicy.LRU:
            return self._evict_lru()
        elif self.replacement_policy == ReplacementPolicy.FIFO:
            return self._evict_fifo()
        elif self.replacement_policy == ReplacementPolicy.CLOCK:
            return self._evict_clock()
        else:
            return self._evict_lru()  # 默认使用LRU
    
    def _evict_lru(self) -> int:
        """LRU页面替换"""
        # 找到最近最少使用且未被固定的页面
        for page_id, frame_index in self.lru_list.items():
            frame = self.buffer_frames[frame_index]
            if frame.pin_count == 0:
                return self._do_evict(frame_index)
        
        # 如果所有页面都被固定，选择第一个
        frame_index = list(self.lru_list.values())[0]
        return self._do_evict(frame_index)
    
    def _evict_fifo(self) -> int:
        """FIFO页面替换"""
        while self.fifo_queue:
            frame_index = self.fifo_queue[0]
            frame = self.buffer_frames[frame_index]
            
            if frame.pin_count == 0:
                self.fifo_queue.pop(0)
                return self._do_evict(frame_index)
            else:
                # 如果被固定，移到队尾
                self.fifo_queue.append(self.fifo_queue.pop(0))
        
        # 如果所有页面都被固定，选择第一个
        frame_index = 0
        return self._do_evict(frame_index)
    
    def _evict_clock(self) -> int:
        """Clock页面替换"""
        start_position = self.clock_hand
        
        while True:
            frame = self.buffer_frames[self.clock_hand]
            
            if frame.pin_count == 0:
                if not frame.reference_bit:
                    # 找到可替换的页面
                    victim_frame = self.clock_hand
                    self.clock_hand = (self.clock_hand + 1) % self.buffer_size
                    return self._do_evict(victim_frame)
                else:
                    # 重置引用位
                    frame.reference_bit = False
            
            self.clock_hand = (self.clock_hand + 1) % self.buffer_size
            
            # 避免死循环
            if self.clock_hand == start_position:
                return self._do_evict(self.clock_hand)
    
    def _do_evict(self, frame_index: int) -> int:
        """执行页面驱逐"""
        frame = self.buffer_frames[frame_index]
        
        # 如果是脏页，先写入磁盘
        if frame.is_dirty and frame.page:
            self.page_manager.save_page(frame.page)
            self.stats['page_writes'] += 1
        
        # 清理映射
        if frame.page_id in self.page_to_frame:
            del self.page_to_frame[frame.page_id]
        
        if frame.page_id in self.lru_list:
            del self.lru_list[frame.page_id]
        
        if frame_index in self.fifo_queue:
            self.fifo_queue.remove(frame_index)
        
        # 重置缓存帧
        frame.page_id = -1
        frame.page = None
        frame.is_dirty = False
        frame.pin_count = 0
        frame.reference_bit = False
        
        self.stats['evictions'] += 1
        
        return frame_index
    
    def _update_access_info(self, frame_index: int):
        """更新页面访问信息"""
        frame = self.buffer_frames[frame_index]
        frame.last_access_time = time.time()
        frame.access_count += 1
        frame.reference_bit = True  # 用于Clock算法
        
        # 更新LRU列表
        if frame.page_id in self.lru_list:
            del self.lru_list[frame.page_id]
        self.lru_list[frame.page_id] = frame_index
        
        # 更新FIFO队列
        if frame_index not in self.fifo_queue:
            self.fifo_queue.append(frame_index)
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        with self._lock:
            total_requests = self.stats['cache_hits'] + self.stats['cache_misses']
            hit_rate = (self.stats['cache_hits'] / total_requests * 100) if total_requests > 0 else 0.0
            
            used_frames = self.buffer_size - len(self.free_frames)
            dirty_frames = sum(1 for frame in self.buffer_frames if frame.is_dirty)
            pinned_frames = sum(1 for frame in self.buffer_frames if frame.pin_count > 0)
            
            return {
                'buffer_size': self.buffer_size,
                'used_frames': used_frames,
                'free_frames': len(self.free_frames),
                'dirty_frames': dirty_frames,
                'pinned_frames': pinned_frames,
                'replacement_policy': self.replacement_policy.value,
                'cache_hit_rate': round(hit_rate, 2),
                **self.stats
            }
    
    def print_cache_status(self):
        """打印缓存状态"""
        stats = self.get_cache_stats()
        
        print("\n" + "=" * 50)
        print("           缓存管理器状态")
        print("=" * 50)
        
        print(f"缓存大小: {stats['buffer_size']} 页")
        print(f"已使用帧: {stats['used_frames']}")
        print(f"空闲帧: {stats['free_frames']}")
        print(f"脏页数: {stats['dirty_frames']}")
        print(f"固定页数: {stats['pinned_frames']}")
        print(f"替换策略: {stats['replacement_policy']}")
        print(f"缓存命中率: {stats['cache_hit_rate']}%")
        print(f"缓存命中: {stats['cache_hits']}")
        print(f"缓存未命中: {stats['cache_misses']}")
        print(f"页面读取: {stats['page_reads']}")
        print(f"页面写入: {stats['page_writes']}")
        print(f"页面驱逐: {stats['evictions']}")

def test_buffer_manager():
    """测试缓存管理器"""
    print("=" * 60)
    print("              缓存管理器测试")
    print("=" * 60)
    
    # 创建小容量的缓存管理器进行测试
    buffer_manager = BufferManager(buffer_size=3, replacement_policy=ReplacementPolicy.LRU)
    
    print("\n1. 创建页面并测试缓存...")
    
    # 创建一些页面
    pages = []
    for i in range(5):
        page = buffer_manager.create_page(PageType.DATA_PAGE)
        if page:
            # 添加一些测试数据
            page.add_record({
                "id": i + 1,
                "data": f"测试数据{i + 1}",
                "timestamp": time.time()
            })
            pages.append(page)
            print(f"  创建页面 {page.header.page_id}")
    
    buffer_manager.print_cache_status()
    
    print("\n2. 测试页面访问和缓存命中...")
    
    # 访问已存在的页面（应该命中缓存）
    for page_id in [1, 2, 3]:
        page = buffer_manager.get_page(page_id)
        if page:
            print(f"  访问页面 {page_id}: 缓存命中")
        else:
            print(f"  访问页面 {page_id}: 未找到")
    
    # 访问被驱逐的页面（应该未命中缓存）
    for page_id in [4, 5]:
        page = buffer_manager.get_page(page_id)
        if page:
            print(f"  访问页面 {page_id}: 从磁盘加载")
        else:
            print(f"  访问页面 {page_id}: 未找到")
    
    buffer_manager.print_cache_status()
    
    print("\n3. 测试页面固定和解除固定...")
    
    # 固定页面
    page = buffer_manager.pin_page(1)
    if page:
        print(f"  固定页面 1")
    
    # 创建更多页面测试驱逐
    for i in range(3):
        page = buffer_manager.create_page(PageType.DATA_PAGE)
        if page:
            print(f"  创建页面 {page.header.page_id} (可能触发驱逐)")
    
    buffer_manager.print_cache_status()
    
    # 解除固定
    buffer_manager.unpin_page(1)
    print("  解除固定页面 1")
    
    print("\n4. 测试脏页写入...")
    
    # 修改页面并标记为脏页
    page = buffer_manager.get_page(1)
    if page:
        page.add_record({"updated": True, "time": time.time()})
        buffer_manager.unpin_page(1, is_dirty=True)
        print("  标记页面 1 为脏页")
    
    # 刷新所有脏页
    flushed = buffer_manager.flush_all_pages()
    print(f"  刷新了 {flushed} 个脏页到磁盘")
    
    buffer_manager.print_cache_status()
    
    print("\n✅ 缓存管理器测试完成!")

if __name__ == "__main__":
    test_buffer_manager()