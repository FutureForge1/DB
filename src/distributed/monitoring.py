"""
性能监控与调优模块

实现分布式查询性能分析、慢查询日志和系统运行状态监控接口
"""

import threading
import time
import json
import statistics
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
import logging
from collections import deque, defaultdict
import queue

class MetricType(Enum):
    """指标类型"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"

class AlertLevel(Enum):
    """告警级别"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class QueryMetrics:
    """查询指标"""
    query_id: str
    sql: str
    node_id: str
    start_time: float
    end_time: Optional[float] = None
    execution_time: Optional[float] = None
    rows_examined: int = 0
    rows_returned: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    cpu_time: float = 0.0
    memory_used: int = 0
    disk_io: int = 0
    network_io: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    error_message: Optional[str] = None
    
    def complete(self, end_time: float = None):
        """完成查询记录"""
        self.end_time = end_time or time.time()
        self.execution_time = self.end_time - self.start_time
    
    @property
    def is_slow_query(self) -> bool:
        """判断是否为慢查询"""
        return self.execution_time and self.execution_time > 1.0  # 1秒以上为慢查询
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class SystemMetrics:
    """系统指标"""
    node_id: str
    timestamp: float
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    disk_usage: float = 0.0
    network_in: float = 0.0
    network_out: float = 0.0
    active_connections: int = 0
    query_rate: float = 0.0
    error_rate: float = 0.0
    response_time: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class Alert:
    """告警"""
    alert_id: str
    node_id: str
    metric_name: str
    level: AlertLevel
    message: str
    value: float
    threshold: float
    timestamp: float
    resolved: bool = False
    resolution_time: Optional[float] = None
    
    def resolve(self):
        """解决告警"""
        self.resolved = True
        self.resolution_time = time.time()

class MetricCollector:
    """指标收集器"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.counters: Dict[str, float] = defaultdict(float)
        self.gauges: Dict[str, float] = defaultdict(float)
        self.histograms: Dict[str, List[float]] = defaultdict(list)
        self.timers: Dict[str, List[float]] = defaultdict(list)
        
        self.lock = threading.RLock()
    
    def increment_counter(self, name: str, value: float = 1.0, tags: Dict[str, str] = None):
        """增加计数器"""
        with self.lock:
            key = self._make_key(name, tags)
            self.counters[key] += value
    
    def set_gauge(self, name: str, value: float, tags: Dict[str, str] = None):
        """设置仪表值"""
        with self.lock:
            key = self._make_key(name, tags)
            self.gauges[key] = value
    
    def record_histogram(self, name: str, value: float, tags: Dict[str, str] = None):
        """记录直方图值"""
        with self.lock:
            key = self._make_key(name, tags)
            self.histograms[key].append(value)
            # 保持最近1000个值
            if len(self.histograms[key]) > 1000:
                self.histograms[key] = self.histograms[key][-1000:]
    
    def record_timer(self, name: str, duration: float, tags: Dict[str, str] = None):
        """记录计时器值"""
        with self.lock:
            key = self._make_key(name, tags)
            self.timers[key].append(duration)
            # 保持最近1000个值
            if len(self.timers[key]) > 1000:
                self.timers[key] = self.timers[key][-1000:]
    
    def _make_key(self, name: str, tags: Dict[str, str] = None) -> str:
        """生成指标键"""
        if not tags:
            return name
        tag_str = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{name}[{tag_str}]"
    
    def get_counter(self, name: str, tags: Dict[str, str] = None) -> float:
        """获取计数器值"""
        with self.lock:
            key = self._make_key(name, tags)
            return self.counters.get(key, 0.0)
    
    def get_gauge(self, name: str, tags: Dict[str, str] = None) -> float:
        """获取仪表值"""
        with self.lock:
            key = self._make_key(name, tags)
            return self.gauges.get(key, 0.0)
    
    def get_histogram_stats(self, name: str, tags: Dict[str, str] = None) -> Dict[str, float]:
        """获取直方图统计"""
        with self.lock:
            key = self._make_key(name, tags)
            values = self.histograms.get(key, [])
            
            if not values:
                return {}
            
            return {
                'count': len(values),
                'min': min(values),
                'max': max(values),
                'mean': statistics.mean(values),
                'median': statistics.median(values),
                'p95': self._percentile(values, 0.95),
                'p99': self._percentile(values, 0.99)
            }
    
    def get_timer_stats(self, name: str, tags: Dict[str, str] = None) -> Dict[str, float]:
        """获取计时器统计"""
        return self.get_histogram_stats(name, tags)
    
    def _percentile(self, values: List[float], p: float) -> float:
        """计算百分位数"""
        if not values:
            return 0.0
        sorted_values = sorted(values)
        index = int(len(sorted_values) * p)
        return sorted_values[min(index, len(sorted_values) - 1)]
    
    def reset(self):
        """重置所有指标"""
        with self.lock:
            self.counters.clear()
            self.gauges.clear()
            self.histograms.clear()
            self.timers.clear()

class SlowQueryLogger:
    """慢查询日志记录器"""
    
    def __init__(self, threshold: float = 1.0, max_entries: int = 10000):
        self.threshold = threshold
        self.max_entries = max_entries
        self.slow_queries: deque = deque(maxlen=max_entries)
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
    
    def log_query(self, query_metrics: QueryMetrics):
        """记录查询"""
        if query_metrics.is_slow_query:
            with self.lock:
                self.slow_queries.append(query_metrics)
                self.logger.warning(
                    f"Slow query detected: {query_metrics.query_id} "
                    f"took {query_metrics.execution_time:.2f}s"
                )
    
    def get_slow_queries(self, limit: int = 100, 
                        start_time: float = None, 
                        end_time: float = None) -> List[QueryMetrics]:
        """获取慢查询列表"""
        with self.lock:
            queries = list(self.slow_queries)
            
            # 时间过滤
            if start_time or end_time:
                filtered_queries = []
                for query in queries:
                    if start_time and query.start_time < start_time:
                        continue
                    if end_time and query.start_time > end_time:
                        continue
                    filtered_queries.append(query)
                queries = filtered_queries
            
            # 按执行时间排序，最慢的在前
            queries.sort(key=lambda q: q.execution_time or 0, reverse=True)
            
            return queries[:limit]
    
    def get_slow_query_stats(self) -> Dict[str, Any]:
        """获取慢查询统计"""
        with self.lock:
            queries = list(self.slow_queries)
            
            if not queries:
                return {
                    'total_slow_queries': 0,
                    'avg_execution_time': 0,
                    'max_execution_time': 0,
                    'most_common_tables': []
                }
            
            execution_times = [q.execution_time for q in queries if q.execution_time]
            table_counts = defaultdict(int)
            
            for query in queries:
                # 简单的表名提取（实际应该用SQL解析器）
                sql_upper = query.sql.upper()
                if 'FROM' in sql_upper:
                    parts = sql_upper.split('FROM')[1].split()
                    if parts:
                        table_name = parts[0].strip()
                        table_counts[table_name] += 1
            
            most_common_tables = sorted(
                table_counts.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:5]
            
            return {
                'total_slow_queries': len(queries),
                'avg_execution_time': statistics.mean(execution_times) if execution_times else 0,
                'max_execution_time': max(execution_times) if execution_times else 0,
                'most_common_tables': most_common_tables
            }

class AlertManager:
    """告警管理器"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.alerts: Dict[str, Alert] = {}
        self.alert_rules: Dict[str, Dict[str, Any]] = {}
        self.alert_callbacks: List[Callable] = []
        
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
    
    def add_alert_rule(self, metric_name: str, threshold: float, 
                      level: AlertLevel, comparison: str = "gt"):
        """添加告警规则"""
        with self.lock:
            self.alert_rules[metric_name] = {
                'threshold': threshold,
                'level': level,
                'comparison': comparison
            }
    
    def check_metric(self, metric_name: str, value: float):
        """检查指标是否触发告警"""
        with self.lock:
            if metric_name not in self.alert_rules:
                return
            
            rule = self.alert_rules[metric_name]
            threshold = rule['threshold']
            level = rule['level']
            comparison = rule['comparison']
            
            should_alert = False
            if comparison == "gt" and value > threshold:
                should_alert = True
            elif comparison == "lt" and value < threshold:
                should_alert = True
            elif comparison == "eq" and abs(value - threshold) < 0.001:
                should_alert = True
            
            alert_key = f"{self.node_id}_{metric_name}"
            
            if should_alert:
                if alert_key not in self.alerts or self.alerts[alert_key].resolved:
                    # 创建新告警
                    alert = Alert(
                        alert_id=f"alert_{int(time.time() * 1000000)}",
                        node_id=self.node_id,
                        metric_name=metric_name,
                        level=level,
                        message=f"{metric_name} value {value} exceeds threshold {threshold}",
                        value=value,
                        threshold=threshold,
                        timestamp=time.time()
                    )
                    self.alerts[alert_key] = alert
                    self._trigger_alert_callbacks(alert)
            else:
                # 解决告警
                if alert_key in self.alerts and not self.alerts[alert_key].resolved:
                    self.alerts[alert_key].resolve()
                    self._trigger_alert_callbacks(self.alerts[alert_key])
    
    def _trigger_alert_callbacks(self, alert: Alert):
        """触发告警回调"""
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                self.logger.error(f"Alert callback error: {e}")
    
    def add_alert_callback(self, callback: Callable):
        """添加告警回调"""
        self.alert_callbacks.append(callback)
    
    def get_active_alerts(self) -> List[Alert]:
        """获取活跃告警"""
        with self.lock:
            return [alert for alert in self.alerts.values() if not alert.resolved]
    
    def get_all_alerts(self, limit: int = 100) -> List[Alert]:
        """获取所有告警"""
        with self.lock:
            alerts = list(self.alerts.values())
            alerts.sort(key=lambda a: a.timestamp, reverse=True)
            return alerts[:limit]

class PerformanceProfiler:
    """性能分析器"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.active_queries: Dict[str, QueryMetrics] = {}
        self.completed_queries: deque = deque(maxlen=10000)
        
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
    
    def start_query_profiling(self, query_id: str, sql: str) -> QueryMetrics:
        """开始查询性能分析"""
        metrics = QueryMetrics(
            query_id=query_id,
            sql=sql,
            node_id=self.node_id,
            start_time=time.time()
        )
        
        with self.lock:
            self.active_queries[query_id] = metrics
        
        return metrics
    
    def end_query_profiling(self, query_id: str, **kwargs) -> Optional[QueryMetrics]:
        """结束查询性能分析"""
        with self.lock:
            if query_id not in self.active_queries:
                return None
            
            metrics = self.active_queries.pop(query_id)
            metrics.complete()
            
            # 更新其他指标
            for key, value in kwargs.items():
                if hasattr(metrics, key):
                    setattr(metrics, key, value)
            
            self.completed_queries.append(metrics)
            return metrics
    
    def get_query_performance_stats(self, hours: int = 1) -> Dict[str, Any]:
        """获取查询性能统计"""
        with self.lock:
            cutoff_time = time.time() - hours * 3600
            recent_queries = [
                q for q in self.completed_queries 
                if q.start_time >= cutoff_time
            ]
            
            if not recent_queries:
                return {}
            
            execution_times = [q.execution_time for q in recent_queries if q.execution_time]
            rows_examined = [q.rows_examined for q in recent_queries]
            rows_returned = [q.rows_returned for q in recent_queries]
            
            return {
                'total_queries': len(recent_queries),
                'avg_execution_time': statistics.mean(execution_times) if execution_times else 0,
                'median_execution_time': statistics.median(execution_times) if execution_times else 0,
                'p95_execution_time': self._percentile(execution_times, 0.95) if execution_times else 0,
                'p99_execution_time': self._percentile(execution_times, 0.99) if execution_times else 0,
                'avg_rows_examined': statistics.mean(rows_examined) if rows_examined else 0,
                'avg_rows_returned': statistics.mean(rows_returned) if rows_returned else 0,
                'slow_query_count': len([q for q in recent_queries if q.is_slow_query]),
                'error_count': len([q for q in recent_queries if q.error_message])
            }
    
    def _percentile(self, values: List[float], p: float) -> float:
        """计算百分位数"""
        if not values:
            return 0.0
        sorted_values = sorted(values)
        index = int(len(sorted_values) * p)
        return sorted_values[min(index, len(sorted_values) - 1)]

class DistributedMonitor:
    """分布式监控器主类"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.metric_collector = MetricCollector(node_id)
        self.slow_query_logger = SlowQueryLogger()
        self.alert_manager = AlertManager(node_id)
        self.profiler = PerformanceProfiler(node_id)
        
        # 系统指标收集
        self.system_metrics_queue = queue.Queue()
        
        # 监控线程
        self.running = False
        self.monitor_thread = None
        self.system_monitor_thread = None
        
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        
        # 设置默认告警规则
        self._setup_default_alert_rules()
    
    def start(self):
        """启动监控器"""
        with self.lock:
            if self.running:
                return
            
            self.running = True
            self.monitor_thread = threading.Thread(target=self._monitor_worker, daemon=True)
            self.system_monitor_thread = threading.Thread(target=self._system_monitor_worker, daemon=True)
            
            self.monitor_thread.start()
            self.system_monitor_thread.start()
            
            self.logger.info(f"Distributed monitor started for node {self.node_id}")
    
    def stop(self):
        """停止监控器"""
        with self.lock:
            if not self.running:
                return
            
            self.running = False
            
            if self.monitor_thread:
                self.monitor_thread.join(timeout=5.0)
            if self.system_monitor_thread:
                self.system_monitor_thread.join(timeout=5.0)
            
            self.logger.info(f"Distributed monitor stopped for node {self.node_id}")
    
    def _setup_default_alert_rules(self):
        """设置默认告警规则"""
        self.alert_manager.add_alert_rule("cpu_usage", 80.0, AlertLevel.WARNING)
        self.alert_manager.add_alert_rule("cpu_usage", 95.0, AlertLevel.CRITICAL)
        self.alert_manager.add_alert_rule("memory_usage", 85.0, AlertLevel.WARNING)
        self.alert_manager.add_alert_rule("memory_usage", 95.0, AlertLevel.CRITICAL)
        self.alert_manager.add_alert_rule("disk_usage", 90.0, AlertLevel.WARNING)
        self.alert_manager.add_alert_rule("disk_usage", 95.0, AlertLevel.CRITICAL)
        self.alert_manager.add_alert_rule("error_rate", 5.0, AlertLevel.WARNING)
        self.alert_manager.add_alert_rule("error_rate", 10.0, AlertLevel.ERROR)
        self.alert_manager.add_alert_rule("response_time", 2.0, AlertLevel.WARNING)
        self.alert_manager.add_alert_rule("response_time", 5.0, AlertLevel.ERROR)
    
    def _monitor_worker(self):
        """监控工作线程"""
        while self.running:
            try:
                # 检查告警
                self._check_alerts()
                time.sleep(10)  # 每10秒检查一次
            except Exception as e:
                self.logger.error(f"Monitor worker error: {e}")
    
    def _system_monitor_worker(self):
        """系统监控工作线程"""
        while self.running:
            try:
                # 收集系统指标
                self._collect_system_metrics()
                time.sleep(30)  # 每30秒收集一次
            except Exception as e:
                self.logger.error(f"System monitor worker error: {e}")
    
    def _check_alerts(self):
        """检查告警"""
        # 检查各种指标的告警
        cpu_usage = self.metric_collector.get_gauge("cpu_usage")
        memory_usage = self.metric_collector.get_gauge("memory_usage")
        disk_usage = self.metric_collector.get_gauge("disk_usage")
        error_rate = self.metric_collector.get_gauge("error_rate")
        response_time = self.metric_collector.get_gauge("response_time")
        
        self.alert_manager.check_metric("cpu_usage", cpu_usage)
        self.alert_manager.check_metric("memory_usage", memory_usage)
        self.alert_manager.check_metric("disk_usage", disk_usage)
        self.alert_manager.check_metric("error_rate", error_rate)
        self.alert_manager.check_metric("response_time", response_time)
    
    def _collect_system_metrics(self):
        """收集系统指标"""
        import psutil
        
        try:
            # 收集CPU使用率
            cpu_percent = psutil.cpu_percent(interval=1)
            self.metric_collector.set_gauge("cpu_usage", cpu_percent)
            
            # 收集内存使用率
            memory = psutil.virtual_memory()
            self.metric_collector.set_gauge("memory_usage", memory.percent)
            
            # 收集磁盘使用率
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            self.metric_collector.set_gauge("disk_usage", disk_percent)
            
            # 收集网络IO
            network = psutil.net_io_counters()
            self.metric_collector.set_gauge("network_in", network.bytes_recv)
            self.metric_collector.set_gauge("network_out", network.bytes_sent)
            
            # 创建系统指标记录
            system_metrics = SystemMetrics(
                node_id=self.node_id,
                timestamp=time.time(),
                cpu_usage=cpu_percent,
                memory_usage=memory.percent,
                disk_usage=disk_percent,
                network_in=network.bytes_recv,
                network_out=network.bytes_sent
            )
            
            self.system_metrics_queue.put(system_metrics)
            
        except ImportError:
            # psutil不可用时的模拟数据
            import random
            self.metric_collector.set_gauge("cpu_usage", random.uniform(10, 80))
            self.metric_collector.set_gauge("memory_usage", random.uniform(20, 70))
            self.metric_collector.set_gauge("disk_usage", random.uniform(30, 60))
        except Exception as e:
            self.logger.error(f"Failed to collect system metrics: {e}")
    
    def record_query(self, query_id: str, sql: str) -> QueryMetrics:
        """记录查询开始"""
        self.metric_collector.increment_counter("total_queries")
        return self.profiler.start_query_profiling(query_id, sql)
    
    def complete_query(self, query_id: str, **kwargs) -> Optional[QueryMetrics]:
        """完成查询记录"""
        metrics = self.profiler.end_query_profiling(query_id, **kwargs)
        
        if metrics:
            # 记录执行时间
            if metrics.execution_time:
                self.metric_collector.record_timer("query_execution_time", metrics.execution_time)
            
            # 记录到慢查询日志
            self.slow_query_logger.log_query(metrics)
            
            # 更新错误率
            if metrics.error_message:
                self.metric_collector.increment_counter("query_errors")
        
        return metrics
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """获取仪表板数据"""
        with self.lock:
            # 系统指标
            system_metrics = {
                'cpu_usage': self.metric_collector.get_gauge("cpu_usage"),
                'memory_usage': self.metric_collector.get_gauge("memory_usage"),
                'disk_usage': self.metric_collector.get_gauge("disk_usage"),
                'network_in': self.metric_collector.get_gauge("network_in"),
                'network_out': self.metric_collector.get_gauge("network_out")
            }
            
            # 查询指标
            query_metrics = {
                'total_queries': self.metric_collector.get_counter("total_queries"),
                'query_errors': self.metric_collector.get_counter("query_errors"),
                'execution_time_stats': self.metric_collector.get_timer_stats("query_execution_time"),
                'performance_stats': self.profiler.get_query_performance_stats()
            }
            
            # 慢查询统计
            slow_query_stats = self.slow_query_logger.get_slow_query_stats()
            
            # 告警信息
            active_alerts = self.alert_manager.get_active_alerts()
            
            return {
                'node_id': self.node_id,
                'timestamp': time.time(),
                'system_metrics': system_metrics,
                'query_metrics': query_metrics,
                'slow_query_stats': slow_query_stats,
                'active_alerts': [alert.to_dict() for alert in active_alerts],
                'alert_count': len(active_alerts)
            }
    
    def get_slow_queries(self, limit: int = 50) -> List[Dict[str, Any]]:
        """获取慢查询列表"""
        slow_queries = self.slow_query_logger.get_slow_queries(limit)
        return [query.to_dict() for query in slow_queries]
    
    def get_system_metrics_history(self, hours: int = 24) -> List[Dict[str, Any]]:
        """获取系统指标历史"""
        cutoff_time = time.time() - hours * 3600
        history = []
        
        # 从队列中获取历史数据（简化实现）
        while not self.system_metrics_queue.empty():
            try:
                metrics = self.system_metrics_queue.get_nowait()
                if metrics.timestamp >= cutoff_time:
                    history.append(metrics.to_dict())
            except queue.Empty:
                break
        
        return sorted(history, key=lambda x: x['timestamp'])
    
    def add_alert_callback(self, callback: Callable):
        """添加告警回调"""
        self.alert_manager.add_alert_callback(callback)
    
    def export_metrics(self) -> Dict[str, Any]:
        """导出所有指标"""
        return {
            'node_id': self.node_id,
            'timestamp': time.time(),
            'counters': dict(self.metric_collector.counters),
            'gauges': dict(self.metric_collector.gauges),
            'histograms': {
                name: self.metric_collector.get_histogram_stats(name.split('[')[0])
                for name in self.metric_collector.histograms.keys()
            },
            'timers': {
                name: self.metric_collector.get_timer_stats(name.split('[')[0])
                for name in self.metric_collector.timers.keys()
            }
        }
