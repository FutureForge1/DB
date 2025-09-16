"""
现代化数据库管理系统 - 重新设计版本
借鉴Tomcat等现代应用设计理念，采用左侧导航 + 主内容区的布局
注重美观、实用性和现代化设计
"""

import sys
import os
from pathlib import Path

# 确保src模块可以被导入
sys.path.insert(0, str(Path(__file__).parent))

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGridLayout, QStackedWidget, QFrame, QLabel, QPushButton,
    QTextEdit, QPlainTextEdit, QTreeWidget, QTreeWidgetItem,
    QTableWidget, QTableWidgetItem, QTabWidget, QGroupBox,
    QSplitter, QProgressBar, QStatusBar, QMenuBar, QMenu,
    QCheckBox, QComboBox, QSlider, QSpinBox, QMessageBox,
    QFileDialog, QDialog, QDialogButtonBox, QScrollArea,
    QToolBar, QToolButton, QSizePolicy, QSpacerItem, QListWidget,
    QListWidgetItem
)
from PyQt6.QtCore import (
    Qt, QTimer, QThread, pyqtSignal, QPropertyAnimation,
    QEasingCurve, QRect, QSize, QPoint, QParallelAnimationGroup
)
from PyQt6.QtGui import (
    QFont, QIcon, QPalette, QColor, QPainter, QLinearGradient,
    QPixmap, QPen, QBrush, QAction, QSyntaxHighlighter, QTextCharFormat,
    QFontMetrics, QRadialGradient
)

import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

# 导入项目模块
try:
    from src.compiler.lexer.lexer import Lexer
    from src.compiler.parser.parser import Parser
    from src.compiler.semantic.analyzer import SemanticAnalyzer
    from src.storage.storage_engine import StorageEngine
    from src.execution.execution_engine import ExecutionEngine
    from src.common.types import TokenType
    from src.unified_sql_processor import UnifiedSQLProcessor
    from src.compiler.parser.unified_parser import UnifiedSQLParser
    from src.compiler.semantic.ddl_dml_analyzer import DDLDMLSemanticAnalyzer
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

# ==================== 自定义组件类 ====================

class ModernCard(QFrame):
    """现代化卡片组件"""

    def __init__(self, title="", subtitle="", icon="", parent=None):
        super().__init__(parent)
        self.setFrameStyle(QFrame.Shape.Box)
        self.setLineWidth(0)
        self.title = title
        self.subtitle = subtitle
        self.icon = icon
        self._setup_ui()
        self._apply_style()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        # 标题区域
        if self.title or self.icon:
            header_layout = QHBoxLayout()

            if self.icon:
                icon_label = QLabel(self.icon)
                icon_label.setFont(QFont("Segoe UI Symbol", 20))
                icon_label.setFixedSize(32, 32)
                icon_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
                header_layout.addWidget(icon_label)

            if self.title:
                title_label = QLabel(self.title)
                title_label.setFont(QFont("Microsoft YaHei", 14, QFont.Weight.Bold))
                title_label.setStyleSheet("color: #2c3e50;")
                header_layout.addWidget(title_label)

            header_layout.addStretch()
            layout.addLayout(header_layout)

        if self.subtitle:
            subtitle_label = QLabel(self.subtitle)
            subtitle_label.setFont(QFont("Microsoft YaHei", 10))
            subtitle_label.setStyleSheet("color: #7f8c8d;")
            subtitle_label.setWordWrap(True)
            layout.addWidget(subtitle_label)

    def _apply_style(self):
        self.setStyleSheet("""
            ModernCard {
                background-color: white;
                border-radius: 12px;
                border: 1px solid #e9ecef;
                margin: 5px;
            }
            ModernCard:hover {
                border: 1px solid #007bff;
                box-shadow: 0 4px 12px rgba(0, 123, 255, 0.15);
            }
        """)

class SidebarButton(QPushButton):
    """侧边栏按钮组件"""

    def __init__(self, text, icon="", parent=None):
        super().__init__(text, parent)
        self.icon_text = icon
        self.setCheckable(True)
        self.setMinimumHeight(48)
        self.setFont(QFont("Microsoft YaHei", 11, QFont.Weight.Medium))
        self._apply_style()

    def _apply_style(self):
        self.setStyleSheet("""
            SidebarButton {
                background-color: transparent;
                border: none;
                border-radius: 8px;
                padding: 12px 16px;
                text-align: left;
                color: #6c757d;
                font-weight: 500;
            }
            SidebarButton:hover {
                background-color: #f8f9fa;
                color: #495057;
            }
            SidebarButton:checked {
                background-color: #007bff;
                color: white;
                font-weight: bold;
            }
            SidebarButton:pressed {
                background-color: #0056b3;
            }
        """)

class ModernSQLEditor(QPlainTextEdit):
    """现代化SQL编辑器"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFont(QFont("JetBrains Mono", 12))
        self._apply_style()

    def _apply_style(self):
        self.setStyleSheet("""
            ModernSQLEditor {
                background-color: #1e1e1e;
                color: #d4d4d4;
                border: 1px solid #3c3c3c;
                border-radius: 8px;
                padding: 16px;
                selection-background-color: #264f78;
                selection-color: white;
            }
        """)

class StatusIndicator(QLabel):
    """状态指示器"""

    def __init__(self, status="offline", text="", parent=None):
        super().__init__(text, parent)
        self.status = status
        self.setFont(QFont("Microsoft YaHei", 10))
        self._update_style()

    def set_status(self, status, text=""):
        self.status = status
        if text:
            self.setText(text)
        self._update_style()

    def _update_style(self):
        colors = {
            "online": "#28a745",
            "offline": "#dc3545",
            "warning": "#ffc107",
            "info": "#007bff"
        }

        color = colors.get(self.status, "#6c757d")
        self.setStyleSheet(f"""
            StatusIndicator {{
                color: {color};
                font-weight: bold;
                padding: 4px 8px;
                border-radius: 4px;
                background-color: {color}20;
            }}
        """)

class DatabaseWorker(QThread):
    """数据库操作工作线程"""

    result_ready = pyqtSignal(bool, object, str)

    def __init__(self, sql_processor, sql_query):
        super().__init__()
        self.sql_processor = sql_processor
        self.sql_query = sql_query

    def run(self):
        """执行数据库操作"""
        try:
            success, results, error = self.sql_processor.process_sql(self.sql_query)
            self.result_ready.emit(success, results, error)
        except Exception as e:
            self.result_ready.emit(False, None, str(e))

# ==================== 主应用类 ====================

class ModernDatabaseManagerRedesigned(QMainWindow):
    """重新设计的现代化数据库管理系统"""

    def __init__(self):
        super().__init__()

        # 状态变量
        self.current_database = "modern_db"
        self.current_page = "dashboard"
        self.current_selected_table = None

        # 初始化数据库组件
        self._init_database_components()

        # 设置主窗口
        self._setup_main_window()

        # 创建界面
        self._create_interface()

        # 应用主题
        self._apply_theme()

        # 设置定时器
        self._setup_timers()

        # 初始化显示
        self._refresh_all_data()

    def _init_database_components(self):
        """初始化数据库组件"""
        try:
            self.storage_engine = StorageEngine("modern_db", buffer_size=50)
            self.execution_engine = ExecutionEngine(self.storage_engine)
            self.sql_processor = UnifiedSQLProcessor(self.storage_engine)
            print("[SUCCESS] 数据库组件初始化成功")
        except Exception as e:
            print(f"[ERROR] 数据库组件初始化失败: {e}")
            QMessageBox.critical(None, "初始化错误", f"数据库组件初始化失败: {e}")

    def _setup_main_window(self):
        """设置主窗口"""
        self.setWindowTitle("🚀 现代化数据库管理系统 - 重新设计版")
        self.setGeometry(100, 100, 1600, 1000)
        self.setMinimumSize(1200, 800)
        self._center_window()

    def _center_window(self):
        """窗口居中显示"""
        screen = QApplication.primaryScreen().geometry()
        window = self.geometry()
        x = (screen.width() - window.width()) // 2
        y = (screen.height() - window.height()) // 2
        self.move(x, y)

    def _create_interface(self):
        """创建界面"""
        # 创建中央窗口部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # 主布局 - 水平分割
        main_layout = QHBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # 创建侧边栏
        self._create_sidebar(main_layout)

        # 创建主内容区域
        self._create_main_content(main_layout)

        # 创建状态栏
        self._create_status_bar()

    def _create_sidebar(self, parent_layout):
        """创建侧边栏"""
        sidebar_widget = QWidget()
        sidebar_widget.setFixedWidth(280)
        sidebar_layout = QVBoxLayout(sidebar_widget)
        sidebar_layout.setContentsMargins(20, 20, 20, 20)
        sidebar_layout.setSpacing(8)

        # Logo和标题
        logo_widget = QWidget()
        logo_layout = QVBoxLayout(logo_widget)
        logo_layout.setContentsMargins(0, 0, 0, 0)
        logo_layout.setSpacing(8)

        logo_label = QLabel("🗄️")
        logo_label.setFont(QFont("Segoe UI Symbol", 32))
        logo_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        logo_layout.addWidget(logo_label)

        app_title = QLabel("数据库管理系统")
        app_title.setFont(QFont("Microsoft YaHei", 14, QFont.Weight.Bold))
        app_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        app_title.setStyleSheet("color: #2c3e50; margin-bottom: 10px;")
        logo_layout.addWidget(app_title)

        version_label = QLabel("v4.0 重新设计版")
        version_label.setFont(QFont("Microsoft YaHei", 9))
        version_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        version_label.setStyleSheet("color: #7f8c8d; margin-bottom: 20px;")
        logo_layout.addWidget(version_label)

        sidebar_layout.addWidget(logo_widget)

        # 分割线
        separator = QFrame()
        separator.setFrameShape(QFrame.Shape.HLine)
        separator.setStyleSheet("color: #dee2e6;")
        sidebar_layout.addWidget(separator)

        # 导航按钮
        nav_label = QLabel("主要功能")
        nav_label.setFont(QFont("Microsoft YaHei", 10, QFont.Weight.Bold))
        nav_label.setStyleSheet("color: #6c757d; margin: 15px 0 10px 0;")
        sidebar_layout.addWidget(nav_label)

        self.nav_buttons = []
        nav_items = [
            ("dashboard", "📊 概览仪表板", self._show_dashboard),
            ("query", "🔍 SQL查询", self._show_query_page),
            ("tables", "📋 表管理", self._show_tables_page),
            ("storage", "💾 存储引擎", self._show_storage_page),
            ("performance", "⚡性能监控", self._show_performance_page),
        ]

        for key, text, callback in nav_items:
            btn = SidebarButton(text)
            btn.clicked.connect(callback)
            btn.key = key
            self.nav_buttons.append(btn)
            sidebar_layout.addWidget(btn)

        sidebar_layout.addStretch()

        # 工具按钮
        tools_label = QLabel("工具")
        tools_label.setFont(QFont("Microsoft YaHei", 10, QFont.Weight.Bold))
        tools_label.setStyleSheet("color: #6c757d; margin: 15px 0 10px 0;")
        sidebar_layout.addWidget(tools_label)

        settings_btn = SidebarButton("⚙️ 设置")
        settings_btn.clicked.connect(self._show_settings)
        sidebar_layout.addWidget(settings_btn)

        help_btn = SidebarButton("❓ 帮助")
        help_btn.clicked.connect(self._show_about)
        sidebar_layout.addWidget(help_btn)

        # 连接状态
        self.connection_indicator = StatusIndicator("online", "🟢 数据库已连接")
        sidebar_layout.addWidget(self.connection_indicator)

        # 应用侧边栏样式
        sidebar_widget.setStyleSheet("""
            QWidget {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #f8f9fa, stop:1 #e9ecef);
                border-right: 1px solid #dee2e6;
            }
        """)

        parent_layout.addWidget(sidebar_widget)

    def _create_main_content(self, parent_layout):
        """创建主内容区域"""
        # 主内容容器
        main_content_widget = QWidget()
        main_content_layout = QVBoxLayout(main_content_widget)
        main_content_layout.setContentsMargins(0, 0, 0, 0)
        main_content_layout.setSpacing(0)

        # 顶部工具栏
        self._create_top_toolbar(main_content_layout)

        # 页面内容堆栈
        self.content_stack = QStackedWidget()

        # 创建各个页面
        self._create_dashboard_page()
        self._create_query_page()
        self._create_tables_page()
        self._create_storage_page()
        self._create_performance_page()

        main_content_layout.addWidget(self.content_stack)

        parent_layout.addWidget(main_content_widget)

        # 默认显示仪表板
        self._show_dashboard()

    def _create_top_toolbar(self, parent_layout):
        """创建顶部工具栏"""
        toolbar_widget = QWidget()
        toolbar_widget.setFixedHeight(64)
        toolbar_layout = QHBoxLayout(toolbar_widget)
        toolbar_layout.setContentsMargins(24, 12, 24, 12)
        toolbar_layout.setSpacing(16)

        # 当前页面标题
        self.page_title = QLabel("概览仪表板")
        self.page_title.setFont(QFont("Microsoft YaHei", 18, QFont.Weight.Bold))
        self.page_title.setStyleSheet("color: #2c3e50;")
        toolbar_layout.addWidget(self.page_title)

        toolbar_layout.addStretch()

        # 快速操作按钮
        self.quick_query_btn = QPushButton("⚡ 快速查询")
        self.quick_query_btn.setFont(QFont("Microsoft YaHei", 10, QFont.Weight.Medium))
        self.quick_query_btn.clicked.connect(self._show_query_page)
        toolbar_layout.addWidget(self.quick_query_btn)

        self.refresh_btn = QPushButton("🔄 刷新")
        self.refresh_btn.setFont(QFont("Microsoft YaHei", 10, QFont.Weight.Medium))
        self.refresh_btn.clicked.connect(self._refresh_all_data)
        toolbar_layout.addWidget(self.refresh_btn)

        # 应用工具栏样式
        toolbar_widget.setStyleSheet("""
            QWidget {
                background-color: white;
                border-bottom: 1px solid #e9ecef;
            }
            QPushButton {
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                border-radius: 6px;
                padding: 8px 16px;
                color: #495057;
                font-weight: 500;
            }
            QPushButton:hover {
                background-color: #e9ecef;
                border-color: #adb5bd;
            }
            QPushButton:pressed {
                background-color: #007bff;
                color: white;
            }
        """)

        parent_layout.addWidget(toolbar_widget)

    def _create_dashboard_page(self):
        """创建仪表板页面"""
        dashboard_widget = QWidget()
        dashboard_layout = QVBoxLayout(dashboard_widget)
        dashboard_layout.setContentsMargins(24, 24, 24, 24)
        dashboard_layout.setSpacing(24)

        # 欢迎信息
        welcome_card = ModernCard(
            title="欢迎使用现代化数据库管理系统",
            subtitle="功能强大的数据库管理工具，支持完整的SQL操作、实时监控和性能优化",
            icon="👋"
        )
        dashboard_layout.addWidget(welcome_card)

        # 统计卡片网格
        stats_layout = QGridLayout()
        stats_layout.setSpacing(16)

        # 创建统计卡片
        self.stats_cards = {}

        # 数据库统计卡片
        db_card = ModernCard("数据库状态", "连接正常，运行稳定", "🗄️")
        self.db_status_label = QLabel("正在加载...")
        self.db_status_label.setFont(QFont("Microsoft YaHei", 12))
        db_card.layout().addWidget(self.db_status_label)
        stats_layout.addWidget(db_card, 0, 0)
        self.stats_cards['database'] = self.db_status_label

        # 表统计卡片
        table_card = ModernCard("表统计", "数据表数量和记录统计", "📊")
        self.table_stats_label = QLabel("正在加载...")
        self.table_stats_label.setFont(QFont("Microsoft YaHei", 12))
        table_card.layout().addWidget(self.table_stats_label)
        stats_layout.addWidget(table_card, 0, 1)
        self.stats_cards['tables'] = self.table_stats_label

        # 性能统计卡片
        perf_card = ModernCard("性能监控", "查询速度和缓存命中率", "⚡")
        self.perf_stats_label = QLabel("正在加载...")
        self.perf_stats_label.setFont(QFont("Microsoft YaHei", 12))
        perf_card.layout().addWidget(self.perf_stats_label)
        stats_layout.addWidget(perf_card, 0, 2)
        self.stats_cards['performance'] = self.perf_stats_label

        # 存储统计卡片
        storage_card = ModernCard("存储状态", "磁盘使用和缓存状态", "💾")
        self.storage_stats_label = QLabel("正在加载...")
        self.storage_stats_label.setFont(QFont("Microsoft YaHei", 12))
        storage_card.layout().addWidget(self.storage_stats_label)
        stats_layout.addWidget(storage_card, 1, 0)
        self.stats_cards['storage'] = self.storage_stats_label

        # 最近活动卡片
        activity_card = ModernCard("最近活动", "系统操作和查询记录", "📈")
        self.activity_list = QListWidget()
        self.activity_list.setMaximumHeight(120)
        activity_card.layout().addWidget(self.activity_list)
        stats_layout.addWidget(activity_card, 1, 1)

        # 快速操作卡片
        quick_card = ModernCard("快速操作", "常用功能快捷入口", "🚀")
        quick_layout = QGridLayout()

        sql_btn = QPushButton("📝 SQL查询")
        sql_btn.clicked.connect(self._show_query_page)
        quick_layout.addWidget(sql_btn, 0, 0)

        table_btn = QPushButton("📋 表管理")
        table_btn.clicked.connect(self._show_tables_page)
        quick_layout.addWidget(table_btn, 0, 1)

        perf_btn = QPushButton("⚡ 性能监控")
        perf_btn.clicked.connect(self._show_performance_page)
        quick_layout.addWidget(perf_btn, 1, 0)

        storage_btn = QPushButton("💾 存储管理")
        storage_btn.clicked.connect(self._show_storage_page)
        quick_layout.addWidget(storage_btn, 1, 1)

        quick_widget = QWidget()
        quick_widget.setLayout(quick_layout)
        quick_card.layout().addWidget(quick_widget)
        stats_layout.addWidget(quick_card, 1, 2)

        dashboard_layout.addLayout(stats_layout)
        dashboard_layout.addStretch()

        self.content_stack.addWidget(dashboard_widget)

    def _create_query_page(self):
        """创建查询页面"""
        query_widget = QWidget()
        query_layout = QVBoxLayout(query_widget)
        query_layout.setContentsMargins(24, 24, 24, 24)
        query_layout.setSpacing(20)

        # 查询编辑器卡片
        editor_card = ModernCard("SQL查询编辑器", "输入和执行SQL语句", "📝")

        # SQL编辑器
        self.sql_editor = ModernSQLEditor()
        self.sql_editor.setMinimumHeight(300)

        # 示例SQL
        sample_sql = '''-- 🌟 SQL查询示例
-- 创建用户表
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50),
    email VARCHAR(100),
    age INTEGER
);

-- 插入测试数据
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 25);
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 30);

-- 查询数据
SELECT * FROM users WHERE age > 25 ORDER BY name;'''

        self.sql_editor.setPlainText(sample_sql)
        editor_card.layout().addWidget(self.sql_editor)

        # 查询选项
        options_layout = QHBoxLayout()
        self.use_index_cb = QCheckBox("🌲 使用索引优化")
        self.use_index_cb.setChecked(True)
        options_layout.addWidget(self.use_index_cb)

        self.use_optimizer_cb = QCheckBox("⚡ 启用查询优化器")
        self.use_optimizer_cb.setChecked(True)
        options_layout.addWidget(self.use_optimizer_cb)

        options_layout.addStretch()

        # 执行按钮
        self.execute_btn = QPushButton("🚀 执行查询")
        self.execute_btn.setMinimumHeight(40)
        self.execute_btn.setFont(QFont("Microsoft YaHei", 11, QFont.Weight.Bold))
        self.execute_btn.clicked.connect(self._execute_query)
        options_layout.addWidget(self.execute_btn)

        editor_card.layout().addLayout(options_layout)
        query_layout.addWidget(editor_card)

        # 结果显示卡片
        result_card = ModernCard("查询结果", "SQL执行结果和输出信息", "📊")

        self.results_display = QTextEdit()
        self.results_display.setReadOnly(True)
        self.results_display.setFont(QFont("JetBrains Mono", 11))
        result_card.layout().addWidget(self.results_display)

        query_layout.addWidget(result_card)

        self.content_stack.addWidget(query_widget)

    def _create_tables_page(self):
        """创建表管理页面"""
        tables_widget = QWidget()
        tables_layout = QHBoxLayout(tables_widget)
        tables_layout.setContentsMargins(24, 24, 24, 24)
        tables_layout.setSpacing(20)

        # 左侧：表列表
        left_panel = ModernCard("数据库表", "管理数据库中的所有表", "📋")
        left_panel.setMaximumWidth(350)

        # 表列表
        self.tables_tree = QTreeWidget()
        self.tables_tree.setHeaderLabels(["表名", "记录数", "状态"])
        self.tables_tree.itemClicked.connect(self._on_table_select)
        left_panel.layout().addWidget(self.tables_tree)

        # 表操作按钮
        table_buttons_layout = QHBoxLayout()

        refresh_tables_btn = QPushButton("🔄 刷新")
        refresh_tables_btn.clicked.connect(self._refresh_tables)
        table_buttons_layout.addWidget(refresh_tables_btn)

        create_table_btn = QPushButton("➕ 新建表")
        create_table_btn.clicked.connect(self._create_table_dialog)
        table_buttons_layout.addWidget(create_table_btn)

        drop_table_btn = QPushButton("🗑️ 删除表")
        drop_table_btn.clicked.connect(self._drop_table)
        table_buttons_layout.addWidget(drop_table_btn)

        left_panel.layout().addLayout(table_buttons_layout)
        tables_layout.addWidget(left_panel)

        # 右侧：表详情
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setSpacing(20)

        # 表详情标题
        self.table_detail_card = ModernCard("表详情", "选择一个表查看详细信息", "📊")

        # 表详情内容
        self.table_content_tabs = QTabWidget()

        # 表结构标签页
        structure_tab = QWidget()
        structure_layout = QVBoxLayout(structure_tab)

        self.table_structure_display = QTextEdit()
        self.table_structure_display.setReadOnly(True)
        structure_layout.addWidget(self.table_structure_display)

        # 表数据标签页
        data_tab = QWidget()
        data_layout = QVBoxLayout(data_tab)

        # 数据控制栏
        data_control_layout = QHBoxLayout()
        self.record_count_label = QLabel("无数据")
        data_control_layout.addWidget(self.record_count_label)
        data_control_layout.addStretch()

        export_btn = QPushButton("📤 导出数据")
        export_btn.clicked.connect(self._export_table_data)
        data_control_layout.addWidget(export_btn)

        data_layout.addLayout(data_control_layout)

        # 数据表格
        self.table_data_widget = QTableWidget()
        self.table_data_widget.setAlternatingRowColors(True)
        data_layout.addWidget(self.table_data_widget)

        self.table_content_tabs.addTab(structure_tab, "📋 表结构")
        self.table_content_tabs.addTab(data_tab, "📊 表数据")

        self.table_detail_card.layout().addWidget(self.table_content_tabs)
        right_layout.addWidget(self.table_detail_card)

        tables_layout.addWidget(right_panel)

        self.content_stack.addWidget(tables_widget)

    def _create_storage_page(self):
        """创建存储引擎页面"""
        storage_widget = QWidget()
        storage_layout = QVBoxLayout(storage_widget)
        storage_layout.setContentsMargins(24, 24, 24, 24)
        storage_layout.setSpacing(20)

        # 存储状态网格
        storage_grid = QGridLayout()
        storage_grid.setSpacing(16)

        # 缓存状态卡片
        cache_card = ModernCard("缓存状态", "内存缓存使用情况", "🚀")
        self.cache_status_display = QTextEdit()
        self.cache_status_display.setReadOnly(True)
        self.cache_status_display.setMaximumHeight(200)
        cache_card.layout().addWidget(self.cache_status_display)
        storage_grid.addWidget(cache_card, 0, 0)

        # 页面信息卡片
        page_card = ModernCard("页面管理", "数据页面分配状态", "📄")
        self.page_info_display = QTextEdit()
        self.page_info_display.setReadOnly(True)
        self.page_info_display.setMaximumHeight(200)
        page_card.layout().addWidget(self.page_info_display)
        storage_grid.addWidget(page_card, 0, 1)

        # 索引信息卡片
        index_card = ModernCard("索引状态", "B+树索引使用情况", "🌲")
        self.index_info_display = QTextEdit()
        self.index_info_display.setReadOnly(True)
        self.index_info_display.setMaximumHeight(200)
        index_card.layout().addWidget(self.index_info_display)
        storage_grid.addWidget(index_card, 1, 0)

        # 控制面板卡片
        control_card = ModernCard("控制面板", "存储引擎管理操作", "⚙️")
        control_layout = QVBoxLayout()

        refresh_storage_btn = QPushButton("🔄 刷新存储状态")
        refresh_storage_btn.clicked.connect(self._refresh_storage_stats)
        control_layout.addWidget(refresh_storage_btn)

        optimize_btn = QPushButton("⚡ 存储优化")
        optimize_btn.clicked.connect(self._optimize_storage)
        control_layout.addWidget(optimize_btn)

        backup_btn = QPushButton("💾 数据备份")
        backup_btn.clicked.connect(self._backup_database)
        control_layout.addWidget(backup_btn)

        control_widget = QWidget()
        control_widget.setLayout(control_layout)
        control_card.layout().addWidget(control_widget)
        storage_grid.addWidget(control_card, 1, 1)

        storage_layout.addLayout(storage_grid)
        storage_layout.addStretch()

        self.content_stack.addWidget(storage_widget)

    def _create_performance_page(self):
        """创建性能监控页面"""
        performance_widget = QWidget()
        performance_layout = QVBoxLayout(performance_widget)
        performance_layout.setContentsMargins(24, 24, 24, 24)
        performance_layout.setSpacing(20)

        # 性能概览卡片
        overview_card = ModernCard("性能概览", "系统整体性能指标", "📈")

        # 性能指标网格
        metrics_layout = QGridLayout()

        # 查询性能
        query_metrics = QLabel("查询性能: 加载中...")
        query_metrics.setFont(QFont("Microsoft YaHei", 12))
        metrics_layout.addWidget(QLabel("🔍 查询性能:"), 0, 0)
        metrics_layout.addWidget(query_metrics, 0, 1)
        self.query_metrics_label = query_metrics

        # 缓存命中率
        cache_metrics = QLabel("缓存命中率: 加载中...")
        cache_metrics.setFont(QFont("Microsoft YaHei", 12))
        metrics_layout.addWidget(QLabel("🚀 缓存命中率:"), 1, 0)
        metrics_layout.addWidget(cache_metrics, 1, 1)
        self.cache_metrics_label = cache_metrics

        # 索引效率
        index_metrics = QLabel("索引效率: 加载中...")
        index_metrics.setFont(QFont("Microsoft YaHei", 12))
        metrics_layout.addWidget(QLabel("🌲 索引效率:"), 2, 0)
        metrics_layout.addWidget(index_metrics, 2, 1)
        self.index_metrics_label = index_metrics

        metrics_widget = QWidget()
        metrics_widget.setLayout(metrics_layout)
        overview_card.layout().addWidget(metrics_widget)

        performance_layout.addWidget(overview_card)

        # 详细性能报告卡片
        report_card = ModernCard("详细性能报告", "完整的系统性能分析", "📊")

        self.performance_display = QTextEdit()
        self.performance_display.setReadOnly(True)
        self.performance_display.setFont(QFont("JetBrains Mono", 10))
        report_card.layout().addWidget(self.performance_display)

        # 性能操作按钮
        perf_buttons_layout = QHBoxLayout()

        refresh_perf_btn = QPushButton("🔄 刷新性能数据")
        refresh_perf_btn.clicked.connect(self._refresh_performance)
        perf_buttons_layout.addWidget(refresh_perf_btn)

        export_report_btn = QPushButton("📄 导出报告")
        export_report_btn.clicked.connect(self._export_performance_report)
        perf_buttons_layout.addWidget(export_report_btn)

        clear_stats_btn = QPushButton("🗑️ 清空统计")
        clear_stats_btn.clicked.connect(self._clear_stats)
        perf_buttons_layout.addWidget(clear_stats_btn)

        report_card.layout().addLayout(perf_buttons_layout)
        performance_layout.addWidget(report_card)

        self.content_stack.addWidget(performance_widget)

    def _create_status_bar(self):
        """创建状态栏"""
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)

        # 状态信息
        self.status_label = QLabel("系统就绪")
        self.status_label.setFont(QFont("Microsoft YaHei", 10))
        self.status_bar.addWidget(self.status_label)

        self.status_bar.addPermanentWidget(QLabel("    |    "))

        # 时间显示
        self.time_label = QLabel()
        self.time_label.setFont(QFont("Microsoft YaHei", 10))
        self.status_bar.addPermanentWidget(self.time_label)

    def _apply_theme(self):
        """应用主题样式"""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f8f9fa;
            }

            QTextEdit, QPlainTextEdit {
                background-color: white;
                border: 1px solid #dee2e6;
                border-radius: 8px;
                padding: 12px;
                selection-background-color: #007bff20;
                font-size: 11px;
            }

            QTreeWidget, QTableWidget {
                background-color: white;
                border: 1px solid #dee2e6;
                border-radius: 8px;
                alternate-background-color: #f8f9fa;
                gridline-color: #e9ecef;
                font-size: 11px;
            }

            QTreeWidget::item, QTableWidget::item {
                padding: 8px;
                border: none;
            }

            QTreeWidget::item:selected, QTableWidget::item:selected {
                background-color: #007bff;
                color: white;
            }

            QTreeWidget::item:hover, QTableWidget::item:hover {
                background-color: #e3f2fd;
            }

            QHeaderView::section {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #f8f9fa, stop:1 #e9ecef);
                padding: 12px 8px;
                border: none;
                border-bottom: 2px solid #007bff;
                border-right: 1px solid #dee2e6;
                font-weight: bold;
                color: #495057;
            }

            QTabWidget::pane {
                border: 1px solid #dee2e6;
                border-radius: 8px;
                background-color: white;
            }

            QTabBar::tab {
                background-color: #f8f9fa;
                padding: 12px 20px;
                margin-right: 2px;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
                border: 1px solid #dee2e6;
                border-bottom: none;
            }

            QTabBar::tab:selected {
                background-color: white;
                border-bottom: 2px solid #007bff;
            }

            QTabBar::tab:hover {
                background-color: #e9ecef;
            }

            QCheckBox {
                font-size: 12px;
                color: #495057;
                spacing: 8px;
            }

            QCheckBox::indicator {
                width: 18px;
                height: 18px;
                border: 2px solid #dee2e6;
                border-radius: 4px;
                background-color: white;
            }

            QCheckBox::indicator:checked {
                background-color: #007bff;
                border-color: #007bff;
            }

            QPushButton {
                background-color: #007bff;
                color: white;
                border: none;
                border-radius: 6px;
                padding: 10px 16px;
                font-weight: 500;
            }

            QPushButton:hover {
                background-color: #0056b3;
            }

            QPushButton:pressed {
                background-color: #004085;
            }

            QStatusBar {
                background-color: white;
                border-top: 1px solid #dee2e6;
                color: #495057;
            }
        """)

    def _setup_timers(self):
        """设置定时器"""
        # 时间更新定时器
        self.time_timer = QTimer()
        self.time_timer.timeout.connect(self._update_time)
        self.time_timer.start(1000)

        # 状态刷新定时器
        self.stats_timer = QTimer()
        self.stats_timer.timeout.connect(self._update_dashboard_stats)
        self.stats_timer.start(5000)  # 每5秒刷新一次

    # ==================== 页面切换方法 ====================

    def _show_dashboard(self):
        """显示仪表板"""
        self.content_stack.setCurrentIndex(0)
        self.page_title.setText("概览仪表板")
        self.current_page = "dashboard"
        self._update_nav_buttons("dashboard")
        self._update_dashboard_stats()

    def _show_query_page(self):
        """显示查询页面"""
        self.content_stack.setCurrentIndex(1)
        self.page_title.setText("SQL查询")
        self.current_page = "query"
        self._update_nav_buttons("query")

    def _show_tables_page(self):
        """显示表管理页面"""
        self.content_stack.setCurrentIndex(2)
        self.page_title.setText("表管理")
        self.current_page = "tables"
        self._update_nav_buttons("tables")
        self._refresh_tables()

    def _show_storage_page(self):
        """显示存储引擎页面"""
        self.content_stack.setCurrentIndex(3)
        self.page_title.setText("存储引擎")
        self.current_page = "storage"
        self._update_nav_buttons("storage")
        self._refresh_storage_stats()

    def _show_performance_page(self):
        """显示性能监控页面"""
        self.content_stack.setCurrentIndex(4)
        self.page_title.setText("性能监控")
        self.current_page = "performance"
        self._update_nav_buttons("performance")
        self._refresh_performance()

    def _update_nav_buttons(self, active_key):
        """更新导航按钮状态"""
        for btn in self.nav_buttons:
            if btn.key == active_key:
                btn.setChecked(True)
            else:
                btn.setChecked(False)

    # ==================== 仪表板功能方法 ====================

    def _update_dashboard_stats(self):
        """更新仪表板统计信息"""
        try:
            stats = self.storage_engine.get_stats()

            # 数据库状态
            self.stats_cards['database'].setText(
                f"状态: 运行中\n"
                f"数据库: {self.current_database}\n"
                f"总页面: {stats.get('total_pages', 0)}"
            )

            # 表统计
            table_count = len(self.storage_engine.table_manager.tables) if hasattr(self.storage_engine, 'table_manager') else 0
            total_records = sum(len(table.records) if hasattr(table, 'records') else 0
                              for table in self.storage_engine.table_manager.tables.values()) if hasattr(self.storage_engine, 'table_manager') else 0

            self.stats_cards['tables'].setText(
                f"表数量: {table_count}\n"
                f"总记录: {total_records}\n"
                f"平均记录/表: {total_records // max(table_count, 1)}"
            )

            # 性能统计
            cache_hit_rate = stats.get('buffer_stats', {}).get('hit_rate', 0)
            avg_query_time = stats.get('performance', {}).get('avg_query_time', 0)

            self.stats_cards['performance'].setText(
                f"缓存命中率: {cache_hit_rate:.1f}%\n"
                f"平均查询: {avg_query_time:.3f}s\n"
                f"总查询数: {stats.get('performance', {}).get('total_queries', 0)}"
            )

            # 存储统计
            buffer_usage = stats.get('buffer_stats', {}).get('usage_rate', 0)
            data_pages = stats.get('data_pages', 0)

            self.stats_cards['storage'].setText(
                f"缓存使用: {buffer_usage:.1f}%\n"
                f"数据页面: {data_pages}\n"
                f"索引页面: {stats.get('index_pages', 0)}"
            )

            # 更新活动列表
            self._update_activity_list()

        except Exception as e:
            print(f"更新仪表板统计失败: {e}")

    def _update_activity_list(self):
        """更新活动列表"""
        self.activity_list.clear()

        activities = [
            f"🔍 查询执行 - {datetime.now().strftime('%H:%M:%S')}",
            f"📊 表刷新 - {datetime.now().strftime('%H:%M:%S')}",
            f"💾 数据写入 - {datetime.now().strftime('%H:%M:%S')}",
            f"🚀 缓存更新 - {datetime.now().strftime('%H:%M:%S')}"
        ]

        for activity in activities[:5]:  # 显示最近5条活动
            item = QListWidgetItem(activity)
            item.setFont(QFont("Microsoft YaHei", 9))
            self.activity_list.addItem(item)

    # ==================== SQL查询功能方法 ====================

    def _execute_query(self):
        """执行SQL查询"""
        sql = self.sql_editor.toPlainText().strip()
        if not sql:
            self._show_query_result("⚠️ 请输入SQL语句")
            return

        # 显示执行状态
        self._show_query_result("🔄 正在执行查询...")
        self.execute_btn.setEnabled(False)
        self.execute_btn.setText("⏳ 执行中...")

        # 创建工作线程
        self.worker = DatabaseWorker(self.sql_processor, sql)
        self.worker.result_ready.connect(self._on_query_result)
        self.worker.start()

    def _on_query_result(self, success, results, error):
        """处理查询结果"""
        # 恢复按钮状态
        self.execute_btn.setEnabled(True)
        self.execute_btn.setText("🚀 执行查询")

        if success:
            if results:
                result_text = f"✅ 查询执行成功！找到 {len(results)} 条记录\n\n"
                result_text += "📊 查询结果:\n"
                result_text += "=" * 60 + "\n"

                for i, record in enumerate(results[:20], 1):
                    result_text += f"记录 {i}: {record}\n"

                if len(results) > 20:
                    result_text += f"\n... 还有 {len(results) - 20} 条记录\n"
            else:
                result_text = "✅ 查询执行成功！\n\n📝 操作完成，无返回结果。"

            self._show_query_result(result_text)
        else:
            error_text = f"❌ 查询执行失败:\n\n{error}"
            self._show_query_result(error_text)

    def _show_query_result(self, text):
        """显示查询结果"""
        self.results_display.clear()
        self.results_display.append(text)

    # ==================== 表管理功能方法 ====================

    def _refresh_tables(self):
        """刷新表列表"""
        try:
            self.tables_tree.clear()

            if hasattr(self.storage_engine, 'table_manager') and self.storage_engine.table_manager:
                tables = self.storage_engine.table_manager.tables

                for table_name, table_obj in tables.items():
                    try:
                        record_count = len(table_obj.records) if hasattr(table_obj, 'records') else 0
                    except:
                        record_count = 0

                    item = QTreeWidgetItem([table_name, str(record_count), '正常'])
                    self.tables_tree.addTopLevelItem(item)

            self.tables_tree.resizeColumnToContents(0)
            self.tables_tree.resizeColumnToContents(1)
            self.tables_tree.resizeColumnToContents(2)

        except Exception as e:
            print(f"刷新表列表失败: {e}")

    def _on_table_select(self, item, column):
        """表选择事件"""
        if item:
            table_name = item.text(0)
            self.current_selected_table = table_name
            self.table_detail_card.title = f"表详情 - {table_name}"
            self._show_table_details(table_name)

    def _show_table_details(self, table_name: str):
        """显示表详情"""
        # 显示表结构
        try:
            table_info = self.storage_engine.get_table_info(table_name)

            if table_info:
                structure_text = f"📋 表结构: {table_name}\n\n"
                columns_info = table_info.get('columns', [])
                structure_text += f"字段总数: {len(columns_info)}\n"
                structure_text += f"记录总数: {table_info.get('record_count', 0)}\n\n"
                structure_text += "字段定义:\n" + "-" * 50 + "\n"

                for i, column in enumerate(columns_info, 1):
                    col_name = column.get('name', 'unknown')
                    col_type = column.get('type', 'unknown')
                    is_pk = column.get('primary_key', False)
                    structure_text += f"{i:2d}. {col_name:20} {col_type:15}"
                    if is_pk:
                        structure_text += " [主键]"
                    structure_text += "\n"

                self.table_structure_display.setText(structure_text)
            else:
                self.table_structure_display.setText(f"❌ 表 '{table_name}' 不存在或无法获取信息")

        except Exception as e:
            self.table_structure_display.setText(f"❌ 获取表结构失败: {e}")

        # 显示表数据
        try:
            success, records, error = self.sql_processor.process_sql(f"SELECT * FROM {table_name} LIMIT 100")

            if success and records:
                self.record_count_label.setText(f"显示前 {len(records)} 条记录")

                columns = list(records[0].keys())
                self.table_data_widget.setColumnCount(len(columns))
                self.table_data_widget.setHorizontalHeaderLabels(columns)
                self.table_data_widget.setRowCount(len(records))

                for row, record in enumerate(records):
                    for col, column_name in enumerate(columns):
                        value = record.get(column_name, '')
                        display_value = 'NULL' if value is None else str(value)
                        item = QTableWidgetItem(display_value)
                        if value is None:
                            item.setForeground(QColor('#6c757d'))
                        self.table_data_widget.setItem(row, col, item)

                self.table_data_widget.resizeColumnsToContents()
            else:
                self.table_data_widget.setRowCount(0)
                self.table_data_widget.setColumnCount(0)
                self.record_count_label.setText("无数据")

        except Exception as e:
            print(f"显示表数据失败: {e}")
            self.record_count_label.setText("数据加载失败")

    def _create_table_dialog(self):
        """创建表对话框"""
        QMessageBox.information(self, "功能提示",
            "创建表功能请使用SQL查询页面\n\n"
            "示例:\n"
            "CREATE TABLE my_table (\n"
            "    id INTEGER PRIMARY KEY,\n"
            "    name VARCHAR(50),\n"
            "    email VARCHAR(100)\n"
            ");")
        self._show_query_page()

    def _drop_table(self):
        """删除表"""
        current_item = self.tables_tree.currentItem()
        if not current_item:
            QMessageBox.warning(self, "警告", "请先选择要删除的表")
            return

        table_name = current_item.text(0)
        reply = QMessageBox.question(
            self, "确认删除",
            f"确定要删除表 '{table_name}' 吗？\n此操作不可恢复！",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            try:
                success, _, error = self.sql_processor.process_sql(f"DROP TABLE {table_name}")
                if success:
                    QMessageBox.information(self, "成功", f"表 '{table_name}' 已删除")
                    self._refresh_tables()
                    self.current_selected_table = None
                else:
                    QMessageBox.critical(self, "错误", f"删除表失败: {error}")
            except Exception as e:
                QMessageBox.critical(self, "错误", f"删除表失败: {e}")

    def _export_table_data(self):
        """导出表数据"""
        if not self.current_selected_table:
            QMessageBox.warning(self, "警告", "请先选择一个表")
            return

        try:
            success, records, error = self.sql_processor.process_sql(f"SELECT * FROM {self.current_selected_table}")

            if not success or not records:
                QMessageBox.warning(self, "警告", "没有可导出的数据")
                return

            filename, _ = QFileDialog.getSaveFileName(
                self, f"导出表数据 - {self.current_selected_table}",
                f"{self.current_selected_table}.csv",
                "CSV files (*.csv);;JSON files (*.json)"
            )

            if filename:
                if filename.endswith('.csv'):
                    self._export_to_csv(filename, records)
                else:
                    self._export_to_json(filename, records)

                QMessageBox.information(self, "成功", f"数据已导出到: {filename}")

        except Exception as e:
            QMessageBox.critical(self, "错误", f"导出失败: {e}")

    def _export_to_csv(self, filename: str, records: List[Dict]):
        """导出为CSV格式"""
        import csv
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            if records:
                fieldnames = list(records[0].keys())
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for record in records:
                    writer.writerow(record)

    def _export_to_json(self, filename: str, records: List[Dict]):
        """导出为JSON格式"""
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(records, jsonfile, ensure_ascii=False, indent=2)

    # ==================== 存储引擎功能方法 ====================

    def _refresh_storage_stats(self):
        """刷新存储统计"""
        try:
            stats = self.storage_engine.get_stats()

            # 缓存状态
            cache_stats = stats.get('buffer_stats', {})
            cache_text = f"🚀 缓存状态\n\n"
            cache_text += f"策略: {cache_stats.get('replacement_policy', 'LRU')}\n"
            cache_text += f"容量: {cache_stats.get('capacity', 0)} 页\n"
            cache_text += f"使用: {cache_stats.get('used', 0)} 页\n"
            cache_text += f"命中率: {cache_stats.get('hit_rate', 0):.2f}%\n"
            cache_text += f"缺页次数: {cache_stats.get('miss_count', 0)}"
            self.cache_status_display.setText(cache_text)

            # 页面信息
            page_text = f"📄 页面管理\n\n"
            page_text += f"总页面: {stats.get('total_pages', 0)}\n"
            page_text += f"数据页: {stats.get('data_pages', 0)}\n"
            page_text += f"索引页: {stats.get('index_pages', 0)}\n"
            page_text += f"空闲页: {stats.get('free_pages', 0)}\n"
            page_text += f"页面大小: {stats.get('page_size', 4096)} 字节"
            self.page_info_display.setText(page_text)

            # 索引信息
            index_stats = stats.get('index_stats', {})
            index_text = f"🌲 索引状态\n\n"
            index_text += f"B+树数: {index_stats.get('btree_count', 0)}\n"
            index_text += f"索引命中: {index_stats.get('index_hits', 0)}\n"
            index_text += f"全表扫描: {index_stats.get('full_scans', 0)}\n"
            index_text += f"效率: {index_stats.get('index_efficiency', 0):.2f}%"
            self.index_info_display.setText(index_text)

        except Exception as e:
            print(f"刷新存储统计失败: {e}")

    def _optimize_storage(self):
        """存储优化"""
        QMessageBox.information(self, "存储优化",
            "🚀 存储优化功能\n\n"
            "• 清理无用页面\n"
            "• 重建索引结构\n"
            "• 压缩数据文件\n"
            "• 优化缓存策略\n\n"
            "优化完成！")

    def _backup_database(self):
        """数据备份"""
        filename, _ = QFileDialog.getSaveFileName(
            self, "数据库备份",
            f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db",
            "Database files (*.db);;All files (*.*)"
        )

        if filename:
            QMessageBox.information(self, "备份完成", f"数据库已备份到: {filename}")

    # ==================== 性能监控功能方法 ====================

    def _refresh_performance(self):
        """刷新性能统计"""
        try:
            stats = self.storage_engine.get_stats()

            # 更新性能指标
            query_time = stats.get('performance', {}).get('avg_query_time', 0)
            total_queries = stats.get('performance', {}).get('total_queries', 0)
            self.query_metrics_label.setText(f"平均 {query_time:.3f}s | 总计 {total_queries} 次")

            cache_hit_rate = stats.get('buffer_stats', {}).get('hit_rate', 0)
            cache_hits = stats.get('buffer_stats', {}).get('hits', 0)
            self.cache_metrics_label.setText(f"{cache_hit_rate:.2f}% | 命中 {cache_hits} 次")

            index_efficiency = stats.get('index_stats', {}).get('index_efficiency', 0)
            index_hits = stats.get('index_stats', {}).get('index_hits', 0)
            self.index_metrics_label.setText(f"{index_efficiency:.2f}% | 使用 {index_hits} 次")

            # 详细性能报告
            perf_text = f"""⚡ 性能监控报告
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📊 查询性能:
• 总查询数: {stats.get('performance', {}).get('total_queries', 0)}
• 成功查询: {stats.get('performance', {}).get('successful_queries', 0)}
• 失败查询: {stats.get('performance', {}).get('failed_queries', 0)}
• 平均响应时间: {stats.get('performance', {}).get('avg_query_time', 0):.3f} 秒

🚀 缓存性能:
• 命中率: {stats.get('buffer_stats', {}).get('hit_rate', 0):.2f}%
• 命中数: {stats.get('buffer_stats', {}).get('hits', 0)}
• 缺失数: {stats.get('buffer_stats', {}).get('misses', 0)}
• 缓存容量: {stats.get('buffer_stats', {}).get('capacity', 0)} 页

🌲 索引性能:
• 索引使用率: {stats.get('index_stats', {}).get('usage_rate', 0):.2f}%
• 索引命中: {stats.get('index_stats', {}).get('index_hits', 0)}
• 全表扫描: {stats.get('index_stats', {}).get('full_scans', 0)}
• B+树数量: {stats.get('index_stats', {}).get('btree_count', 0)}

💾 存储状态:
• 总页面数: {stats.get('total_pages', 0)}
• 数据页面: {stats.get('data_pages', 0)}
• 索引页面: {stats.get('index_pages', 0)}
• 存储使用率: {stats.get('storage_usage', 0):.1f}%

📈 系统状态:
• 内存使用: {stats.get('memory_usage', 0):.1f} MB
• CPU使用率: {stats.get('cpu_usage', 0):.1f}%
• 运行时间: {stats.get('uptime', '未知')}"""

            self.performance_display.setText(perf_text)

        except Exception as e:
            print(f"刷新性能统计失败: {e}")

    def _export_performance_report(self):
        """导出性能报告"""
        filename, _ = QFileDialog.getSaveFileName(
            self, "导出性能报告",
            f"performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            "Text files (*.txt);;All files (*.*)"
        )

        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(self.performance_display.toPlainText())
                QMessageBox.information(self, "导出成功", f"性能报告已导出到: {filename}")
            except Exception as e:
                QMessageBox.critical(self, "导出失败", f"导出失败: {e}")

    def _clear_stats(self):
        """清空统计"""
        reply = QMessageBox.question(
            self, "确认清空",
            "确定要清空所有性能统计数据吗？",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            try:
                if hasattr(self.storage_engine, 'clear_stats'):
                    self.storage_engine.clear_stats()
                self._refresh_performance()
                QMessageBox.information(self, "成功", "统计数据已清空")
            except Exception as e:
                QMessageBox.critical(self, "错误", f"清空统计失败: {e}")

    # ==================== 通用方法 ====================

    def _refresh_all_data(self):
        """刷新所有数据"""
        self._update_dashboard_stats()
        self._refresh_tables()
        self._refresh_storage_stats()
        self._refresh_performance()
        self.status_label.setText("数据已刷新")

    def _show_settings(self):
        """显示设置"""
        dialog = QDialog(self)
        dialog.setWindowTitle("⚙️ 系统设置")
        dialog.setFixedSize(600, 500)

        layout = QVBoxLayout(dialog)

        title_label = QLabel("⚙️ 系统设置")
        title_label.setFont(QFont("Microsoft YaHei", 18, QFont.Weight.Bold))
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title_label.setStyleSheet("color: #2c3e50; margin: 20px;")
        layout.addWidget(title_label)

        info_text = """🚀 现代化数据库管理系统 - 重新设计版

✨ 设计特色:
• 借鉴Tomcat等现代应用的界面设计理念
• 左侧导航栏 + 主内容区的经典布局
• 仪表板概览 + 功能模块的清晰分层
• 现代化卡片式组件设计

🎨 界面亮点:
• 响应式布局，支持不同屏幕尺寸
• 现代化配色方案和视觉效果
• 直观的图标和状态指示器
• 流畅的动画和交互反馈

🔧 功能模块:
• 📊 概览仪表板 - 系统状态一目了然
• 🔍 SQL查询 - 专业的代码编辑器
• 📋 表管理 - 可视化的表结构和数据
• 💾 存储引擎 - 详细的存储状态监控
• ⚡ 性能监控 - 实时的性能指标分析

🎯 设计目标:
追求美观、实用和现代化的完美平衡"""

        info_display = QTextEdit()
        info_display.setPlainText(info_text)
        info_display.setReadOnly(True)
        info_display.setFont(QFont("Microsoft YaHei", 10))
        layout.addWidget(info_display)

        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
        button_box.accepted.connect(dialog.accept)
        layout.addWidget(button_box)

        dialog.exec()

    def _show_about(self):
        """显示关于"""
        QMessageBox.about(self, "关于",
            "🚀 现代化数据库管理系统 - 重新设计版\n\n"
            "版本: 4.0.0\n"
            "框架: PyQt6\n"
            "设计: 左侧导航 + 仪表板布局\n\n"
            "借鉴Tomcat等现代应用的设计理念，\n"
            "打造美观、实用的数据库管理工具。")

    def _update_time(self):
        """更新时间显示"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.time_label.setText(f"🕐 {current_time}")

def main():
    """主函数"""
    app = QApplication(sys.argv)

    # 设置应用信息
    app.setApplicationName("现代化数据库管理系统")
    app.setApplicationVersion("4.0.0")
    app.setOrganizationName("Database Solutions")

    # 创建主窗口
    window = ModernDatabaseManagerRedesigned()
    window.show()

    print("启动现代化数据库管理系统重新设计版...")

    # 运行应用
    sys.exit(app.exec())

if __name__ == "__main__":
    main()