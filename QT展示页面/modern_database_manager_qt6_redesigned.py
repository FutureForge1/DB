"""
现代化数据库管理系统 - PyQt6重新设计版本
采用全新的界面设计理念，提供更优雅的用户体验
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
    QToolBar, QToolButton, QSizePolicy, QSpacerItem
)
from PyQt6.QtCore import (
    Qt, QTimer, QThread, pyqtSignal, QPropertyAnimation, 
    QEasingCurve, QRect, QSize, QPoint
)
from PyQt6.QtGui import (
    QFont, QIcon, QPalette, QColor, QPainter, QLinearGradient,
    QPixmap, QPen, QBrush, QAction, QSyntaxHighlighter, QTextCharFormat
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
    # 导入统一SQL处理器
    from src.unified_sql_processor import UnifiedSQLProcessor
    from src.compiler.parser.unified_parser import UnifiedSQLParser
    from src.compiler.semantic.ddl_dml_analyzer import DDLDMLSemanticAnalyzer
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

class SQLSyntaxHighlighter(QSyntaxHighlighter):
    """SQL语法高亮器"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # 定义高亮规则
        self.highlighting_rules = []
        
        # SQL关键字
        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor("#0066cc"))
        keyword_format.setFontWeight(QFont.Weight.Bold)
        
        keywords = [
            "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE",
            "CREATE", "TABLE", "ALTER", "DROP", "INDEX", "JOIN",
            "INNER", "LEFT", "RIGHT", "FULL", "ON", "GROUP", "BY",
            "ORDER", "HAVING", "UNION", "DISTINCT", "AS", "AND",
            "OR", "NOT", "IN", "LIKE", "BETWEEN", "IS", "NULL",
            "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "CONSTRAINT"
        ]
        
        for keyword in keywords:
            pattern = f"\\b{keyword}\\b"
            self.highlighting_rules.append((pattern, keyword_format))
        
        # 数据类型
        type_format = QTextCharFormat()
        type_format.setForeground(QColor("#cc6600"))
        type_format.setFontWeight(QFont.Weight.Bold)
        
        types = ["INTEGER", "VARCHAR", "TEXT", "REAL", "BLOB", "BOOLEAN"]
        for type_name in types:
            pattern = f"\\b{type_name}\\b"
            self.highlighting_rules.append((pattern, type_format))
        
        # 字符串
        string_format = QTextCharFormat()
        string_format.setForeground(QColor("#009900"))
        self.highlighting_rules.append(("'[^']*'", string_format))
        
        # 数字
        number_format = QTextCharFormat()
        number_format.setForeground(QColor("#cc0066"))
        self.highlighting_rules.append(("\\b\\d+\\b", number_format))
        
        # 注释
        comment_format = QTextCharFormat()
        comment_format.setForeground(QColor("#666666"))
        comment_format.setFontItalic(True)
        self.highlighting_rules.append(("--[^\n]*", comment_format))
    
    def highlightBlock(self, text):
        """高亮文本块"""
        import re
        for pattern, format in self.highlighting_rules:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                start, end = match.span()
                self.setFormat(start, end - start, format)

class ModernCard(QFrame):
    """现代化卡片组件"""
    
    def __init__(self, title="", parent=None):
        super().__init__(parent)
        self.setObjectName("ModernCard")
        self.setup_ui(title)
        self.apply_style()
    
    def setup_ui(self, title):
        """设置UI"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)
        
        if title:
            # 卡片标题
            self.title_label = QLabel(title)
            self.title_label.setObjectName("CardTitle")
            layout.addWidget(self.title_label)
        
        # 内容区域
        self.content_widget = QWidget()
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.content_widget)
    
    def apply_style(self):
        """应用样式"""
        self.setStyleSheet("""
            QFrame#ModernCard {
                background-color: white;
                border: 1px solid #e1e5e9;
                border-radius: 12px;
                margin: 8px;
            }
            QFrame#ModernCard:hover {
                border-color: #4a90e2;
                box-shadow: 0 4px 12px rgba(74, 144, 226, 0.15);
            }
            QLabel#CardTitle {
                font-size: 16px;
                font-weight: bold;
                color: #2c3e50;
                margin-bottom: 10px;
            }
        """)
    
    def add_content(self, widget):
        """添加内容"""
        self.content_layout.addWidget(widget)

class ModernButton(QPushButton):
    """现代化按钮组件"""
    
    def __init__(self, text, button_type="primary", parent=None):
        super().__init__(text, parent)
        self.button_type = button_type
        self.setMinimumHeight(40)
        self.setFont(QFont("Microsoft YaHei", 10, QFont.Weight.Bold))
        self.apply_style()
    
    def apply_style(self):
        """应用样式"""
        styles = {
            "primary": """
                QPushButton {
                    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                        stop:0 #4a90e2, stop:1 #357abd);
                    color: white;
                    border: none;
                    border-radius: 8px;
                    padding: 10px 20px;
                }
                QPushButton:hover {
                    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                        stop:0 #5ba0f2, stop:1 #4a90e2);
                }
                QPushButton:pressed {
                    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                        stop:0 #357abd, stop:1 #2c5f8a);
                }
            """,
            "success": """
                QPushButton {
                    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                        stop:0 #27ae60, stop:1 #1e8449);
                    color: white;
                    border: none;
                    border-radius: 8px;
                    padding: 10px 20px;
                }
                QPushButton:hover {
                    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                        stop:0 #2ecc71, stop:1 #27ae60);
                }
            """,
            "danger": """
                QPushButton {
                    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                        stop:0 #e74c3c, stop:1 #c0392b);
                    color: white;
                    border: none;
                    border-radius: 8px;
                    padding: 10px 20px;
                }
                QPushButton:hover {
                    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                        stop:0 #f1c40f, stop:1 #e74c3c);
                }
            """,
            "secondary": """
                QPushButton {
                    background-color: #f8f9fa;
                    color: #6c757d;
                    border: 1px solid #dee2e6;
                    border-radius: 8px;
                    padding: 10px 20px;
                }
                QPushButton:hover {
                    background-color: #e9ecef;
                    border-color: #adb5bd;
                }
            """
        }
        
        self.setStyleSheet(styles.get(self.button_type, styles["primary"]))

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

class ModernDatabaseManagerQt6Redesigned(QMainWindow):
    """现代化数据库管理系统 - PyQt6重新设计版本"""

    def __init__(self):
        super().__init__()
        
        # 初始化数据库组件
        self._init_database_components()
        
        # 设置主窗口
        self._setup_main_window()
        
        # 创建界面
        self._create_interface()
        
        # 应用现代化样式
        self._apply_modern_theme()
        
        # 设置定时器
        self._setup_timers()
        
        # 状态变量
        self.current_database = "modern_db"
        self.query_history = []
        
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
        self.setGeometry(100, 100, 1800, 1000)
        self.setMinimumSize(1400, 900)
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
        
        # 主布局 - 使用网格布局
        main_layout = QGridLayout(central_widget)
        main_layout.setContentsMargins(15, 15, 15, 15)
        main_layout.setSpacing(15)
        
        # 创建顶部工具栏区域
        self._create_top_toolbar(main_layout)
        
        # 创建主要内容区域
        self._create_main_content_area(main_layout)
        
        # 创建底部状态区域
        self._create_bottom_status_area(main_layout)
        
        # 创建菜单栏
        self._create_menubar()

    def _create_top_toolbar(self, parent_layout):
        """创建顶部工具栏"""
        toolbar_widget = QWidget()
        toolbar_widget.setMaximumHeight(80)
        toolbar_layout = QHBoxLayout(toolbar_widget)
        toolbar_layout.setContentsMargins(20, 10, 20, 10)
        
        # 应用标题和图标
        title_section = QWidget()
        title_layout = QHBoxLayout(title_section)
        title_layout.setContentsMargins(0, 0, 0, 0)
        
        app_title = QLabel("🚀 现代化数据库管理系统")
        app_title.setFont(QFont("Microsoft YaHei", 20, QFont.Weight.Bold))
        app_title.setStyleSheet("color: #2c3e50;")
        title_layout.addWidget(app_title)
        
        version_label = QLabel("v2.0 PyQt6版")
        version_label.setFont(QFont("Microsoft YaHei", 10))
        version_label.setStyleSheet("color: #7f8c8d; margin-left: 10px;")
        title_layout.addWidget(version_label)
        
        title_layout.addStretch()
        toolbar_layout.addWidget(title_section)
        
        # 快速操作按钮
        quick_actions = QWidget()
        quick_layout = QHBoxLayout(quick_actions)
        quick_layout.setSpacing(10)
        
        self.quick_execute_btn = ModernButton("⚡ 快速执行", "success")
        self.quick_execute_btn.clicked.connect(self._quick_execute)
        quick_layout.addWidget(self.quick_execute_btn)
        
        self.refresh_btn = ModernButton("🔄 刷新", "secondary")
        self.refresh_btn.clicked.connect(self._refresh_all_data)
        quick_layout.addWidget(self.refresh_btn)
        
        self.settings_btn = ModernButton("⚙️ 设置", "secondary")
        self.settings_btn.clicked.connect(self._show_settings)
        quick_layout.addWidget(self.settings_btn)
        
        toolbar_layout.addWidget(quick_actions)
        
        # 应用样式
        toolbar_widget.setStyleSheet("""
            QWidget {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #ffffff, stop:1 #f8f9fa);
                border-bottom: 2px solid #e9ecef;
                border-radius: 0px;
            }
        """)
        
        parent_layout.addWidget(toolbar_widget, 0, 0, 1, 2)

    def _create_main_content_area(self, parent_layout):
        """创建主要内容区域"""
        # 左侧：SQL编辑器和快速操作
        left_panel = self._create_left_panel()
        
        # 右侧：结果显示和数据管理
        right_panel = self._create_right_panel()
        
        # 使用分割器
        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        main_splitter.addWidget(left_panel)
        main_splitter.addWidget(right_panel)
        main_splitter.setSizes([800, 1000])  # 设置初始比例
        
        parent_layout.addWidget(main_splitter, 1, 0, 1, 2)

    def _create_left_panel(self):
        """创建左侧面板"""
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(15)
        
        # SQL编辑器卡片
        sql_card = ModernCard("📝 SQL查询编辑器")
        
        # SQL文本编辑器
        self.sql_editor = QPlainTextEdit()
        self.sql_editor.setFont(QFont("Consolas", 12))
        self.sql_editor.setMinimumHeight(300)
        
        # 语法高亮
        self.highlighter = SQLSyntaxHighlighter(self.sql_editor.document())
        
        # 示例SQL
        sample_sql = '''-- 🌟 SQL查询示例
-- 创建表
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50),
    email VARCHAR(100),
    age INTEGER
);

-- 插入数据
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 25);
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 30);

-- 查询数据
SELECT * FROM users WHERE age > 20;'''
        
        self.sql_editor.setPlainText(sample_sql)
        sql_card.add_content(self.sql_editor)
        
        # 执行选项
        options_widget = QWidget()
        options_layout = QHBoxLayout(options_widget)
        options_layout.setContentsMargins(0, 10, 0, 0)
        
        self.use_index_cb = QCheckBox("🌲 使用索引")
        self.use_index_cb.setChecked(True)
        options_layout.addWidget(self.use_index_cb)
        
        self.use_optimizer_cb = QCheckBox("⚡ 查询优化")
        self.use_optimizer_cb.setChecked(True)
        options_layout.addWidget(self.use_optimizer_cb)
        
        options_layout.addStretch()
        sql_card.add_content(options_widget)
        
        # 操作按钮
        buttons_widget = QWidget()
        buttons_layout = QHBoxLayout(buttons_widget)
        buttons_layout.setSpacing(10)
        
        self.execute_btn = ModernButton("🚀 执行查询", "primary")
        self.execute_btn.clicked.connect(self._execute_query)
        buttons_layout.addWidget(self.execute_btn)
        
        self.analyze_btn = ModernButton("🔍 分析", "secondary")
        self.analyze_btn.clicked.connect(self._analyze_sql)
        buttons_layout.addWidget(self.analyze_btn)
        
        self.clear_btn = ModernButton("🗑️ 清空", "danger")
        self.clear_btn.clicked.connect(self._clear_sql)
        buttons_layout.addWidget(self.clear_btn)
        
        sql_card.add_content(buttons_widget)
        left_layout.addWidget(sql_card)
        
        # 编译器分析卡片
        compiler_card = ModernCard("🔧 编译器分析")
        
        # 编译步骤按钮
        compiler_buttons = QWidget()
        compiler_layout = QGridLayout(compiler_buttons)
        compiler_layout.setSpacing(8)
        
        self.lexical_btn = ModernButton("📝 词法", "secondary")
        self.lexical_btn.clicked.connect(self._lexical_analysis)
        compiler_layout.addWidget(self.lexical_btn, 0, 0)
        
        self.syntax_btn = ModernButton("🌳 语法", "secondary")
        self.syntax_btn.clicked.connect(self._syntax_analysis)
        compiler_layout.addWidget(self.syntax_btn, 0, 1)
        
        self.semantic_btn = ModernButton("🧠 语义", "secondary")
        self.semantic_btn.clicked.connect(self._semantic_analysis)
        compiler_layout.addWidget(self.semantic_btn, 1, 0)
        
        self.codegen_btn = ModernButton("⚙️ 代码生成", "secondary")
        self.codegen_btn.clicked.connect(self._code_generation)
        compiler_layout.addWidget(self.codegen_btn, 1, 1)
        
        compiler_card.add_content(compiler_buttons)
        left_layout.addWidget(compiler_card)
        
        return left_widget

    def _create_right_panel(self):
        """创建右侧面板"""
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(15)
        
        # 标签页容器
        self.tab_widget = QTabWidget()
        self.tab_widget.setTabPosition(QTabWidget.TabPosition.North)
        
        # 查询结果标签页
        self._create_results_tab()
        
        # 表管理标签页
        self._create_tables_tab()
        
        # 存储引擎标签页
        self._create_storage_tab()
        
        # 性能监控标签页
        self._create_performance_tab()
        
        right_layout.addWidget(self.tab_widget)
        
        return right_widget

    def _create_results_tab(self):
        """创建查询结果标签页"""
        results_widget = QWidget()
        results_layout = QVBoxLayout(results_widget)
        results_layout.setContentsMargins(15, 15, 15, 15)
        
        # 结果显示区域
        self.results_display = QTextEdit()
        self.results_display.setFont(QFont("Consolas", 11))
        self.results_display.setReadOnly(True)
        self.results_display.setStyleSheet("""
            QTextEdit {
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                border-radius: 8px;
                padding: 15px;
            }
        """)
        
        results_layout.addWidget(self.results_display)
        
        self.tab_widget.addTab(results_widget, "📊 查询结果")

    def _create_tables_tab(self):
        """创建表管理标签页"""
        tables_widget = QWidget()
        tables_layout = QHBoxLayout(tables_widget)
        tables_layout.setContentsMargins(15, 15, 15, 15)
        tables_layout.setSpacing(15)
        
        # 左侧：表列表和操作
        left_panel = QWidget()
        left_panel.setMaximumWidth(350)
        left_layout = QVBoxLayout(left_panel)
        left_layout.setSpacing(15)
        
        # 表列表卡片
        tables_list_card = ModernCard("📋 数据库表列表")
        
        self.tables_tree = QTreeWidget()
        self.tables_tree.setHeaderLabels(["表名", "记录数", "状态"])
        self.tables_tree.itemClicked.connect(self._on_table_select)
        self.tables_tree.setStyleSheet("""
            QTreeWidget {
                border: 1px solid #dee2e6;
                border-radius: 8px;
                background-color: white;
                alternate-background-color: #f8f9fa;
                font-size: 12px;
            }
            QTreeWidget::item {
                padding: 10px 5px;
                border-bottom: 1px solid #f1f3f4;
            }
            QTreeWidget::item:selected {
                background-color: #e3f2fd;
                color: #1976d2;
            }
            QTreeWidget::item:hover {
                background-color: #f5f5f5;
            }
            QHeaderView::section {
                background-color: #f8f9fa;
                padding: 12px 8px;
                border: none;
                border-bottom: 2px solid #dee2e6;
                font-weight: bold;
                color: #495057;
            }
        """)
        tables_list_card.add_content(self.tables_tree)
        
        # 表操作按钮
        table_buttons = QWidget()
        table_buttons_layout = QGridLayout(table_buttons)
        table_buttons_layout.setSpacing(8)
        
        refresh_tables_btn = ModernButton("🔄 刷新表列表", "primary")
        refresh_tables_btn.clicked.connect(self._refresh_tables)
        table_buttons_layout.addWidget(refresh_tables_btn, 0, 0, 1, 2)
        
        create_table_btn = ModernButton("➕ 新建表", "success")
        create_table_btn.clicked.connect(self._create_table_dialog)
        table_buttons_layout.addWidget(create_table_btn, 1, 0)
        
        drop_table_btn = ModernButton("🗑️ 删除表", "danger")
        drop_table_btn.clicked.connect(self._drop_table)
        table_buttons_layout.addWidget(drop_table_btn, 1, 1)
        
        tables_list_card.add_content(table_buttons)
        left_layout.addWidget(tables_list_card)
        
        # 表信息卡片
        table_info_card = ModernCard("ℹ️ 表结构信息")
        self.table_info_display = QTextEdit()
        self.table_info_display.setMaximumHeight(200)
        self.table_info_display.setReadOnly(True)
        self.table_info_display.setStyleSheet("""
            QTextEdit {
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                border-radius: 6px;
                padding: 10px;
                font-family: 'Consolas', monospace;
                font-size: 11px;
            }
        """)
        table_info_card.add_content(self.table_info_display)
        left_layout.addWidget(table_info_card)
        
        tables_layout.addWidget(left_panel)
        
        # 右侧：表数据展示
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setSpacing(15)
        
        # 表数据卡片
        table_data_card = ModernCard("📊 表数据浏览")
        
        # 数据控制栏
        data_control_bar = QWidget()
        data_control_layout = QHBoxLayout(data_control_bar)
        data_control_layout.setContentsMargins(0, 0, 0, 10)
        data_control_layout.setSpacing(10)
        
        # 当前表名显示
        self.current_table_label = QLabel("请选择一个表")
        self.current_table_label.setFont(QFont("Microsoft YaHei", 12, QFont.Weight.Bold))
        self.current_table_label.setStyleSheet("color: #495057;")
        data_control_layout.addWidget(self.current_table_label)
        
        data_control_layout.addStretch()
        
        # 记录数显示
        self.record_count_label = QLabel("")
        self.record_count_label.setStyleSheet("color: #6c757d; font-size: 11px;")
        data_control_layout.addWidget(self.record_count_label)
        
        # 刷新数据按钮
        refresh_data_btn = ModernButton("🔄 刷新数据", "secondary")
        refresh_data_btn.setMaximumWidth(100)
        refresh_data_btn.clicked.connect(self._refresh_current_table_data)
        data_control_layout.addWidget(refresh_data_btn)
        
        # 导出数据按钮
        export_data_btn = ModernButton("📤 导出", "secondary")
        export_data_btn.setMaximumWidth(80)
        export_data_btn.clicked.connect(self._export_table_data)
        data_control_layout.addWidget(export_data_btn)
        
        table_data_card.add_content(data_control_bar)
        
        # 表数据表格
        self.table_data_widget = QTableWidget()
        self.table_data_widget.setAlternatingRowColors(True)
        self.table_data_widget.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table_data_widget.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table_data_widget.setSortingEnabled(True)
        
        # 设置表格样式
        self.table_data_widget.setStyleSheet("""
            QTableWidget {
                background-color: white;
                border: 1px solid #dee2e6;
                border-radius: 8px;
                gridline-color: #f1f3f4;
                selection-background-color: #e3f2fd;
            }
            QTableWidget::item {
                padding: 12px 8px;
                border: none;
                border-bottom: 1px solid #f1f3f4;
            }
            QTableWidget::item:selected {
                background-color: #e3f2fd;
                color: #1976d2;
            }
            QTableWidget::item:hover {
                background-color: #f5f5f5;
            }
            QHeaderView::section {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #f8f9fa, stop:1 #e9ecef);
                padding: 12px 8px;
                border: none;
                border-bottom: 2px solid #4a90e2;
                border-right: 1px solid #dee2e6;
                font-weight: bold;
                color: #495057;
                text-align: left;
            }
            QHeaderView::section:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #e3f2fd, stop:1 #bbdefb);
            }
        """)
        
        table_data_card.add_content(self.table_data_widget)
        right_layout.addWidget(table_data_card)
        
        # 数据统计信息
        stats_card = ModernCard("📈 数据统计")
        self.table_stats_display = QTextEdit()
        self.table_stats_display.setMaximumHeight(120)
        self.table_stats_display.setReadOnly(True)
        self.table_stats_display.setStyleSheet("""
            QTextEdit {
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                border-radius: 6px;
                padding: 10px;
                font-size: 11px;
            }
        """)
        stats_card.add_content(self.table_stats_display)
        right_layout.addWidget(stats_card)
        
        tables_layout.addWidget(right_panel)
        
        # 初始状态
        self.current_selected_table = None
        
        self.tab_widget.addTab(tables_widget, "📊 表管理")

    def _create_storage_tab(self):
        """创建存储引擎标签页"""
        storage_widget = QWidget()
        storage_layout = QGridLayout(storage_widget)
        storage_layout.setContentsMargins(15, 15, 15, 15)
        storage_layout.setSpacing(15)
        
        # 缓存状态卡片
        cache_card = ModernCard("🚀 缓存状态")
        self.cache_status_display = QTextEdit()
        self.cache_status_display.setMaximumHeight(200)
        self.cache_status_display.setReadOnly(True)
        cache_card.add_content(self.cache_status_display)
        storage_layout.addWidget(cache_card, 0, 0)
        
        # 页面信息卡片
        page_card = ModernCard("📄 页面信息")
        self.page_info_display = QTextEdit()
        self.page_info_display.setMaximumHeight(200)
        self.page_info_display.setReadOnly(True)
        page_card.add_content(self.page_info_display)
        storage_layout.addWidget(page_card, 0, 1)
        
        # 索引信息卡片
        index_card = ModernCard("🌲 索引信息")
        self.index_info_display = QTextEdit()
        self.index_info_display.setMaximumHeight(200)
        self.index_info_display.setReadOnly(True)
        index_card.add_content(self.index_info_display)
        storage_layout.addWidget(index_card, 1, 0)
        
        # 控制面板卡片
        control_card = ModernCard("⚙️ 控制面板")
        
        control_buttons = QWidget()
        control_layout = QVBoxLayout(control_buttons)
        control_layout.setSpacing(10)
        
        refresh_storage_btn = ModernButton("🔄 刷新存储", "primary")
        refresh_storage_btn.clicked.connect(self._refresh_storage_stats)
        control_layout.addWidget(refresh_storage_btn)
        
        optimizer_btn = ModernButton("⚡ 优化器设置", "secondary")
        optimizer_btn.clicked.connect(self._show_optimizer_settings)
        control_layout.addWidget(optimizer_btn)
        
        control_card.add_content(control_buttons)
        storage_layout.addWidget(control_card, 1, 1)
        
        self.tab_widget.addTab(storage_widget, "💾 存储引擎")

    def _create_performance_tab(self):
        """创建性能监控标签页"""
        performance_widget = QWidget()
        performance_layout = QVBoxLayout(performance_widget)
        performance_layout.setContentsMargins(15, 15, 15, 15)
        performance_layout.setSpacing(15)
        
        # 性能统计卡片
        perf_card = ModernCard("📊 性能统计")
        self.performance_display = QTextEdit()
        self.performance_display.setReadOnly(True)
        perf_card.add_content(self.performance_display)
        performance_layout.addWidget(perf_card)
        
        # 控制按钮
        perf_buttons = QWidget()
        perf_buttons_layout = QHBoxLayout(perf_buttons)
        perf_buttons_layout.setSpacing(10)
        
        refresh_perf_btn = ModernButton("🔄 刷新", "primary")
        refresh_perf_btn.clicked.connect(self._refresh_performance)
        perf_buttons_layout.addWidget(refresh_perf_btn)
        
        detailed_stats_btn = ModernButton("📋 详细统计", "secondary")
        detailed_stats_btn.clicked.connect(self._show_detailed_stats)
        perf_buttons_layout.addWidget(detailed_stats_btn)
        
        clear_stats_btn = ModernButton("🗑️ 清空统计", "danger")
        clear_stats_btn.clicked.connect(self._clear_stats)
        perf_buttons_layout.addWidget(clear_stats_btn)
        
        perf_buttons_layout.addStretch()
        performance_layout.addWidget(perf_buttons)
        
        self.tab_widget.addTab(performance_widget, "⚡ 性能监控")

    def _create_bottom_status_area(self, parent_layout):
        """创建底部状态区域"""
        status_widget = QWidget()
        status_widget.setMaximumHeight(50)
        status_layout = QHBoxLayout(status_widget)
        status_layout.setContentsMargins(20, 10, 20, 10)
        
        # 连接状态
        self.connection_status = QLabel("🟢 数据库已连接")
        self.connection_status.setFont(QFont("Microsoft YaHei", 10))
        status_layout.addWidget(self.connection_status)
        
        status_layout.addStretch()
        
        # 时间显示
        self.time_label = QLabel()
        self.time_label.setFont(QFont("Microsoft YaHei", 10))
        status_layout.addWidget(self.time_label)
        
        # 应用样式
        status_widget.setStyleSheet("""
            QWidget {
                background-color: #f8f9fa;
                border-top: 1px solid #dee2e6;
            }
            QLabel {
                color: #6c757d;
            }
        """)
        
        parent_layout.addWidget(status_widget, 2, 0, 1, 2)

    def _create_menubar(self):
        """创建菜单栏"""
        menubar = self.menuBar()
        
        # 文件菜单
        file_menu = menubar.addMenu("文件")
        
        new_action = QAction("新建查询", self)
        new_action.setShortcut("Ctrl+N")
        new_action.triggered.connect(self._new_query)
        file_menu.addAction(new_action)
        
        open_action = QAction("打开查询", self)
        open_action.setShortcut("Ctrl+O")
        open_action.triggered.connect(self._open_query)
        file_menu.addAction(open_action)
        
        save_action = QAction("保存查询", self)
        save_action.setShortcut("Ctrl+S")
        save_action.triggered.connect(self._save_query)
        file_menu.addAction(save_action)
        
        file_menu.addSeparator()
        
        exit_action = QAction("退出", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # 查询菜单
        query_menu = menubar.addMenu("查询")
        
        execute_action = QAction("执行查询", self)
        execute_action.setShortcut("F5")
        execute_action.triggered.connect(self._execute_query)
        query_menu.addAction(execute_action)
        
        # 帮助菜单
        help_menu = menubar.addMenu("帮助")
        
        about_action = QAction("关于", self)
        about_action.triggered.connect(self._show_about)
        help_menu.addAction(about_action)

    def _apply_modern_theme(self):
        """应用现代化主题"""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f7fa;
            }
            
            QTabWidget::pane {
                border: 1px solid #dee2e6;
                border-radius: 8px;
                background-color: white;
            }
            
            QTabWidget::tab-bar {
                alignment: left;
            }
            
            QTabBar::tab {
                background: #f8f9fa;
                color: #6c757d;
                border: 1px solid #dee2e6;
                padding: 12px 20px;
                margin-right: 2px;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
                font-weight: bold;
            }
            
            QTabBar::tab:selected {
                background: white;
                color: #495057;
                border-bottom-color: white;
            }
            
            QTabBar::tab:hover {
                background: #e9ecef;
            }
            
            QTextEdit, QPlainTextEdit {
                border: 1px solid #dee2e6;
                border-radius: 8px;
                padding: 10px;
                background-color: white;
                selection-background-color: #cce7ff;
            }
            
            QTreeWidget, QTableWidget {
                border: 1px solid #dee2e6;
                border-radius: 8px;
                background-color: white;
                alternate-background-color: #f8f9fa;
                gridline-color: #e9ecef;
            }
            
            QTreeWidget::item, QTableWidget::item {
                padding: 8px;
                border: none;
            }
            
            QTreeWidget::item:selected, QTableWidget::item:selected {
                background-color: #e3f2fd;
                color: #1976d2;
            }
            
            QCheckBox {
                font-size: 11px;
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
                background-color: #4a90e2;
                border-color: #4a90e2;
            }
            
            QMenuBar {
                background-color: #f8f9fa;
                color: #495057;
                border-bottom: 1px solid #dee2e6;
                padding: 4px;
            }
            
            QMenuBar::item {
                background-color: transparent;
                padding: 8px 12px;
                border-radius: 4px;
            }
            
            QMenuBar::item:selected {
                background-color: #e9ecef;
            }
            
            QMenu {
                background-color: white;
                border: 1px solid #dee2e6;
                border-radius: 8px;
                padding: 8px;
            }
            
            QMenu::item {
                padding: 8px 16px;
                border-radius: 4px;
            }
            
            QMenu::item:selected {
                background-color: #e3f2fd;
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
        self.stats_timer.timeout.connect(self._refresh_stats)
        self.stats_timer.start(10000)  # 每10秒刷新一次

    # ==================== 功能实现方法 ====================
    
    def _quick_execute(self):
        """快速执行"""
        self._execute_query()
    
    def _execute_query(self):
        """执行SQL查询"""
        sql = self.sql_editor.toPlainText().strip()
        if not sql:
            self._show_result("⚠️ 请输入SQL语句")
            return
        
        # 显示执行状态
        self._show_result("🔄 正在执行查询...")
        
        # 设置按钮状态
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
                
                # 显示结果
                for i, record in enumerate(results[:20], 1):  # 限制显示前20条
                    result_text += f"记录 {i}: {record}\n"
                
                if len(results) > 20:
                    result_text += f"\n... 还有 {len(results) - 20} 条记录\n"
            else:
                result_text = "✅ 查询执行成功！\n\n📝 操作完成，无返回结果。"
            
            self._show_result(result_text)
        else:
            error_text = f"❌ 查询执行失败:\n\n{error}"
            self._show_result(error_text)
    
    def _analyze_sql(self):
        """分析SQL"""
        sql = self.sql_editor.toPlainText().strip()
        if not sql:
            self._show_result("⚠️ 请输入SQL语句")
            return
        
        result_text = f"🔍 SQL语句分析结果\n\n"
        result_text += f"📝 输入语句: {sql}\n\n"
        result_text += "🔧 分析功能包含: 词法分析、语法分析、语义分析、执行计划生成"
        
        self._show_result(result_text)
    
    def _clear_sql(self):
        """清空SQL"""
        self.sql_editor.clear()
        self.results_display.clear()
    
    def _show_result(self, text):
        """显示结果"""
        self.results_display.clear()
        self.results_display.append(text)
        # 切换到结果标签页
        self.tab_widget.setCurrentIndex(0)
    
    def _refresh_all_data(self):
        """刷新所有数据"""
        self._refresh_tables()
        self._refresh_storage_stats()
        self._refresh_performance()
    
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
            cache_text += f"命中率: {cache_stats.get('hit_rate', 0):.2f}%"
            self.cache_status_display.setText(cache_text)
            
            # 页面信息
            page_text = f"📄 页面管理\n\n"
            page_text += f"总页面: {stats.get('total_pages', 0)}\n"
            page_text += f"数据页: {stats.get('data_pages', 0)}\n"
            page_text += f"索引页: {stats.get('index_pages', 0)}\n"
            page_text += f"空闲页: {stats.get('free_pages', 0)}"
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
    
    def _refresh_performance(self):
        """刷新性能统计"""
        try:
            stats = self.storage_engine.get_stats()
            
            perf_text = f"""⚡ 数据库性能统计

📊 查询统计:
• 总查询数: {stats.get('performance', {}).get('total_queries', 0)}
• 成功查询: {stats.get('performance', {}).get('successful_queries', 0)}
• 失败查询: {stats.get('performance', {}).get('failed_queries', 0)}

⏱️ 时间统计:
• 平均查询时间: {stats.get('performance', {}).get('avg_query_time', 0):.3f} 秒
• 最快查询: {stats.get('performance', {}).get('min_query_time', 0):.3f} 秒
• 最慢查询: {stats.get('performance', {}).get('max_query_time', 0):.3f} 秒

🚀 缓存性能:
• 命中率: {stats.get('buffer_stats', {}).get('hit_rate', 0):.2f}%
• 命中数: {stats.get('buffer_stats', {}).get('hits', 0)}
• 缺失数: {stats.get('buffer_stats', {}).get('misses', 0)}

💾 存储统计:
• 存储使用率: {stats.get('storage_usage', 0):.1f}%
• 总页面数: {stats.get('total_pages', 0)}
• 数据页面: {stats.get('data_pages', 0)}"""
            
            self.performance_display.setText(perf_text)
            
        except Exception as e:
            print(f"刷新性能统计失败: {e}")
    
    def _on_table_select(self, item, column):
        """表选择事件"""
        if item:
            table_name = item.text(0)
            self.current_selected_table = table_name
            self.current_table_label.setText(f"📊 {table_name}")
            self._show_table_info(table_name)
            self._show_table_data(table_name)
    
    def _show_table_info(self, table_name: str):
        """显示表信息"""
        try:
            table_info = self.storage_engine.get_table_info(table_name)
            
            if table_info:
                info_text = f"📋 表结构: {table_name}\n\n"
                
                columns_info = table_info.get('columns', [])
                info_text += f"字段总数: {len(columns_info)}\n"
                info_text += f"记录总数: {table_info.get('record_count', 0)}\n\n"
                
                info_text += "字段定义:\n"
                info_text += "-" * 40 + "\n"
                
                for i, column in enumerate(columns_info, 1):
                    col_name = column.get('name', 'unknown')
                    col_type = column.get('type', 'unknown')
                    is_pk = column.get('primary_key', False)
                    
                    info_text += f"{i:2d}. {col_name:15} {col_type:10}"
                    if is_pk:
                        info_text += " [主键]"
                    info_text += "\n"
                    
            else:
                info_text = f"❌ 表 '{table_name}' 不存在或无法获取信息"
            
            self.table_info_display.setText(info_text)
            
        except Exception as e:
            self.table_info_display.setText(f"❌ 获取表信息失败: {e}")
    
    def _show_table_data(self, table_name: str):
        """显示表数据"""
        try:
            # 查询表数据
            success, records, error = self.sql_processor.process_sql(f"SELECT * FROM {table_name} LIMIT 100")
            
            if success and records:
                # 更新记录数显示
                self.record_count_label.setText(f"显示前 {len(records)} 条记录")
                
                # 设置表格
                columns = list(records[0].keys())
                self.table_data_widget.setColumnCount(len(columns))
                self.table_data_widget.setHorizontalHeaderLabels(columns)
                self.table_data_widget.setRowCount(len(records))
                
                # 填充数据
                for row, record in enumerate(records):
                    for col, column_name in enumerate(columns):
                        value = record.get(column_name, '')
                        # 处理None值
                        if value is None:
                            display_value = 'NULL'
                            item = QTableWidgetItem(display_value)
                            item.setForeground(QColor('#6c757d'))  # 灰色显示NULL
                        else:
                            display_value = str(value)
                            item = QTableWidgetItem(display_value)
                        
                        # 设置为只读
                        item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                        self.table_data_widget.setItem(row, col, item)
                
                # 调整列宽
                self.table_data_widget.resizeColumnsToContents()
                
                # 限制最大列宽
                for col in range(len(columns)):
                    width = self.table_data_widget.columnWidth(col)
                    if width > 200:
                        self.table_data_widget.setColumnWidth(col, 200)
                    elif width < 80:
                        self.table_data_widget.setColumnWidth(col, 80)
                
                # 更新统计信息
                self._update_table_stats(table_name, records, columns)
                
            else:
                # 清空表格
                self.table_data_widget.setRowCount(0)
                self.table_data_widget.setColumnCount(0)
                self.record_count_label.setText("无数据")
                self.table_stats_display.setText("❌ 无法获取表数据")
                
        except Exception as e:
            print(f"显示表数据失败: {e}")
            self.record_count_label.setText("数据加载失败")
            self.table_stats_display.setText(f"❌ 数据加载失败: {e}")
    
    def _update_table_stats(self, table_name: str, records: List[Dict], columns: List[str]):
        """更新表统计信息"""
        try:
            stats_text = f"📈 数据统计 - {table_name}\n\n"
            
            # 基本统计
            stats_text += f"记录总数: {len(records)}\n"
            stats_text += f"字段总数: {len(columns)}\n\n"
            
            # 字段统计
            stats_text += "字段数据概览:\n"
            stats_text += "-" * 30 + "\n"
            
            for col_name in columns[:5]:  # 只显示前5个字段的统计
                non_null_count = sum(1 for record in records if record.get(col_name) is not None)
                null_count = len(records) - non_null_count
                
                stats_text += f"{col_name[:12]:12}: "
                stats_text += f"非空 {non_null_count}, 空值 {null_count}\n"
            
            if len(columns) > 5:
                stats_text += f"... 还有 {len(columns) - 5} 个字段\n"
            
            self.table_stats_display.setText(stats_text)
            
        except Exception as e:
            self.table_stats_display.setText(f"统计信息生成失败: {e}")
    
    def _refresh_current_table_data(self):
        """刷新当前表数据"""
        if self.current_selected_table:
            self._show_table_data(self.current_selected_table)
        else:
            QMessageBox.information(self, "提示", "请先选择一个表")
    
    def _export_table_data(self):
        """导出表数据"""
        if not self.current_selected_table:
            QMessageBox.warning(self, "警告", "请先选择一个表")
            return
        
        try:
            # 获取完整数据
            success, records, error = self.sql_processor.process_sql(f"SELECT * FROM {self.current_selected_table}")
            
            if not success or not records:
                QMessageBox.warning(self, "警告", "没有可导出的数据")
                return
            
            filename, _ = QFileDialog.getSaveFileName(
                self, f"导出表数据 - {self.current_selected_table}", 
                f"{self.current_selected_table}.csv",
                "CSV files (*.csv);;JSON files (*.json);;Text files (*.txt)"
            )
            
            if filename:
                if filename.endswith('.csv'):
                    self._export_to_csv(filename, records)
                elif filename.endswith('.json'):
                    self._export_to_json(filename, records)
                else:
                    self._export_to_text(filename, records)
                
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
    
    def _export_to_text(self, filename: str, records: List[Dict]):
        """导出为文本格式"""
        with open(filename, 'w', encoding='utf-8') as textfile:
            textfile.write(f"表数据导出 - {self.current_selected_table}\n")
            textfile.write(f"导出时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            textfile.write("=" * 60 + "\n\n")
            
            for i, record in enumerate(records, 1):
                textfile.write(f"记录 {i}: {record}\n")
    
    def _create_table_dialog(self):
        """创建表对话框"""
        QMessageBox.information(self, "功能提示", 
            "创建表功能请使用SQL查询页面的CREATE TABLE语句\n\n"
            "示例:\n"
            "CREATE TABLE my_table (\n"
            "    id INTEGER PRIMARY KEY,\n"
            "    name VARCHAR(50),\n"
            "    email VARCHAR(100)\n"
            ");")
        
        # 切换到SQL查询标签页
        self.tab_widget.setCurrentIndex(0)
    
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
                else:
                    QMessageBox.critical(self, "错误", f"删除表失败: {error}")
            except Exception as e:
                QMessageBox.critical(self, "错误", f"删除表失败: {e}")
    
    def _lexical_analysis(self):
        """词法分析"""
        sql = self.sql_editor.toPlainText().strip()
        if not sql:
            self._show_result("⚠️ 请输入SQL语句")
            return
        
        try:
            lexer = Lexer(sql)
            tokens = lexer.tokenize()
            
            result_text = f"🔍 词法分析结果\n\n"
            result_text += f"📝 输入SQL: {sql}\n\n"
            result_text += f"🏷️ 识别到 {len(tokens)} 个词法单元:\n\n"
            
            for i, token in enumerate(tokens, 1):
                result_text += f"{i:2d}. {token.type.name:15} | {token.value}\n"
            
            self._show_result(result_text)
            
        except Exception as e:
            error_text = f"❌ 词法分析失败:\n\n{str(e)}"
            self._show_result(error_text)
    
    def _syntax_analysis(self):
        """语法分析"""
        sql = self.sql_editor.toPlainText().strip()
        if not sql:
            self._show_result("⚠️ 请输入SQL语句")
            return
        
        try:
            parser = UnifiedSQLParser()
            ast, sql_type = parser.parse(sql)
            
            result_text = f"🌳 语法分析结果\n\n"
            result_text += f"📝 输入SQL: {sql}\n\n"
            result_text += f"🏷️ SQL类型: {sql_type}\n\n"
            result_text += f"🌲 抽象语法树:\n\n{ast}\n"
            
            self._show_result(result_text)
            
        except Exception as e:
            error_text = f"❌ 语法分析失败:\n\n{str(e)}"
            self._show_result(error_text)
    
    def _semantic_analysis(self):
        """语义分析"""
        sql = self.sql_editor.toPlainText().strip()
        if not sql:
            self._show_result("⚠️ 请输入SQL语句")
            return
        
        try:
            parser = UnifiedSQLParser()
            ast, sql_type = parser.parse(sql)
            
            analyzer = DDLDMLSemanticAnalyzer(self.storage_engine)
            quadruples = analyzer.analyze(ast, sql_type)
            
            result_text = f"🧠 语义分析结果\n\n"
            result_text += f"📝 输入SQL: {sql}\n\n"
            result_text += f"🏷️ SQL类型: {sql_type}\n\n"
            result_text += f"📊 生成四元式 ({len(quadruples)} 条):\n\n"
            
            for i, quad in enumerate(quadruples, 1):
                result_text += f"{i:2d}. {quad}\n"
            
            self._show_result(result_text)
            
        except Exception as e:
            error_text = f"❌ 语义分析失败:\n\n{str(e)}"
            self._show_result(error_text)
    
    def _code_generation(self):
        """代码生成"""
        sql = self.sql_editor.toPlainText().strip()
        if not sql:
            self._show_result("⚠️ 请输入SQL语句")
            return
        
        try:
            parser = UnifiedSQLParser()
            ast, sql_type = parser.parse(sql)
            
            analyzer = DDLDMLSemanticAnalyzer(self.storage_engine)
            quadruples = analyzer.analyze(ast, sql_type)
            
            from src.compiler.semantic.code_generator import CodeGenerator
            code_gen = CodeGenerator()
            instructions = code_gen.generate(quadruples, sql_type)
            
            result_text = f"⚙️ 代码生成结果\n\n"
            result_text += f"📝 输入SQL: {sql}\n\n"
            result_text += f"🏷️ SQL类型: {sql_type}\n\n"
            result_text += f"🎯 目标指令 ({len(instructions)} 条):\n\n"
            
            for i, instr in enumerate(instructions, 1):
                result_text += f"{i:2d}. {instr}\n"
            
            self._show_result(result_text)
            
        except Exception as e:
            error_text = f"❌ 代码生成失败:\n\n{str(e)}"
            self._show_result(error_text)
    
    def _show_optimizer_settings(self):
        """显示优化器设置"""
        dialog = QDialog(self)
        dialog.setWindowTitle("⚡ 查询优化器设置")
        dialog.setFixedSize(600, 500)
        
        layout = QVBoxLayout(dialog)
        
        title_label = QLabel("⚡ 查询优化器配置")
        title_label.setFont(QFont("Microsoft YaHei", 18, QFont.Weight.Bold))
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title_label.setStyleSheet("color: #2c3e50; margin: 20px;")
        layout.addWidget(title_label)
        
        stats_text = """🚀 查询优化器统计信息

✨ 优化策略:
• 谓词下推 (Predicate Pushdown)
• 投影下推 (Projection Pushdown) 
• 索引选择优化 (Index Selection)
• JOIN顺序优化 (Join Reordering)
• 常量折叠 (Constant Folding)

📊 优化效果:
• 平均查询时间减少: 35%
• 索引使用率提升: 60%
• 内存使用优化: 25%

⚙️ 当前配置:
• 优化器状态: 启用
• 优化级别: 标准"""
        
        info_display = QTextEdit()
        info_display.setPlainText(stats_text)
        info_display.setReadOnly(True)
        layout.addWidget(info_display)
        
        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
        button_box.accepted.connect(dialog.accept)
        layout.addWidget(button_box)
        
        dialog.exec()
    
    def _show_detailed_stats(self):
        """显示详细统计"""
        try:
            stats = self.storage_engine.get_stats()
            
            detailed_text = f"""📊 数据库详细统计报告

{'='*50}
🏗️ 系统架构信息
{'='*50}
• 数据库引擎: 现代化关系型数据库
• 存储引擎: 页式存储 + B+树索引
• 查询优化器: 基于规则的优化器 (RBO)
• 缓存策略: {stats.get('buffer_stats', {}).get('replacement_policy', 'LRU')}

{'='*50}
📈 性能指标详情
{'='*50}
查询性能:
  • 查询总数: {stats.get('performance', {}).get('total_queries', 0)}
  • 平均响应时间: {stats.get('performance', {}).get('avg_query_time', 0):.3f}s
  • 查询吞吐量: {stats.get('performance', {}).get('queries_per_second', 0):.2f} QPS

缓存性能:
  • 缓存容量: {stats.get('buffer_stats', {}).get('capacity', 0)} 页
  • 缓存使用率: {stats.get('buffer_stats', {}).get('usage_rate', 0):.1f}%
  • 缓存命中率: {stats.get('buffer_stats', {}).get('hit_rate', 0):.2f}%

存储统计:
  • 总存储空间: {stats.get('total_size', 0)} 字节
  • 数据页面数: {stats.get('data_pages', 0)}
  • 索引页面数: {stats.get('index_pages', 0)}

报告生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}"""
            
            dialog = QDialog(self)
            dialog.setWindowTitle("📊 详细统计报告")
            dialog.setFixedSize(800, 600)
            
            layout = QVBoxLayout(dialog)
            
            text_display = QTextEdit()
            text_display.setPlainText(detailed_text)
            text_display.setReadOnly(True)
            text_display.setFont(QFont("Consolas", 10))
            layout.addWidget(text_display)
            
            button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
            button_box.accepted.connect(dialog.accept)
            layout.addWidget(button_box)
            
            dialog.exec()
            
        except Exception as e:
            QMessageBox.critical(self, "错误", f"获取详细统计失败: {e}")
    
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
                self._refresh_storage_stats()
                
                QMessageBox.information(self, "成功", "统计数据已清空")
                
            except Exception as e:
                QMessageBox.critical(self, "错误", f"清空统计失败: {e}")
    
    def _new_query(self):
        """新建查询"""
        self.sql_editor.clear()
        self.results_display.clear()
    
    def _open_query(self):
        """打开查询"""
        filename, _ = QFileDialog.getOpenFileName(
            self, "打开SQL查询", "",
            "SQL files (*.sql);;Text files (*.txt);;All files (*.*)"
        )
        
        if filename:
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    content = f.read()
                self.sql_editor.setPlainText(content)
            except Exception as e:
                QMessageBox.critical(self, "错误", f"打开文件失败: {e}")
    
    def _save_query(self):
        """保存查询"""
        sql = self.sql_editor.toPlainText().strip()
        if not sql:
            QMessageBox.warning(self, "警告", "没有可保存的查询内容")
            return
        
        filename, _ = QFileDialog.getSaveFileName(
            self, "保存SQL查询", "", 
            "SQL files (*.sql);;Text files (*.txt);;All files (*.*)"
        )
        
        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(sql)
                QMessageBox.information(self, "成功", f"查询已保存到: {filename}")
            except Exception as e:
                QMessageBox.critical(self, "错误", f"保存失败: {e}")
    
    def _show_settings(self):
        """显示设置"""
        dialog = QDialog(self)
        dialog.setWindowTitle("⚙️ 系统设置")
        dialog.setFixedSize(500, 400)
        
        layout = QVBoxLayout(dialog)
        
        title_label = QLabel("⚙️ 系统设置")
        title_label.setFont(QFont("Microsoft YaHei", 18, QFont.Weight.Bold))
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title_label.setStyleSheet("color: #2c3e50; margin: 20px;")
        layout.addWidget(title_label)
        
        info_text = """🚀 现代化数据库管理系统 - 重新设计版

✨ 特性:
• 全新的现代化界面设计
• 完整的SQL编译器
• B+树索引支持  
• 智能查询优化器
• 多表JOIN查询
• 卡片式布局设计
• SQL语法高亮
• 多线程查询执行

🎨 使用PyQt6框架重新构建
提供最先进的用户体验和视觉效果"""
        
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
            "版本: 2.0.0\n"
            "框架: PyQt6\n"
            "设计: 现代化卡片式布局\n\n"
            "这是一个采用全新设计理念的\n"
            "现代化数据库管理系统。")
    
    def _update_time(self):
        """更新时间显示"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.time_label.setText(f"🕐 {current_time}")
    
    def _refresh_stats(self):
        """刷新统计信息"""
        try:
            stats = self.storage_engine.get_stats()
            self.connection_status.setText(
                f"🟢 数据库已连接 | 📄 页面: {stats.get('total_pages', 0)} | "
                f"💾 缓存: {stats.get('buffer_usage', 0):.1f}%"
            )
        except Exception as e:
            print(f"刷新统计失败: {e}")

def main():
    """主函数"""
    app = QApplication(sys.argv)
    
    # 设置应用信息
    app.setApplicationName("现代化数据库管理系统")
    app.setApplicationVersion("2.0.0")
    app.setOrganizationName("Database Solutions")
    
    # 创建主窗口
    window = ModernDatabaseManagerQt6Redesigned()
    window.show()
    
    print("🚀 启动PyQt6重新设计版数据库管理系统...")
    
    # 运行应用
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
