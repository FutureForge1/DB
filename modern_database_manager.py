"""
现代化数据库管理系统 - 主应用程序
展示从SQL编译器到存储引擎的完整数据库系统功能
"""

import sys
import os
from pathlib import Path

# 确保src模块可以被导入
sys.path.insert(0, str(Path(__file__).parent))

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import json
import time
import re
from threading import Thread
from typing import Dict, List, Optional, Any, Tuple

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


class SQLSyntaxHighlighter:
    """SQL语法高亮器 - 企业级数据库管理工具风格"""
    
    def __init__(self, text_widget, color_scheme):
        self.text_widget = text_widget
        self.colors = color_scheme
        
        # SQL关键字分类
        self.sql_keywords = {
            'primary': [
                'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 
                'CREATE', 'ALTER', 'DROP', 'TABLE', 'INDEX', 'VIEW',
                'DATABASE', 'SCHEMA', 'TRIGGER', 'PROCEDURE', 'FUNCTION'
            ],
            'secondary': [
                'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'ON',
                'GROUP', 'ORDER', 'BY', 'HAVING', 'UNION', 'INTERSECT',
                'EXCEPT', 'DISTINCT', 'ALL', 'TOP', 'LIMIT', 'OFFSET'
            ],
            'data_types': [
                'INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT',
                'VARCHAR', 'CHAR', 'TEXT', 'NVARCHAR', 'NCHAR',
                'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC', 'REAL',
                'DATE', 'TIME', 'DATETIME', 'TIMESTAMP', 'YEAR',
                'BOOLEAN', 'BOOL', 'BIT', 'BINARY', 'VARBINARY',
                'BLOB', 'CLOB', 'JSON', 'XML'
            ],
            'functions': [
                'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'UPPER', 'LOWER',
                'SUBSTRING', 'LENGTH', 'CONCAT', 'TRIM', 'LTRIM', 'RTRIM',
                'ROUND', 'CEIL', 'FLOOR', 'ABS', 'SQRT', 'POWER',
                'NOW', 'CURDATE', 'CURTIME', 'YEAR', 'MONTH', 'DAY',
                'CAST', 'CONVERT', 'COALESCE', 'ISNULL', 'NULLIF'
            ],
            'operators': [
                'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE',
                'IS', 'NULL', 'TRUE', 'FALSE', 'CASE', 'WHEN', 'THEN',
                'ELSE', 'END', 'AS', 'ASC', 'DESC'
            ],
            'constraints': [
                'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'UNIQUE',
                'CHECK', 'DEFAULT', 'NOT', 'NULL', 'AUTO_INCREMENT',
                'IDENTITY', 'CONSTRAINT'
            ]
        }
        
        # 配置文本标签样式
        self._configure_tags()
        
        # 绑定事件
        self.text_widget.bind('<KeyRelease>', self._on_key_release)
        self.text_widget.bind('<Button-1>', self._on_click)
        self.text_widget.bind('<Control-v>', self._on_paste)
        
        # 延迟高亮标志
        self._highlight_after_id = None
    
    def _configure_tags(self):
        """配置语法高亮标签样式"""
        # SQL关键字 - 主要关键字（蓝色加粗）
        self.text_widget.tag_configure('sql_primary', 
                                      foreground='#1e40af',  # 更深的蓝色
                                      font=('楷体', 14, 'bold'))
        
        # SQL关键字 - 次要关键字（深蓝色）
        self.text_widget.tag_configure('sql_secondary',
                                      foreground='#1e3a8a',  # 更深的深蓝色
                                      font=('楷体', 14, 'bold'))
        
        # 数据类型（紫色）
        self.text_widget.tag_configure('sql_datatype',
                                      foreground='#7c3aed',  # 更深的紫色
                                      font=('楷体', 14, 'bold'))
        
        # 函数名（橙色）
        self.text_widget.tag_configure('sql_function',
                                      foreground='#ea580c',  # 更深的橙色
                                      font=('楷体', 14, 'bold'))
        
        # 操作符（深灰色加粗）
        self.text_widget.tag_configure('sql_operator',
                                      foreground='#374151',  # 更深的灰色
                                      font=('楷体', 14, 'bold'))
        
        # 约束关键字（绿色）
        self.text_widget.tag_configure('sql_constraint',
                                      foreground='#059669',  # 更深的绿色
                                      font=('楷体', 14, 'bold'))
        
        # 字符串（绿色斜体）
        self.text_widget.tag_configure('sql_string',
                                      foreground='#16a34a',  # 更深的绿色
                                      font=('楷体', 14, 'italic'))
        
        # 数字（蓝绿色）
        self.text_widget.tag_configure('sql_number',
                                      foreground='#0891b2',  # 更深的蓝绿色
                                      font=('楷体', 14))
        
        # 注释（灰色斜体）
        self.text_widget.tag_configure('sql_comment',
                                      foreground='#6b7280',  # 更深的灰色
                                      font=('楷体', 14, 'italic'))
        
        # 表名/列名（深青色）
        self.text_widget.tag_configure('sql_identifier',
                                      foreground='#0369a1',  # 更深的青色
                                      font=('楷体', 14))
        
        # 符号（灰色）
        self.text_widget.tag_configure('sql_symbol',
                                      foreground='#4b5563',  # 更深的灰色
                                      font=('楷体', 14))
    
    def _on_key_release(self, event=None):
        """按键释放事件处理"""
        self._schedule_highlight()
    
    def _on_click(self, event=None):
        """鼠标点击事件处理"""
        self._schedule_highlight()
    
    def _on_paste(self, event=None):
        """粘贴事件处理"""
        self.text_widget.after(10, self._schedule_highlight)
    
    def _schedule_highlight(self):
        """调度延迟高亮"""
        if self._highlight_after_id:
            self.text_widget.after_cancel(self._highlight_after_id)
        self._highlight_after_id = self.text_widget.after(100, self._highlight_syntax)
    
    def _highlight_syntax(self):
        """执行语法高亮"""
        try:
            # 获取文本内容
            content = self.text_widget.get('1.0', tk.END)
            
            # 清除所有现有标签
            for tag in ['sql_primary', 'sql_secondary', 'sql_datatype', 'sql_function',
                       'sql_operator', 'sql_constraint', 'sql_string', 'sql_number',
                       'sql_comment', 'sql_identifier', 'sql_symbol']:
                self.text_widget.tag_remove(tag, '1.0', tk.END)
            
            # 高亮注释（优先处理）
            self._highlight_comments(content)
            
            # 高亮字符串（优先处理）
            self._highlight_strings(content)
            
            # 高亮数字
            self._highlight_numbers(content)
            
            # 高亮SQL关键字
            self._highlight_keywords(content)
            
            # 高亮符号
            self._highlight_symbols(content)
            
        except Exception as e:
            # 忽略高亮过程中的错误，避免影响用户输入
            pass
    
    def _highlight_comments(self, content: str):
        """高亮注释"""
        # 单行注释 --
        for match in re.finditer(r'--.*$', content, re.MULTILINE):
            start_idx = self._get_text_index(content, match.start())
            end_idx = self._get_text_index(content, match.end())
            self.text_widget.tag_add('sql_comment', start_idx, end_idx)
        
        # 多行注释 /* */
        for match in re.finditer(r'/\*.*?\*/', content, re.DOTALL):
            start_idx = self._get_text_index(content, match.start())
            end_idx = self._get_text_index(content, match.end())
            self.text_widget.tag_add('sql_comment', start_idx, end_idx)
    
    def _highlight_strings(self, content: str):
        """高亮字符串"""
        # 单引号字符串
        for match in re.finditer(r"'[^']*'", content):
            start_idx = self._get_text_index(content, match.start())
            end_idx = self._get_text_index(content, match.end())
            self.text_widget.tag_add('sql_string', start_idx, end_idx)
        
        # 双引号字符串
        for match in re.finditer(r'"[^"]*"', content):
            start_idx = self._get_text_index(content, match.start())
            end_idx = self._get_text_index(content, match.end())
            self.text_widget.tag_add('sql_string', start_idx, end_idx)
    
    def _highlight_numbers(self, content: str):
        """高亮数字"""
        # 整数和浮点数
        for match in re.finditer(r'\b\d+\.?\d*\b', content):
            start_idx = self._get_text_index(content, match.start())
            end_idx = self._get_text_index(content, match.end())
            self.text_widget.tag_add('sql_number', start_idx, end_idx)
    
    def _highlight_keywords(self, content: str):
        """高亮SQL关键字"""
        # 按类别高亮关键字
        keyword_categories = [
            ('sql_primary', self.sql_keywords['primary']),
            ('sql_secondary', self.sql_keywords['secondary']),
            ('sql_datatype', self.sql_keywords['data_types']),
            ('sql_function', self.sql_keywords['functions']),
            ('sql_operator', self.sql_keywords['operators']),
            ('sql_constraint', self.sql_keywords['constraints'])
        ]
        
        for tag, keywords in keyword_categories:
            for keyword in keywords:
                # 使用单词边界确保完整匹配
                pattern = r'\b' + re.escape(keyword) + r'\b'
                for match in re.finditer(pattern, content, re.IGNORECASE):
                    start_idx = self._get_text_index(content, match.start())
                    end_idx = self._get_text_index(content, match.end())
                    self.text_widget.tag_add(tag, start_idx, end_idx)
    
    def _highlight_symbols(self, content: str):
        """高亮SQL符号"""
        symbols = [r'\(', r'\)', r',', r';', r'=', r'<', r'>', r'\+', r'-', r'\*', r'/', r'%']
        
        for symbol in symbols:
            for match in re.finditer(symbol, content):
                start_idx = self._get_text_index(content, match.start())
                end_idx = self._get_text_index(content, match.end())
                self.text_widget.tag_add('sql_symbol', start_idx, end_idx)
    
    def _get_text_index(self, content: str, pos: int) -> str:
        """将字符位置转换为Tkinter文本索引"""
        lines_before = content[:pos].count('\n')
        line_start = content.rfind('\n', 0, pos) + 1
        column = pos - line_start
        return f"{lines_before + 1}.{column}"
    
    def highlight_now(self):
        """立即执行语法高亮"""
        self._highlight_syntax()

class ModernDatabaseManager:
    """主应用"""

    def __init__(self):
        """初始化应用"""
        self.root = tk.Tk()
        self.root.title("🚀 现代化数据库管理系统")
        self.root.geometry("1500x1000")
        self.root.state('zoomed')  # Windows下最大化
        
        
        # 现代化主题配置
        self._setup_modern_theme()

        # 初始化后端组件
        self._init_database_components()

        # 创建界面
        self._create_menu()
        self._create_main_interface()

        # 状态变量
        self.current_database = "main_db"
        self.query_history = []

    def _setup_modern_theme(self):
        """设置现代化主题"""
        # 配置主窗口样式
        self.root.configure(bg='#f5f6fa')
        
        # 定义现代化颜色主题 - 使用更现代的配色方案
        self.colors = {
            'primary': '#667eea',      # 现代紫蓝色
            'primary_dark': '#5a67d8', # 深紫蓝色
            'secondary': '#4c51bf',    # 深紫色
            'success': '#48bb78',      # 现代绿色
            'warning': '#ed8936',      # 现代橙色
            'danger': '#f56565',       # 现代红色
            'info': '#4299e1',         # 信息蓝色
            'light': '#f7fafc',        # 极浅灰色
            'dark': '#2d3748',         # 深灰色
            'white': '#ffffff',        # 纯白色
            'text_primary': '#2d3748', # 主要文字颜色
            'text_secondary': '#718096',# 次要文字颜色
            'text_light': '#a0aec0',   # 浅色文字
            'bg_main': '#ffffff',      # 主背景
            'bg_secondary': '#f7fafc', # 次要背景
            'bg_tertiary': '#edf2f7',  # 第三背景色
            'border': '#e2e8f0',       # 边框颜色
            'border_light': '#f1f5f9', # 浅边框
            'hover': '#ebf8ff',        # 悬停颜色
            'hover_dark': '#bee3f8',   # 深悬停颜色
            'shadow': 'rgba(0, 0, 0, 0.1)', # 阴影颜色
            'accent': '#ed64a6',       # 强调色（粉色）
            'gradient_start': '#667eea', # 渐变起始色
            'gradient_end': '#764ba2',   # 渐变结束色
        }
        
        # 配置ttk样式
        self.style = ttk.Style()
        self.style.theme_use('clam')  # 使用clam主题作为基础
        
        # 配置Notebook（标签页）样式 - 现代化设计
        self.style.configure('Modern.TNotebook', 
                           background=self.colors['bg_secondary'],
                           borderwidth=0,
                           tabmargins=[0, 5, 0, 0])
        self.style.configure('Modern.TNotebook.Tab',
                           background=self.colors['bg_tertiary'],
                           foreground=self.colors['text_primary'],  # 更深的颜色
                           padding=[24, 12],
                           font=('楷体', 12, 'bold'),  # 更大更粗
                           borderwidth=0)
        self.style.map('Modern.TNotebook.Tab',
                      background=[('selected', self.colors['primary']),
                                ('active', self.colors['hover_dark'])],
                      foreground=[('selected', self.colors['white']),
                                ('active', self.colors['primary'])])
        
        # 配置Frame样式 - 添加圆角和阴影效果
        self.style.configure('Modern.TFrame',
                           background=self.colors['bg_main'],
                           borderwidth=0,
                           relief='flat')
        
        self.style.configure('Card.TFrame',
                           background=self.colors['bg_main'],
                           borderwidth=1,
                           relief='solid',
                           bordercolor=self.colors['border_light'])
        
        # 配置LabelFrame样式 - 现代化卡片设计
        self.style.configure('Modern.TLabelframe',
                           background=self.colors['bg_main'],
                           borderwidth=1,
                           relief='solid',
                           bordercolor=self.colors['border'])
        self.style.configure('Modern.TLabelframe.Label',
                           background=self.colors['bg_main'],
                           foreground=self.colors['primary'],
                           font=('楷体', 13, 'bold'))  # 更大字体
        
        # 配置Button样式 - 现代化按钮
        self.style.configure('Modern.TButton',
                           background=self.colors['primary'],
                           foreground=self.colors['white'],
                           borderwidth=0,
                           focuscolor='none',
                           font=('楷体', 11, 'bold'),  # 更大更粗
                           padding=[16, 10])  # 更大的内边距
        self.style.map('Modern.TButton',
                      background=[('active', self.colors['primary_dark']),
                                ('pressed', self.colors['secondary'])],
                      relief=[('pressed', 'flat')])
        
        # 配置成功按钮样式
        self.style.configure('Success.TButton',
                           background=self.colors['success'],
                           foreground=self.colors['white'],
                           borderwidth=0,
                           focuscolor='none',
                           font=('楷体', 11, 'bold'),
                           padding=[16, 10])
        self.style.map('Success.TButton',
                      background=[('active', '#38a169'),
                                ('pressed', '#2f855a')])
        
        # 配置危险按钮样式
        self.style.configure('Danger.TButton',
                           background=self.colors['danger'],
                           foreground=self.colors['white'],
                           borderwidth=0,
                           focuscolor='none',
                           font=('楷体', 11, 'bold'),
                           padding=[16, 10])
        self.style.map('Danger.TButton',
                      background=[('active', '#e53e3e'),
                                ('pressed', '#c53030')])
        
        # 配置信息按钮样式
        self.style.configure('Info.TButton',
                           background=self.colors['info'],
                           foreground=self.colors['white'],
                           borderwidth=0,
                           focuscolor='none',
                           font=('楷体', 11, 'bold'),
                           padding=[16, 10])
        self.style.map('Info.TButton',
                      background=[('active', '#3182ce'),
                                ('pressed', '#2c5282')])
        
        # 配置警告按钮样式
        self.style.configure('Warning.TButton',
                           background=self.colors['warning'],
                           foreground=self.colors['white'],
                           borderwidth=0,
                           focuscolor='none',
                           font=('楷体', 11, 'bold'),
                           padding=[16, 10])
        self.style.map('Warning.TButton',
                      background=[('active', '#dd6b20'),
                                ('pressed', '#c05621')])
        
        # 配置Entry样式 - 现代化输入框
        self.style.configure('Modern.TEntry',
                           borderwidth=2,
                           relief='solid',
                           bordercolor=self.colors['border'],
                           focuscolor=self.colors['primary'],
                           font=('楷体', 13),  # 更大字体
                           padding=[12, 10])  # 更大内边距
        self.style.map('Modern.TEntry',
                      bordercolor=[('focus', self.colors['primary'])])
        
        # 配置Label样式 - 现代化标签
        self.style.configure('Title.TLabel',
                           background=self.colors['bg_main'],
                           foreground=self.colors['text_primary'],
                           font=('楷体', 18, 'bold'))  # 更大字体
        self.style.configure('Subtitle.TLabel',
                           background=self.colors['bg_main'],
                           foreground=self.colors['text_primary'],  # 更深颜色
                           font=('楷体', 12, 'bold'))  # 更大更粗
        self.style.configure('Caption.TLabel',
                           background=self.colors['bg_main'],
                           foreground=self.colors['text_secondary'],  # 更深颜色
                           font=('楷体', 11))  # 更大字体
        
        # 配置Treeview样式 - 现代化表格
        self.style.configure('Modern.Treeview',
                           background=self.colors['bg_main'],
                           foreground=self.colors['text_primary'],
                           borderwidth=1,
                           relief='solid',
                           bordercolor=self.colors['border'],
                           font=('楷体', 11))  # 更大字体
        self.style.configure('Modern.Treeview.Heading',
                           background=self.colors['bg_tertiary'],
                           foreground=self.colors['text_primary'],
                           font=('楷体', 12, 'bold'),  # 更大字体
                           borderwidth=1,
                           relief='solid',
                           bordercolor=self.colors['border'])
        self.style.map('Modern.Treeview',
                      background=[('selected', self.colors['primary']),
                                ('focus', self.colors['hover'])],
                      foreground=[('selected', self.colors['white'])])
        
        # 配置Scrollbar样式 - 现代化滚动条
        self.style.configure('Modern.Vertical.TScrollbar',
                           background=self.colors['bg_tertiary'],
                           borderwidth=0,
                           arrowcolor=self.colors['text_light'],
                           troughcolor=self.colors['bg_secondary'])
        self.style.configure('Modern.Horizontal.TScrollbar',
                           background=self.colors['bg_tertiary'],
                           borderwidth=0,
                           arrowcolor=self.colors['text_light'],
                           troughcolor=self.colors['bg_secondary'])
        
        # 配置Checkbutton样式 - 现代化复选框
        self.style.configure('Modern.TCheckbutton',
                           background=self.colors['bg_main'],
                           foreground=self.colors['text_primary'],
                           font=('楷体', 11, 'bold'),  # 更大更粗
                           focuscolor='none')
        self.style.map('Modern.TCheckbutton',
                      background=[('active', self.colors['hover']),
                                ('selected', self.colors['bg_main'])],
                      foreground=[('active', self.colors['primary'])])
        
        # 配置Progressbar样式 - 现代化进度条
        self.style.configure('Modern.TProgressbar',
                           background=self.colors['primary'],
                           troughcolor=self.colors['bg_tertiary'],
                           borderwidth=0,
                           lightcolor=self.colors['primary'],
                           darkcolor=self.colors['primary_dark'])

    def _init_database_components(self):
        """初始化数据库组件"""
        try:
            # 创建存储引擎
            self.storage_engine = StorageEngine("modern_db", buffer_size=50)

            # 创建执行引擎
            self.execution_engine = ExecutionEngine(self.storage_engine)

            print("[SUCCESS] 数据库组件初始化成功")

        except Exception as e:
            print(f"[ERROR] 数据库组件初始化失败: {e}")
            self.storage_engine = None
            self.execution_engine = None

    def _create_menu(self):
        """创建菜单栏"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        # 文件菜单
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="文件", menu=file_menu)
        file_menu.add_command(label="新建数据库", command=self._new_database)
        file_menu.add_command(label="打开数据库", command=self._open_database)
        file_menu.add_command(label="保存查询", command=self._save_query)
        file_menu.add_separator()
        file_menu.add_command(label="退出", command=self._quit_app)

        # 工具菜单
        tools_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="工具", menu=tools_menu)
        tools_menu.add_command(label="编译器分析", command=self._show_compiler_analysis)
        tools_menu.add_command(label="存储引擎状态", command=self._show_storage_status)
        tools_menu.add_command(label="性能统计", command=self._show_performance_stats)

        # 帮助菜单
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="帮助", menu=help_menu)
        help_menu.add_command(label="关于", command=self._show_about)

    def _create_header(self, parent):
        """创建现代化应用标题栏"""
        # 主标题容器 - 使用渐变背景效果
        header_container = ttk.Frame(parent, style='Modern.TFrame')
        header_container.pack(fill=tk.X, pady=(0, 20))
        
        # 创建一个带背景色的header frame
        header_frame = tk.Frame(header_container, 
                               bg=self.colors['primary'], 
                               height=80)
        header_frame.pack(fill=tk.X, padx=0, pady=0)
        header_frame.pack_propagate(False)
        
        # 左侧：应用标题和状态
        left_frame = tk.Frame(header_frame, bg=self.colors['primary'])
        left_frame.pack(side=tk.LEFT, fill=tk.Y, padx=30, pady=15)
        
        # 主标题 - 使用更大更醒目的字体
        title_label = tk.Label(left_frame, 
                              text="现代化数据库管理系统", 
                              font=('楷体', 20, 'bold'),  # 更大字体
                              fg=self.colors['white'],
                              bg=self.colors['primary'])
        title_label.pack(anchor=tk.W)
        
        # 副标题 - 更精简的描述
        subtitle_label = tk.Label(left_frame,
                                 text="高性能SQL数据库 · 智能查询优化 · 现代化管理界面",
                                 font=('楷体', 10),
                                 fg=self.colors['light'],
                                 bg=self.colors['primary'])
        subtitle_label.pack(anchor=tk.W, pady=(5, 0))
        
        # 右侧：状态信息卡片
        right_frame = tk.Frame(header_frame, bg=self.colors['primary'])
        right_frame.pack(side=tk.RIGHT, padx=30, pady=15)
        
        # 状态卡片容器
        status_container = tk.Frame(right_frame, 
                                   bg=self.colors['white'], 
                                   relief='flat',
                                   bd=0)
        status_container.pack(side=tk.RIGHT)
        
        # 添加内边距
        self.status_frame = tk.Frame(status_container, bg=self.colors['white'])
        self.status_frame.pack(padx=20, pady=15)
        
        # 状态标题
        status_title = tk.Label(self.status_frame,
                               text="系统状态",
                               font=('楷体', 13, 'bold'),  # 更大字体
                               fg=self.colors['text_primary'],
                               bg=self.colors['white'])
        status_title.pack(anchor=tk.W)
        
        # 数据库状态 - 使用更现代的状态指示器
        db_status_frame = tk.Frame(self.status_frame, bg=self.colors['white'])
        db_status_frame.pack(anchor=tk.W, pady=(8, 4))
        
        # 状态点
        db_status_dot = tk.Label(db_status_frame,
                                text="●",
                                font=('Times New Roman', 12),
                                fg=self.colors['success'],
                                bg=self.colors['white'])
        db_status_dot.pack(side=tk.LEFT)
        
        self.db_status_label = tk.Label(db_status_frame,
                                       text=" 数据库已连接",
                                       font=('楷体', 11, 'bold'),  # 更大更粗字体
                                       fg=self.colors['text_primary'],
                                       bg=self.colors['white'])
        self.db_status_label.pack(side=tk.LEFT)
        
        # 优化器状态
        opt_status_frame = tk.Frame(self.status_frame, bg=self.colors['white'])
        opt_status_frame.pack(anchor=tk.W, pady=(4, 0))
        
        # 状态点
        opt_status_dot = tk.Label(opt_status_frame,
                                 text="●",
                                 font=('Arial', 12),
                                 fg=self.colors['info'],
                                 bg=self.colors['white'])
        opt_status_dot.pack(side=tk.LEFT)
        
        self.optimizer_status_label = tk.Label(opt_status_frame,
                                             text=" 查询优化器已启用",
                                             font=('楷体', 11, 'bold'),  # 更大更粗字体
                                             fg=self.colors['text_primary'],
                                             bg=self.colors['white'])
        self.optimizer_status_label.pack(side=tk.LEFT)

    def _create_main_interface(self):
        """创建现代化主界面"""
        # 创建主框架 - 使用更好的背景色
        main_frame = tk.Frame(self.root, bg=self.colors['bg_secondary'])
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 创建标题栏
        self._create_header(main_frame)

        # 创建内容容器
        content_frame = ttk.Frame(main_frame, style='Modern.TFrame')
        content_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 20))

        # 创建Notebook用于标签页 - 改进样式
        self.notebook = ttk.Notebook(content_frame, style='Modern.TNotebook')
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # 创建各个标签页
        self._create_query_tab()
        self._create_compiler_tab()
        self._create_storage_tab()
        self._create_tables_tab()
        self._create_performance_tab()
        self._create_distributed_tab()  # 新增分布式功能标签页

        # 创建底部状态栏
        self._create_status_bar()

    def _create_query_tab(self):
        """创建SQL查询标签页"""
        query_frame = ttk.Frame(self.notebook, style='Modern.TFrame')
        self.notebook.add(query_frame, text="🔍 SQL查询执行")

        # 创建分割面板
        paned_window = ttk.PanedWindow(query_frame, orient=tk.VERTICAL)
        paned_window.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # 上部分：SQL输入区域
        top_frame = ttk.LabelFrame(paned_window, text="📝 SQL查询输入", 
                                  padding="15", style='Modern.TLabelframe')
        paned_window.add(top_frame, weight=1)

        # SQL输入区域
        input_container = ttk.Frame(top_frame, style='Modern.TFrame')
        input_container.pack(fill=tk.BOTH, expand=True)
        
        # SQL输入文本框 - 现代化样式与语法高亮
        self.sql_text = scrolledtext.ScrolledText(
            input_container,
            height=8,
            font=('Times New Roman', 14),  # SQL使用英文字体
            wrap=tk.WORD,
            bg=self.colors['bg_main'],
            fg='#1f2937',  # 更深的文字颜色
            insertbackground=self.colors['primary'],
            selectbackground=self.colors['hover_dark'],
            relief='solid',
            borderwidth=2,
            highlightthickness=1,
            highlightcolor=self.colors['primary'],
            highlightbackground=self.colors['border'],
            padx=15,  # 更大的内边距
            pady=10,
            undo=True,  # 启用撤销功能
            maxundo=20
        )
        self.sql_text.pack(fill=tk.BOTH, expand=True, pady=(0, 15))
        
        # 初始化SQL语法高亮器
        self.sql_highlighter = SQLSyntaxHighlighter(self.sql_text, self.colors)

        # 示例SQL语句 - 展示语法高亮效果
        sample_sql = """  -- 创建作者表 (authors)
CREATE TABLE authors (
    author_id INT PRIMARY KEY, -- 作者ID, 主键
    author_name VARCHAR(100) NOT NULL UNIQUE  -- 作者姓名, 不能为空, 且唯一
);
"""

        self.sql_text.insert(tk.END, sample_sql)
        
        # 触发初始语法高亮
        self.sql_highlighter.highlight_now()
        
        # 添加快捷键绑定
        self.sql_text.bind('<Control-Return>', lambda e: self._execute_query())
        self.sql_text.bind('<F5>', lambda e: self._execute_query())
        self.sql_text.bind('<Control-r>', lambda e: self._analyze_sql())
        self.sql_text.bind('<Control-l>', lambda e: self._clear_query())
        
        # 快捷键提示
        shortcut_frame = ttk.Frame(input_container, style='Modern.TFrame')
        shortcut_frame.pack(fill=tk.X, pady=(5, 0))
        
        shortcut_label = ttk.Label(shortcut_frame,
                                  text="💡 快捷键: Ctrl+Enter/F5=执行 | Ctrl+R=分析 | Ctrl+L=清空",
                                  style='Caption.TLabel')
        shortcut_label.pack(anchor=tk.W)

        # 按钮框架
        button_frame = ttk.Frame(top_frame, style='Modern.TFrame')
        button_frame.pack(fill=tk.X, pady=(5, 0))

        # 主要操作按钮 - 使用现代化布局
        primary_buttons = ttk.Frame(button_frame, style='Modern.TFrame')
        primary_buttons.pack(side=tk.LEFT, fill=tk.Y)
        
        ttk.Button(primary_buttons, text="🚀 执行查询", 
                  command=self._execute_query, style='Success.TButton').pack(side=tk.LEFT, padx=(0, 12))
        ttk.Button(primary_buttons, text="🔍 分析SQL", 
                  command=self._analyze_sql, style='Info.TButton').pack(side=tk.LEFT, padx=(0, 12))
        ttk.Button(primary_buttons, text="🗑️ 清空", 
                  command=self._clear_query, style='Danger.TButton').pack(side=tk.LEFT, padx=(0, 12))
        
        # 分隔线
        separator = ttk.Separator(button_frame, orient=tk.VERTICAL)
        separator.pack(side=tk.LEFT, fill=tk.Y, padx=15)
        
        # 索引模式控制 - 改进样式
        index_frame = ttk.Frame(button_frame, style='Modern.TFrame')
        index_frame.pack(side=tk.LEFT, padx=(15, 0), fill=tk.Y)
        
        # 索引选项标签
        index_label = ttk.Label(index_frame, text="查询模式:", 
                               style='Caption.TLabel')
        index_label.pack(side=tk.LEFT, padx=(0, 8))
        
        self.use_index_var = tk.BooleanVar(value=True)
        index_check = ttk.Checkbutton(index_frame, text="🌲 使用B+树索引", 
                                     variable=self.use_index_var,
                                     style='Modern.TCheckbutton')
        index_check.pack(side=tk.LEFT, padx=(0, 12))
        
        ttk.Button(index_frame, text="⚡ 性能对比", 
                  command=self._compare_performance, style='Warning.TButton').pack(side=tk.LEFT)
        
        # 右侧按钮
        ttk.Button(button_frame, text="💾 保存查询", 
                  command=self._save_query, style='Modern.TButton').pack(side=tk.RIGHT)

        # 下部分：结果显示区域
        bottom_frame = ttk.LabelFrame(paned_window, text="📊 查询结果与执行信息", 
                                     padding="15", style='Modern.TLabelframe')
        paned_window.add(bottom_frame, weight=2)

        # 创建结果显示的Notebook
        result_notebook = ttk.Notebook(bottom_frame, style='Modern.TNotebook')
        result_notebook.pack(fill=tk.BOTH, expand=True)

        # 结果表格标签页
        result_frame = ttk.Frame(result_notebook)
        result_notebook.add(result_frame, text="📊 结果数据")

        # 结果表格 - 使用现代化样式
        columns = ("Column1", "Column2", "Column3", "Column4", "Column5")
        self.result_tree = ttk.Treeview(result_frame, columns=columns, show='headings',
                                       style='Modern.Treeview')

        # 设置列标题
        for col in columns:
            self.result_tree.heading(col, text=col)
            self.result_tree.column(col, width=120, anchor='center')

        # 添加现代化滚动条
        result_scrollbar_y = ttk.Scrollbar(result_frame, orient=tk.VERTICAL, 
                                          command=self.result_tree.yview,
                                          style='Modern.Vertical.TScrollbar')
        result_scrollbar_x = ttk.Scrollbar(result_frame, orient=tk.HORIZONTAL, 
                                          command=self.result_tree.xview,
                                          style='Modern.Horizontal.TScrollbar')
        self.result_tree.configure(yscrollcommand=result_scrollbar_y.set, 
                                  xscrollcommand=result_scrollbar_x.set)

        self.result_tree.grid(row=0, column=0, sticky='nsew', padx=(0, 1), pady=(0, 1))
        result_scrollbar_y.grid(row=0, column=1, sticky='ns')
        result_scrollbar_x.grid(row=1, column=0, sticky='ew')

        result_frame.grid_rowconfigure(0, weight=1)
        result_frame.grid_columnconfigure(0, weight=1)

        # 执行信息标签页
        info_frame = ttk.Frame(result_notebook)
        result_notebook.add(info_frame, text="📋 执行信息")

        self.info_text = scrolledtext.ScrolledText(
            info_frame,
            height=10,
            font=('Times New Roman', 12),  # 查询结果使用英文字体
            state=tk.DISABLED,
            bg=self.colors['bg_main'],
            fg='#1f2937',  # 更深的文字颜色
            relief='solid',
            borderwidth=2,
            highlightthickness=1,
            highlightcolor=self.colors['border'],
            highlightbackground=self.colors['border_light'],
            padx=15,  # 更大内边距
            pady=10
        )
        self.info_text.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)

    def _create_compiler_tab(self):
        """创建编译器分析标签页"""
        compiler_frame = ttk.Frame(self.notebook, style='Modern.TFrame')
        self.notebook.add(compiler_frame, text="🔧 SQL编译器")

        # 创建分割面板
        paned_window = ttk.PanedWindow(compiler_frame, orient=tk.HORIZONTAL)
        paned_window.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # 左侧：输入和控制
        left_frame = ttk.LabelFrame(paned_window, text="📝 编译器输入", 
                                   padding="15", style='Modern.TLabelframe')
        paned_window.add(left_frame, weight=1)

        # SQL输入框 - 现代化样式与语法高亮
        ttk.Label(left_frame, text="💬 输入SQL语句:", style='Subtitle.TLabel').pack(anchor=tk.W, pady=(0, 8))
        self.compiler_sql_text = scrolledtext.ScrolledText(
            left_frame,
            height=6,
            font=('Times New Roman', 13),  # SQL编译器使用英文字体
            bg=self.colors['bg_main'],
            fg='#1f2937',  # 更深的文字颜色
            insertbackground=self.colors['primary'],
            selectbackground=self.colors['hover_dark'],
            relief='solid',
            borderwidth=2,
            highlightthickness=1,
            highlightcolor=self.colors['primary'],
            highlightbackground=self.colors['border'],
            padx=12,  # 更大内边距
            pady=8,
            undo=True,
            maxundo=20
        )
        self.compiler_sql_text.pack(fill=tk.BOTH, expand=True, pady=(0, 20))
        
        # 为编译器SQL输入框也添加语法高亮
        self.compiler_sql_highlighter = SQLSyntaxHighlighter(self.compiler_sql_text, self.colors)

        # 示例SQL - 编译器分析示例
        compiler_sample = """  """
        
        self.compiler_sql_text.insert(tk.END, compiler_sample)
        
        # 触发初始语法高亮
        self.compiler_sql_highlighter.highlight_now()

        # 控制按钮 - 现代化设计
        ttk.Label(left_frame, text="🔧 编译步骤:", style='Subtitle.TLabel').pack(anchor=tk.W, pady=(0, 12))
        
        # 按钮容器
        buttons_container = ttk.Frame(left_frame, style='Modern.TFrame')
        buttons_container.pack(fill=tk.X)
        
        # 编译步骤按钮 - 使用不同的颜色区分
        ttk.Button(buttons_container, text="🔍 词法分析", 
                  command=self._lexical_analysis, style='Info.TButton').pack(fill=tk.X, pady=4)
        ttk.Button(buttons_container, text="🌳 语法分析", 
                  command=self._syntax_analysis, style='Modern.TButton').pack(fill=tk.X, pady=4)
        ttk.Button(buttons_container, text="✅ 语义分析", 
                  command=self._semantic_analysis, style='Warning.TButton').pack(fill=tk.X, pady=4)
        ttk.Button(buttons_container, text="⚙️ 代码生成", 
                  command=self._code_generation, style='Success.TButton').pack(fill=tk.X, pady=4)

        # 右侧：分析结果
        right_frame = ttk.LabelFrame(paned_window, text="📊 编译分析结果", 
                                    padding="15", style='Modern.TLabelframe')
        paned_window.add(right_frame, weight=2)

        # 创建结果显示的Notebook
        compiler_notebook = ttk.Notebook(right_frame, style='Modern.TNotebook')
        compiler_notebook.pack(fill=tk.BOTH, expand=True)

        # 词法分析结果
        self.lexer_frame = ttk.Frame(compiler_notebook)
        compiler_notebook.add(self.lexer_frame, text="词法分析")

        self.lexer_result = scrolledtext.ScrolledText(
            self.lexer_frame,
            font=('Times New Roman', 10),
            state=tk.DISABLED
        )
        self.lexer_result.pack(fill=tk.BOTH, expand=True)

        # 语法分析结果
        self.parser_frame = ttk.Frame(compiler_notebook)
        compiler_notebook.add(self.parser_frame, text="语法分析")

        self.parser_result = scrolledtext.ScrolledText(
            self.parser_frame,
            font=('Times New Roman', 10),
            state=tk.DISABLED
        )
        self.parser_result.pack(fill=tk.BOTH, expand=True)

        # 语义分析结果
        self.semantic_frame = ttk.Frame(compiler_notebook)
        compiler_notebook.add(self.semantic_frame, text="语义分析")

        self.semantic_result = scrolledtext.ScrolledText(
            self.semantic_frame,
            font=('Times New Roman', 10),
            state=tk.DISABLED
        )
        self.semantic_result.pack(fill=tk.BOTH, expand=True)

        # 目标代码
        self.codegen_frame = ttk.Frame(compiler_notebook)
        compiler_notebook.add(self.codegen_frame, text="目标代码")

        self.codegen_result = scrolledtext.ScrolledText(
            self.codegen_frame,
            font=('Times New Roman', 10),
            state=tk.DISABLED
        )
        self.codegen_result.pack(fill=tk.BOTH, expand=True)

    def _create_storage_tab(self):
        """创建存储引擎标签页"""
        storage_frame = ttk.Frame(self.notebook, style='Modern.TFrame')
        self.notebook.add(storage_frame, text="💾 存储引擎")

        # 创建分割面板
        paned_window = ttk.PanedWindow(storage_frame, orient=tk.VERTICAL)
        paned_window.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # 上部分：存储统计信息
        stats_frame = ttk.LabelFrame(paned_window, text="📊 存储引擎统计", 
                                    padding="15", style='Modern.TLabelframe')
        paned_window.add(stats_frame, weight=1)

        # 统计信息显示
        self.storage_stats_text = scrolledtext.ScrolledText(
            stats_frame,
            height=10,
            font=('Times New Roman', 10),
            state=tk.DISABLED,
            bg=self.colors['bg_main'],
            fg=self.colors['text_primary'],
            relief='solid',
            borderwidth=1
        )
        self.storage_stats_text.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        # 控制按钮
        control_frame = ttk.Frame(stats_frame, style='Modern.TFrame')
        control_frame.pack(anchor=tk.E)
        
        ttk.Button(control_frame, text="🔄 刷新统计", 
                  command=self._refresh_storage_stats, style='Modern.TButton').pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(control_frame, text="⚙️ 优化设置", 
                  command=self._show_optimizer_settings, style='Success.TButton').pack(side=tk.LEFT)

        # 下部分：缓存和页面管理
        cache_frame = ttk.LabelFrame(paned_window, text="🗄️ 缓存和页面管理", 
                                    padding="15", style='Modern.TLabelframe')
        paned_window.add(cache_frame, weight=1)

        # 创建缓存信息的Notebook
        cache_notebook = ttk.Notebook(cache_frame, style='Modern.TNotebook')
        cache_notebook.pack(fill=tk.BOTH, expand=True)

        # 缓存状态
        cache_status_frame = ttk.Frame(cache_notebook)
        cache_notebook.add(cache_status_frame, text="缓存状态")

        self.cache_status_text = scrolledtext.ScrolledText(
            cache_status_frame,
            font=('Times New Roman', 10),
            state=tk.DISABLED
        )
        self.cache_status_text.pack(fill=tk.BOTH, expand=True)

        # 页面信息
        page_info_frame = ttk.Frame(cache_notebook)
        cache_notebook.add(page_info_frame, text="页面信息")

        self.page_info_text = scrolledtext.ScrolledText(
            page_info_frame,
            font=('Times New Roman', 10),
            state=tk.DISABLED
        )
        self.page_info_text.pack(fill=tk.BOTH, expand=True)

        # 索引信息
        index_info_frame = ttk.Frame(cache_notebook)
        cache_notebook.add(index_info_frame, text="索引信息")

        self.index_info_text = scrolledtext.ScrolledText(
            index_info_frame,
            font=('Times New Roman', 10),
            state=tk.DISABLED
        )
        self.index_info_text.pack(fill=tk.BOTH, expand=True)

    def _create_tables_tab(self):
        """创建现代化表管理标签页"""
        tables_frame = ttk.Frame(self.notebook, style='Modern.TFrame')
        self.notebook.add(tables_frame, text="📋 表管理")

        # 创建分割面板
        paned_window = ttk.PanedWindow(tables_frame, orient=tk.HORIZONTAL)
        paned_window.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        # 左侧：表列表和操作
        left_frame = ttk.LabelFrame(paned_window, text="📊 数据库表", 
                                   padding="15", style='Modern.TLabelframe')
        paned_window.add(left_frame, weight=1)

        # 表列表 - 使用现代化样式
        self.tables_listbox = tk.Listbox(left_frame, 
                                        font=('Times New Roman', 12, 'bold'),  # 表名使用英文字体
                                        bg=self.colors['bg_main'],
                                        fg='#1f2937',  # 更深的文字颜色
                                        selectbackground=self.colors['primary'],
                                        selectforeground=self.colors['white'],
                                        relief='solid',
                                        borderwidth=1,
                                        highlightthickness=1,
                                        highlightcolor=self.colors['primary'],
                                        highlightbackground=self.colors['border'])
        self.tables_listbox.pack(fill=tk.BOTH, expand=True, pady=(0, 15))
        self.tables_listbox.bind('<<ListboxSelect>>', self._on_table_select)

        # 表操作按钮 - 现代化布局
        table_buttons_frame = ttk.Frame(left_frame, style='Modern.TFrame')
        table_buttons_frame.pack(fill=tk.X)

        ttk.Button(table_buttons_frame, text="🔄 刷新", 
                  command=self._refresh_tables, style='Info.TButton').pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(table_buttons_frame, text="➕ 创建表", 
                  command=self._create_table_dialog, style='Success.TButton').pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(table_buttons_frame, text="🗑️ 删除表", 
                  command=self._drop_table, style='Danger.TButton').pack(side=tk.LEFT)

        # 右侧：表结构和数据
        right_frame = ttk.LabelFrame(paned_window, text="📋 表详细信息", 
                                    padding="15", style='Modern.TLabelframe')
        paned_window.add(right_frame, weight=2)

        # 创建表信息的Notebook
        table_notebook = ttk.Notebook(right_frame, style='Modern.TNotebook')
        table_notebook.pack(fill=tk.BOTH, expand=True)

        # 表结构标签页
        schema_frame = ttk.Frame(table_notebook, style='Modern.TFrame')
        table_notebook.add(schema_frame, text="🏗️ 表结构")

        # 列信息表格 - 现代化样式
        columns = ("列名", "类型", "长度", "主键", "唯一", "可空", "默认值")
        self.schema_tree = ttk.Treeview(schema_frame, columns=columns, show='headings',
                                       style='Modern.Treeview')

        for col in columns:
            self.schema_tree.heading(col, text=col)
            self.schema_tree.column(col, width=90, anchor='center')

        schema_scrollbar = ttk.Scrollbar(schema_frame, orient=tk.VERTICAL, 
                                        command=self.schema_tree.yview,
                                        style='Modern.Vertical.TScrollbar')
        self.schema_tree.configure(yscrollcommand=schema_scrollbar.set)

        self.schema_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 2))
        schema_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # 表数据标签页
        data_frame = ttk.Frame(table_notebook, style='Modern.TFrame')
        table_notebook.add(data_frame, text="📊 表数据")

        self.data_tree = ttk.Treeview(data_frame, show='headings',
                                     style='Modern.Treeview')

        data_scrollbar_y = ttk.Scrollbar(data_frame, orient=tk.VERTICAL, 
                                        command=self.data_tree.yview,
                                        style='Modern.Vertical.TScrollbar')
        data_scrollbar_x = ttk.Scrollbar(data_frame, orient=tk.HORIZONTAL, 
                                        command=self.data_tree.xview,
                                        style='Modern.Horizontal.TScrollbar')
        self.data_tree.configure(yscrollcommand=data_scrollbar_y.set, 
                                xscrollcommand=data_scrollbar_x.set)

        self.data_tree.grid(row=0, column=0, sticky='nsew', padx=(0, 2), pady=(0, 2))
        data_scrollbar_y.grid(row=0, column=1, sticky='ns')
        data_scrollbar_x.grid(row=1, column=0, sticky='ew')

        data_frame.grid_rowconfigure(0, weight=1)
        data_frame.grid_columnconfigure(0, weight=1)

    def _create_performance_tab(self):
        """创建现代化性能监控标签页"""
        perf_frame = ttk.Frame(self.notebook, style='Modern.TFrame')
        self.notebook.add(perf_frame, text="📈 性能监控")

        # 性能统计显示
        perf_stats_frame = ttk.LabelFrame(perf_frame, text="📊 性能统计", 
                                         padding="15", style='Modern.TLabelframe')
        perf_stats_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        self.perf_text = scrolledtext.ScrolledText(
            perf_stats_frame,
            font=('Times New Roman', 12),  # 性能数据使用英文字体
            state=tk.DISABLED,
            bg=self.colors['bg_main'],
            fg='#1f2937',  # 更深的文字颜色
            relief='solid',
            borderwidth=2,
            highlightthickness=1,
            highlightcolor=self.colors['border'],
            highlightbackground=self.colors['border_light'],
            padx=15,  # 更大内边距
            pady=10
        )
        self.perf_text.pack(fill=tk.BOTH, expand=True, pady=(0, 15))

    def _create_distributed_tab(self):
        """创建分布式功能标签页"""
        distributed_frame = ttk.Frame(self.notebook, style='Modern.TFrame')
        self.notebook.add(distributed_frame, text="🌐 分布式管理")
        
        # 创建分布式数据库实例（如果还没有）
        if not hasattr(self, 'distributed_db'):
            try:
                from src.distributed.distributed_database import DistributedDatabase
                self.distributed_db = None  # 初始化为None，由用户选择是否启用
            except ImportError:
                # 如果分布式模块不可用，显示提示信息
                error_label = ttk.Label(distributed_frame, 
                                      text="⚠️ 分布式功能模块不可用", 
                                      style='Subtitle.TLabel')
                error_label.pack(expand=True)
                return
        
        # 主容器
        main_container = ttk.Frame(distributed_frame, style='Modern.TFrame')
        main_container.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        
        # 分布式状态区域
        status_frame = ttk.LabelFrame(main_container, text="🌐 集群状态", 
                                    padding="15", style='Modern.TLabelframe')
        status_frame.pack(fill=tk.X, pady=(0, 15))
        
        # 状态显示
        self.distributed_status_text = scrolledtext.ScrolledText(
            status_frame,
            height=6,
            font=('Times New Roman', 11),
            state=tk.DISABLED,
            bg=self.colors['bg_main'],
            fg='#1f2937',
            relief='solid',
            borderwidth=2,
            padx=10,
            pady=8
        )
        self.distributed_status_text.pack(fill=tk.X, pady=(0, 10))
        
        # 控制按钮区域
        control_frame = ttk.Frame(status_frame, style='Modern.TFrame')
        control_frame.pack(fill=tk.X)
        
        # 启动/停止集群按钮
        self.cluster_start_btn = ttk.Button(control_frame, text="🚀 启动集群", 
                                          style='Success.TButton',
                                          command=self._start_distributed_cluster)
        self.cluster_start_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        self.cluster_stop_btn = ttk.Button(control_frame, text="🛑 停止集群", 
                                         style='Danger.TButton',
                                         command=self._stop_distributed_cluster,
                                         state=tk.DISABLED)
        self.cluster_stop_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        # 刷新状态按钮
        ttk.Button(control_frame, text="🔄 刷新状态", 
                  style='Info.TButton',
                  command=self._refresh_distributed_status).pack(side=tk.LEFT, padx=(0, 10))
        
        # 功能区域 - 使用Notebook
        functions_notebook = ttk.Notebook(main_container, style='Modern.TNotebook')
        functions_notebook.pack(fill=tk.BOTH, expand=True)
        
        # 分片管理标签页
        self._create_sharding_tab(functions_notebook)
        
        # 复制管理标签页
        self._create_replication_tab(functions_notebook)
        
        # 分布式事务标签页
        self._create_transaction_tab(functions_notebook)
        
        # 监控标签页
        self._create_monitoring_tab(functions_notebook)
        
        # 初始化状态显示
        self._refresh_distributed_status()
    
    def _create_sharding_tab(self, parent_notebook):
        """创建分片管理标签页"""
        shard_frame = ttk.Frame(parent_notebook, style='Modern.TFrame')
        parent_notebook.add(shard_frame, text="📊 分片管理")
        
        # 分片表创建区域
        create_frame = ttk.LabelFrame(shard_frame, text="创建分片表", 
                                    padding="15", style='Modern.TLabelframe')
        create_frame.pack(fill=tk.X, padx=15, pady=15)
        
        # 表名输入
        ttk.Label(create_frame, text="表名:", style='Subtitle.TLabel').grid(row=0, column=0, sticky=tk.W, pady=5)
        self.shard_table_name = ttk.Entry(create_frame, style='Modern.TEntry', width=20)
        self.shard_table_name.grid(row=0, column=1, padx=(10, 0), pady=5, sticky=tk.W)
        
        # 分片键输入
        ttk.Label(create_frame, text="分片键:", style='Subtitle.TLabel').grid(row=1, column=0, sticky=tk.W, pady=5)
        self.shard_key = ttk.Entry(create_frame, style='Modern.TEntry', width=20)
        self.shard_key.grid(row=1, column=1, padx=(10, 0), pady=5, sticky=tk.W)
        
        # 分片类型选择
        ttk.Label(create_frame, text="分片类型:", style='Subtitle.TLabel').grid(row=2, column=0, sticky=tk.W, pady=5)
        self.shard_type = ttk.Combobox(create_frame, values=["hash", "range", "directory"], 
                                     state="readonly", width=18)
        self.shard_type.set("hash")
        self.shard_type.grid(row=2, column=1, padx=(10, 0), pady=5, sticky=tk.W)
        
        # 分片数量
        ttk.Label(create_frame, text="分片数量:", style='Subtitle.TLabel').grid(row=3, column=0, sticky=tk.W, pady=5)
        self.shard_count = ttk.Spinbox(create_frame, from_=1, to=10, width=18)
        self.shard_count.set("3")
        self.shard_count.grid(row=3, column=1, padx=(10, 0), pady=5, sticky=tk.W)
        
        # 创建按钮
        ttk.Button(create_frame, text="创建分片表", 
                  style='Success.TButton',
                  command=self._create_sharded_table).grid(row=4, column=0, columnspan=2, pady=15)
        
        # 分片信息显示区域
        info_frame = ttk.LabelFrame(shard_frame, text="分片信息", 
                                  padding="15", style='Modern.TLabelframe')
        info_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=(0, 15))
        
        self.shard_info_text = scrolledtext.ScrolledText(
            info_frame,
            font=('Times New Roman', 11),
            bg=self.colors['bg_main'],
            fg='#1f2937',
            relief='solid',
            borderwidth=2,
            padx=10,
            pady=8
        )
        self.shard_info_text.pack(fill=tk.BOTH, expand=True)
    
    def _create_replication_tab(self, parent_notebook):
        """创建复制管理标签页"""
        replication_frame = ttk.Frame(parent_notebook, style='Modern.TFrame')
        parent_notebook.add(replication_frame, text="🔄 复制管理")
        
        # 复制组创建区域
        create_frame = ttk.LabelFrame(replication_frame, text="复制组管理", 
                                    padding="15", style='Modern.TLabelframe')
        create_frame.pack(fill=tk.X, padx=15, pady=15)
        
        # 复制组ID输入
        ttk.Label(create_frame, text="复制组ID:", style='Subtitle.TLabel').grid(row=0, column=0, sticky=tk.W, pady=5)
        self.replication_group_id = ttk.Entry(create_frame, style='Modern.TEntry', width=20)
        self.replication_group_id.grid(row=0, column=1, padx=(10, 0), pady=5, sticky=tk.W)
        
        # 一致性级别选择
        ttk.Label(create_frame, text="一致性级别:", style='Subtitle.TLabel').grid(row=1, column=0, sticky=tk.W, pady=5)
        self.consistency_level = ttk.Combobox(create_frame, 
                                            values=["eventual", "strong", "weak"], 
                                            state="readonly", width=18)
        self.consistency_level.set("eventual")
        self.consistency_level.grid(row=1, column=1, padx=(10, 0), pady=5, sticky=tk.W)
        
        # 按钮区域
        button_frame = ttk.Frame(create_frame, style='Modern.TFrame')
        button_frame.grid(row=2, column=0, columnspan=2, pady=15)
        
        ttk.Button(button_frame, text="创建复制组", 
                  style='Success.TButton',
                  command=self._create_replication_group).pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Button(button_frame, text="加入复制组", 
                  style='Info.TButton',
                  command=self._join_replication_group).pack(side=tk.LEFT, padx=(0, 10))
        
        # 复制状态显示区域
        status_frame = ttk.LabelFrame(replication_frame, text="复制状态", 
                                    padding="15", style='Modern.TLabelframe')
        status_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=(0, 15))
        
        self.replication_status_text = scrolledtext.ScrolledText(
            status_frame,
            font=('Times New Roman', 11),
            bg=self.colors['bg_main'],
            fg='#1f2937',
            relief='solid',
            borderwidth=2,
            padx=10,
            pady=8
        )
        self.replication_status_text.pack(fill=tk.BOTH, expand=True)
    
    def _create_transaction_tab(self, parent_notebook):
        """创建分布式事务标签页"""
        transaction_frame = ttk.Frame(parent_notebook, style='Modern.TFrame')
        parent_notebook.add(transaction_frame, text="💳 分布式事务")
        
        # 事务控制区域
        control_frame = ttk.LabelFrame(transaction_frame, text="事务控制", 
                                     padding="15", style='Modern.TLabelframe')
        control_frame.pack(fill=tk.X, padx=15, pady=15)
        
        # 隔离级别选择
        ttk.Label(control_frame, text="隔离级别:", style='Subtitle.TLabel').grid(row=0, column=0, sticky=tk.W, pady=5)
        self.isolation_level = ttk.Combobox(control_frame, 
                                          values=["read_uncommitted", "read_committed", 
                                                "repeatable_read", "serializable"], 
                                          state="readonly", width=20)
        self.isolation_level.set("read_committed")
        self.isolation_level.grid(row=0, column=1, padx=(10, 0), pady=5, sticky=tk.W)
        
        # 事务操作按钮
        button_frame = ttk.Frame(control_frame, style='Modern.TFrame')
        button_frame.grid(row=1, column=0, columnspan=2, pady=15)
        
        ttk.Button(button_frame, text="开始事务", 
                  style='Success.TButton',
                  command=self._begin_transaction).pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Button(button_frame, text="提交事务", 
                  style='Info.TButton',
                  command=self._commit_transaction).pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Button(button_frame, text="回滚事务", 
                  style='Warning.TButton',
                  command=self._rollback_transaction).pack(side=tk.LEFT, padx=(0, 10))
        
        # 事务状态显示区域
        status_frame = ttk.LabelFrame(transaction_frame, text="事务状态", 
                                    padding="15", style='Modern.TLabelframe')
        status_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=(0, 15))
        
        self.transaction_status_text = scrolledtext.ScrolledText(
            status_frame,
            font=('Times New Roman', 11),
            bg=self.colors['bg_main'],
            fg='#1f2937',
            relief='solid',
            borderwidth=2,
            padx=10,
            pady=8
        )
        self.transaction_status_text.pack(fill=tk.BOTH, expand=True)
        
        # 当前事务ID
        self.current_transaction_id = None
    
    def _create_monitoring_tab(self, parent_notebook):
        """创建监控标签页"""
        monitoring_frame = ttk.Frame(parent_notebook, style='Modern.TFrame')
        parent_notebook.add(monitoring_frame, text="📈 性能监控")
        
        # 监控控制区域
        control_frame = ttk.LabelFrame(monitoring_frame, text="监控控制", 
                                     padding="15", style='Modern.TLabelframe')
        control_frame.pack(fill=tk.X, padx=15, pady=15)
        
        # 监控按钮
        ttk.Button(control_frame, text="📊 获取性能指标", 
                  style='Info.TButton',
                  command=self._get_performance_metrics).pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Button(control_frame, text="🐌 查看慢查询", 
                  style='Warning.TButton',
                  command=self._get_slow_queries).pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Button(control_frame, text="🔄 刷新监控", 
                  style='Modern.TButton',
                  command=self._refresh_monitoring).pack(side=tk.LEFT, padx=(0, 10))
        
        # 监控信息显示区域
        info_frame = ttk.LabelFrame(monitoring_frame, text="监控信息", 
                                  padding="15", style='Modern.TLabelframe')
        info_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=(0, 15))
        
        self.monitoring_text = scrolledtext.ScrolledText(
            info_frame,
            font=('Times New Roman', 11),
            bg=self.colors['bg_main'],
            fg='#1f2937',
            relief='solid',
            borderwidth=2,
            padx=10,
            pady=8
        )
        self.monitoring_text.pack(fill=tk.BOTH, expand=True)
    
    # 分布式功能的事件处理方法
    def _start_distributed_cluster(self):
        """启动分布式集群"""
        try:
            from src.distributed.distributed_database import DistributedDatabase
            
            # 创建3节点集群
            cluster_members = ["node1", "node2", "node3"]
            self.distributed_db = DistributedDatabase("node1", cluster_members)
            self.distributed_db.start()
            
            # 让其他节点加入集群
            for member in cluster_members[1:]:
                self.distributed_db.join_cluster(member, f"endpoint_{member}")
            
            # 更新按钮状态
            self.cluster_start_btn.config(state=tk.DISABLED)
            self.cluster_stop_btn.config(state=tk.NORMAL)
            
            self._update_distributed_status("✅ 分布式集群启动成功！\n包含节点: " + ", ".join(cluster_members))
            self._refresh_distributed_status()
            
        except Exception as e:
            self._update_distributed_status(f"❌ 启动集群失败: {str(e)}")
    
    def _stop_distributed_cluster(self):
        """停止分布式集群"""
        try:
            if hasattr(self, 'distributed_db') and self.distributed_db:
                self.distributed_db.stop()
                self.distributed_db = None
            
            # 更新按钮状态
            self.cluster_start_btn.config(state=tk.NORMAL)
            self.cluster_stop_btn.config(state=tk.DISABLED)
            
            self._update_distributed_status("🛑 分布式集群已停止")
            
        except Exception as e:
            self._update_distributed_status(f"❌ 停止集群失败: {str(e)}")
    
    def _refresh_distributed_status(self):
        """刷新分布式状态"""
        try:
            if hasattr(self, 'distributed_db') and self.distributed_db:
                status = self.distributed_db.get_cluster_status()
                health = self.distributed_db.health_check()
                
                status_text = f"""🌐 集群状态信息:
{'='*50}
节点ID: {status.get('cluster', {}).get('node_id', 'Unknown')}
角色: {status.get('cluster', {}).get('role', 'Unknown')}
领导者: {status.get('cluster', {}).get('leader_id', 'None')}
总节点数: {status.get('cluster', {}).get('total_members', 0)}
活跃节点数: {status.get('cluster', {}).get('active_members', 0)}
集群健康度: {status.get('health', {}).get('cluster_health_percentage', 0):.1f}%

🏥 健康检查:
{'='*50}
系统状态: {health.get('status', 'Unknown')}
运行状态: {'正常' if self.distributed_db.running else '停止'}"""
                
                self._update_distributed_status(status_text)
            else:
                self._update_distributed_status("🔴 分布式集群未启动\n\n点击 '🚀 启动集群' 按钮开始使用分布式功能")
                
        except Exception as e:
            self._update_distributed_status(f"❌ 获取状态失败: {str(e)}")
    
    def _update_distributed_status(self, message):
        """更新分布式状态显示"""
        self.distributed_status_text.config(state=tk.NORMAL)
        self.distributed_status_text.delete(1.0, tk.END)
        self.distributed_status_text.insert(tk.END, message)
        self.distributed_status_text.config(state=tk.DISABLED)
    
    def _create_sharded_table(self):
        """创建分片表"""
        try:
            if not hasattr(self, 'distributed_db') or not self.distributed_db:
                messagebox.showwarning("警告", "请先启动分布式集群")
                return
            
            table_name = self.shard_table_name.get().strip()
            shard_key = self.shard_key.get().strip()
            shard_type_str = self.shard_type.get()
            shard_count = int(self.shard_count.get())
            
            if not table_name or not shard_key:
                messagebox.showwarning("警告", "请填写表名和分片键")
                return
            
            from src.distributed.sharding import ShardingType
            shard_type = ShardingType(shard_type_str)
            
            nodes = ["node1", "node2", "node3"]
            success = self.distributed_db.create_sharded_table(
                table_name, shard_key, shard_type, shard_count, nodes
            )
            
            if success:
                messagebox.showinfo("成功", f"分片表 '{table_name}' 创建成功")
                self._refresh_shard_info()
            else:
                messagebox.showerror("错误", f"创建分片表 '{table_name}' 失败")
                
        except Exception as e:
            messagebox.showerror("错误", f"创建分片表失败: {str(e)}")
    
    def _refresh_shard_info(self):
        """刷新分片信息"""
        try:
            if not hasattr(self, 'distributed_db') or not self.distributed_db:
                return
            
            stats = self.distributed_db.shard_manager.get_statistics()
            info_text = "📊 分片表统计信息:\n" + "="*50 + "\n"
            info_text += f"分片表总数: {stats['total_sharded_tables']}\n\n"
            
            for table_name, table_info in stats['tables'].items():
                shard_info = self.distributed_db.get_shard_info(table_name)
                if shard_info:
                    info_text += f"表名: {table_name}\n"
                    info_text += f"分片键: {shard_info['shard_key']}\n"
                    info_text += f"分片类型: {shard_info['shard_type']}\n"
                    info_text += f"分片数量: {shard_info['total_shards']}\n"
                    info_text += "分片详情:\n"
                    for shard in shard_info['shards']:
                        info_text += f"  - {shard['shard_id']} (节点: {shard['node_id']}, 状态: {shard['status']})\n"
                    info_text += "\n"
            
            self.shard_info_text.delete(1.0, tk.END)
            self.shard_info_text.insert(tk.END, info_text)
            
        except Exception as e:
            self.shard_info_text.delete(1.0, tk.END)
            self.shard_info_text.insert(tk.END, f"❌ 获取分片信息失败: {str(e)}")
    
    def _create_replication_group(self):
        """创建复制组"""
        try:
            if not hasattr(self, 'distributed_db') or not self.distributed_db:
                messagebox.showwarning("警告", "请先启动分布式集群")
                return
            
            group_id = self.replication_group_id.get().strip()
            consistency = self.consistency_level.get()
            
            if not group_id:
                messagebox.showwarning("警告", "请输入复制组ID")
                return
            
            from src.distributed.replication import ConsistencyLevel
            consistency_level = ConsistencyLevel(consistency)
            
            success = self.distributed_db.create_replication_group(group_id, consistency_level)
            
            if success:
                messagebox.showinfo("成功", f"复制组 '{group_id}' 创建成功")
                self._refresh_replication_status()
            else:
                messagebox.showerror("错误", f"创建复制组 '{group_id}' 失败")
                
        except Exception as e:
            messagebox.showerror("错误", f"创建复制组失败: {str(e)}")
    
    def _join_replication_group(self):
        """加入复制组"""
        try:
            if not hasattr(self, 'distributed_db') or not self.distributed_db:
                messagebox.showwarning("警告", "请先启动分布式集群")
                return
            
            group_id = self.replication_group_id.get().strip()
            
            if not group_id:
                messagebox.showwarning("警告", "请输入复制组ID")
                return
            
            success = self.distributed_db.join_replication_group(group_id, "master")
            
            if success:
                messagebox.showinfo("成功", f"已加入复制组 '{group_id}' 作为主节点")
                self._refresh_replication_status()
            else:
                messagebox.showerror("错误", f"加入复制组 '{group_id}' 失败")
                
        except Exception as e:
            messagebox.showerror("错误", f"加入复制组失败: {str(e)}")
    
    def _refresh_replication_status(self):
        """刷新复制状态"""
        try:
            if not hasattr(self, 'distributed_db') or not self.distributed_db:
                return
            
            status = self.distributed_db.replication_manager.get_all_groups_status()
            
            status_text = "🔄 复制组状态:\n" + "="*50 + "\n"
            
            if status:
                for group_id, group_status in status.items():
                    status_text += f"复制组: {group_id}\n"
                    status_text += f"一致性级别: {group_status.get('consistency_level', 'Unknown')}\n"
                    status_text += f"复制模式: {group_status.get('replication_mode', 'Unknown')}\n"
                    
                    master = group_status.get('master')
                    if master:
                        status_text += f"主节点: {master.get('node_id', 'Unknown')}\n"
                    
                    slaves = group_status.get('slaves', [])
                    status_text += f"从节点数: {len(slaves)}\n"
                    
                    status_text += f"当前序列号: {group_status.get('current_sequence', 0)}\n"
                    status_text += f"日志数量: {group_status.get('log_count', 0)}\n\n"
            else:
                status_text += "暂无复制组\n"
            
            self.replication_status_text.delete(1.0, tk.END)
            self.replication_status_text.insert(tk.END, status_text)
            
        except Exception as e:
            self.replication_status_text.delete(1.0, tk.END)
            self.replication_status_text.insert(tk.END, f"❌ 获取复制状态失败: {str(e)}")
    
    def _begin_transaction(self):
        """开始事务"""
        try:
            if not hasattr(self, 'distributed_db') or not self.distributed_db:
                messagebox.showwarning("警告", "请先启动分布式集群")
                return
            
            isolation = self.isolation_level.get()
            
            from src.distributed.transaction import IsolationLevel
            isolation_level = IsolationLevel(isolation)
            
            self.current_transaction_id = self.distributed_db.begin_transaction(isolation_level)
            
            messagebox.showinfo("成功", f"事务已开始\nID: {self.current_transaction_id}")
            self._refresh_transaction_status()
            
        except Exception as e:
            messagebox.showerror("错误", f"开始事务失败: {str(e)}")
    
    def _commit_transaction(self):
        """提交事务"""
        try:
            if not self.current_transaction_id:
                messagebox.showwarning("警告", "没有活跃的事务")
                return
            
            if not hasattr(self, 'distributed_db') or not self.distributed_db:
                messagebox.showwarning("警告", "分布式集群未启动")
                return
            
            success = self.distributed_db.commit_transaction(self.current_transaction_id)
            
            if success:
                messagebox.showinfo("成功", f"事务 {self.current_transaction_id} 提交成功")
            else:
                messagebox.showerror("错误", f"事务 {self.current_transaction_id} 提交失败")
            
            self.current_transaction_id = None
            self._refresh_transaction_status()
            
        except Exception as e:
            messagebox.showerror("错误", f"提交事务失败: {str(e)}")
    
    def _rollback_transaction(self):
        """回滚事务"""
        try:
            if not self.current_transaction_id:
                messagebox.showwarning("警告", "没有活跃的事务")
                return
            
            if not hasattr(self, 'distributed_db') or not self.distributed_db:
                messagebox.showwarning("警告", "分布式集群未启动")
                return
            
            success = self.distributed_db.abort_transaction(self.current_transaction_id)
            
            if success:
                messagebox.showinfo("成功", f"事务 {self.current_transaction_id} 已回滚")
            else:
                messagebox.showerror("错误", f"事务 {self.current_transaction_id} 回滚失败")
            
            self.current_transaction_id = None
            self._refresh_transaction_status()
            
        except Exception as e:
            messagebox.showerror("错误", f"回滚事务失败: {str(e)}")
    
    def _refresh_transaction_status(self):
        """刷新事务状态"""
        try:
            if not hasattr(self, 'distributed_db') or not self.distributed_db:
                return
            
            stats = self.distributed_db.transaction_manager.get_statistics()
            
            status_text = "💳 分布式事务状态:\n" + "="*50 + "\n"
            status_text += f"当前事务ID: {self.current_transaction_id or '无'}\n\n"
            
            coordinator_stats = stats.get('coordinator', {})
            status_text += f"协调器统计:\n"
            status_text += f"  活跃事务数: {coordinator_stats.get('active_transactions', 0)}\n"
            status_text += f"  节点ID: {coordinator_stats.get('node_id', 'Unknown')}\n\n"
            
            participant_stats = stats.get('participant', {})
            status_text += f"参与者统计:\n"
            status_text += f"  参与事务数: {participant_stats.get('participant_transactions', 0)}\n\n"
            
            lock_stats = stats.get('locks', {})
            status_text += f"锁统计:\n"
            status_text += f"  总锁数: {lock_stats.get('total_locks', 0)}\n"
            status_text += f"  锁定资源数: {lock_stats.get('locked_resources', 0)}\n"
            status_text += f"  等待请求数: {lock_stats.get('waiting_requests', 0)}\n"
            status_text += f"  持锁事务数: {lock_stats.get('transactions_with_locks', 0)}\n"
            
            self.transaction_status_text.delete(1.0, tk.END)
            self.transaction_status_text.insert(tk.END, status_text)
            
        except Exception as e:
            self.transaction_status_text.delete(1.0, tk.END)
            self.transaction_status_text.insert(tk.END, f"❌ 获取事务状态失败: {str(e)}")
    
    def _get_performance_metrics(self):
        """获取性能指标"""
        try:
            if not hasattr(self, 'distributed_db') or not self.distributed_db:
                messagebox.showwarning("警告", "请先启动分布式集群")
                return
            
            metrics = self.distributed_db.get_performance_metrics()
            
            import json
            metrics_text = "📊 性能指标:\n" + "="*50 + "\n"
            metrics_text += json.dumps(metrics, indent=2, ensure_ascii=False, default=str)
            
            self.monitoring_text.delete(1.0, tk.END)
            self.monitoring_text.insert(tk.END, metrics_text)
            
        except Exception as e:
            self.monitoring_text.delete(1.0, tk.END)
            self.monitoring_text.insert(tk.END, f"❌ 获取性能指标失败: {str(e)}")
    
    def _get_slow_queries(self):
        """获取慢查询"""
        try:
            if not hasattr(self, 'distributed_db') or not self.distributed_db:
                messagebox.showwarning("警告", "请先启动分布式集群")
                return
            
            slow_queries = self.distributed_db.get_slow_queries(20)
            
            queries_text = "🐌 慢查询日志:\n" + "="*50 + "\n"
            
            if slow_queries:
                for i, query in enumerate(slow_queries, 1):
                    queries_text += f"查询 {i}:\n"
                    queries_text += f"  ID: {query.get('query_id', 'Unknown')}\n"
                    queries_text += f"  SQL: {query.get('sql', 'Unknown')[:100]}...\n"
                    queries_text += f"  执行时间: {query.get('execution_time', 0):.3f}s\n"
                    queries_text += f"  返回行数: {query.get('rows_returned', 0)}\n\n"
            else:
                queries_text += "暂无慢查询记录\n"
            
            self.monitoring_text.delete(1.0, tk.END)
            self.monitoring_text.insert(tk.END, queries_text)
            
        except Exception as e:
            self.monitoring_text.delete(1.0, tk.END)
            self.monitoring_text.insert(tk.END, f"❌ 获取慢查询失败: {str(e)}")
    
    def _refresh_monitoring(self):
        """刷新监控信息"""
        try:
            if not hasattr(self, 'distributed_db') or not self.distributed_db:
                return
            
            system_status = self.distributed_db.get_system_status()
            
            import json
            status_text = "📈 系统监控信息:\n" + "="*50 + "\n"
            status_text += json.dumps(system_status, indent=2, ensure_ascii=False, default=str)
            
            self.monitoring_text.delete(1.0, tk.END)
            self.monitoring_text.insert(tk.END, status_text)
            
        except Exception as e:
            self.monitoring_text.delete(1.0, tk.END)
            self.monitoring_text.insert(tk.END, f"❌ 刷新监控信息失败: {str(e)}")

        # 控制按钮 - 现代化布局
        perf_buttons_frame = ttk.Frame(perf_stats_frame, style='Modern.TFrame')
        perf_buttons_frame.pack(fill=tk.X)

        ttk.Button(perf_buttons_frame, text="🔄 刷新", 
                  command=self._refresh_performance, style='Info.TButton').pack(side=tk.LEFT, padx=(0, 12))
        ttk.Button(perf_buttons_frame, text="📊 详细统计", 
                  command=self._show_detailed_stats, style='Modern.TButton').pack(side=tk.LEFT, padx=(0, 12))
        ttk.Button(perf_buttons_frame, text="🧹 清除统计", 
                  command=self._clear_stats, style='Warning.TButton').pack(side=tk.LEFT)

    def _create_status_bar(self):
        """创建现代化状态栏"""
        # 状态栏容器
        status_container = tk.Frame(self.root, bg=self.colors['bg_tertiary'], height=32)
        status_container.pack(fill=tk.X, side=tk.BOTTOM)
        status_container.pack_propagate(False)

        # 状态栏内容框架
        status_frame = tk.Frame(status_container, bg=self.colors['bg_tertiary'])
        status_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=6)

        # 左侧状态信息
        left_status = tk.Frame(status_frame, bg=self.colors['bg_tertiary'])
        left_status.pack(side=tk.LEFT, fill=tk.Y)

        # 状态指示器
        self.status_dot = tk.Label(left_status,
                                  text="●",
                                  font=('Arial', 10),
                                  fg=self.colors['success'],
                                  bg=self.colors['bg_tertiary'])
        self.status_dot.pack(side=tk.LEFT, padx=(0, 6))

        self.status_label = tk.Label(
            left_status,
            text="就绪 | 数据库: main_db | 存储引擎: 运行中",
            font=('楷体', 11, 'bold'),  # 更大更粗字体
            fg=self.colors['text_primary'],  # 更深颜色
            bg=self.colors['bg_tertiary']
        )
        self.status_label.pack(side=tk.LEFT)

        # 右侧时间和系统信息
        right_status = tk.Frame(status_frame, bg=self.colors['bg_tertiary'])
        right_status.pack(side=tk.RIGHT)

        # 版本信息
        version_label = tk.Label(right_status,
                                text="v2.0.0",
                                font=('楷体', 10, 'bold'),  # 更大更粗字体
                                fg=self.colors['text_secondary'],  # 更深颜色
                                bg=self.colors['bg_tertiary'])
        version_label.pack(side=tk.RIGHT, padx=(0, 15))

        # 分隔符
        separator = tk.Label(right_status,
                           text="|",
                           font=('Arial', 8),
                           fg=self.colors['text_light'],
                           bg=self.colors['bg_tertiary'])
        separator.pack(side=tk.RIGHT, padx=8)

        # 时间标签
        self.time_label = tk.Label(right_status,
                                  font=('楷体', 11, 'bold'),  # 更大更粗字体
                                  fg=self.colors['text_primary'],  # 更深颜色
                                  bg=self.colors['bg_tertiary'])
        self.time_label.pack(side=tk.RIGHT)

        # 更新时间
        self._update_time()

    def _update_time(self):
        """更新时间显示"""
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self.time_label.config(text=current_time)
        self.root.after(1000, self._update_time)

    def _update_status(self, message: str, status_type: str = "info"):
        """更新状态栏"""
        self.status_label.config(text=message)
        
        # 根据状态类型更新颜色
        if hasattr(self, 'status_dot'):
            color_map = {
                "success": self.colors['success'],
                "error": self.colors['danger'],
                "warning": self.colors['warning'],
                "info": self.colors['info'],
                "ready": self.colors['success']
            }
            # 更新状态点颜色
            if hasattr(self, 'status_dot'):
                self.status_dot.config(fg=color_map.get(status_type, self.colors['success']))
        
    def _format_error_message(self, error_msg: str) -> str:
        """格式化错误信息，使其更加用户友好"""
        # 清理错误消息
        error_msg = error_msg.strip()
        
        # 检测错误类型并提供友好描述
        if "LexicalError" in error_msg or "词法错误" in error_msg:
            return self._format_lexical_error(error_msg)
        elif "SyntaxError" in error_msg or "语法错误" in error_msg:
            return self._format_syntax_error(error_msg)
        elif "SemanticError" in error_msg or "语义错误" in error_msg:
            return self._format_semantic_error(error_msg)
        elif "Error at line" in error_msg:
            return self._format_compiler_error(error_msg)
        else:
            return f" {error_msg}"
    
    def _format_lexical_error(self, error_msg: str) -> str:
        """格式化词法错误"""
        if "非法字符" in error_msg:
            return f"  词法错误: {error_msg}\n  提示: 检查是否有无效的字符或符号"
        elif "字符串没有正确结束" in error_msg:
            return f"  词法错误: {error_msg}\n  提示: 检查字符串是否有配对的引号"
        else:
            return f"  词法错误: {error_msg}"
    
    def _format_syntax_error(self, error_msg: str) -> str:
        """格式化语法错误"""
        if "期望" in error_msg and "发现" in error_msg:
            return f"  语法错误: {error_msg}\n 检查SQL语句的语法结构是否正确"
        elif "不能接受" in error_msg:
            return f"  语法错误: {error_msg}\n 请检查该位置的SQL语法"
        else:
            return f"  语法错误: {error_msg}"
    
    def _format_semantic_error(self, error_msg: str) -> str:
        """格式化语义错误"""
        if "表不存在" in error_msg:
            return f"  语义错误: {error_msg}\n  请确认表名是否正确，或先创建该表"
        elif "列不存在" in error_msg:
            return f"  语义错误: {error_msg}\n  请检查列名是否正确"
        else:
            return f"  语义错误: {error_msg}"
    
    def _format_compiler_error(self, error_msg: str) -> str:
        """格式化编译器通用错误"""
        # 提取行列信息
        import re
        line_col_match = re.search(r"line (\d+), column (\d+)", error_msg)
        if line_col_match:
            line, col = line_col_match.groups()
            return f" 第 {line} 行，第 {col} 列: {error_msg}\n  💡 请检查该位置的SQL语法"
        else:
            return f" {error_msg}"

    # 查询执行相关方法
    def _execute_query(self):
        """执行SQL查询"""
        sql = self.sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "请输入SQL查询语句")
            return

        self._update_status("正在执行查询...")

        # 在单独线程中执行查询
        thread = Thread(target=self._execute_query_thread, args=(sql,))
        thread.daemon = True
        thread.start()

    def _execute_query_thread(self, sql: str):
        """在线程中执行查询"""
        try:
            # 记录查询历史
            self.query_history.append({
                'sql': sql,
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                'status': 'executing'
            })

            # 更新信息显示
            self._update_info_display(f"开始执行查询: {time.strftime('%H:%M:%S')}\n")
            self._update_info_display(f"SQL: {sql}\n")

            if not self.storage_engine:
                self._update_info_display("错误: 存储引擎未初始化\n")
                return

            # 使用真实的SQL处理器
            try:
                # 修改：使用统一SQL处理器
                sql_processor = UnifiedSQLProcessor(self.storage_engine)
                
                # 设置索引使用模式
                use_index = self.use_index_var.get()
                if hasattr(sql_processor, 'execution_engine') and sql_processor.execution_engine:
                    sql_processor.execution_engine.set_index_mode(use_index)
                
                index_status = "使用B+树索引" if use_index else "使用全表扫描"
                self._update_info_display(f"查询模式: {index_status}\n")
                
                success, results, error_msg = sql_processor.process_sql(sql)

                if success:
                    self._update_info_display("SQL执行成功\n")

                    # 显示结果
                    if results:
                        handled = False
                        if isinstance(results[0], dict):
                            # 统一SQL处理器的DDL/工具型结果格式处理
                            # 1) SHOW INDEX 风格: [{'results': [ {index_row...}, ... ]}]
                            if 'results' in results[0] and isinstance(results[0]['results'], list):
                                index_rows = results[0]['results']
                                if index_rows:
                                    self._display_query_results(index_rows, "索引信息")
                                    self._update_info_display(f"  索引条目: {len(index_rows)}\n")
                                else:
                                    self._update_info_display("  （无索引）\n")
                                handled = True
                            # 2) CREATE/DROP 等消息风格: [{'message': '...'}]
                            elif 'message' in results[0]:
                                for r in results:
                                    msg = r.get('message') or r
                                    self._update_info_display(f"{msg}\n")
                                handled = True
                            # 3) 旧风格: 带 operation/status
                            elif 'operation' in results[0]:
                                for result in results:
                                    if result.get('status') == 'success':
                                        self._update_info_display(f"   {result.get('message', '操作成功')}\n")
                                    else:
                                        self._update_info_display(f"   {result.get('message', '操作失败')}\n")
                                handled = True
                        
                        if not handled:
                            # SELECT查询结果或通用行集
                            self._display_query_results(results, "查询结果")
                            self._update_info_display(f"  返回 {len(results)} 条记录\n")
                    else:
                        self._update_info_display("  执行成功，无返回结果\n")
                else:
                    formatted_error = self._format_error_message(error_msg)
                    self._update_info_display(f" SQL执行失败:\n{formatted_error}\n")
                    self.query_history[-1]['status'] = 'error'
                    return

            except Exception as e:
                formatted_error = self._format_error_message(str(e))
                self._update_info_display(f" SQL处理器执行错误:\n{formatted_error}\n")
                import traceback
                error_details = traceback.format_exc()
                self._update_info_display(f"详细错误信息:\n{error_details}\n")
                self.query_history[-1]['status'] = 'error'
                return

            # 更新状态
            self.query_history[-1]['status'] = 'completed'

            # 刷新表列表（如果是DDL操作）
            sql_upper = sql.upper().strip()
            # 修改：检查更多类型的DDL操作，包括通过统一SQL处理器执行的语句
            if any(sql_upper.startswith(op) for op in ['CREATE TABLE', 'DROP TABLE', 'ALTER TABLE']):
                self.root.after(0, self._refresh_tables)
            # 添加：检查统一SQL处理器返回的结果中是否包含表操作
            elif success and results and isinstance(results[0], dict):
                # 检查结果中是否包含表操作相关的消息
                for result in results:
                    message = result.get('message', '')
                    if any(keyword in message.upper() for keyword in ['TABLE', 'CREATED', 'DROPPED']):
                        self.root.after(0, self._refresh_tables)
                        break
            
            # 刷新当前表数据（如果是DML操作：UPDATE, DELETE, INSERT）
            if success and any(sql_upper.startswith(op) for op in ['UPDATE', 'DELETE', 'INSERT']):
                # 获取当前选中的表
                selection = self.tables_listbox.curselection()
                if selection:
                    current_table = self.tables_listbox.get(selection[0])
                    # 延迟刷新表数据显示
                    self.root.after(100, lambda: self._show_table_data(current_table))
                else:
                    # 尝试从SQL语句中提取表名
                    import re
                    table_match = None
                    if sql_upper.startswith('UPDATE'):
                        table_match = re.search(r'UPDATE\s+(\w+)', sql_upper)
                    elif sql_upper.startswith('DELETE'):
                        table_match = re.search(r'DELETE\s+FROM\s+(\w+)', sql_upper)
                    elif sql_upper.startswith('INSERT'):
                        table_match = re.search(r'INSERT\s+INTO\s+(\w+)', sql_upper)
                    
                    if table_match:
                        inferred_table = table_match.group(1).lower()
                        # 验证表是否存在
                        try:
                            available_tables = self.storage_engine.list_tables()
                            if inferred_table in available_tables:
                                # 延迟刷新表数据显示
                                self.root.after(100, lambda: self._show_table_data(inferred_table))
                        except Exception as e:
                            pass

        except Exception as e:
            self._update_info_display(f"查询执行错误: {str(e)}\n")
            if self.query_history:
                self.query_history[-1]['status'] = 'error'
        finally:
            self.root.after(0, lambda: self._update_status("查询执行完成"))

    # 旧的简化SQL处理方法已被删除，现在完全使用src/sql_processor.py中的真实SQL处理器

    def _display_query_results(self, results: List[Dict[str, Any]], table_name: str):
        """显示查询结果"""
        # 清空现有结果
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)

        if not results:
            return

        # 获取所有列名
        all_columns = set()
        for record in results:
            all_columns.update(record.keys())

        all_columns = list(all_columns)

        # 重新配置表格列
        self.result_tree['columns'] = all_columns
        self.result_tree['show'] = 'headings'

        # 设置列标题和宽度
        for col in all_columns:
            self.result_tree.heading(col, text=col)
            self.result_tree.column(col, width=120, anchor='center')

        # 添加数据行
        for record in results:
            values = [str(record.get(col, '')) for col in all_columns]
            self.result_tree.insert('', tk.END, values=values)

    def _update_info_display(self, message: str):
        """更新信息显示"""
        self.root.after(0, self._append_info, message)

    def _append_info(self, message: str):
        """在主线程中添加信息"""
        self.info_text.config(state=tk.NORMAL)
        self.info_text.insert(tk.END, message)
        self.info_text.see(tk.END)
        self.info_text.config(state=tk.DISABLED)

    # 编译器相关方法
    def _analyze_sql(self):
        """分析SQL语句"""
        sql = self.sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "请输入SQL查询语句")
            return

        self._update_status("正在分析SQL...")

        # 使用真实的SQL处理器进行分析
        try:
            # 使用统一SQL解析器
            unified_parser = UnifiedSQLParser(sql)
            ast, sql_type = unified_parser.parse()

            # 显示词法分析结果
            self._lexical_analysis_internal(sql)

            # 显示语法分析结果（显示AST）
            self._show_ast_result(ast, sql_type, sql)

            # 显示语义分析结果
            if ast:
                self._semantic_analysis_with_ast(ast, sql_type, sql)

        except Exception as e:
            # 如果统一解析器失败，回退到基础分析
            self._lexical_analysis_internal(sql)
            self._syntax_analysis_internal(sql)
            self._semantic_analysis_internal(sql)

        # 切换到编译器标签页
        self.notebook.select(1)  # 编译器是第二个标签页

        self._update_status("SQL分析完成")

    def _show_ast_result(self, ast, sql_type: str, sql: str):
        """显示AST结果"""
        self.parser_result.config(state=tk.NORMAL)
        self.parser_result.delete(1.0, tk.END)

        result_text = "=" * 60 + "\n"
        result_text += "             语法分析结果\n"
        result_text += "=" * 60 + "\n"
        result_text += f"输入SQL: {sql}\n"
        result_text += f"SQL类型: {sql_type}\n"
        result_text += "-" * 60 + "\n"

        if ast:
            result_text += "抽象语法树 (AST):\n"
            result_text += str(ast)
        else:
            result_text += "语法分析失败或未生成AST\n"

        result_text += "\n-" * 60 + "\n"
        result_text += "语法分析完成！\n"

        self.parser_result.insert(1.0, result_text)
        self.parser_result.config(state=tk.DISABLED)

    def _semantic_analysis_with_ast(self, ast, sql_type: str, sql: str):
        """使用AST进行语义分析"""
        try:
            # 根据SQL类型选择语义分析器
            if sql_type in ["DDL", "DML"]:
                semantic_analyzer = DDLDMLSemanticAnalyzer()
            else:
                # 修改：使用统一语义分析器，传入存储引擎
                from src.compiler.semantic.analyzer import SemanticAnalyzer
                semantic_analyzer = SemanticAnalyzer(self.storage_engine)

            quadruples = semantic_analyzer.analyze(ast)

            # 显示语义分析结果
            self.semantic_result.config(state=tk.NORMAL)
            self.semantic_result.delete(1.0, tk.END)

            result_text = "=" * 60 + "\n"
            result_text += "             语义分析结果\n"
            result_text += "=" * 60 + "\n"
            result_text += f"输入SQL: {sql}\n"
            result_text += f"SQL类型: {sql_type}\n"
            result_text += "-" * 60 + "\n"

            if quadruples:
                result_text += "生成的中间代码 (四元式):\n"
                for i, quad in enumerate(quadruples, 1):
                    result_text += f"{i:2d}. {quad}\n"
            else:
                result_text += "未生成中间代码\n"

            # 符号表信息
            if hasattr(semantic_analyzer, 'symbol_table'):
                result_text += f"\n{semantic_analyzer.symbol_table}\n"

            result_text += "-" * 60 + "\n"
            result_text += "语义分析完成！\n"

            self.semantic_result.insert(1.0, result_text)
            self.semantic_result.config(state=tk.DISABLED)

            # 生成目标代码
            self._code_generation_with_quadruples(quadruples, sql_type, sql)

        except Exception as e:
            self.semantic_result.config(state=tk.NORMAL)
            self.semantic_result.delete(1.0, tk.END)
            self.semantic_result.insert(1.0, f"语义分析错误: {str(e)}")
            self.semantic_result.config(state=tk.DISABLED)

    def _code_generation_with_quadruples(self, quadruples, sql_type: str, sql: str):
        """使用四元式生成目标代码"""
        try:
            # 使用真实的代码生成器
            if sql_type in ["DDL", "DML"]:
                # DDL/DML语句直接使用四元式
                target_instructions = [str(quad) for quad in quadruples]
            else:
                # SELECT查询使用代码生成器
                from src.compiler.codegen.translator import QuadrupleTranslator
                translator = QuadrupleTranslator()
                target_instructions = translator.generate_target_code(quadruples)

            # 显示代码生成结果
            self.codegen_result.config(state=tk.NORMAL)
            self.codegen_result.delete(1.0, tk.END)

            result_text = "=" * 60 + "\n"
            result_text += "             目标代码生成结果\n"
            result_text += "=" * 60 + "\n"
            result_text += f"输入SQL: {sql}\n"
            result_text += f"SQL类型: {sql_type}\n"
            result_text += "-" * 60 + "\n"

            # 生成目标指令
            result_text += "生成的目标指令:\n"
            for i, instruction in enumerate(target_instructions, 1):
                result_text += f"{i:2d}. {instruction}\n"

            result_text += "\n中间代码:\n"
            if quadruples:
                for i, quad in enumerate(quadruples, 1):
                    result_text += f"{i:2d}. {quad}\n"

            result_text += "-" * 60 + "\n"
            result_text += "代码生成完成！\n"

            self.codegen_result.insert(1.0, result_text)
            self.codegen_result.config(state=tk.DISABLED)

        except Exception as e:
            self.codegen_result.config(state=tk.NORMAL)
            self.codegen_result.delete(1.0, tk.END)
            self.codegen_result.insert(1.0, f"代码生成错误: {str(e)}")
            self.codegen_result.config(state=tk.DISABLED)

    def _lexical_analysis(self):
        """词法分析"""
        sql = self.compiler_sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "请输入SQL语句")
            return

        self._lexical_analysis_internal(sql)

    def _lexical_analysis_internal(self, sql: str):
        """内部词法分析方法（扩展版本）"""
        try:
            # 使用扩展的词法分析器
            lexer = Lexer(sql)
            tokens = lexer.tokenize()

            # 显示词法分析结果
            self.lexer_result.config(state=tk.NORMAL)
            self.lexer_result.delete(1.0, tk.END)

            result_text = "=" * 60 + "\n"
            result_text += "             词法分析结果\n"
            result_text += "=" * 60 + "\n"
            result_text += "-" * 60 + "\n"
            result_text += f"输入SQL: {sql}\n"
            result_text += f"识别Token数: {len(tokens)}\n"
            result_text += "-" * 60 + "\n"
            result_text += f"{'序号':<4} {'类型':<15} {'值':<20} {'位置':<10}\n"
            result_text += "-" * 60 + "\n"

            for i, token in enumerate(tokens):
                location = f"{token.line}:{token.column}"
                result_text += f"{i+1:<4} {token.type.value:<15} {repr(token.value):<20} {location:<10}\n"

            result_text += "-" * 60 + "\n"
            result_text += f"词法分析完成！共识别 {len(tokens)} 个Token\n"

            self.lexer_result.insert(1.0, result_text)
            self.lexer_result.config(state=tk.DISABLED)

        except Exception as e:
            self.lexer_result.config(state=tk.NORMAL)
            self.lexer_result.delete(1.0, tk.END)
            self.lexer_result.insert(1.0, f"词法分析错误: {str(e)}")
            self.lexer_result.config(state=tk.DISABLED)

    def _syntax_analysis(self):
        """语法分析"""
        sql = self.compiler_sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "请输入SQL语句")
            return

        self._syntax_analysis_internal(sql)

    def _syntax_analysis_internal(self, sql: str):
        """内部语法分析方法"""
        try:
            # 使用扩展的统一SQL解析器
            unified_parser = UnifiedSQLParser(sql)
            ast, sql_type = unified_parser.parse()

            # 显示语法分析结果
            self.parser_result.config(state=tk.NORMAL)
            self.parser_result.delete(1.0, tk.END)

            result_text = "=" * 60 + "\n"
            result_text += "             扩展语法分析结果\n"
            result_text += "=" * 60 + "\n"
            result_text += f"输入SQL: {sql}\n"
            result_text += f"SQL类型: {sql_type}\n"
            result_text += "-" * 60 + "\n"

            if ast:
                result_text += "抽象语法树 (AST):\n"
                result_text += str(ast)
                result_text += "\n\n✅ 语法分析成功！\n"

            else:
                result_text += "❌ 语法分析失败或未生成AST\n"

            result_text += "\n"
            if hasattr(unified_parser, 'parse_steps') and unified_parser.parse_steps:
                result_text += "分析步骤:\n"
                for i, step in enumerate(unified_parser.parse_steps, 1):
                    result_text += f"{i:2d}. {step}\n"

            result_text += "-" * 60 + "\n"
            result_text += "扩展语法分析完成！\n"

            self.parser_result.insert(1.0, result_text)
            self.parser_result.config(state=tk.DISABLED)

        except Exception as e:
            self.parser_result.config(state=tk.NORMAL)
            self.parser_result.delete(1.0, tk.END)
            self.parser_result.insert(1.0, f"语法分析错误: {str(e)}")
            self.parser_result.config(state=tk.DISABLED)

    def _semantic_analysis(self):
        """语义分析"""
        sql = self.compiler_sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "请输入SQL语句")
            return

        self._semantic_analysis_internal(sql)

    def _semantic_analysis_internal(self, sql: str):
        """内部语义分析方法"""
        try:
            # 使用扩展的统一SQL解析器
            unified_parser = UnifiedSQLParser(sql)
            ast, sql_type = unified_parser.parse()

            # 语义分析
            if ast:
                # 使用统一语义分析器，传入存储引擎
                analyzer = SemanticAnalyzer(self.storage_engine)
                quadruples = analyzer.analyze(ast)

                # 显示语义分析结果
                self.semantic_result.config(state=tk.NORMAL)
                self.semantic_result.delete(1.0, tk.END)

                result_text = "=" * 60 + "\n"
                result_text += "             语义分析结果\n"
                result_text += "=" * 60 + "\n"
                result_text += f"输入SQL: {sql}\n"
                result_text += f"SQL类型: {sql_type}\n"
                result_text += "-" * 60 + "\n"

                if quadruples:
                    result_text += "生成的中间代码 (四元式):\n"
                    for i, quad in enumerate(quadruples, 1):
                        result_text += f"{i:2d}. {quad}\n"
                else:
                    result_text += "未生成中间代码\n"

                # 符号表信息
                if hasattr(analyzer, 'symbol_table'):
                    result_text += f"\n{analyzer.symbol_table}\n"

                result_text += "-" * 60 + "\n"
                result_text += "语义分析完成！\n"

                self.semantic_result.insert(1.0, result_text)
                self.semantic_result.config(state=tk.DISABLED)
            else:
                self.semantic_result.config(state=tk.NORMAL)
                self.semantic_result.delete(1.0, tk.END)
                self.semantic_result.insert(1.0, "语义分析失败: AST为空")
                self.semantic_result.config(state=tk.DISABLED)

        except Exception as e:
            self.semantic_result.config(state=tk.NORMAL)
            self.semantic_result.delete(1.0, tk.END)
            self.semantic_result.insert(1.0, f"语义分析错误: {str(e)}")
            self.semantic_result.config(state=tk.DISABLED)

    def _code_generation(self):
        """代码生成"""
        sql = self.compiler_sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "请输入SQL语句")
            return

        try:
            # 使用扩展的统一SQL解析器
            unified_parser = UnifiedSQLParser(sql)
            ast, sql_type = unified_parser.parse()

            if ast:
                # 修改：使用统一语义分析器，传入存储引擎
                analyzer = SemanticAnalyzer(self.storage_engine)
                quadruples = analyzer.analyze(ast)

                # 代码生成（简化版本）
                self.codegen_result.config(state=tk.NORMAL)
                self.codegen_result.delete(1.0, tk.END)

                result_text = "=" * 60 + "\n"
                result_text += "             目标代码生成结果\n"
                result_text += "=" * 60 + "\n"
                result_text += f"输入SQL: {sql}\n"
                result_text += f"SQL类型: {sql_type}\n"
                result_text += f"解析器类型: 扩展SQL解析器\n"
                result_text += "-" * 60 + "\n"

                # 生成目标指令
                result_text += "生成的目标指令:\n"

                # 根据SQL类型生成不同的指令
                sql_upper = sql.upper().strip()
                if sql_upper.startswith('SELECT'):
                    result_text += "1. OPEN table_name\n"
                    result_text += "2. SCAN table_name\n"
                    result_text += "3. FILTER conditions\n"
                    result_text += "4. PROJECT columns\n"
                    result_text += "5. OUTPUT results\n"
                    result_text += "6. CLOSE table_name\n"
                elif sql_upper.startswith('INSERT'):
                    result_text += "1. OPEN table_name\n"
                    result_text += "2. INSERT record\n"
                    result_text += "3. UPDATE_INDEXES\n"
                    result_text += "4. CLOSE table_name\n"
                elif sql_upper.startswith('CREATE'):
                    result_text += "1. CREATE_TABLE schema\n"
                    result_text += "2. ALLOCATE_PAGES\n"
                    result_text += "3. UPDATE_METADATA\n"
                else:
                    result_text += "目标代码生成功能开发中...\n"

                result_text += "\n中间代码:\n"
                if quadruples:
                    for i, quad in enumerate(quadruples, 1):
                        result_text += f"{i:2d}. {quad}\n"

                result_text += "-" * 60 + "\n"
                result_text += "代码生成完成！\n"

                self.codegen_result.insert(1.0, result_text)
                self.codegen_result.config(state=tk.DISABLED)
            else:
                self.codegen_result.config(state=tk.NORMAL)
                self.codegen_result.delete(1.0, tk.END)
                self.codegen_result.insert(1.0, "代码生成失败: AST为空")
                self.codegen_result.config(state=tk.DISABLED)

        except Exception as e:
            self.codegen_result.config(state=tk.NORMAL)
            self.codegen_result.delete(1.0, tk.END)
            self.codegen_result.insert(1.0, f"代码生成错误: {str(e)}")
            self.codegen_result.config(state=tk.DISABLED)

    # 存储引擎相关方法
    def _refresh_storage_stats(self):
        """刷新存储引擎统计"""
        if not self.storage_engine:
            return

        try:
            stats = self.storage_engine.get_stats()

            self.storage_stats_text.config(state=tk.NORMAL)
            self.storage_stats_text.delete(1.0, tk.END)

            stats_text = "=" * 60 + "\n"
            stats_text += "             存储引擎统计信息\n"
            stats_text += "=" * 60 + "\n"
            stats_text += f"运行时间: {stats['uptime_seconds']} 秒\n"
            stats_text += f"数据库表数: {stats['tables']}\n"
            stats_text += "\n--- 操作统计 ---\n"
            storage_stats = stats['storage_stats']
            stats_text += f"执行查询: {storage_stats['queries_executed']}\n"
            stats_text += f"插入记录: {storage_stats['records_inserted']}\n"
            stats_text += f"更新记录: {storage_stats['records_updated']}\n"
            stats_text += f"删除记录: {storage_stats['records_deleted']}\n"

            stats_text += "\n--- 缓存统计 ---\n"
            cache_stats = stats['cache_stats']
            # 显示当前使用的缓存替换策略
            replacement_policy = cache_stats.get('replacement_policy', 'LRU')
            stats_text += f"缓存替换策略: {replacement_policy}\n"
            stats_text += f"缓存命中率: {cache_stats['cache_hit_rate']}%\n"
            stats_text += f"缓存命中: {cache_stats['cache_hits']}\n"
            stats_text += f"缓存未命中: {cache_stats['cache_misses']}\n"
            stats_text += f"已使用帧: {cache_stats['used_frames']}/{cache_stats['buffer_size']}\n"
            stats_text += f"脏页数: {cache_stats['dirty_frames']}\n"
            
            # 添加查询优化统计
            if hasattr(self.storage_engine, 'get_optimization_stats'):
                opt_stats = self.storage_engine.get_optimization_stats()
                stats_text += "\n--- 查询优化 ---\n"
                stats_text += f"优化器状态: {'启用' if opt_stats.get('optimization_enabled', False) else '禁用'}\n"
                stats_text += f"已应用优化: {opt_stats.get('optimizations_applied', 0)} 次\n"
                stats_text += f"优化总耗时: {opt_stats.get('optimization_time', 0.0):.4f}秒\n"

            stats_text += "\n--- 页面统计 ---\n"
            page_stats = stats['page_stats']
            stats_text += f"总页数: {page_stats['total_pages']}\n"
            stats_text += f"总记录数: {page_stats['total_records']}\n"
            stats_text += f"下一页ID: {page_stats['next_page_id']}\n"

            stats_text += "=" * 60 + "\n"
            stats_text += f"统计更新时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"

            self.storage_stats_text.insert(1.0, stats_text)
            self.storage_stats_text.config(state=tk.DISABLED)

            # 更新缓存状态
            self._update_cache_status(cache_stats)

            # 更新页面信息
            self._update_page_info(page_stats)

        except Exception as e:
            self.storage_stats_text.config(state=tk.NORMAL)
            self.storage_stats_text.delete(1.0, tk.END)
            self.storage_stats_text.insert(1.0, f"获取存储统计信息失败: {str(e)}")
            self.storage_stats_text.config(state=tk.DISABLED)

    def _update_cache_status(self, cache_stats: Dict[str, Any]):
        """更新缓存状态显示"""
        self.cache_status_text.config(state=tk.NORMAL)
        self.cache_status_text.delete(1.0, tk.END)

        cache_text = "缓存管理器状态\n"
        cache_text += "=" * 40 + "\n"
        
        # 显示缓存替换策略
        replacement_policy = cache_stats.get('replacement_policy', 'LRU')
        cache_text += f"替换策略: {replacement_policy}\n"
        cache_text += f"缓存大小: {cache_stats['buffer_size']} 页\n"
        cache_text += f"已使用: {cache_stats['used_frames']} 页\n"
        cache_text += f"空闲: {cache_stats['buffer_size'] - cache_stats['used_frames']} 页\n"
        cache_text += f"脏页: {cache_stats['dirty_frames']} 页\n"
        
        # 添加策略描述
        policy_desc = {
            'LRU': '最近最少使用算法',
            'FIFO': '先进先出算法',
            'CLOCK': '时钟页面替换算法'
        }
        desc = policy_desc.get(replacement_policy, '未知策略')
        cache_text += f"策略描述: {desc}\n"
        cache_text += "-" * 40 + "\n"
        
        cache_text += f"命中率: {cache_stats['cache_hit_rate']}%\n"
        cache_text += f"总命中: {cache_stats['cache_hits']}\n"
        cache_text += f"总未命中: {cache_stats['cache_misses']}\n"

        # 计算利用率
        utilization = (cache_stats['used_frames'] / cache_stats['buffer_size']) * 100
        cache_text += f"利用率: {utilization:.1f}%\n"

        self.cache_status_text.insert(1.0, cache_text)
        self.cache_status_text.config(state=tk.DISABLED)

    def _update_page_info(self, page_stats: Dict[str, Any]):
        """更新页面信息显示"""
        self.page_info_text.config(state=tk.NORMAL)
        self.page_info_text.delete(1.0, tk.END)

        page_text = "页面管理器信息\n"
        page_text += "=" * 40 + "\n"
        page_text += f"总页数: {page_stats['total_pages']}\n"
        page_text += f"总记录数: {page_stats['total_records']}\n"
        page_text += f"下一页ID: {page_stats['next_page_id']}\n"

        # 计算平均每页记录数
        if page_stats['total_pages'] > 0:
            avg_records = page_stats['total_records'] / page_stats['total_pages']
            page_text += f"平均每页记录数: {avg_records:.1f}\n"

        self.page_info_text.insert(1.0, page_text)
        self.page_info_text.config(state=tk.DISABLED)

        # 更新索引信息
        self._update_index_info()

    def _update_index_info(self):
        """更新索引信息显示"""
        self.index_info_text.config(state=tk.NORMAL)
        self.index_info_text.delete(1.0, tk.END)

        index_text = "索引管理器信息\n"
        index_text += "=" * 40 + "\n"
        index_text += "B+树索引支持: 已启用\n"
        index_text += "索引类型: B+树, 哈希\n"
        index_text += "复合索引: 支持\n"
        index_text += "唯一索引: 支持\n"

        # 如果有索引管理器，显示更多信息
        if hasattr(self.storage_engine, 'index_manager'):
            index_text += "\n当前索引:\n"
            index_text += "- 索引信息获取功能开发中...\n"

        self.index_info_text.insert(1.0, index_text)
        self.index_info_text.config(state=tk.DISABLED)

    # 表管理相关方法
    def _refresh_tables(self):
        """刷新表列表"""
        if not self.storage_engine:
            return

        self.tables_listbox.delete(0, tk.END)

        try:
            tables = self.storage_engine.list_tables()
            for table in tables:
                self.tables_listbox.insert(tk.END, table)
        except Exception as e:
            print(f"Error refreshing tables: {e}")

    def _on_table_select(self, event):
        """表选择事件"""
        selection = self.tables_listbox.curselection()
        if selection:
            table_name = self.tables_listbox.get(selection[0])
            self._show_table_info(table_name)

    def _show_table_info(self, table_name: str):
        """显示表信息"""
        try:
            # 获取表信息
            table_info = self.storage_engine.get_table_info(table_name)
            if not table_info:
                return

            # 清空现有信息
            for item in self.schema_tree.get_children():
                self.schema_tree.delete(item)

            # 显示表结构
            columns_info = table_info.get('columns', [])  # 期望为列信息字典列表
            primary_key_name = table_info.get('primary_key')

            # 优先使用详细的列信息（来自TableManager.get_table_info）
            if isinstance(columns_info, list) and columns_info and isinstance(columns_info[0], dict):
                for col in columns_info:
                    name = col.get('name', '')
                    col_type = col.get('type', 'UNKNOWN')
                    max_len = col.get('max_length') or ''
                    is_pk = col.get('primary_key', False)
                    is_unique = col.get('unique', False)
                    nullable = col.get('nullable', True)
                    default_val = col.get('default_value')
                    values = (
                        name,
                        col_type,
                        max_len,
                        '是' if (is_pk or (primary_key_name and name == primary_key_name)) else '否',
                        '是' if is_unique else '否',
                        '是' if nullable else '否',
                        '' if default_val is None else str(default_val)
                    )
                    self.schema_tree.insert('', tk.END, values=values)
            else:
                # 回退：仅有列名列表时
                for col in (columns_info or []):
                    values = (
                        col,
                        'UNKNOWN',
                        '',
                        '是' if (primary_key_name and col == primary_key_name) else '否',
                        '否',
                        '是',
                        ''
                    )
                    self.schema_tree.insert('', tk.END, values=values)

            # 显示表数据
            self._show_table_data(table_name)

        except Exception as e:
            print(f"Error showing table info: {e}")

    def _show_table_data(self, table_name: str):
        """显示表数据"""
        try:
            # 清空现有数据
            for item in self.data_tree.get_children():
                self.data_tree.delete(item)

            # 查询表数据
            records = self.storage_engine.select(table_name)

            if records:
                # 获取所有列名
                all_columns = set()
                for record in records:
                    all_columns.update(record.keys())

                all_columns = list(all_columns)

                # 重新配置数据表格
                self.data_tree['columns'] = all_columns
                self.data_tree['show'] = 'headings'

                # 设置列标题
                for col in all_columns:
                    self.data_tree.heading(col, text=col)
                    self.data_tree.column(col, width=100, anchor='center')

                # 添加数据行
                for record in records:
                    values = [str(record.get(col, '')) for col in all_columns]
                    self.data_tree.insert('', tk.END, values=values)

        except Exception as e:
            print(f"Error showing table data: {e}")

    def _create_table_dialog(self):
        """创建表对话框"""
        dialog = CreateTableDialog(self.root, self.storage_engine)
        if dialog.result:
            self._refresh_tables()

    def _drop_table(self):
        """删除表"""
        selection = self.tables_listbox.curselection()
        if not selection:
            messagebox.showwarning("警告", "请选择要删除的表")
            return

        table_name = self.tables_listbox.get(selection[0])

        if messagebox.askyesno("确认删除", f"确定要删除表 '{table_name}' 吗？\n此操作不可撤销！"):
            try:
                if self.storage_engine.drop_table(table_name):
                    messagebox.showinfo("成功", f"表 '{table_name}' 已删除")
                    self._refresh_tables()
                else:
                    messagebox.showerror("错误", f"删除表 '{table_name}' 失败")
            except Exception as e:
                messagebox.showerror("错误", f"删除表时发生错误: {str(e)}")

    # 性能监控相关方法
    def _refresh_performance(self):
        """刷新性能统计"""
        self._refresh_storage_stats()  # 重用存储统计的逻辑

        # 在性能标签页显示更详细的信息
        if self.storage_engine:
            stats = self.storage_engine.get_stats()

            self.perf_text.config(state=tk.NORMAL)
            self.perf_text.delete(1.0, tk.END)

            perf_text = "=" * 60 + "\n"
            perf_text += "             性能监控统计\n"
            perf_text += "=" * 60 + "\n"

            # 系统概览
            perf_text += "系统概览:\n"
            perf_text += f"  运行时间: {stats['uptime_seconds']} 秒\n"
            perf_text += f"  数据库表数: {stats['tables']}\n"

            # 操作统计
            storage_stats = stats['storage_stats']
            perf_text += f"\n操作统计:\n"
            perf_text += f"  查询执行: {storage_stats['queries_executed']}\n"
            perf_text += f"  记录插入: {storage_stats['records_inserted']}\n"
            perf_text += f"  记录更新: {storage_stats['records_updated']}\n"
            perf_text += f"  记录删除: {storage_stats['records_deleted']}\n"

            # 性能指标
            cache_stats = stats['cache_stats']
            perf_text += f"\n缓存性能:\n"
            # 显示当前使用的缓存替换策略
            replacement_policy = cache_stats.get('replacement_policy', 'LRU')
            perf_text += f"  替换策略: {replacement_policy}\n"
            perf_text += f"  命中率: {cache_stats['cache_hit_rate']}%\n"
            perf_text += f"  总访问: {cache_stats['cache_hits'] + cache_stats['cache_misses']}\n"
            perf_text += f"  命中数: {cache_stats['cache_hits']}\n"
            perf_text += f"  未命中数: {cache_stats['cache_misses']}\n"
            
            # 查询优化性能
            if hasattr(self.storage_engine, 'get_optimization_stats'):
                opt_stats = self.storage_engine.get_optimization_stats()
                perf_text += f"\n查询优化:\n"
                perf_text += f"  优化器状态: {'启用' if opt_stats.get('optimization_enabled', False) else '禁用'}\n"
                perf_text += f"  已应用优化: {opt_stats.get('optimizations_applied', 0)} 次\n"
                perf_text += f"  优化总耗时: {opt_stats.get('optimization_time', 0.0):.4f}秒\n"

            # 存储效率
            page_stats = stats['page_stats']
            if page_stats['total_pages'] > 0:
                avg_records = page_stats['total_records'] / page_stats['total_pages']
                perf_text += f"\n存储效率:\n"
                perf_text += f"  平均页面利用率: {avg_records:.1f} 记录/页\n"
                perf_text += f"  存储空间: {page_stats['total_pages']} 页\n"

            # 查询历史统计
            if self.query_history:
                completed_queries = [q for q in self.query_history if q['status'] == 'completed']
                error_queries = [q for q in self.query_history if q['status'] == 'error']

                perf_text += f"\n查询统计:\n"
                perf_text += f"  总查询数: {len(self.query_history)}\n"
                perf_text += f"  成功查询: {len(completed_queries)}\n"
                perf_text += f"  失败查询: {len(error_queries)}\n"

                if self.query_history:
                    success_rate = (len(completed_queries) / len(self.query_history)) * 100
                    perf_text += f"  成功率: {success_rate:.1f}%\n"

            perf_text += "\n" + "=" * 60 + "\n"
            perf_text += f"统计更新时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"

            self.perf_text.insert(1.0, perf_text)
            self.perf_text.config(state=tk.DISABLED)

    def _show_detailed_stats(self):
        """显示详细统计"""
        if not self.storage_engine:
            messagebox.showwarning("警告", "存储引擎未初始化")
            return

        # 创建详细统计窗口
        stats_window = tk.Toplevel(self.root)
        stats_window.title("详细性能统计")
        stats_window.geometry("800x600")

        # 创建文本显示区域
        text_area = scrolledtext.ScrolledText(
            stats_window,
            font=('Times New Roman', 10),
            wrap=tk.WORD
        )
        text_area.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        try:
            stats = self.storage_engine.get_stats()

            detailed_text = "=" * 80 + "\n"
            detailed_text += "                    详细性能统计报告\n"
            detailed_text += "=" * 80 + "\n"

            # 系统信息
            detailed_text += f"生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            detailed_text += f"运行时长: {stats['uptime_seconds']} 秒\n\n"

            # 存储统计
            storage_stats = stats['storage_stats']
            detailed_text += "存储操作统计:\n"
            detailed_text += f"  查询执行总数: {storage_stats['queries_executed']}\n"
            detailed_text += f"  记录插入总数: {storage_stats['records_inserted']}\n"
            detailed_text += f"  记录更新总数: {storage_stats['records_updated']}\n"
            detailed_text += f"  记录删除总数: {storage_stats['records_deleted']}\n"
            detailed_text += f"  总操作数: {sum(storage_stats.values()) - storage_stats.get('start_time', 0)}\n\n"

            # 缓存详细统计
            cache_stats = stats['cache_stats']
            detailed_text += "缓存系统详细统计:\n"
            # 显示当前使用的缓存替换策略
            replacement_policy = cache_stats.get('replacement_policy', 'LRU')
            policy_descriptions = {
                'LRU': '最近最少使用 (Least Recently Used)',
                'FIFO': '先进先出 (First In First Out)',
                'CLOCK': '时钟算法 (Clock Algorithm)'
            }
            policy_desc = policy_descriptions.get(replacement_policy, '未知策略')
            detailed_text += f"  缓存替换策略: {replacement_policy} - {policy_desc}\n"
            detailed_text += f"  缓存池大小: {cache_stats['buffer_size']} 页\n"
            detailed_text += f"  已使用页框: {cache_stats['used_frames']}\n"
            detailed_text += f"  空闲页框: {cache_stats['buffer_size'] - cache_stats['used_frames']}\n"
            detailed_text += f"  脏页数量: {cache_stats['dirty_frames']}\n"
            detailed_text += f"  缓存命中: {cache_stats['cache_hits']}\n"
            detailed_text += f"  缓存未命中: {cache_stats['cache_misses']}\n"
            detailed_text += f"  命中率: {cache_stats['cache_hit_rate']}%\n"

            total_accesses = cache_stats['cache_hits'] + cache_stats['cache_misses']
            if total_accesses > 0:
                detailed_text += f"  总访问次数: {total_accesses}\n"
                utilization = (cache_stats['used_frames'] / cache_stats['buffer_size']) * 100
                detailed_text += f"  缓存利用率: {utilization:.1f}%\n"

            detailed_text += "\n"

            # 页面详细统计
            page_stats = stats['page_stats']
            detailed_text += "页面管理详细统计:\n"
            detailed_text += f"  总页面数: {page_stats['total_pages']}\n"
            detailed_text += f"  总记录数: {page_stats['total_records']}\n"
            detailed_text += f"  下一页ID: {page_stats['next_page_id']}\n"

            if page_stats['total_pages'] > 0:
                avg_records = page_stats['total_records'] / page_stats['total_pages']
                detailed_text += f"  平均页面记录数: {avg_records:.2f}\n"
                detailed_text += f"  存储密度: {avg_records:.2f} 记录/页\n"

            detailed_text += "\n"

            # 表统计
            detailed_text += "数据表统计:\n"
            detailed_text += f"  数据表总数: {stats['tables']}\n"

            try:
                tables = self.storage_engine.list_tables()
                for table in tables:
                    table_info = self.storage_engine.get_table_info(table)
                    if table_info:
                        detailed_text += f"    表 '{table}': {table_info.get('record_count', 0)} 记录, {table_info.get('page_count', 0)} 页\n"
            except:
                detailed_text += "    详细表信息获取失败\n"

            detailed_text += "\n"

            # 查询历史分析
            if self.query_history:
                detailed_text += "查询历史分析:\n"
                detailed_text += f"  历史查询总数: {len(self.query_history)}\n"

                completed = [q for q in self.query_history if q['status'] == 'completed']
                errors = [q for q in self.query_history if q['status'] == 'error']
                executing = [q for q in self.query_history if q['status'] == 'executing']

                detailed_text += f"  成功查询: {len(completed)}\n"
                detailed_text += f"  失败查询: {len(errors)}\n"
                detailed_text += f"  执行中查询: {len(executing)}\n"

                if self.query_history:
                    success_rate = (len(completed) / len(self.query_history)) * 100
                    detailed_text += f"  查询成功率: {success_rate:.1f}%\n"

                # 最近的查询
                detailed_text += "\n  最近查询记录:\n"
                for i, query in enumerate(self.query_history[-5:], 1):  # 显示最近5条
                    status_icon = "✅" if query['status'] == 'completed' else "❌" if query['status'] == 'error' else "⏳"
                    sql_preview = query['sql'][:50] + "..." if len(query['sql']) > 50 else query['sql']
                    detailed_text += f"    {i}. {status_icon} {query['timestamp']} - {sql_preview}\n"

            detailed_text += "\n" + "=" * 80 + "\n"
            detailed_text += "报告结束\n"

            text_area.insert(1.0, detailed_text)

        except Exception as e:
            text_area.insert(1.0, f"生成详细统计报告时发生错误: {str(e)}")

    def _clear_stats(self):
        """清除统计信息"""
        if messagebox.askyesno("确认清除", "确定要清除所有统计信息吗？"):
            # 清除查询历史
            self.query_history.clear()

            # 如果存储引擎支持清除统计，则清除
            if hasattr(self.storage_engine, 'clear_stats'):
                self.storage_engine.clear_stats()

            messagebox.showinfo("清除完成", "统计信息已清除")
            self._refresh_performance()

    # 通用方法
    def _clear_query(self):
        """清空查询"""
        self.sql_text.delete(1.0, tk.END)
        # 清空后重新应用语法高亮
        if hasattr(self, 'sql_highlighter'):
            self.sql_highlighter.highlight_now()
    
    def _compare_performance(self):
        """对比查询性能"""
        sql = self.sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "请输入SQL查询语句")
            return
        
        # 只对SELECT查询进行性能对比
        if not sql.upper().strip().startswith('SELECT'):
            messagebox.showinfo("提示", "性能对比功能只支持SELECT查询语句")
            return
        
        self._update_status("正在进行性能对比...")
        
        # 在单独线程中执行性能对比
        thread = Thread(target=self._performance_comparison_thread, args=(sql,))
        thread.daemon = True
        thread.start()
    
    def _performance_comparison_thread(self, sql: str):
        """在线程中执行性能对比"""
        try:
            if not self.storage_engine:
                self._update_info_display("错误: 存储引擎未初始化\n")
                return
            
            self._update_info_display(f"\n{'='*60}\n")
            self._update_info_display(f"性能对比测试开始: {time.strftime('%H:%M:%S')}\n")
            self._update_info_display(f"SQL: {sql}\n")
            
            # 检查查询是否适合使用索引
            import re
            table_match = re.search(r'FROM\s+(\w+)', sql.upper())
            where_match = re.search(r'WHERE\s+(\w+)\s*[=<>!]+\s*(\w+|\d+|\'[^\']*\')', sql.upper())
            
            if not table_match:
                self._update_info_display("❌ 无法从SQL中提取表名，无法进行性能对比\n")
                return
                
            table_name = table_match.group(1).lower()
            
            # 直接使用存储引擎的性能对比功能
            if where_match:
                field_name = where_match.group(1).lower()
                field_value = where_match.group(2)
                
                # 检查查询类型并提取操作符
                operator = None
                if '=' in sql.upper() and not ('>' in sql.upper() or '<' in sql.upper() or '!' in sql.upper()):
                    operator = '='
                elif '>=' in sql.upper():
                    operator = '>='
                elif '<=' in sql.upper():
                    operator = '<='
                elif '>' in sql.upper():
                    operator = '>'
                elif '<' in sql.upper():
                    operator = '<'
                elif '!=' in sql.upper():
                    operator = '!='
                elif '<>' in sql.upper():
                    operator = '<>'
                
                # 去除引号并转换数据类型
                if field_value.startswith("'") and field_value.endswith("'"):
                    field_value = field_value[1:-1]
                elif field_value.isdigit():
                    field_value = int(field_value)
                
                if operator:
                    # 构建查询条件
                    if operator == '=':
                        where_condition = {field_name: field_value}
                    else:
                        # 范围查询，使用操作符映射
                        op_mapping = {
                            '>': '$gt', '>=': '$gte',
                            '<': '$lt', '<=': '$lte',
                            '!=': '$ne', '<>': '$ne'
                        }
                        where_condition = {field_name: {op_mapping[operator]: field_value}}
                    
                    # 执行性能对比
                    performance_data = self.storage_engine.select_with_performance(
                        table_name, where=where_condition
                    )
                    
                    # 显示结果
                    self._update_info_display(f"\n🔍 查询条件: {field_name} {operator} {field_value}\n")
                    self._update_info_display(f"📊 全表扫描时间: {performance_data['full_scan_time']:.6f} 秒\n")
                    self._update_info_display(f"⚡ 索引查询时间: {performance_data['index_time']:.6f} 秒\n")
                    
                    if performance_data['index_used']:
                        self._update_info_display(f"🎯 使用的索引: {performance_data['index_used']}\n")
                        if operator == '=':
                            self._update_info_display(f"🔑 查询类型: 等值查询\n")
                        else:
                            self._update_info_display(f"🔄 查询类型: 范围查询 ({operator})\n")
                        
                        speedup = performance_data['speedup_ratio']
                        if speedup > 1:
                            self._update_info_display(f"🚀 性能提升: {speedup:.2f}倍\n")
                        else:
                            self._update_info_display(f"📈 性能比率: {speedup:.2f}\n")
                    else:
                        self._update_info_display(f"⚠️  没有可用的索引，使用全表扫描\n")
                    
                    # 验证结果一致性
                    if len(performance_data['full_scan_results']) == len(performance_data['index_results']):
                        self._update_info_display(f"✅ 结果一致性验证通过 ({len(performance_data['full_scan_results'])} 条记录)\n")
                        
                        # 显示查询结果
                        self._display_query_results(performance_data['index_results'], "性能对比查询结果")
                    else:
                        self._update_info_display(f"❌ 结果不一致! 全表扫描: {len(performance_data['full_scan_results'])} 条, 索引查询: {len(performance_data['index_results'])} 条\n")
                else:
                    self._update_info_display(f"❌ 无法解析查询条件中的操作符\n")
                    
            else:
                self._update_info_display("⚠️  此查询没有WHERE条件，无法有效利用索引\n")
                # 对没有WHERE条件的查询，只做简单的时间对比
                
                start_time = time.time()
                results = self.storage_engine.select(table_name, use_index=False)
                full_scan_time = time.time() - start_time
                
                start_time = time.time()
                results = self.storage_engine.select(table_name, use_index=True)
                index_time = time.time() - start_time
                
                self._update_info_display(f"📊 全表扫描时间: {full_scan_time:.6f} 秒\n")
                self._update_info_display(f"⚡ 索引扫描时间: {index_time:.6f} 秒\n")
                self._update_info_display(f"📋 查询结果: {len(results)} 条记录\n")
                
                self._display_query_results(results, "性能对比查询结果")
            
            self._update_info_display(f"性能对比测试完成: {time.strftime('%H:%M:%S')}\n")
            self._update_info_display(f"{'='*60}\n\n")
            
        except Exception as e:
            self._update_info_display(f"❌ 性能对比测试出错: {str(e)}\n")
            import traceback
            error_details = traceback.format_exc()
            self._update_info_display(f"详细错误信息:\n{error_details}\n")
        
        finally:
            self.root.after(0, lambda: self._update_status("就绪"))

    def _save_query(self):
        """保存查询"""
        sql = self.sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "没有要保存的查询")
            return

        filename = filedialog.asksaveasfilename(
            defaultextension=".sql",
            filetypes=[("SQL files", "*.sql"), ("All files", "*.*")]
        )

        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(sql)
                messagebox.showinfo("保存成功", f"查询已保存到 {filename}")
            except Exception as e:
                messagebox.showerror("保存失败", f"保存文件时发生错误: {str(e)}")

    # 菜单相关方法
    def _new_database(self):
        """新建数据库"""
        messagebox.showinfo("提示", "新建数据库功能开发中...")

    def _open_database(self):
        """打开数据库"""
        messagebox.showinfo("提示", "打开数据库功能开发中...")

    def _show_compiler_analysis(self):
        """显示编译器分析"""
        self.notebook.select(1)  # 切换到编译器标签页

    def _show_storage_status(self):
        """显示存储引擎状态"""
        self.notebook.select(2)  # 切换到存储引擎标签页
        self._refresh_storage_stats()

    def _show_performance_stats(self):
        """显示性能统计"""
        self.notebook.select(4)  # 切换到性能监控标签页
        self._refresh_performance()

    def _show_about(self):
        """显示关于信息"""
        about_text = """现代化数据库管理系统



这是一个完整的数据库管理系统实现，包括:
• SQL编译器 (词法分析、语法分析、语义分析)
• 存储引擎 (页管理、缓存、索引)
• 查询执行引擎 (含智能查询优化器)
• 现代化图形界面

技术特性:
• B+树索引
• 多种缓存算法 (LRU、FIFO、Clock)
• 智能查询优化器 (谓词下推、索引优化等)
• 事务支持 (开发中)
• 多种数据类型
• SQL标准支持"""

        messagebox.showinfo("关于", about_text)

    def _show_optimizer_settings(self):
        """显示查询优化器设置对话框"""
        settings_window = tk.Toplevel(self.root)
        settings_window.title("查询优化器设置")
        settings_window.geometry("500x400")
        settings_window.transient(self.root)
        settings_window.grab_set()
        
        # 主框架
        main_frame = ttk.Frame(settings_window, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 标题
        title_label = ttk.Label(main_frame, text="🚀 查询优化器配置", 
                               font=('Microsoft YaHei', 14, 'bold'))
        title_label.pack(pady=(0, 20))
        
        # 统计信息显示
        stats_frame = ttk.LabelFrame(main_frame, text="当前优化统计", padding="10")
        stats_frame.pack(fill=tk.X, pady=(0, 15))
        
        stats_text = tk.Text(stats_frame, height=8, width=50, 
                            font=('Consolas', 9), state=tk.DISABLED)
        stats_text.pack(fill=tk.BOTH, expand=True)
        
        # 显示当前统计
        if hasattr(self.storage_engine, 'get_optimization_stats'):
            opt_stats = self.storage_engine.get_optimization_stats()
            stats_content = f"""✅ 优化器状态: {'启用' if opt_stats.get('optimization_enabled', False) else '禁用'}
📊 已应用优化: {opt_stats.get('optimizations_applied', 0)} 次
⏱️ 优化总耗时: {opt_stats.get('optimization_time', 0.0):.4f} 秒

🎯 支持的优化策略:
• 谓词下推优化 - 将过滤条件尽早应用，减少数据量
• 投影下推优化 - 尽早进行列投影，减少数据传输  
• 索引选择优化 - 根据查询条件智能选择索引
• JOIN顺序优化 - 优化多表连接的执行顺序
• 常量折叠优化 - 在编译时计算常量表达式
• 死代码消除 - 移除不会被执行的冗余指令

💡 优化器会根据表大小、索引可用性和查询模式
   自动选择最优的执行策略，提升查询性能。"""
            
            stats_text.config(state=tk.NORMAL)
            stats_text.insert(tk.END, stats_content)
            stats_text.config(state=tk.DISABLED)
        
        # 按钮框架
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(20, 0))
        
        ttk.Button(button_frame, text="✨ 测试优化", 
                  command=lambda: self._test_optimizer(settings_window)).pack(side=tk.LEFT)
        ttk.Button(button_frame, text="关闭", 
                  command=settings_window.destroy).pack(side=tk.RIGHT)
    
    def _test_optimizer(self, window):
        """测试查询优化器"""
        try:
            # 执行一个测试查询来演示优化器
            test_sql = "SELECT * FROM books WHERE id > 5;"
            self.sql_text.delete(1.0, tk.END)
            self.sql_text.insert(1.0, test_sql)
            
            messagebox.showinfo("测试", f"已设置测试查询：\n{test_sql}\n\n请在SQL标签页中执行查看优化效果！")
            window.destroy()
            
            # 切换到SQL标签页
            self.notebook.select(0)
            
        except Exception as e:
            messagebox.showerror("错误", f"测试失败: {str(e)}")

    def _quit_app(self):
        """退出应用"""
        if messagebox.askyesno("退出", "确定要退出应用吗？"):
            # 关闭存储引擎
            if self.storage_engine:
                try:
                    # 刷新所有脏页到磁盘
                    self.storage_engine.flush_all()
                    # 关闭存储引擎
                    self.storage_engine.shutdown()
                    print("[INFO] 存储引擎已正确关闭，数据已持久化")
                except Exception as e:
                    print(f"[ERROR] 关闭存储引擎时发生错误: {e}")
                    import traceback
                    traceback.print_exc()

            self.root.quit()

    def run(self):
        """运行应用"""
        # 注册关闭时的清理回调
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        
        # 初始化显示
        self._refresh_tables()
        self._refresh_storage_stats()

        # 显示欢迎信息
        welcome_msg = """🎉 欢迎使用现代化数据库管理系统！

✨ 核心功能特色：
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔧 完整的SQL编译器支持 (词法·语法·语义分析)
🚀 高性能存储引擎 (页管理·缓存优化)  
🌲 B+树索引优化 (快速查询·范围检索)
📊 实时性能监控 (统计分析·性能对比)
🎨 现代化图形界面 (直观操作·美观设计)

💡 快速开始：
  1. 在"SQL查询执行"标签页中运行示例查询
  2. 使用"SQL编译器"查看编译过程
  3. 在"表管理"中创建和管理数据表
  4. 通过"性能监控"观察系统运行状态

祝您使用愉快！🚀"""

        self._append_info(welcome_msg)

        # 启动主循环
        self.root.mainloop()

    def _on_closing(self):
        """应用关闭时的清理工作"""
        try:
            print("🔄 正在保存数据...")
            # 刷新所有脏页到磁盘
            if self.storage_engine:
                flushed_pages = self.storage_engine.flush_all()
                print(f"✅ 已保存 {flushed_pages} 个页面到磁盘")
                
                # 可选：显示保存状态给用户
                if flushed_pages > 0:
                    self._update_status(f"已保存 {flushed_pages} 个页面到磁盘")
            
            print("👋 应用正常关闭")
        except Exception as e:
            print(f"❌ 关闭时保存数据失败: {e}")
        finally:
            # 确保窗口正常关闭
            self.root.destroy()


class CreateTableDialog:
    """创建表对话框"""

    def __init__(self, parent, storage_engine):
        self.parent = parent
        self.storage_engine = storage_engine
        self.result = False

        self._create_dialog()

    def _create_dialog(self):
        """创建对话框"""
        self.dialog = tk.Toplevel(self.parent)
        self.dialog.title("创建新表")
        self.dialog.geometry("600x400")
        self.dialog.transient(self.parent)
        self.dialog.grab_set()

        # 表名输入
        name_frame = ttk.Frame(self.dialog)
        name_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(name_frame, text="表名:").pack(side=tk.LEFT)
        self.table_name_var = tk.StringVar()
        ttk.Entry(name_frame, textvariable=self.table_name_var, width=30).pack(side=tk.LEFT, padx=(10, 0))

        # 列定义区域
        columns_frame = ttk.LabelFrame(self.dialog, text="列定义", padding="10")
        columns_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # 列定义表格
        columns = ("列名", "类型", "长度", "主键", "唯一", "可空", "默认值")
        self.columns_tree = ttk.Treeview(columns_frame, columns=columns, show='headings', height=10)

        for col in columns:
            self.columns_tree.heading(col, text=col)
            self.columns_tree.column(col, width=80)

        tree_scroll = ttk.Scrollbar(columns_frame, orient=tk.VERTICAL, command=self.columns_tree.yview)
        self.columns_tree.configure(yscrollcommand=tree_scroll.set)

        self.columns_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # 列操作按钮
        col_buttons_frame = ttk.Frame(columns_frame)
        col_buttons_frame.pack(fill=tk.X, pady=(10, 0))

        ttk.Button(col_buttons_frame, text="添加列", command=self._add_column).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(col_buttons_frame, text="删除列", command=self._delete_column).pack(side=tk.LEFT, padx=5)

        # 对话框按钮
        button_frame = ttk.Frame(self.dialog)
        button_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Button(button_frame, text="创建", command=self._create_table).pack(side=tk.RIGHT, padx=(5, 0))
        ttk.Button(button_frame, text="取消", command=self._cancel).pack(side=tk.RIGHT)

        # 默认添加一个ID列
        self._add_default_column()

        # 居中显示
        self.dialog.update_idletasks()
        x = (self.dialog.winfo_screenwidth() - self.dialog.winfo_reqwidth()) // 2
        y = (self.dialog.winfo_screenheight() - self.dialog.winfo_reqheight()) // 2
        self.dialog.geometry(f"+{x}+{y}")

    def _add_default_column(self):
        """添加默认的ID列"""
        self.columns_tree.insert('', tk.END, values=("id", "INTEGER", "", "是", "否", "否", ""))

    def _add_column(self):
        """添加列"""
        column_dialog = AddColumnDialog(self.dialog)
        if column_dialog.result:
            col_info = column_dialog.result
            values = (
                col_info['name'],
                col_info['type'],
                col_info.get('length', ''),
                '是' if col_info.get('primary_key', False) else '否',
                '是' if col_info.get('unique', False) else '否',
                '是' if col_info.get('nullable', True) else '否',
                col_info.get('default_value', '')
            )
            self.columns_tree.insert('', tk.END, values=values)

    def _delete_column(self):
        """删除选中的列"""
        selection = self.columns_tree.selection()
        if selection:
            self.columns_tree.delete(selection[0])
        else:
            messagebox.showwarning("警告", "请选择要删除的列")

    def _create_table(self):
        """创建表"""
        table_name = self.table_name_var.get().strip()
        if not table_name:
            messagebox.showerror("错误", "请输入表名")
            return

        # 获取列定义
        columns = []
        for item in self.columns_tree.get_children():
            values = self.columns_tree.item(item)['values']

            column = {
                'name': values[0],
                'type': values[1],
                'nullable': values[5] == '是',
                'primary_key': values[3] == '是',
                'unique': values[4] == '是'
            }

            # 添加长度信息
            if values[2]:
                try:
                    column['max_length'] = int(values[2])
                except ValueError:
                    pass

            # 添加默认值
            if values[6]:
                column['default_value'] = values[6]

            columns.append(column)

        if not columns:
            messagebox.showerror("错误", "至少需要定义一个列")
            return

        # 创建表
        try:
            if self.storage_engine.create_table(table_name, columns):
                messagebox.showinfo("成功", f"表 '{table_name}' 创建成功")
                self.result = True
                self.dialog.destroy()
            else:
                messagebox.showerror("错误", f"创建表 '{table_name}' 失败")
        except Exception as e:
            messagebox.showerror("错误", f"创建表时发生错误: {str(e)}")

    def _cancel(self):
        """取消"""
        self.dialog.destroy()


class AddColumnDialog:
    """添加列对话框"""

    def __init__(self, parent):
        self.parent = parent
        self.result = None

        self._create_dialog()

    def _create_dialog(self):
        """创建对话框"""
        self.dialog = tk.Toplevel(self.parent)
        self.dialog.title("添加列")
        self.dialog.geometry("300x250")
        self.dialog.transient(self.parent)
        self.dialog.grab_set()

        # 列名
        name_frame = ttk.Frame(self.dialog)
        name_frame.pack(fill=tk.X, padx=10, pady=5)
        ttk.Label(name_frame, text="列名:").pack(side=tk.LEFT)
        self.name_var = tk.StringVar()
        ttk.Entry(name_frame, textvariable=self.name_var).pack(side=tk.RIGHT, fill=tk.X, expand=True)

        # 数据类型
        type_frame = ttk.Frame(self.dialog)
        type_frame.pack(fill=tk.X, padx=10, pady=5)
        ttk.Label(type_frame, text="类型:").pack(side=tk.LEFT)
        self.type_var = tk.StringVar(value="INTEGER")
        type_combo = ttk.Combobox(type_frame, textvariable=self.type_var,
                                  values=["INTEGER", "STRING", "FLOAT", "BOOLEAN"],
                                  state="readonly")
        type_combo.pack(side=tk.RIGHT, fill=tk.X, expand=True)

        # 长度
        length_frame = ttk.Frame(self.dialog)
        length_frame.pack(fill=tk.X, padx=10, pady=5)
        ttk.Label(length_frame, text="长度:").pack(side=tk.LEFT)
        self.length_var = tk.StringVar()
        ttk.Entry(length_frame, textvariable=self.length_var).pack(side=tk.RIGHT, fill=tk.X, expand=True)

        # 选项
        options_frame = ttk.LabelFrame(self.dialog, text="选项", padding="10")
        options_frame.pack(fill=tk.X, padx=10, pady=5)

        self.primary_key_var = tk.BooleanVar()
        ttk.Checkbutton(options_frame, text="主键", variable=self.primary_key_var).pack(anchor=tk.W)

        self.unique_var = tk.BooleanVar()
        ttk.Checkbutton(options_frame, text="唯一", variable=self.unique_var).pack(anchor=tk.W)

        self.nullable_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, text="可为空", variable=self.nullable_var).pack(anchor=tk.W)

        # 默认值
        default_frame = ttk.Frame(self.dialog)
        default_frame.pack(fill=tk.X, padx=10, pady=5)
        ttk.Label(default_frame, text="默认值:").pack(side=tk.LEFT)
        self.default_var = tk.StringVar()
        ttk.Entry(default_frame, textvariable=self.default_var).pack(side=tk.RIGHT, fill=tk.X, expand=True)

        # 按钮
        button_frame = ttk.Frame(self.dialog)
        button_frame.pack(fill=tk.X, padx=10, pady=10)

        ttk.Button(button_frame, text="确定", command=self._ok).pack(side=tk.RIGHT, padx=(5, 0))
        ttk.Button(button_frame, text="取消", command=self._cancel).pack(side=tk.RIGHT)

        # 居中显示
        self.dialog.update_idletasks()
        x = (self.dialog.winfo_screenwidth() - self.dialog.winfo_reqwidth()) // 2
        y = (self.dialog.winfo_screenheight() - self.dialog.winfo_reqheight()) // 2
        self.dialog.geometry(f"+{x}+{y}")

    def _ok(self):
        """确定"""
        name = self.name_var.get().strip()
        if not name:
            messagebox.showerror("错误", "请输入列名")
            return

        column_info = {
            'name': name,
            'type': self.type_var.get(),
            'primary_key': self.primary_key_var.get(),
            'unique': self.unique_var.get(),
            'nullable': self.nullable_var.get()
        }

        # 添加长度
        length = self.length_var.get().strip()
        if length:
            try:
                column_info['length'] = int(length)
            except ValueError:
                messagebox.showerror("错误", "长度必须是数字")
                return

        # 添加默认值
        default_value = self.default_var.get().strip()
        if default_value:
            column_info['default_value'] = default_value

        self.result = column_info
        self.dialog.destroy()

    def _cancel(self):
        """取消"""
        self.dialog.destroy()


def main():
    """主函数"""
    try:
        app = ModernDatabaseManager()
        app.run()
    except Exception as e:
        print(f"应用启动失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()