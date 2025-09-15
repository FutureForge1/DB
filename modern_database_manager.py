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
from threading import Thread
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

class ModernDatabaseManager:
    """现代化数据库管理系统主应用"""

    def __init__(self):
        """初始化应用"""
        self.root = tk.Tk()
        self.root.title("现代化数据库管理系统")
        self.root.geometry("1400x900")
        self.root.configure(bg='#f0f0f0')

        # 初始化后端组件
        self._init_database_components()

        # 创建界面
        self._create_menu()
        self._create_main_interface()

        # 状态变量
        self.current_database = "main_db"
        self.query_history = []

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

    def _create_main_interface(self):
        """创建主界面"""
        # 创建主框架
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # 创建Notebook用于标签页
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # 创建各个标签页
        self._create_query_tab()
        self._create_compiler_tab()
        self._create_storage_tab()
        self._create_tables_tab()
        self._create_performance_tab()

        # 创建底部状态栏
        self._create_status_bar()

    def _create_query_tab(self):
        """创建SQL查询标签页"""
        query_frame = ttk.Frame(self.notebook)
        self.notebook.add(query_frame, text="🔍 SQL查询执行")

        # 创建分割面板
        paned_window = ttk.PanedWindow(query_frame, orient=tk.VERTICAL)
        paned_window.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 上部分：SQL输入区域
        top_frame = ttk.LabelFrame(paned_window, text="SQL查询输入", padding="10")
        paned_window.add(top_frame, weight=1)

        # SQL输入文本框
        self.sql_text = scrolledtext.ScrolledText(
            top_frame,
            height=8,
            font=('Consolas', 12),
            wrap=tk.WORD
        )
        self.sql_text.pack(fill=tk.BOTH, expand=True)

        # 示例SQL语句
        sample_sql = """-- 示例SQL语句
-- 1. 创建表
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50),
    email VARCHAR(100),
    age INTEGER
);

-- 2. 插入数据
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 25);

-- 3. 查询数据
SELECT * FROM users WHERE age > 20;"""

        self.sql_text.insert(tk.END, sample_sql)

        # 按钮框架
        button_frame = ttk.Frame(top_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))

        ttk.Button(button_frame, text="🚀 执行查询", command=self._execute_query).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="🔍 分析SQL", command=self._analyze_sql).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="🗑️ 清空", command=self._clear_query).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="💾 保存", command=self._save_query).pack(side=tk.RIGHT, padx=5)

        # 下部分：结果显示区域
        bottom_frame = ttk.LabelFrame(paned_window, text="查询结果", padding="10")
        paned_window.add(bottom_frame, weight=2)

        # 创建结果显示的Notebook
        result_notebook = ttk.Notebook(bottom_frame)
        result_notebook.pack(fill=tk.BOTH, expand=True)

        # 结果表格标签页
        result_frame = ttk.Frame(result_notebook)
        result_notebook.add(result_frame, text="📊 结果数据")

        # 结果表格
        columns = ("Column1", "Column2", "Column3", "Column4", "Column5")
        self.result_tree = ttk.Treeview(result_frame, columns=columns, show='headings')

        # 设置列标题
        for col in columns:
            self.result_tree.heading(col, text=col)
            self.result_tree.column(col, width=100)

        # 添加滚动条
        result_scrollbar_y = ttk.Scrollbar(result_frame, orient=tk.VERTICAL, command=self.result_tree.yview)
        result_scrollbar_x = ttk.Scrollbar(result_frame, orient=tk.HORIZONTAL, command=self.result_tree.xview)
        self.result_tree.configure(yscrollcommand=result_scrollbar_y.set, xscrollcommand=result_scrollbar_x.set)

        self.result_tree.grid(row=0, column=0, sticky='nsew')
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
            font=('Consolas', 10),
            state=tk.DISABLED
        )
        self.info_text.pack(fill=tk.BOTH, expand=True)

    def _create_compiler_tab(self):
        """创建编译器分析标签页"""
        compiler_frame = ttk.Frame(self.notebook)
        self.notebook.add(compiler_frame, text="🔧 SQL编译器")

        # 创建分割面板
        paned_window = ttk.PanedWindow(compiler_frame, orient=tk.HORIZONTAL)
        paned_window.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 左侧：输入和控制
        left_frame = ttk.LabelFrame(paned_window, text="编译器输入", padding="10")
        paned_window.add(left_frame, weight=1)

        # SQL输入框
        ttk.Label(left_frame, text="输入SQL语句:").pack(anchor=tk.W)
        self.compiler_sql_text = scrolledtext.ScrolledText(
            left_frame,
            height=6,
            font=('Consolas', 10)
        )
        self.compiler_sql_text.pack(fill=tk.BOTH, expand=True, pady=(5, 10))

        # 示例SQL
        self.compiler_sql_text.insert(tk.END, "SELECT name, age FROM users WHERE age > 25;")

        # 控制按钮
        ttk.Button(left_frame, text="🔍 词法分析", command=self._lexical_analysis).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="🌳 语法分析", command=self._syntax_analysis).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="✅ 语义分析", command=self._semantic_analysis).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="⚙️ 代码生成", command=self._code_generation).pack(fill=tk.X, pady=2)

        # 右侧：分析结果
        right_frame = ttk.LabelFrame(paned_window, text="编译分析结果", padding="10")
        paned_window.add(right_frame, weight=2)

        # 创建结果显示的Notebook
        compiler_notebook = ttk.Notebook(right_frame)
        compiler_notebook.pack(fill=tk.BOTH, expand=True)

        # 词法分析结果
        self.lexer_frame = ttk.Frame(compiler_notebook)
        compiler_notebook.add(self.lexer_frame, text="词法分析")

        self.lexer_result = scrolledtext.ScrolledText(
            self.lexer_frame,
            font=('Consolas', 10),
            state=tk.DISABLED
        )
        self.lexer_result.pack(fill=tk.BOTH, expand=True)

        # 语法分析结果
        self.parser_frame = ttk.Frame(compiler_notebook)
        compiler_notebook.add(self.parser_frame, text="语法分析")

        self.parser_result = scrolledtext.ScrolledText(
            self.parser_frame,
            font=('Consolas', 10),
            state=tk.DISABLED
        )
        self.parser_result.pack(fill=tk.BOTH, expand=True)

        # 语义分析结果
        self.semantic_frame = ttk.Frame(compiler_notebook)
        compiler_notebook.add(self.semantic_frame, text="语义分析")

        self.semantic_result = scrolledtext.ScrolledText(
            self.semantic_frame,
            font=('Consolas', 10),
            state=tk.DISABLED
        )
        self.semantic_result.pack(fill=tk.BOTH, expand=True)

        # 目标代码
        self.codegen_frame = ttk.Frame(compiler_notebook)
        compiler_notebook.add(self.codegen_frame, text="目标代码")

        self.codegen_result = scrolledtext.ScrolledText(
            self.codegen_frame,
            font=('Consolas', 10),
            state=tk.DISABLED
        )
        self.codegen_result.pack(fill=tk.BOTH, expand=True)

    def _create_storage_tab(self):
        """创建存储引擎标签页"""
        storage_frame = ttk.Frame(self.notebook)
        self.notebook.add(storage_frame, text="💾 存储引擎")

        # 创建分割面板
        paned_window = ttk.PanedWindow(storage_frame, orient=tk.VERTICAL)
        paned_window.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 上部分：存储统计信息
        stats_frame = ttk.LabelFrame(paned_window, text="存储引擎统计", padding="10")
        paned_window.add(stats_frame, weight=1)

        # 统计信息显示
        self.storage_stats_text = scrolledtext.ScrolledText(
            stats_frame,
            height=10,
            font=('Consolas', 10),
            state=tk.DISABLED
        )
        self.storage_stats_text.pack(fill=tk.BOTH, expand=True)

        # 刷新按钮
        ttk.Button(stats_frame, text="🔄 刷新统计", command=self._refresh_storage_stats).pack(anchor=tk.E, pady=(10, 0))

        # 下部分：缓存和页面管理
        cache_frame = ttk.LabelFrame(paned_window, text="缓存和页面管理", padding="10")
        paned_window.add(cache_frame, weight=1)

        # 创建缓存信息的Notebook
        cache_notebook = ttk.Notebook(cache_frame)
        cache_notebook.pack(fill=tk.BOTH, expand=True)

        # 缓存状态
        cache_status_frame = ttk.Frame(cache_notebook)
        cache_notebook.add(cache_status_frame, text="缓存状态")

        self.cache_status_text = scrolledtext.ScrolledText(
            cache_status_frame,
            font=('Consolas', 10),
            state=tk.DISABLED
        )
        self.cache_status_text.pack(fill=tk.BOTH, expand=True)

        # 页面信息
        page_info_frame = ttk.Frame(cache_notebook)
        cache_notebook.add(page_info_frame, text="页面信息")

        self.page_info_text = scrolledtext.ScrolledText(
            page_info_frame,
            font=('Consolas', 10),
            state=tk.DISABLED
        )
        self.page_info_text.pack(fill=tk.BOTH, expand=True)

        # 索引信息
        index_info_frame = ttk.Frame(cache_notebook)
        cache_notebook.add(index_info_frame, text="索引信息")

        self.index_info_text = scrolledtext.ScrolledText(
            index_info_frame,
            font=('Consolas', 10),
            state=tk.DISABLED
        )
        self.index_info_text.pack(fill=tk.BOTH, expand=True)

    def _create_tables_tab(self):
        """创建表管理标签页"""
        tables_frame = ttk.Frame(self.notebook)
        self.notebook.add(tables_frame, text="📋 表管理")

        # 创建分割面板
        paned_window = ttk.PanedWindow(tables_frame, orient=tk.HORIZONTAL)
        paned_window.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 左侧：表列表和操作
        left_frame = ttk.LabelFrame(paned_window, text="数据库表", padding="10")
        paned_window.add(left_frame, weight=1)

        # 表列表
        self.tables_listbox = tk.Listbox(left_frame, font=('Consolas', 10))
        self.tables_listbox.pack(fill=tk.BOTH, expand=True)
        self.tables_listbox.bind('<<ListboxSelect>>', self._on_table_select)

        # 表操作按钮
        table_buttons_frame = ttk.Frame(left_frame)
        table_buttons_frame.pack(fill=tk.X, pady=(10, 0))

        ttk.Button(table_buttons_frame, text="🔄 刷新", command=self._refresh_tables).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(table_buttons_frame, text="➕ 创建表", command=self._create_table_dialog).pack(side=tk.LEFT, padx=5)
        ttk.Button(table_buttons_frame, text="🗑️ 删除表", command=self._drop_table).pack(side=tk.LEFT, padx=5)

        # 右侧：表结构和数据
        right_frame = ttk.LabelFrame(paned_window, text="表详细信息", padding="10")
        paned_window.add(right_frame, weight=2)

        # 创建表信息的Notebook
        table_notebook = ttk.Notebook(right_frame)
        table_notebook.pack(fill=tk.BOTH, expand=True)

        # 表结构标签页
        schema_frame = ttk.Frame(table_notebook)
        table_notebook.add(schema_frame, text="表结构")

        # 列信息表格
        columns = ("列名", "类型", "长度", "主键", "唯一", "可空", "默认值")
        self.schema_tree = ttk.Treeview(schema_frame, columns=columns, show='headings')

        for col in columns:
            self.schema_tree.heading(col, text=col)
            self.schema_tree.column(col, width=80)

        schema_scrollbar = ttk.Scrollbar(schema_frame, orient=tk.VERTICAL, command=self.schema_tree.yview)
        self.schema_tree.configure(yscrollcommand=schema_scrollbar.set)

        self.schema_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        schema_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # 表数据标签页
        data_frame = ttk.Frame(table_notebook)
        table_notebook.add(data_frame, text="表数据")

        self.data_tree = ttk.Treeview(data_frame, show='headings')

        data_scrollbar_y = ttk.Scrollbar(data_frame, orient=tk.VERTICAL, command=self.data_tree.yview)
        data_scrollbar_x = ttk.Scrollbar(data_frame, orient=tk.HORIZONTAL, command=self.data_tree.xview)
        self.data_tree.configure(yscrollcommand=data_scrollbar_y.set, xscrollcommand=data_scrollbar_x.set)

        self.data_tree.grid(row=0, column=0, sticky='nsew')
        data_scrollbar_y.grid(row=0, column=1, sticky='ns')
        data_scrollbar_x.grid(row=1, column=0, sticky='ew')

        data_frame.grid_rowconfigure(0, weight=1)
        data_frame.grid_columnconfigure(0, weight=1)

    def _create_performance_tab(self):
        """创建性能监控标签页"""
        perf_frame = ttk.Frame(self.notebook)
        self.notebook.add(perf_frame, text="📈 性能监控")

        # 性能统计显示
        perf_stats_frame = ttk.LabelFrame(perf_frame, text="性能统计", padding="10")
        perf_stats_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.perf_text = scrolledtext.ScrolledText(
            perf_stats_frame,
            font=('Consolas', 10),
            state=tk.DISABLED
        )
        self.perf_text.pack(fill=tk.BOTH, expand=True)

        # 控制按钮
        perf_buttons_frame = ttk.Frame(perf_stats_frame)
        perf_buttons_frame.pack(fill=tk.X, pady=(10, 0))

        ttk.Button(perf_buttons_frame, text="🔄 刷新", command=self._refresh_performance).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(perf_buttons_frame, text="📊 详细统计", command=self._show_detailed_stats).pack(side=tk.LEFT, padx=5)
        ttk.Button(perf_buttons_frame, text="🧹 清除统计", command=self._clear_stats).pack(side=tk.LEFT, padx=5)

    def _create_status_bar(self):
        """创建状态栏"""
        status_frame = ttk.Frame(self.root)
        status_frame.pack(fill=tk.X, side=tk.BOTTOM, padx=10, pady=(0, 5))

        self.status_label = ttk.Label(
            status_frame,
            text="就绪 | 数据库: main_db | 存储引擎: 运行中",
            relief=tk.SUNKEN
        )
        self.status_label.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # 时间标签
        self.time_label = ttk.Label(status_frame)
        self.time_label.pack(side=tk.RIGHT)

        # 更新时间
        self._update_time()

    def _update_time(self):
        """更新时间显示"""
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self.time_label.config(text=current_time)
        self.root.after(1000, self._update_time)

    def _update_status(self, message: str):
        """更新状态栏"""
        self.status_label.config(text=message)

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
                success, results, error_msg = sql_processor.process_sql(sql)

                if success:
                    self._update_info_display("✅ SQL执行成功\n")

                    # 显示结果
                    if results:
                        if isinstance(results[0], dict) and 'operation' in results[0]:
                            # DDL/DML操作结果
                            for result in results:
                                if result.get('status') == 'success':
                                    self._update_info_display(f"  ✅ {result.get('message', '操作成功')}\n")
                                else:
                                    self._update_info_display(f"  ❌ {result.get('message', '操作失败')}\n")
                        else:
                            # SELECT查询结果
                            self._display_query_results(results, "查询结果")
                            self._update_info_display(f"  返回 {len(results)} 条记录\n")
                    else:
                        self._update_info_display("  执行成功，无返回结果\n")
                else:
                    self._update_info_display(f"❌ SQL执行失败: {error_msg}\n")
                    self.query_history[-1]['status'] = 'error'
                    return

            except Exception as e:
                self._update_info_display(f"❌ SQL处理器执行错误: {str(e)}\n")
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
        """内部词法分析方法"""
        try:
            lexer = Lexer(sql)
            tokens = lexer.tokenize()

            # 显示词法分析结果
            self.lexer_result.config(state=tk.NORMAL)
            self.lexer_result.delete(1.0, tk.END)

            result_text = "=" * 60 + "\n"
            result_text += "             词法分析结果\n"
            result_text += "=" * 60 + "\n"
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
            # 先进行词法分析
            lexer = Lexer(sql)
            tokens = lexer.tokenize()

            # 然后进行语法分析
            parser = Parser(tokens)
            ast = parser.parse()

            # 显示语法分析结果
            self.parser_result.config(state=tk.NORMAL)
            self.parser_result.delete(1.0, tk.END)

            result_text = "=" * 60 + "\n"
            result_text += "             语法分析结果\n"
            result_text += "=" * 60 + "\n"
            result_text += f"输入SQL: {sql}\n"
            result_text += "-" * 60 + "\n"

            if ast:
                result_text += "抽象语法树 (AST):\n"
                result_text += str(ast)
            else:
                result_text += "语法分析失败或未生成AST\n"

            result_text += "\n"
            if hasattr(parser, 'parse_steps') and parser.parse_steps:
                result_text += "分析步骤:\n"
                for i, step in enumerate(parser.parse_steps, 1):
                    result_text += f"{i:2d}. {step}\n"

            result_text += "-" * 60 + "\n"
            result_text += "语法分析完成！\n"

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
            # 词法分析
            lexer = Lexer(sql)
            tokens = lexer.tokenize()

            # 语法分析
            parser = Parser(tokens)
            ast = parser.parse()

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
            # 完整的编译过程
            lexer = Lexer(sql)
            tokens = lexer.tokenize()

            parser = Parser(tokens)
            ast = parser.parse()

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
            stats_text += f"缓存命中率: {cache_stats['cache_hit_rate']}%\n"
            stats_text += f"缓存命中: {cache_stats['cache_hits']}\n"
            stats_text += f"缓存未命中: {cache_stats['cache_misses']}\n"
            stats_text += f"已使用帧: {cache_stats['used_frames']}/{cache_stats['buffer_size']}\n"
            stats_text += f"脏页数: {cache_stats['dirty_frames']}\n"

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
        cache_text += f"缓存大小: {cache_stats['buffer_size']} 页\n"
        cache_text += f"已使用: {cache_stats['used_frames']} 页\n"
        cache_text += f"空闲: {cache_stats['buffer_size'] - cache_stats['used_frames']} 页\n"
        cache_text += f"脏页: {cache_stats['dirty_frames']} 页\n"
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
            columns = table_info.get('columns', [])
            primary_key = table_info.get('primary_key', [])

            # 如果有详细的列信息，显示它
            if hasattr(self.storage_engine.table_manager, 'get_table_schema'):
                try:
                    schema = self.storage_engine.table_manager.get_table_schema(table_name)
                    if schema:
                        for col in schema.columns:
                            values = (
                                col.name,
                                col.column_type.value,
                                col.max_length or '',
                                '是' if col.is_primary_key else '否',
                                '是' if col.is_unique else '否',
                                '是' if col.nullable else '否',
                                col.default_value or ''
                            )
                            self.schema_tree.insert('', tk.END, values=values)
                except:
                    # 如果获取详细schema失败，显示基本信息
                    for col in columns:
                        values = (col, 'UNKNOWN', '',
                                '是' if col in primary_key else '否',
                                '否', '是', '')
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
            perf_text += f"  命中率: {cache_stats['cache_hit_rate']}%\n"
            perf_text += f"  总访问: {cache_stats['cache_hits'] + cache_stats['cache_misses']}\n"
            perf_text += f"  命中数: {cache_stats['cache_hits']}\n"
            perf_text += f"  未命中数: {cache_stats['cache_misses']}\n"

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
            font=('Consolas', 10),
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

版本: 1.0
开发者: AI助手

这是一个完整的数据库管理系统实现，包括:
• SQL编译器 (词法分析、语法分析、语义分析)
• 存储引擎 (页管理、缓存、索引)
• 查询执行引擎
• 现代化图形界面

技术特性:
• B+树索引
• LRU缓存算法
• 事务支持 (开发中)
• 多种数据类型
• SQL标准支持"""

        messagebox.showinfo("关于", about_text)

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
        # 初始化显示
        self._refresh_tables()
        self._refresh_storage_stats()

        # 显示欢迎信息
        welcome_msg = """欢迎使用现代化数据库管理系统！

功能特色：
• 完整的SQL编译器支持
• 高性能存储引擎
• B+树索引优化
• 实时性能监控
• 直观的图形界面

请开始使用各个功能标签页探索系统能力。"""

        self._append_info(welcome_msg)

        # 启动主循环
        self.root.mainloop()


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