"""
现代化数据库管理系统 - CustomTkinter版本
使用CustomTkinter框架提供更现代化的用户界面
"""

import sys
import os
from pathlib import Path

# 确保src模块可以被导入
sys.path.insert(0, str(Path(__file__).parent))

import customtkinter as ctk
from tkinter import messagebox, filedialog
import json
import time
from threading import Thread
from typing import Dict, List, Optional, Any

# 导入项目模块
try:
    from src.unified_sql_processor import UnifiedSQLProcessor
    from src.storage.storage_engine import StorageEngine
    from src.execution.execution_engine import ExecutionEngine
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

# 设置CustomTkinter外观
ctk.set_appearance_mode("system")  # "system", "dark", "light"
ctk.set_default_color_theme("blue")  # "blue", "green", "dark-blue"

class ModernDatabaseManagerCTK:
    """现代化数据库管理系统 - CustomTkinter版本"""

    def __init__(self):
        """初始化应用"""
        # 创建主窗口
        self.root = ctk.CTk()
        self.root.title("🚀 现代化数据库管理系统 - CustomTkinter版")
        self.root.geometry("1600x1000")
        
        # 设置图标（如果有的话）
        try:
            # self.root.iconbitmap("icon.ico")
            pass
        except:
            pass
        
        # 初始化数据库组件
        self._init_database_components()
        
        # 创建界面
        self._create_main_interface()
        
        # 状态变量
        self.current_database = "modern_db"
        self.query_history = []
        
        # 自动刷新统计信息
        self._refresh_storage_stats()

    def _init_database_components(self):
        """初始化数据库组件"""
        try:
            # 创建存储引擎
            self.storage_engine = StorageEngine("modern_db", buffer_size=50)
            
            # 创建执行引擎
            self.execution_engine = ExecutionEngine(self.storage_engine)
            
            # 创建统一SQL处理器
            self.sql_processor = UnifiedSQLProcessor(self.storage_engine)
            
            print("[SUCCESS] 数据库组件初始化成功")
            
        except Exception as e:
            print(f"[ERROR] 数据库组件初始化失败: {e}")
            messagebox.showerror("初始化错误", f"数据库组件初始化失败: {e}")

    def _create_main_interface(self):
        """创建主界面"""
        # 创建侧边栏
        self._create_sidebar()
        
        # 创建主内容区域
        self._create_main_content()
        
        # 创建底部状态栏
        self._create_status_bar()

    def _create_sidebar(self):
        """创建侧边栏"""
        # 侧边栏框架
        self.sidebar_frame = ctk.CTkFrame(self.root, width=250, corner_radius=0)
        self.sidebar_frame.grid(row=0, column=0, rowspan=4, sticky="nsew")
        self.sidebar_frame.grid_rowconfigure(4, weight=1)
        
        # 应用标题
        self.logo_label = ctk.CTkLabel(
            self.sidebar_frame, 
            text="🚀 数据库管理系统",
            font=ctk.CTkFont(size=20, weight="bold")
        )
        self.logo_label.grid(row=0, column=0, padx=20, pady=(20, 10))
        
        # 副标题
        self.subtitle_label = ctk.CTkLabel(
            self.sidebar_frame,
            text="现代化数据库解决方案",
            font=ctk.CTkFont(size=12)
        )
        self.subtitle_label.grid(row=1, column=0, padx=20, pady=(0, 20))
        
        # 导航按钮
        self.sql_button = ctk.CTkButton(
            self.sidebar_frame,
            text="🔍 SQL查询",
            command=lambda: self._switch_tab("sql"),
            height=40,
            font=ctk.CTkFont(size=14)
        )
        self.sql_button.grid(row=2, column=0, padx=20, pady=10, sticky="ew")
        
        self.compiler_button = ctk.CTkButton(
            self.sidebar_frame,
            text="🔧 SQL编译器",
            command=lambda: self._switch_tab("compiler"),
            height=40,
            font=ctk.CTkFont(size=14)
        )
        self.compiler_button.grid(row=3, column=0, padx=20, pady=10, sticky="ew")
        
        self.storage_button = ctk.CTkButton(
            self.sidebar_frame,
            text="💾 存储引擎",
            command=lambda: self._switch_tab("storage"),
            height=40,
            font=ctk.CTkFont(size=14)
        )
        self.storage_button.grid(row=4, column=0, padx=20, pady=10, sticky="ew")
        
        self.tables_button = ctk.CTkButton(
            self.sidebar_frame,
            text="📊 表管理",
            command=lambda: self._switch_tab("tables"),
            height=40,
            font=ctk.CTkFont(size=14)
        )
        self.tables_button.grid(row=5, column=0, padx=20, pady=10, sticky="ew")
        
        self.performance_button = ctk.CTkButton(
            self.sidebar_frame,
            text="⚡ 性能监控",
            command=lambda: self._switch_tab("performance"),
            height=40,
            font=ctk.CTkFont(size=14)
        )
        self.performance_button.grid(row=6, column=0, padx=20, pady=10, sticky="ew")
        
        # 设置按钮
        self.settings_button = ctk.CTkButton(
            self.sidebar_frame,
            text="⚙️ 设置",
            command=self._show_settings,
            height=40,
            font=ctk.CTkFont(size=14)
        )
        self.settings_button.grid(row=7, column=0, padx=20, pady=(10, 20), sticky="ew")
        
        # 外观模式切换
        self.appearance_mode_label = ctk.CTkLabel(self.sidebar_frame, text="外观模式:", anchor="w")
        self.appearance_mode_label.grid(row=8, column=0, padx=20, pady=(20, 0), sticky="w")
        
        self.appearance_mode_optionemenu = ctk.CTkOptionMenu(
            self.sidebar_frame,
            values=["Light", "Dark", "System"],
            command=self._change_appearance_mode_event
        )
        self.appearance_mode_optionemenu.grid(row=9, column=0, padx=20, pady=(10, 10), sticky="ew")
        
        # 缩放设置
        self.scaling_label = ctk.CTkLabel(self.sidebar_frame, text="UI缩放:", anchor="w")
        self.scaling_label.grid(row=10, column=0, padx=20, pady=(20, 0), sticky="w")
        
        self.scaling_optionemenu = ctk.CTkOptionMenu(
            self.sidebar_frame,
            values=["80%", "90%", "100%", "110%", "120%"],
            command=self._change_scaling_event
        )
        self.scaling_optionemenu.grid(row=11, column=0, padx=20, pady=(10, 20), sticky="ew")

    def _create_main_content(self):
        """创建主内容区域"""
        # 主内容框架
        self.main_frame = ctk.CTkFrame(self.root, corner_radius=0)
        self.main_frame.grid(row=0, column=1, rowspan=3, sticky="nsew", padx=(20, 20), pady=20)
        self.main_frame.grid_rowconfigure(0, weight=1)
        self.main_frame.grid_columnconfigure(0, weight=1)
        
        # 创建各个标签页内容
        self._create_sql_tab()
        
        # 设置当前标签页
        self.current_tab = "sql"
        self._switch_tab("sql")

    def _create_sql_tab(self):
        """创建SQL查询标签页"""
        self.sql_frame = ctk.CTkFrame(self.main_frame)
        
        # 标题
        self.sql_title = ctk.CTkLabel(
            self.sql_frame,
            text="🔍 SQL查询执行器",
            font=ctk.CTkFont(size=24, weight="bold")
        )
        self.sql_title.grid(row=0, column=0, columnspan=2, padx=20, pady=(20, 10), sticky="w")
        
        # 副标题
        self.sql_subtitle = ctk.CTkLabel(
            self.sql_frame,
            text="支持完整SQL语法，包含查询优化器和多表JOIN",
            font=ctk.CTkFont(size=14),
            text_color=("gray10", "gray90")
        )
        self.sql_subtitle.grid(row=1, column=0, columnspan=2, padx=20, pady=(0, 20), sticky="w")
        
        # SQL输入区域
        self.sql_input_frame = ctk.CTkFrame(self.sql_frame)
        self.sql_input_frame.grid(row=2, column=0, columnspan=2, padx=20, pady=(0, 20), sticky="ew")
        self.sql_input_frame.grid_columnconfigure(0, weight=1)
        
        # SQL输入标签
        self.sql_input_label = ctk.CTkLabel(
            self.sql_input_frame,
            text="📝 输入SQL语句:",
            font=ctk.CTkFont(size=16, weight="bold")
        )
        self.sql_input_label.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="w")
        
        # SQL文本框
        self.sql_textbox = ctk.CTkTextbox(
            self.sql_input_frame,
            height=200,
            font=ctk.CTkFont(family="Consolas", size=12)
        )
        self.sql_textbox.grid(row=1, column=0, padx=20, pady=(0, 10), sticky="ew")
        
        # 示例SQL
        sample_sql = '''-- 🌟 示例SQL语句
-- 1. 创建表
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50),
    email VARCHAR(100),
    age INTEGER
);

-- 2. 插入数据
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 25);
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 30);

-- 3. 查询数据
SELECT * FROM users WHERE age > 20;

-- 4. 多表查询 (需要先创建departments表)
-- SELECT u.name, d.name FROM users u JOIN departments d ON u.dept_id = d.id;'''
        
        self.sql_textbox.insert("0.0", sample_sql)
        
        # 按钮区域
        self.button_frame = ctk.CTkFrame(self.sql_input_frame)
        self.button_frame.grid(row=2, column=0, padx=20, pady=(10, 20), sticky="ew")
        self.button_frame.grid_columnconfigure((0, 1, 2, 3), weight=1)
        
        # 执行按钮
        self.execute_button = ctk.CTkButton(
            self.button_frame,
            text="🚀 执行查询",
            command=self._execute_query,
            height=40,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=("#1f538d", "#14375e"),
            hover_color=("#14375e", "#1f538d")
        )
        self.execute_button.grid(row=0, column=0, padx=10, pady=10, sticky="ew")
        
        # 分析按钮
        self.analyze_button = ctk.CTkButton(
            self.button_frame,
            text="🔍 分析SQL",
            command=self._analyze_sql,
            height=40,
            font=ctk.CTkFont(size=14)
        )
        self.analyze_button.grid(row=0, column=1, padx=10, pady=10, sticky="ew")
        
        # 清空按钮
        self.clear_button = ctk.CTkButton(
            self.button_frame,
            text="🗑️ 清空",
            command=self._clear_query,
            height=40,
            font=ctk.CTkFont(size=14),
            fg_color=("#b22222", "#8b0000"),
            hover_color=("#8b0000", "#b22222")
        )
        self.clear_button.grid(row=0, column=2, padx=10, pady=10, sticky="ew")
        
        # 保存按钮
        self.save_button = ctk.CTkButton(
            self.button_frame,
            text="💾 保存",
            command=self._save_query,
            height=40,
            font=ctk.CTkFont(size=14)
        )
        self.save_button.grid(row=0, column=3, padx=10, pady=10, sticky="ew")
        
        # 选项区域
        self.options_frame = ctk.CTkFrame(self.sql_input_frame)
        self.options_frame.grid(row=3, column=0, padx=20, pady=(0, 20), sticky="ew")
        
        # 索引选项
        self.use_index_var = ctk.BooleanVar(value=True)
        self.index_checkbox = ctk.CTkCheckBox(
            self.options_frame,
            text="🌲 使用B+树索引",
            variable=self.use_index_var,
            font=ctk.CTkFont(size=14)
        )
        self.index_checkbox.grid(row=0, column=0, padx=20, pady=15, sticky="w")
        
        # 优化器选项
        self.use_optimizer_var = ctk.BooleanVar(value=True)
        self.optimizer_checkbox = ctk.CTkCheckBox(
            self.options_frame,
            text="⚡ 启用查询优化器",
            variable=self.use_optimizer_var,
            font=ctk.CTkFont(size=14)
        )
        self.optimizer_checkbox.grid(row=0, column=1, padx=20, pady=15, sticky="w")
        
        # 结果显示区域
        self.result_frame = ctk.CTkFrame(self.sql_frame)
        self.result_frame.grid(row=3, column=0, columnspan=2, padx=20, pady=(0, 20), sticky="nsew")
        self.result_frame.grid_rowconfigure(1, weight=1)
        self.result_frame.grid_columnconfigure(0, weight=1)
        
        # 结果标题
        self.result_label = ctk.CTkLabel(
            self.result_frame,
            text="📊 查询结果",
            font=ctk.CTkFont(size=16, weight="bold")
        )
        self.result_label.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="w")
        
        # 结果文本框
        self.result_textbox = ctk.CTkTextbox(
            self.result_frame,
            height=300,
            font=ctk.CTkFont(family="Consolas", size=11)
        )
        self.result_textbox.grid(row=1, column=0, padx=20, pady=(0, 20), sticky="nsew")
        
        # 配置网格权重
        self.sql_frame.grid_rowconfigure(3, weight=1)
        self.sql_frame.grid_columnconfigure(0, weight=1)

    def _create_status_bar(self):
        """创建状态栏"""
        self.status_frame = ctk.CTkFrame(self.root, height=30, corner_radius=0)
        self.status_frame.grid(row=3, column=0, columnspan=2, sticky="ew", padx=0, pady=0)
        self.status_frame.grid_columnconfigure(1, weight=1)
        
        # 状态标签
        self.status_label = ctk.CTkLabel(
            self.status_frame,
            text="🟢 数据库已连接 | ⚡ 查询优化器已启用",
            font=ctk.CTkFont(size=12)
        )
        self.status_label.grid(row=0, column=0, padx=20, pady=5, sticky="w")
        
        # 时间标签
        self.time_label = ctk.CTkLabel(
            self.status_frame,
            text="",
            font=ctk.CTkFont(size=12)
        )
        self.time_label.grid(row=0, column=1, padx=20, pady=5, sticky="e")
        
        # 更新时间
        self._update_time()

    def _switch_tab(self, tab_name):
        """切换标签页"""
        # 隐藏所有标签页
        for widget in self.main_frame.winfo_children():
            widget.grid_remove()
        
        # 显示选中的标签页
        if tab_name == "sql":
            self.sql_frame.grid(row=0, column=0, sticky="nsew", padx=20, pady=20)
        
        # 更新按钮状态
        self._update_button_states(tab_name)
        self.current_tab = tab_name

    def _update_button_states(self, active_tab):
        """更新按钮状态"""
        buttons = {
            "sql": self.sql_button,
            "compiler": self.compiler_button,
            "storage": self.storage_button,
            "tables": self.tables_button,
            "performance": self.performance_button
        }
        
        for tab, button in buttons.items():
            if tab == active_tab:
                button.configure(fg_color=("#1f538d", "#14375e"))
            else:
                button.configure(fg_color=("gray75", "gray25"))

    def _execute_query(self):
        """执行SQL查询"""
        sql = self.sql_textbox.get("0.0", "end-1c").strip()
        if not sql:
            self._show_result("⚠️ 请输入SQL语句")
            return
        
        try:
            # 显示执行状态
            self._show_result("🔄 正在执行查询...")
            self.root.update()
            
            # 设置索引使用
            self.execution_engine.set_index_mode(self.use_index_var.get())
            
            # 执行SQL
            success, results, error = self.sql_processor.process_sql(sql)
            
            if success:
                if results:
                    result_text = f"✅ 查询执行成功！找到 {len(results)} 条记录\\n\\n"
                    result_text += "📊 查询结果:\\n"
                    result_text += "=" * 50 + "\\n"
                    
                    # 显示结果
                    for i, record in enumerate(results[:10], 1):  # 限制显示前10条
                        result_text += f"记录 {i}: {record}\\n"
                    
                    if len(results) > 10:
                        result_text += f"\\n... 还有 {len(results) - 10} 条记录\\n"
                    
                    # 显示优化信息
                    if hasattr(self.execution_engine, 'get_optimization_stats'):
                        opt_stats = self.execution_engine.get_optimization_stats()
                        if opt_stats.get('optimizations_applied', 0) > 0:
                            result_text += f"\\n🚀 查询优化: 应用了 {opt_stats['optimizations_applied']} 项优化\\n"
                else:
                    result_text = "✅ 查询执行成功！\\n\\n📝 操作完成，无返回结果。"
                
                self._show_result(result_text)
                
            else:
                error_text = f"❌ 查询执行失败:\\n\\n{error}"
                self._show_result(error_text)
                
        except Exception as e:
            error_text = f"💥 执行过程中发生错误:\\n\\n{str(e)}"
            self._show_result(error_text)

    def _analyze_sql(self):
        """分析SQL语句"""
        sql = self.sql_textbox.get("0.0", "end-1c").strip()
        if not sql:
            self._show_result("⚠️ 请输入SQL语句")
            return
        
        result_text = f"🔍 SQL语句分析结果\\n\\n"
        result_text += f"📝 输入语句: {sql}\\n\\n"
        result_text += "🔧 分析功能正在开发中...\\n"
        result_text += "将包含: 词法分析、语法分析、语义分析、执行计划等"
        
        self._show_result(result_text)

    def _clear_query(self):
        """清空查询"""
        self.sql_textbox.delete("0.0", "end")
        self.result_textbox.delete("0.0", "end")

    def _save_query(self):
        """保存查询"""
        sql = self.sql_textbox.get("0.0", "end-1c").strip()
        if not sql:
            messagebox.showwarning("警告", "没有可保存的查询内容")
            return
        
        filename = filedialog.asksaveasfilename(
            defaultextension=".sql",
            filetypes=[("SQL files", "*.sql"), ("Text files", "*.txt"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(sql)
                messagebox.showinfo("成功", f"查询已保存到: {filename}")
            except Exception as e:
                messagebox.showerror("错误", f"保存失败: {e}")

    def _show_result(self, text):
        """显示结果"""
        self.result_textbox.delete("0.0", "end")
        self.result_textbox.insert("0.0", text)

    def _refresh_storage_stats(self):
        """刷新存储统计"""
        try:
            stats = self.storage_engine.get_stats()
            # 这里可以更新状态栏或其他统计显示
            pass
        except Exception as e:
            print(f"刷新统计失败: {e}")

    def _show_settings(self):
        """显示设置对话框"""
        settings_window = ctk.CTkToplevel(self.root)
        settings_window.title("⚙️ 系统设置")
        settings_window.geometry("500x400")
        settings_window.transient(self.root)
        
        # 设置内容
        title_label = ctk.CTkLabel(
            settings_window,
            text="⚙️ 系统设置",
            font=ctk.CTkFont(size=20, weight="bold")
        )
        title_label.pack(pady=20)
        
        info_text = """🚀 现代化数据库管理系统

✨ 特性:
• 完整的SQL编译器
• B+树索引支持
• 智能查询优化器
• 多表JOIN查询
• 现代化界面设计

🎨 使用CustomTkinter框架构建
提供更好的用户体验和视觉效果"""
        
        info_textbox = ctk.CTkTextbox(settings_window, height=200)
        info_textbox.pack(padx=20, pady=20, fill="both", expand=True)
        info_textbox.insert("0.0", info_text)
        info_textbox.configure(state="disabled")
        
        close_button = ctk.CTkButton(
            settings_window,
            text="关闭",
            command=settings_window.destroy,
            height=40
        )
        close_button.pack(pady=20)

    def _change_appearance_mode_event(self, new_appearance_mode: str):
        """改变外观模式"""
        ctk.set_appearance_mode(new_appearance_mode)

    def _change_scaling_event(self, new_scaling: str):
        """改变UI缩放"""
        new_scaling_float = int(new_scaling.replace("%", "")) / 100
        ctk.set_widget_scaling(new_scaling_float)

    def _update_time(self):
        """更新时间显示"""
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self.time_label.configure(text=f"🕐 {current_time}")
        self.root.after(1000, self._update_time)

    def run(self):
        """运行应用"""
        # 配置网格权重
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(1, weight=1)
        
        print("🚀 启动CustomTkinter版数据库管理系统...")
        self.root.mainloop()

if __name__ == "__main__":
    app = ModernDatabaseManagerCTK()
    app.run()
