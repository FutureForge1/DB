"""
数据库系统 Streamlit 前端
可视化展示SQL查询从词法分析到执行的完整过程
"""

import streamlit as st
import sys
from pathlib import Path
import traceback
import pandas as pd
import json
import os

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from src.compiler.lexer.lexer import Lexer
from src.compiler.parser.unified_parser import UnifiedSQLParser
from src.compiler.semantic.analyzer import SemanticAnalyzer
from src.compiler.semantic.extended_analyzer import ExtendedSemanticAnalyzer
from src.compiler.semantic.ddl_dml_analyzer import DDLDMLSemanticAnalyzer
from src.compiler.codegen.translator import QuadrupleTranslator
from src.compiler.codegen.translator import IntegratedCodeGenerator
from src.execution.execution_engine import ExecutionEngine
from src.storage.storage_engine import StorageEngine
from src.sql_processor import SQLProcessor

# 页面配置
st.set_page_config(
    page_title="数据库系统测试平台",
    page_icon="🗃️",
    layout="wide"
)

# 初始化存储引擎
@st.cache_resource
def init_storage():
    """初始化存储引擎和测试数据"""
    storage = StorageEngine("streamlit_db")
    
    # 创建学生表
    students_columns = [
        {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
        {'name': 'name', 'type': 'STRING', 'max_length': 50},
        {'name': 'age', 'type': 'INTEGER'},
        {'name': 'grade', 'type': 'FLOAT'},
        {'name': 'major', 'type': 'STRING', 'max_length': 30}
    ]
    
    try:
        storage.create_table("students", students_columns)
    except:
        pass  # 表可能已存在
    
    # 创建课程表
    courses_columns = [
        {'name': 'course_id', 'type': 'INTEGER', 'primary_key': True},
        {'name': 'student_id', 'type': 'INTEGER'},
        {'name': 'course_name', 'type': 'STRING', 'max_length': 50},
        {'name': 'score', 'type': 'FLOAT'}
    ]
    
    try:
        storage.create_table("courses", courses_columns)
    except:
        pass  # 表可能已存在
    
    return storage

# 插入测试数据
def insert_test_data(storage):
    """插入测试数据"""
    # 学生数据
    students_data = [
        {'id': 1, 'name': 'Alice', 'age': 20, 'grade': 85.5, 'major': 'CS'},
        {'id': 2, 'name': 'Bob', 'age': 22, 'grade': 92.0, 'major': 'Math'},
        {'id': 3, 'name': 'Charlie', 'age': 19, 'grade': 78.5, 'major': 'CS'},
        {'id': 4, 'name': 'Diana', 'age': 21, 'grade': 96.0, 'major': 'Physics'},
        {'id': 5, 'name': 'Eve', 'age': 23, 'grade': 88.0, 'major': 'CS'},
        {'id': 6, 'name': 'Frank', 'age': 20, 'grade': 75.0, 'major': 'Math'}
    ]
    
    # 课程数据
    courses_data = [
        {'course_id': 101, 'student_id': 1, 'course_name': 'Database', 'score': 90.0},
        {'course_id': 102, 'student_id': 1, 'course_name': 'Algorithms', 'score': 85.0},
        {'course_id': 103, 'student_id': 2, 'course_name': 'Calculus', 'score': 95.0},
        {'course_id': 104, 'student_id': 3, 'course_name': 'Database', 'score': 82.0},
        {'course_id': 105, 'student_id': 4, 'course_name': 'Physics', 'score': 98.0}
    ]
    
    try:
        # 清空现有数据并插入新数据
        for student in students_data:
            try:
                storage.insert("students", student)
            except:
                pass  # 可能重复插入
        
        for course in courses_data:
            try:
                storage.insert("courses", course)
            except:
                pass  # 可能重复插入
    except Exception as e:
        st.error(f"插入数据时出错: {e}")

def display_tokens(tokens):
    """显示Token分析结果"""
    st.subheader("🔤 词法分析结果")
    
    token_data = []
    for i, token in enumerate(tokens):
        if token.type.name != 'EOF':
            token_data.append({
                '序号': i + 1,
                'Token类型': token.type.name,
                'Token值': token.value,
                '行号': token.line,
                '列号': token.column
            })
    
    if token_data:
        df = pd.DataFrame(token_data)
        st.dataframe(df, use_container_width=True)
        st.success(f"✅ 成功识别 {len(token_data)} 个Token")
    else:
        st.warning("⚠️ 未识别到有效Token")

def display_ast(ast):
    """显示AST结构"""
    st.subheader("🌳 语法分析结果 (AST)")
    
    def ast_to_dict(node, depth=0):
        """将AST节点转换为字典格式"""
        result = {
            'type': node.type.value,
            'value': node.value,
            'depth': depth,
            'children': []
        }
        
        for child in node.children:
            result['children'].append(ast_to_dict(child, depth + 1))
        
        return result
    
    if ast:
        ast_dict = ast_to_dict(ast)
        
        # 创建AST的文本表示
        def format_ast(node_dict, indent=0):
            lines = []
            indent_str = "  " * indent
            lines.append(f"{indent_str}{node_dict['type']}: {node_dict.get('value', '')}")
            
            for child in node_dict['children']:
                lines.extend(format_ast(child, indent + 1))
            
            return lines
        
        ast_text = "\n".join(format_ast(ast_dict))
        st.code(ast_text, language="text")
        st.success("✅ 语法分析成功")
    else:
        st.error("❌ 语法分析失败")

def display_quadruples(quadruples):
    """显示四元式"""
    st.subheader("🔄 语义分析结果 (四元式)")
    
    if quadruples:
        quad_data = []
        for i, quad in enumerate(quadruples):
            # 处理可能为列表的操作数
            arg1_display = quad.arg1 if not isinstance(quad.arg1, list) else str(quad.arg1)
            arg2_display = quad.arg2 if not isinstance(quad.arg2, list) else str(quad.arg2)
            
            quad_data.append({
                '序号': i + 1,
                '操作': quad.op,
                '操作数1': arg1_display or '-',
                '操作数2': arg2_display or '-',
                '结果': quad.result or '-'
            })
        
        df = pd.DataFrame(quad_data)
        st.dataframe(df, use_container_width=True)
        st.success(f"✅ 生成 {len(quadruples)} 个四元式")
    else:
        st.warning("⚠️ 未生成四元式")

def display_instructions(instructions):
    """显示目标指令"""
    st.subheader("⚙️ 目标代码生成结果")
    
    if instructions:
        inst_data = []
        for i, inst in enumerate(instructions):
            # 处理可能为列表的操作数
            operands_display = ' '.join(str(op) for op in inst.operands) if inst.operands and not isinstance(inst.operands, list) else str(inst.operands) if isinstance(inst.operands, list) else '-'
            
            inst_data.append({
                '序号': i + 1,
                '指令': inst.op.value,
                '操作数': operands_display,
                '结果': inst.result or '-',
                '注释': inst.comment or '-'
            })
        
        df = pd.DataFrame(inst_data)
        st.dataframe(df, use_container_width=True)
        st.success(f"✅ 生成 {len(instructions)} 条目标指令")
    else:
        st.info("ℹ️ DDL/DML操作无需生成目标指令")

def display_results(results):
    """显示查询结果"""
    st.subheader("📊 查询执行结果")
    
    if results:
        try:
            # 检查是否为DDL/DML操作的结果
            if isinstance(results, list) and len(results) > 0 and isinstance(results[0], dict):
                # DDL/DML操作结果
                df = pd.DataFrame(results)
                st.dataframe(df, use_container_width=True)
                st.success(f"✅ 操作成功完成")
            else:
                # SELECT查询结果
                df = pd.DataFrame(results)
                st.dataframe(df, use_container_width=True)
                st.success(f"✅ 查询成功，返回 {len(results)} 条记录")
        except Exception as e:
            # 如果DataFrame创建失败，以文本形式显示结果
            st.write("查询结果:")
            st.write(results)
            st.success(f"✅ 查询成功，返回 {len(results) if isinstance(results, list) else 1} 条记录")
    else:
        st.info("ℹ️ 操作成功完成，无返回数据")

def is_complex_query(sql: str) -> bool:
    """检测是否为复杂查询"""
    complex_keywords = [
        'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL',
        'COUNT', 'SUM', 'AVG', 'MAX', 'MIN',
        'GROUP', 'ORDER', 'HAVING', 'ASC', 'DESC',
        'LIMIT', 'OFFSET'
    ]
    
    sql_upper = sql.upper()
    for keyword in complex_keywords:
        if keyword in sql_upper:
            return True
    return False

def save_database_state(storage):
    """保存数据库状态"""
    try:
        # 刷新所有脏页到磁盘
        storage.flush_all()
        st.success("✅ 数据库状态已保存")
    except Exception as e:
        st.error(f"保存数据库状态时出错: {e}")

def load_database_state():
    """加载数据库状态"""
    try:
        # 重新初始化存储引擎会自动加载现有数据
        storage = init_storage()
        st.success("✅ 数据库状态已加载")
        return storage
    except Exception as e:
        st.error(f"加载数据库状态时出错: {e}")
        return None

def display_persistent_data_info(storage):
    """显示数据持久化信息"""
    st.subheader("💾 数据持久化信息")
    
    try:
        # 显示数据目录信息
        data_dir = storage.data_dir
        st.write(f"**数据目录**: {data_dir}")
        
        # 显示表信息
        tables = storage.list_tables()
        if tables:
            st.write("**数据表**:")
            # 使用selectbox来选择要查看的表，避免嵌套expander
            selected_table = st.selectbox("选择表查看详细信息:", [""] + tables, key="table_selector")
            
            # 显示所有表的基本信息
            table_data = []
            for table_name in tables:
                try:
                    table_info = storage.get_table_info(table_name)
                    if table_info:
                        table_data.append({
                            '表名': table_name,
                            '记录数': table_info.get('record_count', 0),
                            '页数': len(table_info.get('pages', [])),
                            '主键': table_info.get('primary_key', '无')
                        })
                    else:
                        table_data.append({
                            '表名': table_name,
                            '记录数': '无法获取',
                            '页数': '无法获取',
                            '主键': '无法获取'
                        })
                except Exception as e:
                    table_data.append({
                        '表名': table_name,
                        '记录数': f'错误: {e}',
                        '页数': f'错误: {e}',
                        '主键': f'错误: {e}'
                    })
            
            if table_data:
                df_tables = pd.DataFrame(table_data)
                st.dataframe(df_tables, use_container_width=True)
            
            # 如果选择了表，显示该表的详细信息
            if selected_table and selected_table in tables:
                st.markdown("---")
                st.write(f"**表 '{selected_table}' 的详细信息**:")
                try:
                    table_info = storage.get_table_info(selected_table)
                    if table_info:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.write(f"**记录数**: {table_info.get('record_count', 0)}")
                        with col2:
                            st.write(f"**页数**: {len(table_info.get('pages', []))}")
                        with col3:
                            st.write(f"**主键**: {table_info.get('primary_key', '无')}")
                        
                        # 显示列信息
                        st.write("**列信息**:")
                        columns_data = []
                        for col in table_info.get('columns', []):
                            columns_data.append({
                                '列名': col['name'],
                                '类型': col['column_type'],
                                '主键': '是' if col.get('is_primary_key', False) else '否',
                                '唯一': '是' if col.get('is_unique', False) else '否',
                                '可空': '是' if col.get('nullable', True) else '否'
                            })
                        
                        if columns_data:
                            df_cols = pd.DataFrame(columns_data)
                            st.dataframe(df_cols, use_container_width=True)
                        
                        # 显示表数据
                        st.write("**表数据**:")
                        try:
                            # 获取表的所有数据
                            table_data = storage.select(selected_table)
                            if table_data:
                                df_data = pd.DataFrame(table_data)
                                st.dataframe(df_data, use_container_width=True)
                            else:
                                st.info("表中暂无数据")
                        except Exception as e:
                            st.error(f"获取表数据时出错: {e}")
                    else:
                        st.error("无法获取表信息")
                except Exception as e:
                    st.error(f"获取表详细信息时出错: {e}")
        else:
            st.write("暂无数据表")
            
        # 显示索引信息
        if hasattr(storage, 'index_manager') and storage.index_manager:
            indexes = storage.index_manager.list_indexes()
            if indexes:
                st.write("**索引**:")
                index_data = []
                for index_name in indexes:
                    try:
                        index = storage.index_manager.get_index(index_name)
                        if index:
                            index_data.append({
                                '索引名': index_name,
                                '表名': index.table_name,
                                '列': ', '.join(index.columns),
                                '唯一性': '是' if index.is_unique else '否'
                            })
                        else:
                            index_data.append({
                                '索引名': index_name,
                                '表名': '无法获取',
                                '列': '无法获取',
                                '唯一性': '无法获取'
                            })
                    except Exception as e:
                        index_data.append({
                            '索引名': index_name,
                            '表名': f'错误: {e}',
                            '列': f'错误: {e}',
                            '唯一性': f'错误: {e}'
                        })
                
                if index_data:
                    df_indexes = pd.DataFrame(index_data)
                    st.dataframe(df_indexes, use_container_width=True)
            else:
                st.write("暂无索引")
        else:
            st.write("索引管理器未初始化")
            
    except Exception as e:
        st.error(f"显示数据持久化信息时出错: {e}")

def main():
    """主界面"""
    st.title("🗃️ 数据库系统测试平台")
    st.markdown("可视化展示SQL查询从词法分析到执行的完整过程")
    
    # 初始化
    storage = init_storage()
    insert_test_data(storage)
    
    # 侧边栏 - 示例SQL
    st.sidebar.header("📝 SQL示例")
    example_queries = {
        "基础查询": "SELECT * FROM students;",
        "列投影": "SELECT name, age FROM students;",
        "WHERE条件": "SELECT * FROM students WHERE age > 20;",
        "成绩筛选": "SELECT name, grade FROM students WHERE grade >= 90;",
        "专业筛选": "SELECT name FROM students WHERE major = 'CS';",
        "课程查询": "SELECT * FROM courses;",
        "COUNT聚合": "SELECT COUNT(*) FROM students;",
        "AVG聚合": "SELECT AVG(grade) FROM students;",
        "SUM聚合": "SELECT SUM(grade) FROM students;",
        "MAX聚合": "SELECT MAX(grade) FROM students;",
        "MIN聚合": "SELECT MIN(grade) FROM students;",
        "复杂聚合": "SELECT COUNT(*), AVG(grade), MAX(grade), MIN(grade), SUM(grade) FROM students;",
        "创建表": "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, price DECIMAL(10,2));",
        "添加列": "ALTER TABLE students ADD COLUMN email VARCHAR(100);",
        "创建索引": "CREATE INDEX idx_student_name ON students (name);",
        "创建复合索引": "CREATE INDEX idx_student_name_age ON students (name, age);",
        "创建唯一索引": "CREATE UNIQUE INDEX idx_student_id ON students (id);",
        # 复杂查询示例
        "ORDER BY查询": "SELECT name, age FROM students ORDER BY age DESC;",
        "GROUP BY查询": "SELECT major, COUNT(*) FROM students GROUP BY major;",
        "LIMIT查询": "SELECT name, grade FROM students ORDER BY grade DESC LIMIT 3;",
        "复合查询": "SELECT major, COUNT(*) as student_count, AVG(grade) as avg_grade FROM students GROUP BY major ORDER BY avg_grade DESC;",
        # JOIN查询示例
        "INNER JOIN": "SELECT s.name, c.course_name, c.score FROM students s INNER JOIN courses c ON s.id = c.student_id;",
        "LEFT JOIN": "SELECT s.name, c.course_name FROM students s LEFT JOIN courses c ON s.id = c.student_id;",
        "JOIN带条件": "SELECT s.name, c.course_name, c.score FROM students s JOIN courses c ON s.id = c.student_id WHERE c.score > 85;",
        "JOIN多条件": "SELECT s.name, s.major, c.course_name, c.score FROM students s JOIN courses c ON s.id = c.student_id WHERE s.major = 'CS';"
    }
    
    selected_example = st.sidebar.selectbox("选择示例SQL", list(example_queries.keys()))
    if st.sidebar.button("使用此示例"):
        st.session_state.sql_input = example_queries[selected_example]
    
    # 显示表结构
    with st.sidebar.expander("📋 数据表结构"):
        st.write("**students表:**")
        st.code("id (INTEGER), name (STRING), age (INTEGER), grade (FLOAT), major (STRING)")
        st.write("**courses表:**")
        st.code("course_id (INTEGER), student_id (INTEGER), course_name (STRING), score (FLOAT)")
    
    # 显示索引信息
    with st.sidebar.expander("🔍 索引信息"):
        try:
            # 获取存储引擎中的索引信息
            if hasattr(storage, 'index_manager') and storage.index_manager:
                indexes = storage.index_manager.list_indexes()
                if indexes:
                    st.write("**现有索引:**")
                    for index_name in indexes:
                        index = storage.index_manager.get_index(index_name)
                        if index:
                            st.write(f"- {index_name}: {index.table_name}({', '.join(index.columns)})")
                            st.write(f"  唯一性: {'是' if index.is_unique else '否'}")
                            st.write(f"  阶数: {index.order}")
                else:
                    st.write("暂无索引")
            else:
                st.write("索引管理器未初始化")
        except Exception as e:
            st.write(f"无法获取索引信息: {e}")
    
    # 数据库管理功能
    with st.sidebar.expander("💾 数据库管理"):
        col1, col2 = st.columns(2)
        with col1:
            if st.button("保存状态"):
                save_database_state(storage)
        with col2:
            if st.button("重新加载"):
                storage = load_database_state()
                st.experimental_rerun()
    
    # 数据持久化信息
    with st.sidebar.expander("🗄️ 数据持久化"):
        display_persistent_data_info(storage)
    
    # 主界面输入
    sql_input = st.text_area(
        "请输入SQL查询语句:", 
        value=st.session_state.get('sql_input', 'SELECT * FROM students;'),
        height=100
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        execute_button = st.button("🚀 执行查询", type="primary")
    with col2:
        show_details = st.checkbox("显示详细执行过程", value=True)
    
    if execute_button and sql_input.strip():
        try:
            # 检测是否为复杂查询
            is_complex = is_complex_query(sql_input)
            
            if show_details:
                st.markdown("---")
                st.header("📋 执行过程详情")
                
                # 1. 词法分析
                with st.container():
                    try:
                        lexer = Lexer(sql_input)
                        tokens = lexer.tokenize()
                        display_tokens(tokens)
                    except Exception as e:
                        st.error(f"词法分析失败: {e}")
                        return
                
                # 2. 语法分析
                with st.container():
                    try:
                        # 使用统一解析器
                        unified_parser = UnifiedSQLParser(sql_input)
                        ast, sql_type = unified_parser.parse()
                        display_ast(ast)
                        st.info(f"🔍 检测到SQL类型: {sql_type}")
                    except Exception as e:
                        st.error(f"语法分析失败: {e}")
                        return
                
                # 3. 语义分析
                with st.container():
                    try:
                        # 根据SQL类型和复杂性选择语义分析器
                        if sql_type == "SELECT":
                            # SELECT查询根据复杂性选择分析器
                            if is_complex:
                                analyzer = ExtendedSemanticAnalyzer()
                                st.info("🔍 使用扩展语义分析器处理复杂查询")
                            else:
                                analyzer = SemanticAnalyzer()
                                st.info("🔍 使用基础语义分析器处理简单查询")
                        else:
                            # DDL/DML使用新的分析器
                            analyzer = DDLDMLSemanticAnalyzer()
                        
                        quadruples = analyzer.analyze(ast)
                        display_quadruples(quadruples)
                        
                        # 显示语义错误（如果有）
                        if hasattr(analyzer, 'get_errors'):
                            errors = analyzer.get_errors()
                            if errors:
                                st.warning(f"⚠️ 语义警告: {'；'.join(errors)}")
                    except Exception as e:
                        st.error(f"语义分析失败: {e}")
                        return
                
                # 4. 目标代码生成（仅SELECT查询）
                instructions = []
                if sql_type == "SELECT":
                    with st.container():
                        try:
                            # 根据复杂性选择代码生成器
                            if is_complex:
                                translator = IntegratedCodeGenerator()
                                st.info("🔍 使用集成代码生成器处理复杂查询")
                            else:
                                translator = QuadrupleTranslator()
                                st.info("🔍 使用基础代码生成器处理简单查询")
                            
                            instructions = translator.generate_target_code(quadruples)
                            display_instructions(instructions)
                        except Exception as e:
                            st.error(f"目标代码生成失败: {e}")
                            return
                
                # 5. 执行结果
                st.markdown("---")
            
            # 执行查询
            st.header("🎯 最终查询结果")
            processor = SQLProcessor(storage)  # 确保使用正确的存储引擎
            success, results, error = processor.process_sql(sql_input)
            
            if success:
                display_results(results)
                
                # 执行统计
                stats = processor.get_stats()
                execution_stats = stats['execution_stats']
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("执行指令数", execution_stats.get('instructions_executed', 0))
                with col2:
                    st.metric("扫描记录数", execution_stats.get('records_scanned', 0))
                with col3:
                    st.metric("过滤记录数", execution_stats.get('records_filtered', 0))
                with col4:
                    st.metric("输出记录数", execution_stats.get('records_output', 0))
                
            else:
                st.error(f"❌ 查询执行失败: {error}")
                
        except Exception as e:
            st.error(f"系统错误: {e}")
            if st.checkbox("显示详细错误信息"):
                st.code(traceback.format_exc())
    
    # 页脚信息
    st.markdown("---")
    st.markdown(
        """
        ### 📚 功能说明
        - **词法分析**: 将SQL语句分解为Token
        - **语法分析**: 构建抽象语法树(AST)
        - **语义分析**: 生成四元式中间代码
        - **目标代码生成**: 转换为可执行指令
        - **执行引擎**: 执行指令并返回结果
        
        ### 🎯 支持的SQL功能
        - ✅ 基础SELECT查询
        - ✅ WHERE条件查询 (>, >=, <, <=, =, !=)
        - ✅ 多列投影和单列投影
        - ✅ DDL语句 (CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE INDEX)
        - ✅ DML语句 (INSERT INTO, UPDATE, DELETE FROM)
        - ✅ 聚合函数 (COUNT, AVG, SUM, MAX, MIN)
        - ✅ 排序查询 (ORDER BY)
        - ✅ 分组查询 (GROUP BY)
        - ✅ LIMIT/OFFSET查询
        - ✅ JOIN查询 (INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN)
        - ✅ 索引支持 (B+树索引、复合索引、唯一索引)
        
        ### 🌳 B+树索引特性
        - ✅ 支持复合键索引 (多列组合索引)
        - ✅ 支持唯一性约束
        - ✅ 高效的等值查询和范围查询
        - ✅ 完整的插入、删除操作
        
        ### 🔧 管理工具
        - [🔍 B+树索引管理器](./streamlit_index_manager.py) - 专门的索引管理界面
        - [🗄️ 数据库管理系统](./streamlit_database_manager.py) - 数据库状态监控界面
        - [📊 项目仪表板](./streamlit_project_dashboard.py) - 项目整体架构展示
        
        ### 📊 快速导航
        - [返回主页](./streamlit_app.py)
        - [查看索引](./streamlit_index_manager.py)
        - [管理数据库](./streamlit_database_manager.py)
        - [项目概览](./streamlit_project_dashboard.py)
        """
    )

if __name__ == "__main__":
    # 初始化处理器
    storage = init_storage()
    main()