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

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from src.compiler.lexer.lexer import Lexer
from src.compiler.parser.unified_parser import UnifiedSQLParser
from src.compiler.semantic.analyzer import SemanticAnalyzer
from src.compiler.semantic.extended_analyzer import ExtendedSemanticAnalyzer
from src.compiler.semantic.ddl_dml_analyzer import DDLDMLSemanticAnalyzer
from src.compiler.codegen.translator import QuadrupleTranslator
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
        "创建索引": "CREATE INDEX idx_student_name ON students (name);"
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
            processor = SQLProcessor(storage)
            
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
                        # 根据SQL类型选择语义分析器
                        if sql_type == "SELECT":
                            # SELECT查询使用扩展分析器来支持聚合函数
                            analyzer = ExtendedSemanticAnalyzer()
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
                            translator = QuadrupleTranslator()
                            instructions = translator.translate(quadruples)
                            display_instructions(instructions)
                        except Exception as e:
                            st.error(f"目标代码生成失败: {e}")
                            return
                
                # 5. 执行结果
                st.markdown("---")
            
            # 执行查询
            st.header("🎯 最终查询结果")
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
        - ❌ 排序查询 (ORDER BY)
        - ❌ 分组查询 (GROUP BY)
        """
    )

if __name__ == "__main__":
    main()