#!/usr/bin/env python3
"""
数据库管理系统主入口
"""

import sys
import os
from pathlib import Path

# 添加当前目录到Python路径
sys.path.insert(0, str(Path(__file__).parent))

# 导入编译器组件
from src.compiler.lexer.lexer import Lexer
from src.compiler.parser.parser import Parser
from src.compiler.semantic.code_generator import EnhancedSemanticAnalyzer
from src.compiler.codegen.translator import IntegratedCodeGenerator
from src.common.types import LexicalError, SyntaxError, SemanticError

def print_banner():
    """打印系统启动横幅"""
    print("=" * 60)
    print("    大型平台软件设计实习 - 数据库管理系统")
    print("    Database Management System Implementation")
    print("=" * 60)
    print()

def show_help():
    """显示使用帮助"""
    print("使用方法:")
    print("  python main.py <sql_file>         # 执行SQL文件")
    print("  python main.py -i                # 交互模式")
    print("  python main.py -h, --help        # 显示帮助")
    print("  python main.py --test             # 运行测试")
    print()
    print("示例:")
    print("  python main.py examples/sample.sql")
    print("  python main.py -i")

def process_sql_query(sql: str) -> bool:
    """处理单个SQL查询"""
    print(f"\n正在处理SQL: {sql}")
    print("-" * 60)
    
    try:
        # 步骤 1: 词法分析
        print("\n步骤 1: 词法分析")
        lexer = Lexer(sql)
        tokens = lexer.tokenize()
        
        print(f"识别了 {len(tokens)} 个Token:")
        for i, token in enumerate(tokens):
            if token.type.value != 'EOF':
                print(f"  {i+1:2d}: {token.type.value:<15} '{token.value}' at {token.line}:{token.column}")
        
        # 输出四元式格式
        print("\n四元式格式 [type, value, line, column]:")
        tuples = lexer.get_token_tuples()
        for i, (token_type, value, line, col) in enumerate(tuples):
            if token_type != 'EOF':
                print(f"  {i+1:2d}: [{token_type}, {repr(value)}, {line}, {col}]")
        
        # 步骤 2: 语法分析
        print("\n步骤 2: 语法分析")
        parser = Parser(tokens)
        ast = parser.parse()
        
        if ast:
            print("✅ 语法分析成功！")
            print("\n抽象语法树:")
            print("-" * 40)
            print(ast)
        else:
            print("❌ 语法分析失败")
            return False
        
        # 步骤 3: 语义分析
        print("\n步骤 3: 语义分析")
        analyzer = EnhancedSemanticAnalyzer()
        semantic_success = analyzer.analyze_with_tokens(ast, tokens)
        
        if semantic_success:
            print("✅ 语义分析成功！")
            
            # 获取中间代码
            intermediate_code = analyzer.get_quadruples()
            
            # 打印中间代码
            analyzer.print_results()
        else:
            print("❌ 语义分析失败")
            return False
        
        # 步骤 4: 目标代码生成
        print("\n步骤 4: 目标代码生成")
        code_generator = IntegratedCodeGenerator()
        target_instructions = code_generator.generate_target_code(intermediate_code)
        
        if target_instructions:
            print("✅ 目标代码生成成功！")
            
            # 代码优化
            optimized_instructions = code_generator.optimize_target_code(target_instructions)
            
            print(f"\n最终生成了 {len(optimized_instructions)} 条目标指令")
        else:
            print("❌ 目标代码生成失败")
            return False
        
        print("\n✅ 完整的SQL编译流程成功！")
        print("✨ 已完成: 词法分析 → 语法分析 → 语义分析 → 目标代码生成")
        return True
        
    except LexicalError as e:
        print(f"\n❌ 词法错误: {e}")
        return False
    except SyntaxError as e:
        print(f"\n❌ 语法错误: {e}")
        return False
    except SemanticError as e:
        print(f"\n❌ 语义错误: {e}")
        return False
    except Exception as e:
        print(f"\n❌ 未知错误: {e}")
        return False

def interactive_mode():
    """交互模式"""
    print("进入交互模式 (输入 'quit' 或 'exit' 退出)")
    print("当前支持的SQL语法: SELECT column_list FROM table_name WHERE condition;")
    print()
    
    while True:
        try:
            sql = input("SQL> ").strip()
            if sql.lower() in ['quit', 'exit']:
                print("再见!")
                break
            elif sql == '':
                continue
            elif sql.lower() == 'help':
                print("支持的SQL语法:")
                print("  SELECT column_list FROM table_name WHERE condition;")
                print("  示例: SELECT name, age FROM students WHERE age > 18;")
                continue
            
            # 使用新的SQL处理函数
            process_sql_query(sql)
            
        except KeyboardInterrupt:
            print("\n再见!")
            break
        except Exception as e:
            print(f"错误: {e}")

def process_sql_file(file_path):
    """处理SQL文件"""
    if not os.path.exists(file_path):
        print(f"错误: 文件 '{file_path}' 不存在")
        return False
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print(f"正在处理文件: {file_path}")
        print("文件内容:")
        print("-" * 40)
        print(content)
        print("-" * 40)
        
        # 解析和处理每个SQL语句
        # 先删除注释
        lines = content.split('\n')
        clean_lines = []
        for line in lines:
            # 删除注释
            comment_pos = line.find('--')
            if comment_pos >= 0:
                line = line[:comment_pos]
            if line.strip():
                clean_lines.append(line.strip())
        
        clean_content = ' '.join(clean_lines)
        sql_statements = [stmt.strip() for stmt in clean_content.split(';') if stmt.strip()]
        
        print(f"\n检测到 {len(sql_statements)} 个SQL语句")
        
        success_count = 0
        for i, sql in enumerate(sql_statements, 1):
            if sql:
                print(f"\n\n{'='*80}")
                print(f"正在处理第 {i} 个SQL语句")
                print(f"{'='*80}")
                if process_sql_query(sql + ';'):
                    success_count += 1
        
        print(f"\n\n{'='*80}")
        print(f"文件处理完成: {success_count}/{len(sql_statements)} 个SQL语句成功处理")
        print(f"{'='*80}")
        
        return success_count == len(sql_statements)
        
    except Exception as e:
        print(f"错误: 读取文件失败 - {e}")
        return False

def run_tests():
    """运行测试"""
    print("运行系统测试...")
    print("[提示] 测试模块尚未实现，请等待后续开发...")

def main():
    """主函数"""
    print_banner()
    
    if len(sys.argv) == 1:
        # 没有参数，显示帮助
        show_help()
    elif len(sys.argv) == 2:
        arg = sys.argv[1]
        if arg in ['-h', '--help']:
            show_help()
        elif arg == '-i':
            interactive_mode()
        elif arg == '--test':
            run_tests()
        else:
            # 处理SQL文件
            process_sql_file(arg)
    else:
        print("错误: 参数过多")
        show_help()
        sys.exit(1)

if __name__ == "__main__":
    main()