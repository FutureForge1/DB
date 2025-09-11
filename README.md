# 大型平台软件设计实习 - 数据库管理系统

这是一个完整的数据库管理系统实现，包含SQL编译器前端和后端执行引擎。

## 项目结构

```
DB/
├── src/                          # 源代码目录
│   ├── compiler/                 # SQL编译器前端
│   │   ├── lexer/               # 词法分析器
│   │   ├── parser/              # 语法分析器
│   │   ├── semantic/            # 语义分析器
│   │   └── codegen/             # 代码生成器
│   ├── storage/                 # 存储管理系统
│   │   ├── page/                # 页管理
│   │   ├── buffer/              # 缓存管理
│   │   └── table/               # 表结构管理
│   ├── execution/               # 执行引擎
│   └── common/                  # 公共模块
├── tests/                       # 测试文件
├── examples/                    # 示例SQL文件
└── docs/                        # 文档

```

## 系统功能

1. **SQL编译器前端**
   - 词法分析器：将SQL语句分解为Token
   - 语法分析器：使用LL(1)文法进行语法检查
   - 语义分析器：进行语义检查并生成中间代码
   - 代码生成器：将中间代码转换为目标代码

2. **后端存储系统**
   - 基于页的存储管理
   - 缓存管理器(Buffer Manager)
   - 表结构和记录管理

3. **执行引擎**
   - 目标代码解释执行
   - 查询操作执行(SCAN, FILTER, PROJECT等)

## 编译运行

```bash
# 编译项目
python -m src.main

# 运行示例
python -m src.main examples/sample.sql
```

## 支持的SQL语法

目前支持简化的SQL语法：
- SELECT column_list FROM table_name WHERE condition;
- 基本的条件表达式
- 常见的比较运算符

## 开发进度

- [x] 项目结构设计
- [ ] 词法分析器实现
- [ ] 语法分析器实现
- [ ] 语义分析器实现
- [ ] 代码生成器实现
- [ ] 存储系统实现
- [ ] 执行引擎实现
- [ ] 系统集成与测试