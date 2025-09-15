# 大型平台软件设计实习 - 数据库管理系统

这是一个完整的数据库管理系统实现，包含SQL编译器前端和后端执行引擎。

# 可以改进的空间
1. SQL语法支持扩展
~~复杂查询支持
JOIN操作：实现表连接（INNER JOIN, LEFT JOIN, RIGHT JOIN等）
子查询：支持嵌套查询
GROUP BY/HAVING：分组查询和分组过滤
ORDER BY：排序查询
LIMIT/OFFSET：结果集分页***~~
高级数据类型
日期时间类型：DATE, TIME, DATETIME, TIMESTAMP
二进制类型：BLOB
数组类型：支持数组字段
JSON类型：原生JSON支持
3. 查询优化器
查询计划优化
基于成本的优化：根据数据统计信息选择最优执行计划
索引选择：智能选择合适的索引
连接顺序优化：优化多表连接顺序
谓词下推：将过滤条件下推到存储层
执行计划缓存
计划缓存：缓存查询计划避免重复优化
自适应优化：根据执行反馈调整计划
4. 存储系统增强
索引系统
B+树索引：实现高效的范围查询索引
哈希索引：等值查询优化
复合索引：多列组合索引
全文索引：文本搜索支持
存储引擎优化
压缩存储：数据压缩减少存储空间
列式存储：针对分析型查询的列式存储
分区表：大表水平分区
物化视图：预计算结果存储
5. 事务和并发控制
ACID事务
原子性：事务回滚机制
一致性：约束检查和数据一致性保证
隔离性：多版本并发控制(MVCC)
持久性：WAL(Write-Ahead Logging)日志
并发控制
锁机制：行级锁、表级锁
死锁检测：自动死锁检测和解决
读写分离：主从复制架构
6. 系统管理和监控
管理功能
用户权限管理：用户认证和授权
角色管理：基于角色的访问控制
审计日志：操作审计跟踪
备份恢复：数据备份和恢复机制
性能监控
查询分析：慢查询分析和优化建议
资源监控：CPU、内存、IO监控
统计信息：表和索引统计信息收集
性能报告：定期性能报告生成
7. 网络和分布式支持
网络协议
SQL协议：标准SQL协议支持
连接池：客户端连接管理
SSL支持：安全连接
API接口：RESTful API支持
分布式能力
分片：数据水平分片
复制：主从复制、多主复制
一致性协议：Raft或Paxos协议实现
分布式事务：跨节点事务支持

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
│   │   ├── table/               # 表结构管理
│   │   └── index/               # 索引管理
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
   - B+树索引支持

3. **执行引擎**
   - 目标代码解释执行
   - 查询操作执行(SCAN, FILTER, PROJECT等)

## 编译运行

### 命令行运行
```bash
# 编译项目
python -m src.main

# 运行示例
python -m src.main examples/sample.sql
```

### Web界面运行
```
# 启动主数据库测试平台（包含数据持久化展示功能）
python -m streamlit run streamlit_app.py

# 访问地址: http://localhost:8501

# 启动Tomcat风格的集成管理器（推荐）
python start_db_manager.py

# 访问地址: http://localhost:8080

# 或启动传统的多页面管理器
python start_manager.py
```

## 支持的SQL语法

目前支持简化的SQL语法：
- SELECT column_list FROM table_name WHERE condition;
- 基本的条件表达式
- 常见的比较运算符
- DDL语句（CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE INDEX）
- DML语句（INSERT INTO, UPDATE, DELETE FROM）

## 使用说明

详细使用说明请参见 [SYSTEM_USAGE.md](SYSTEM_USAGE.md) 文件。

## 开发进度

- [x] 项目结构设计
- [x] 词法分析器实现
- [x] 语法分析器实现
- [x] 语义分析器实现
- [x] 代码生成器实现
- [x] 存储系统实现
- [x] 执行引擎实现
- [x] 系统集成与测试
- [x] Web界面实现
- [x] B+树索引实现