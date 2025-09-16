# SQL功能测试用例完整指南

本文档提供了现代化数据库管理系统支持的所有SQL功能的测试用例，按功能分类，从基础到高级，帮助您全面测试系统能力。

## 🗂️ 测试目录

1. [📋 DDL（数据定义语言）测试](#ddl-数据定义语言测试)
2. [📝 DML（数据操作语言）测试](#dml-数据操作语言测试)
3. [🔍 查询功能测试](#查询功能测试)
4. [🌳 B+树索引测试](#b树索引测试)
5. [⚡ 复杂查询测试](#复杂查询测试)
6. [🎯 约束和验证测试](#约束和验证测试)
7. [📊 性能测试用例](#性能测试用例)

---

## 📋 DDL（数据定义语言）测试

### 1.1 基础表创建测试

#### 创建简单整数表
```sql
CREATE TABLE simple_numbers (
    id INTEGER PRIMARY KEY,
    value INTEGER
);
```

#### 创建用户信息表
```sql
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    age INTEGER,
    salary FLOAT,
    is_active BOOLEAN DEFAULT true
);
```

#### 创建产品表
```sql
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price FLOAT,
    stock_quantity INTEGER,
    description TEXT,
    created_date DATE
);
```

### 1.2 各种数据类型测试

#### 数据类型全覆盖表
```sql
CREATE TABLE data_types_test (
    id INTEGER PRIMARY KEY,
    int_col INTEGER,
    float_col FLOAT,
    string_col VARCHAR(255),
    text_col TEXT,
    bool_col BOOLEAN,
    date_col DATE,
    timestamp_col TIMESTAMP
);
```

### 1.3 约束测试

#### 主键约束测试
```sql
CREATE TABLE pk_test (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50)
);
```

#### 唯一约束测试
```sql
CREATE TABLE unique_test (
    id INTEGER PRIMARY KEY,
    email VARCHAR(100) UNIQUE,
    username VARCHAR(50) UNIQUE
);
```

#### 非空约束测试
```sql
CREATE TABLE not_null_test (
    id INTEGER PRIMARY KEY,
    required_field VARCHAR(50) NOT NULL,
    optional_field VARCHAR(50)
);
```

#### 默认值测试
```sql
CREATE TABLE default_values_test (
    id INTEGER PRIMARY KEY,
    status VARCHAR(20) DEFAULT 'pending',
    score INTEGER DEFAULT 0,
    is_enabled BOOLEAN DEFAULT true
);
```

### 1.4 删除表测试

#### 删除单个表
```sql
DROP TABLE simple_numbers;
```

#### 删除多个表（依次执行）
```sql
DROP TABLE data_types_test;
DROP TABLE unique_test;
DROP TABLE not_null_test;
```

---

## 📝 DML（数据操作语言）测试

### 2.1 INSERT 插入测试

#### 基础插入测试
```sql
-- 先创建测试表
CREATE TABLE employees (
    emp_id INTEGER PRIMARY KEY,
    name VARCHAR(50),
    department VARCHAR(30),
    salary FLOAT,
    hire_date DATE
);

-- 插入完整记录
INSERT INTO employees VALUES (1, 'Alice Johnson', 'Engineering', 75000.0, '2023-01-15');
INSERT INTO employees VALUES (2, 'Bob Smith', 'Marketing', 65000.0, '2023-02-20');
INSERT INTO employees VALUES (3, 'Charlie Brown', 'Engineering', 80000.0, '2023-01-10');
INSERT INTO employees VALUES (4, 'Diana Prince', 'HR', 70000.0, '2023-03-05');
INSERT INTO employees VALUES (5, 'Eve Wilson', 'Finance', 72000.0, '2023-02-28');
```

#### 部分字段插入测试
```sql
-- 插入部分字段（测试默认值）
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(50),
    order_status VARCHAR(20) DEFAULT 'pending',
    total_amount FLOAT
);

INSERT INTO orders (order_id, customer_name, total_amount) VALUES (101, 'John Doe', 299.99);
INSERT INTO orders (order_id, customer_name, total_amount) VALUES (102, 'Jane Smith', 459.50);
```

#### 批量插入测试
```sql
-- 创建学生表
CREATE TABLE students (
    student_id INTEGER PRIMARY KEY,
    name VARCHAR(50),
    major VARCHAR(30),
    gpa FLOAT,
    year INTEGER
);

-- 批量插入学生数据
INSERT INTO students VALUES (1, 'Alex Chen', 'Computer Science', 3.8, 3);
INSERT INTO students VALUES (2, 'Maria Garcia', 'Mathematics', 3.9, 2);
INSERT INTO students VALUES (3, 'James Wilson', 'Physics', 3.7, 4);
INSERT INTO students VALUES (4, 'Sarah Johnson', 'Chemistry', 3.6, 1);
INSERT INTO students VALUES (5, 'David Kim', 'Computer Science', 3.5, 2);
INSERT INTO students VALUES (6, 'Emily Davis', 'Biology', 3.8, 3);
INSERT INTO students VALUES (7, 'Michael Brown', 'Mathematics', 3.4, 1);
INSERT INTO students VALUES (8, 'Lisa Wang', 'Physics', 3.9, 4);
```

### 2.2 UPDATE 更新测试

#### 基础更新测试
```sql
-- 更新单个字段
UPDATE employees SET salary = 78000.0 WHERE emp_id = 1;

-- 更新多个字段
UPDATE employees SET salary = 85000.0, department = 'Senior Engineering' WHERE name = 'Charlie Brown';

-- 条件更新
UPDATE students SET gpa = 3.9 WHERE major = 'Computer Science' AND year = 3;
```

#### 批量更新测试
```sql
-- 给所有工程师加薪
UPDATE employees SET salary = salary * 1.1 WHERE department = 'Engineering';

-- 更新所有待处理订单状态
UPDATE orders SET order_status = 'processing' WHERE order_status = 'pending';
```

### 2.3 DELETE 删除测试

#### 条件删除测试
```sql
-- 删除特定记录
DELETE FROM students WHERE student_id = 7;

-- 根据条件删除
DELETE FROM employees WHERE department = 'Marketing' AND salary < 70000;

-- 删除低GPA学生
DELETE FROM students WHERE gpa < 3.5;
```

#### 批量删除测试
```sql
-- 删除所有一年级学生
DELETE FROM students WHERE year = 1;

-- 删除已完成的订单
DELETE FROM orders WHERE order_status = 'completed';
```

---

## 🔍 查询功能测试

### 3.1 基础查询测试

#### 全表查询
```sql
-- 查询所有员工
SELECT * FROM employees;

-- 查询所有学生
SELECT * FROM students;

-- 查询所有订单
SELECT * FROM orders;
```

#### 字段选择查询
```sql
-- 选择特定字段
SELECT name, salary FROM employees;

-- 多表字段选择
SELECT student_id, name, major FROM students;

-- 重命名字段查询
SELECT name AS employee_name, salary AS monthly_salary FROM employees;
```

### 3.2 条件查询测试

#### 等值条件查询
```sql
-- 单条件查询
SELECT * FROM employees WHERE department = 'Engineering';

-- 精确匹配
SELECT * FROM students WHERE major = 'Computer Science';

-- 数值匹配
SELECT * FROM students WHERE year = 3;
```

#### 比较操作查询
```sql
-- 大于条件
SELECT * FROM employees WHERE salary > 70000;

-- 小于条件
SELECT * FROM students WHERE gpa < 3.7;

-- 大于等于条件
SELECT * FROM employees WHERE salary >= 75000;

-- 小于等于条件
SELECT * FROM students WHERE year <= 2;

-- 不等于条件
SELECT * FROM employees WHERE department <> 'HR';
```

#### 范围查询测试
```sql
-- 薪资范围查询
SELECT * FROM employees WHERE salary > 70000 AND salary < 80000;

-- GPA范围查询
SELECT * FROM students WHERE gpa >= 3.5 AND gpa <= 3.8;

-- 年级范围查询
SELECT * FROM students WHERE year > 1 AND year < 4;
```

### 3.3 复合条件查询

#### AND 条件测试
```sql
-- 多条件AND查询
SELECT * FROM students WHERE major = 'Computer Science' AND year = 2;

-- 三个条件AND查询
SELECT * FROM students WHERE major = 'Mathematics' AND gpa > 3.5 AND year > 1;
```

#### OR 条件测试
```sql
-- OR条件查询
SELECT * FROM students WHERE major = 'Physics' OR major = 'Chemistry';

-- 复合OR条件
SELECT * FROM students WHERE year = 1 OR year = 4;
```

#### 复杂条件组合
```sql
-- AND和OR组合
SELECT * FROM students WHERE (major = 'Computer Science' OR major = 'Mathematics') AND gpa > 3.6;

-- 复杂条件组合
SELECT * FROM employees WHERE (department = 'Engineering' AND salary > 75000) OR (department = 'Finance' AND salary > 70000);
```

---

## 🌳 B+树索引测试

### 4.1 索引创建测试

#### 单列索引
```sql
-- 为员工表创建薪资索引
CREATE INDEX idx_employee_salary ON employees (salary);

-- 为学生表创建GPA索引
CREATE INDEX idx_student_gpa ON students (gpa);

-- 为产品表创建价格索引
CREATE INDEX idx_product_price ON products (price);
```

#### 复合索引
```sql
-- 创建专业和年级的复合索引
CREATE INDEX idx_student_major_year ON students (major, year);

-- 创建部门和薪资的复合索引
CREATE INDEX idx_employee_dept_salary ON employees (department, salary);
```

#### 唯一索引
```sql
-- 创建邮箱唯一索引
CREATE UNIQUE INDEX idx_employee_email ON employees (email);

-- 创建学生ID唯一索引
CREATE UNIQUE INDEX idx_student_id ON students (student_id);
```

### 4.2 索引查询优化测试

#### 等值查询优化测试
```sql
-- 这些查询应该使用索引
SELECT * FROM employees WHERE salary = 75000;
SELECT * FROM students WHERE gpa = 3.8;
SELECT * FROM students WHERE major = 'Computer Science';
```

#### 范围查询优化测试
```sql
-- B+树范围查询优势测试
SELECT * FROM employees WHERE salary BETWEEN 70000 AND 80000;
SELECT * FROM students WHERE gpa > 3.5 AND gpa < 3.9;
SELECT * FROM products WHERE price >= 100 AND price <= 500;
```

#### 复合索引查询测试
```sql
-- 利用复合索引的查询
SELECT * FROM students WHERE major = 'Computer Science' AND year = 3;
SELECT * FROM employees WHERE department = 'Engineering' AND salary > 75000;
```

### 4.3 索引性能测试

#### 大数据量插入测试
```sql
-- 创建大容量测试表
CREATE TABLE performance_test (
    id INTEGER PRIMARY KEY,
    indexed_field INTEGER,
    random_value FLOAT,
    text_field VARCHAR(100)
);

-- 创建索引
CREATE INDEX idx_perf_indexed_field ON performance_test (indexed_field);

-- 批量插入数据（模拟）
INSERT INTO performance_test VALUES (1, 100, 123.45, 'Test data 1');
INSERT INTO performance_test VALUES (2, 200, 234.56, 'Test data 2');
INSERT INTO performance_test VALUES (3, 150, 345.67, 'Test data 3');
-- ... 继续插入更多数据
INSERT INTO performance_test VALUES (100, 5000, 999.99, 'Test data 100');
```

#### 索引查询性能对比
```sql
-- 有索引的查询（应该更快）
SELECT * FROM performance_test WHERE indexed_field = 150;
SELECT * FROM performance_test WHERE indexed_field > 200 AND indexed_field < 400;

-- 无索引字段查询（用于对比）
SELECT * FROM performance_test WHERE random_value > 500.0;
```

---

## ⚡ 复杂查询测试

### 5.1 聚合函数测试

#### 基本聚合函数
```sql
-- COUNT 计数
SELECT COUNT(*) FROM employees;
SELECT COUNT(*) FROM students WHERE major = 'Computer Science';

-- SUM 求和
SELECT SUM(salary) FROM employees;
SELECT SUM(salary) FROM employees WHERE department = 'Engineering';

-- AVG 平均值
SELECT AVG(salary) FROM employees;
SELECT AVG(gpa) FROM students;

-- MAX 最大值
SELECT MAX(salary) FROM employees;
SELECT MAX(gpa) FROM students WHERE major = 'Mathematics';

-- MIN 最小值
SELECT MIN(salary) FROM employees;
SELECT MIN(gpa) FROM students WHERE year = 1;
```

### 5.2 GROUP BY 分组测试

#### 基础分组查询
```sql
-- 按部门分组统计员工数量
SELECT department, COUNT(*) FROM employees GROUP BY department;

-- 按专业分组统计平均GPA
SELECT major, AVG(gpa) FROM students GROUP BY major;

-- 按年级分组统计学生数量
SELECT year, COUNT(*) FROM students GROUP BY year;
```

#### 分组聚合查询
```sql
-- 每个部门的平均薪资
SELECT department, AVG(salary) FROM employees GROUP BY department;

-- 每个专业的最高GPA
SELECT major, MAX(gpa) FROM students GROUP BY major;

-- 每个年级的学生数量和平均GPA
SELECT year, COUNT(*), AVG(gpa) FROM students GROUP BY year;
```

### 5.3 ORDER BY 排序测试

#### 单字段排序
```sql
-- 按薪资升序排序
SELECT * FROM employees ORDER BY salary ASC;

-- 按GPA降序排序
SELECT * FROM students ORDER BY gpa DESC;

-- 按名字字母顺序排序
SELECT * FROM students ORDER BY name ASC;
```

#### 多字段排序
```sql
-- 先按专业，再按GPA排序
SELECT * FROM students ORDER BY major ASC, gpa DESC;

-- 先按部门，再按薪资排序
SELECT * FROM employees ORDER BY department ASC, salary DESC;
```

### 5.4 LIMIT 限制结果测试

#### 基础限制查询
```sql
-- 查询前5名员工
SELECT * FROM employees ORDER BY salary DESC LIMIT 5;

-- 查询GPA最高的3名学生
SELECT * FROM students ORDER BY gpa DESC LIMIT 3;

-- 查询薪资最低的2名员工
SELECT * FROM employees ORDER BY salary ASC LIMIT 2;
```

#### 分页查询测试
```sql
-- 分页查询：每页5条，第1页
SELECT * FROM students ORDER BY student_id LIMIT 5 OFFSET 0;

-- 分页查询：每页5条，第2页
SELECT * FROM students ORDER BY student_id LIMIT 5 OFFSET 5;

-- 分页查询：每页3条，第3页
SELECT * FROM employees ORDER BY emp_id LIMIT 3 OFFSET 6;
```

---

## 🎯 约束和验证测试

### 6.1 主键约束测试

#### 主键唯一性测试
```sql
-- 创建主键测试表
CREATE TABLE pk_constraint_test (
    id INTEGER PRIMARY KEY,
    value VARCHAR(50)
);

-- 正常插入
INSERT INTO pk_constraint_test VALUES (1, 'First');
INSERT INTO pk_constraint_test VALUES (2, 'Second');

-- 尝试插入重复主键（应该失败）
INSERT INTO pk_constraint_test VALUES (1, 'Duplicate');
```

### 6.2 非空约束测试

#### NOT NULL 测试
```sql
-- 创建非空约束测试表
CREATE TABLE not_null_constraint_test (
    id INTEGER PRIMARY KEY,
    required_field VARCHAR(50) NOT NULL,
    optional_field VARCHAR(50)
);

-- 正常插入
INSERT INTO not_null_constraint_test VALUES (1, 'Required Value', 'Optional');

-- 尝试插入NULL值到非空字段（应该失败）
INSERT INTO not_null_constraint_test (id, optional_field) VALUES (2, 'Only Optional');
```

### 6.3 默认值测试

#### DEFAULT 值测试
```sql
-- 创建默认值测试表
CREATE TABLE default_constraint_test (
    id INTEGER PRIMARY KEY,
    status VARCHAR(20) DEFAULT 'active',
    score INTEGER DEFAULT 0,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入使用默认值
INSERT INTO default_constraint_test (id) VALUES (1);

-- 插入覆盖默认值
INSERT INTO default_constraint_test VALUES (2, 'inactive', 100, '2023-12-01');
```

### 6.4 数据类型验证测试

#### 类型约束测试
```sql
-- 创建数据类型测试表
CREATE TABLE type_validation_test (
    id INTEGER PRIMARY KEY,
    int_field INTEGER,
    float_field FLOAT,
    string_field VARCHAR(10),
    bool_field BOOLEAN
);

-- 正确的数据类型插入
INSERT INTO type_validation_test VALUES (1, 123, 123.45, 'Valid', true);

-- 尝试插入错误的数据类型（系统应该处理或拒绝）
-- INSERT INTO type_validation_test VALUES (2, 'not_int', 'not_float', 'TooLongString', 'not_bool');
```

---

## 📊 性能测试用例

### 7.1 大数据量测试

#### 批量数据插入测试
```sql
-- 创建大容量测试表
CREATE TABLE large_data_test (
    id INTEGER PRIMARY KEY,
    category INTEGER,
    value FLOAT,
    description VARCHAR(100),
    status BOOLEAN
);

-- 批量插入测试数据
INSERT INTO large_data_test VALUES (1, 1, 100.5, 'Record 1', true);
INSERT INTO large_data_test VALUES (2, 1, 200.5, 'Record 2', false);
INSERT INTO large_data_test VALUES (3, 2, 150.3, 'Record 3', true);
INSERT INTO large_data_test VALUES (4, 2, 300.8, 'Record 4', true);
INSERT INTO large_data_test VALUES (5, 3, 250.2, 'Record 5', false);
-- ... 继续插入更多数据到1000条以测试性能
```

### 7.2 索引性能对比测试

#### 创建索引前后对比
```sql
-- 创建大数据表
CREATE TABLE index_performance_test (
    id INTEGER PRIMARY KEY,
    search_field INTEGER,
    data_field VARCHAR(100)
);

-- 插入大量测试数据
-- （这里需要插入足够多的数据来看出性能差异）

-- 无索引查询测试
SELECT * FROM index_performance_test WHERE search_field = 500;

-- 创建索引
CREATE INDEX idx_search_field ON index_performance_test (search_field);

-- 有索引查询测试（应该明显更快）
SELECT * FROM index_performance_test WHERE search_field = 500;
```

### 7.3 复杂查询性能测试

#### 多表关联查询测试
```sql
-- 复杂条件查询
SELECT e.name, e.salary, s.major, s.gpa
FROM employees e, students s
WHERE e.emp_id = s.student_id
  AND e.salary > 70000
  AND s.gpa > 3.5;

-- 分组聚合性能测试
SELECT department, COUNT(*), AVG(salary), MAX(salary), MIN(salary)
FROM employees
GROUP BY department
ORDER BY AVG(salary) DESC;
```

### 7.4 缓存性能测试

#### 缓存命中率测试
```sql
-- 重复查询测试（第二次应该从缓存读取）
SELECT * FROM employees WHERE salary > 75000;
SELECT * FROM employees WHERE salary > 75000;
SELECT * FROM employees WHERE salary > 75000;

-- 不同查询测试缓存替换
SELECT * FROM students WHERE major = 'Computer Science';
SELECT * FROM students WHERE gpa > 3.7;
SELECT * FROM products WHERE price < 100;
```

---

## 🚀 综合测试场景

### 完整的电商系统模拟

#### 创建电商相关表
```sql
-- 用户表
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    registration_date DATE
);

-- 商品表
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category_id INTEGER,
    price FLOAT,
    stock_quantity INTEGER,
    description TEXT
);

-- 订单表
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date DATE,
    total_amount FLOAT,
    status VARCHAR(20) DEFAULT 'pending'
);

-- 订单详情表
CREATE TABLE order_items (
    item_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price FLOAT
);
```

#### 插入测试数据
```sql
-- 插入客户数据
INSERT INTO customers VALUES (1, 'alice_chen', 'alice@email.com', '13800138000', '2023-01-15');
INSERT INTO customers VALUES (2, 'bob_wang', 'bob@email.com', '13800138001', '2023-02-20');
INSERT INTO customers VALUES (3, 'charlie_li', 'charlie@email.com', '13800138002', '2023-03-10');

-- 插入商品数据
INSERT INTO products VALUES (101, 'iPhone 15', 1, 6999.00, 50, 'Latest iPhone model');
INSERT INTO products VALUES (102, 'MacBook Pro', 1, 12999.00, 30, 'Professional laptop');
INSERT INTO products VALUES (103, 'AirPods Pro', 1, 1899.00, 100, 'Wireless earphones');

-- 插入订单数据
INSERT INTO orders VALUES (1001, 1, '2023-04-01', 8898.00, 'completed');
INSERT INTO orders VALUES (1002, 2, '2023-04-02', 12999.00, 'processing');
INSERT INTO orders VALUES (1003, 1, '2023-04-03', 1899.00, 'shipped');

-- 插入订单详情
INSERT INTO order_items VALUES (1, 1001, 101, 1, 6999.00);
INSERT INTO order_items VALUES (2, 1001, 103, 1, 1899.00);
INSERT INTO order_items VALUES (3, 1002, 102, 1, 12999.00);
INSERT INTO order_items VALUES (4, 1003, 103, 1, 1899.00);
```

#### 创建索引优化查询
```sql
-- 为常用查询字段创建索引
CREATE INDEX idx_orders_customer_id ON orders (customer_id);
CREATE INDEX idx_orders_date ON orders (order_date);
CREATE INDEX idx_products_category ON products (category_id);
CREATE INDEX idx_products_price ON products (price);
```

#### 复杂业务查询测试
```sql
-- 查询客户订单统计
SELECT c.username, COUNT(o.order_id) as order_count, SUM(o.total_amount) as total_spent
FROM customers c, orders o
WHERE c.customer_id = o.customer_id
GROUP BY c.customer_id, c.username
ORDER BY total_spent DESC;

-- 查询热销商品
SELECT p.product_name, SUM(oi.quantity) as total_sold, SUM(oi.quantity * oi.unit_price) as revenue
FROM products p, order_items oi
WHERE p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_sold DESC;

-- 查询客户购买历史
SELECT c.username, o.order_date, o.total_amount, o.status
FROM customers c, orders o
WHERE c.customer_id = o.customer_id AND c.username = 'alice_chen'
ORDER BY o.order_date DESC;
```

---

## 📋 测试执行指南

### 如何使用这些测试用例

1. **启动数据库管理系统**
   ```bash
   python start_database_manager.py
   ```

2. **按顺序执行测试**
   - 先执行DDL测试创建表结构
   - 然后执行DML测试插入数据
   - 接着执行查询测试验证功能
   - 最后执行性能测试

3. **监控系统性能**
   - 在"存储引擎"标签页查看缓存命中率
   - 在"性能监控"标签页查看执行统计
   - 观察查询响应时间

4. **验证结果**
   - 检查查询结果的正确性
   - 验证约束是否生效
   - 确认索引是否提升了查询性能

### 预期测试结果

✅ **成功指标**
- 所有DDL语句成功创建表和索引
- 数据插入操作成功完成
- 查询返回正确结果
- 索引查询比全表扫描更快
- 约束正确阻止非法数据

⚠️ **注意事项**
- 某些高级SQL功能可能仍在开发中
- 复杂JOIN操作可能需要特殊处理
- 大数据量测试需要足够的系统资源

---

## 🎯 测试建议

1. **循序渐进**: 从简单的单表操作开始，逐步测试复杂功能
2. **性能监控**: 始终关注系统性能指标，特别是缓存命中率
3. **错误处理**: 测试各种边界情况和错误输入
4. **功能验证**: 确保每个功能都符合SQL标准预期

通过这些全面的测试用例，您可以充分验证数据库管理系统的各项功能，并发现潜在的性能瓶颈和改进空间！🚀