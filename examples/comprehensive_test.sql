-- 综合测试SQL文件
-- 测试所有已实现的功能

-- 1. 基础SELECT查询
SELECT * FROM students;

-- 2. 列投影查询
SELECT name, age FROM students;

-- 3. WHERE条件查询
SELECT * FROM students WHERE age > 20;

-- 4. 成绩筛选查询
SELECT name, grade FROM students WHERE grade >= 90;

-- 5. COUNT聚合函数
SELECT COUNT(*) FROM students;

-- 6. AVG聚合函数
SELECT AVG(grade) FROM students;

-- 7. SUM聚合函数
SELECT SUM(grade) FROM students;

-- 8. MAX聚合函数
SELECT MAX(grade) FROM students;

-- 9. MIN聚合函数
SELECT MIN(grade) FROM students;

-- 10. 复杂聚合函数组合
SELECT COUNT(*), AVG(grade), MAX(grade), MIN(grade), SUM(grade) FROM students;

-- 11. DDL语句 - 创建表
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2)
);

-- 12. DDL语句 - 添加列
ALTER TABLE students ADD COLUMN email VARCHAR(100);

-- 13. DDL语句 - 创建索引
CREATE INDEX idx_student_name ON students (name);

-- 14. DML语句 - 插入数据
INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 999.99);

-- 15. DML语句 - 更新数据
UPDATE students SET grade = 95.0 WHERE id = 1;

-- 16. DML语句 - 删除数据
DELETE FROM products WHERE id = 1;