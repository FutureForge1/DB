@echo off
title 现代化数据库管理系统启动器
color 0B

echo ===============================================================
echo                   现代化数据库管理系统启动器
echo ===============================================================
echo.

echo 🔍 正在检查Python环境...

REM 检查Python是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ 错误: 未找到Python环境
    echo 请先安装Python 3.7或更高版本
    echo 下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo ✅ Python环境检查通过

echo.
echo 🚀 正在启动数据库管理系统...
echo 请稍候...

REM 启动Python应用
python start_database_manager.py

echo.
echo 程序已退出
pause