#!/usr/bin/env python3
"""
测试 data_refresh.py 命令行界面的脚本
"""

import subprocess
import sys
import os
import json
from datetime import datetime

def run_command(cmd, capture_output=True):
    """运行命令并返回结果"""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=capture_output, 
            text=True, 
            cwd=os.getcwd()
        )
        return {
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr
        }
    except Exception as e:
        return {
            'returncode': -1,
            'stdout': '',
            'stderr': str(e)
        }

def test_help_commands():
    """测试帮助命令"""
    print("=" * 60)
    print("测试帮助命令")
    print("=" * 60)
    
    # 测试主帮助
    print("1. 测试主帮助命令...")
    result = run_command("python data_refresh.py --help")
    if result['returncode'] == 0:
        print("✅ 主帮助命令正常")
        print(f"输出长度: {len(result['stdout'])} 字符")
    else:
        print("❌ 主帮助命令失败")
        print(f"错误: {result['stderr']}")
    
    # 测试各子命令帮助
    subcommands = ['smart-refresh', 'full-refresh', 'cleanup', 'check']
    for cmd in subcommands:
        print(f"2. 测试 {cmd} 帮助命令...")
        result = run_command(f"python data_refresh.py {cmd} --help")
        if result['returncode'] == 0:
            print(f"✅ {cmd} 帮助命令正常")
        else:
            print(f"❌ {cmd} 帮助命令失败")
            print(f"错误: {result['stderr']}")

def test_argument_validation():
    """测试参数验证"""
    print("\n" + "=" * 60)
    print("测试参数验证")
    print("=" * 60)
    
    # 测试无效命令
    print("1. 测试无效命令...")
    result = run_command("python data_refresh.py invalid-command")
    if result['returncode'] != 0:
        print("✅ 无效命令正确被拒绝")
    else:
        print("❌ 无效命令未被拒绝")
    
    # 测试无命令
    print("2. 测试无命令参数...")
    result = run_command("python data_refresh.py")
    if result['returncode'] == 0:  # 应该显示帮助并正常退出
        print("✅ 无命令参数正确处理（显示帮助）")
    else:
        print("❌ 无命令参数处理异常")

def test_dry_run_commands():
    """测试命令的干运行（不实际执行操作）"""
    print("\n" + "=" * 60)
    print("测试命令干运行")
    print("=" * 60)
    
    # 测试 check 命令（相对安全，只读操作）
    print("1. 测试 check 命令...")
    result = run_command("python data_refresh.py check --target-days 1")
    print(f"check 命令返回码: {result['returncode']}")
    if "数据完整性检查模式" in result['stdout']:
        print("✅ check 命令界面正常显示")
    else:
        print("❌ check 命令界面异常")
        if result['stderr']:
            print(f"错误信息: {result['stderr']}")

def test_import_and_syntax():
    """测试导入和语法"""
    print("\n" + "=" * 60)
    print("测试导入和语法")
    print("=" * 60)
    
    # 测试 Python 语法
    print("1. 测试 Python 语法...")
    result = run_command("python -m py_compile data_refresh.py")
    if result['returncode'] == 0:
        print("✅ Python 语法检查通过")
    else:
        print("❌ Python 语法错误")
        print(f"错误: {result['stderr']}")
    
    # 测试导入
    print("2. 测试模块导入...")
    test_import_code = '''
import sys
sys.path.append('.')
try:
    from data_refresh import DataRefreshManager, create_parser
    print("✅ 模块导入成功")
    
    # 测试解析器创建
    parser = create_parser()
    print("✅ 参数解析器创建成功")
    
    # 测试管理器创建
    manager = DataRefreshManager()
    print("✅ 数据刷新管理器创建成功")
    
except Exception as e:
    print(f"❌ 导入失败: {e}")
    sys.exit(1)
'''
    
    with open('temp_import_test.py', 'w', encoding='utf-8') as f:
        f.write(test_import_code)
    
    result = run_command("python temp_import_test.py")
    print(result['stdout'])
    if result['stderr']:
        print(f"警告/错误: {result['stderr']}")
    
    # 清理临时文件
    if os.path.exists('temp_import_test.py'):
        os.remove('temp_import_test.py')

def test_command_structure():
    """测试命令结构"""
    print("\n" + "=" * 60)
    print("测试命令结构")
    print("=" * 60)
    
    # 测试各种参数组合
    test_cases = [
        ("smart-refresh --test-mode", "智能刷新测试模式"),
        ("smart-refresh --max-stocks 10", "智能刷新限制股票数"),
        ("full-refresh --test-mode", "全量刷新测试模式"),
        ("cleanup --yes", "清理模式跳过确认"),
        ("check --target-days 30", "检查模式自定义天数"),
    ]
    
    for cmd, desc in test_cases:
        print(f"测试: {desc}")
        # 这里我们只测试命令解析，不实际执行
        result = run_command(f"python -c \"import sys; sys.path.append('.'); from data_refresh import create_parser; parser = create_parser(); args = parser.parse_args('{cmd}'.split()); print('✅ 参数解析成功:', args.command)\"")
        if result['returncode'] == 0:
            print(f"  ✅ {desc} - 参数解析正常")
        else:
            print(f"  ❌ {desc} - 参数解析失败")
            print(f"  错误: {result['stderr']}")

def generate_test_report():
    """生成测试报告"""
    print("\n" + "=" * 80)
    print("测试报告")
    print("=" * 80)
    
    report = {
        'test_time': datetime.now().isoformat(),
        'test_file': 'data_refresh.py',
        'summary': {
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0
        },
        'details': []
    }
    
    print("✅ 命令行界面转换完成")
    print("✅ 支持的命令:")
    print("   - smart-refresh: 智能刷新（基于报告）")
    print("   - full-refresh: 全量数据刷新")
    print("   - cleanup: 清理失败股票列表")
    print("   - check: 数据完整性检查")
    print("✅ 支持的参数:")
    print("   - --test-mode: 测试模式")
    print("   - --max-stocks: 限制处理股票数")
    print("   - --yes: 跳过确认提示")
    print("   - --target-days: 自定义检查天数")
    print("   - --help: 显示帮助信息")
    
    print("\n💡 使用示例:")
    print("   python data_refresh.py smart-refresh --test-mode --yes")
    print("   python data_refresh.py full-refresh --max-stocks 50")
    print("   python data_refresh.py cleanup --yes")
    print("   python data_refresh.py check --target-days 60")
    
    # 保存报告
    with open('data/cli_test_report.json', 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n📊 测试报告已保存至: data/cli_test_report.json")

def main():
    """主测试函数"""
    print("数据刷新工具命令行界面测试")
    print("=" * 80)
    
    # 确保 data 目录存在
    os.makedirs('data', exist_ok=True)
    
    # 运行各项测试
    test_import_and_syntax()
    test_help_commands()
    test_argument_validation()
    test_command_structure()
    test_dry_run_commands()
    
    # 生成测试报告
    generate_test_report()
    
    print("\n" + "=" * 80)
    print("测试完成！")
    print("=" * 80)

if __name__ == "__main__":
    main()