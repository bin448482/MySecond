#!/usr/bin/env python3
"""
检查akshare可用的API
"""

import akshare as ak
import inspect

def check_available_apis():
    """检查akshare中可用的股票历史数据API"""
    print("🔍 检查akshare可用的股票历史数据API...")
    print("=" * 60)
    
    # 获取akshare模块中所有的函数
    all_functions = [name for name, obj in inspect.getmembers(ak) if inspect.isfunction(obj)]
    
    # 筛选股票历史数据相关的API
    stock_hist_apis = [func for func in all_functions if 'stock' in func and 'hist' in func]
    
    print("📋 找到的股票历史数据API:")
    for i, api in enumerate(stock_hist_apis, 1):
        print(f"   {i:2d}. {api}")
    
    print(f"\n总共找到 {len(stock_hist_apis)} 个相关API")
    
    # 测试几个常见的API
    test_apis = [
        'stock_zh_a_hist',           # 东方财富
        'stock_zh_a_daily',          # 可能的日线数据API
        'stock_individual_info_em',  # 个股信息
    ]
    
    print(f"\n🧪 测试常见API是否存在:")
    for api in test_apis:
        if hasattr(ak, api):
            print(f"   ✅ {api} - 存在")
        else:
            print(f"   ❌ {api} - 不存在")
    
    return stock_hist_apis

def test_working_api():
    """测试一个确实能工作的API"""
    print(f"\n🧪 测试基本的股票历史数据获取...")
    
    try:
        # 测试最基本的API
        print("尝试 ak.stock_zh_a_hist...")
        data = ak.stock_zh_a_hist(
            symbol="000001",
            period="daily", 
            start_date="20241201",
            end_date="20241210",
            adjust="qfq"
        )
        
        if not data.empty:
            print(f"✅ 成功获取数据: {len(data)} 条记录")
            print("数据列名:", list(data.columns))
            print("数据示例:")
            print(data.head(2).to_string())
            return True
        else:
            print("⚠️  获取到空数据")
            return False
            
    except Exception as e:
        print(f"❌ API调用失败: {e}")
        return False

def main():
    print("🚀 开始检查akshare API...")
    
    # 检查可用API
    apis = check_available_apis()
    
    # 测试基本API
    success = test_working_api()
    
    if success:
        print(f"\n✅ 基本API工作正常")
    else:
        print(f"\n❌ 基本API也无法工作，可能是网络问题")
    
    print(f"\n💡 建议:")
    print(f"   1. 如果基本API工作，问题在于我添加的新API不存在")
    print(f"   2. 如果基本API也不工作，是网络连接问题")
    print(f"   3. 可以尝试使用其他时间段的数据")

if __name__ == "__main__":
    main()