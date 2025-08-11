"""
系统测试脚本
用于验证短线选股工具的各个模块功能
"""

import sys
import traceback
from datetime import datetime

# 导入所有模块
from database import DatabaseManager
from data_fetcher import DataFetcher
from technical_indicators import TechnicalIndicators
from stock_selector import StockSelector
from output_manager import OutputManager
from utils import config_manager, logger


def test_database():
    """测试数据库模块"""
    print("=" * 50)
    print("测试数据库模块")
    print("=" * 50)
    
    try:
        db = DatabaseManager()
        stats = db.get_database_stats()
        
        print("✅ 数据库初始化成功")
        print("数据库统计:")
        for table, count in stats.items():
            print(f"  {table}: {count} 条记录")
        
        return True, db
        
    except Exception as e:
        print(f"❌ 数据库测试失败: {e}")
        traceback.print_exc()
        return False, None


def test_config():
    """测试配置模块"""
    print("\n" + "=" * 50)
    print("测试配置模块")
    print("=" * 50)
    
    try:
        # 测试配置读取
        db_path = config_manager.get('database.path')
        max_results = config_manager.get('output.max_results')
        markets = config_manager.get('filters.markets')
        
        print("✅ 配置文件读取成功")
        print(f"  数据库路径: {db_path}")
        print(f"  最大结果数: {max_results}")
        print(f"  允许市场: {markets}")
        
        return True
        
    except Exception as e:
        print(f"❌ 配置测试失败: {e}")
        traceback.print_exc()
        return False


def test_data_fetcher(db):
    """测试数据获取模块"""
    print("\n" + "=" * 50)
    print("测试数据获取模块")
    print("=" * 50)
    
    try:
        fetcher = DataFetcher(db)
        
        # 测试获取股票列表（少量）
        print("测试获取股票列表...")
        stock_list = fetcher.get_stock_list()
        
        if not stock_list.empty:
            print(f"✅ 获取股票列表成功，共 {len(stock_list)} 只股票")
            print("前5只股票:")
            for _, row in stock_list.head(5).iterrows():
                print(f"  {row.get('代码', 'N/A')} {row.get('名称', 'N/A')}")
        else:
            print("⚠️  股票列表为空，可能需要网络连接")
        
        return True, fetcher
        
    except Exception as e:
        print(f"❌ 数据获取测试失败: {e}")
        traceback.print_exc()
        return False, None


def test_technical_indicators(db):
    """测试技术指标模块"""
    print("\n" + "=" * 50)
    print("测试技术指标模块")
    print("=" * 50)
    
    try:
        tech = TechnicalIndicators(db)
        
        # 测试技术指标计算（使用模拟数据）
        import pandas as pd
        import numpy as np
        
        # 创建模拟价格数据
        dates = pd.date_range('2024-01-01', periods=60, freq='D')
        prices = 10 + np.cumsum(np.random.randn(60) * 0.1)
        close_prices = pd.Series(prices, index=dates)
        
        # 测试MACD计算
        macd_data = tech.calculate_macd(close_prices)
        print("✅ MACD计算成功")
        print(f"  MACD数据形状: {macd_data.shape}")
        
        # 测试RSI计算
        rsi_data = tech.calculate_rsi(close_prices)
        print("✅ RSI计算成功")
        print(f"  RSI最新值: {rsi_data.iloc[-1]:.2f}")
        
        # 测试均线计算
        ma_data = tech.calculate_moving_averages(close_prices)
        print("✅ 均线计算成功")
        print(f"  均线数据形状: {ma_data.shape}")
        
        return True, tech
        
    except Exception as e:
        print(f"❌ 技术指标测试失败: {e}")
        traceback.print_exc()
        return False, None


def test_stock_selector(db):
    """测试选股模块"""
    print("\n" + "=" * 50)
    print("测试选股模块")
    print("=" * 50)
    
    try:
        selector = StockSelector(db)
        
        # 测试获取股票基础数据
        stock_data = selector.get_stock_basic_data()
        print(f"✅ 获取股票基础数据成功，共 {len(stock_data)} 只股票")
        
        # 测试基础筛选（使用模拟数据）
        if not stock_data.empty:
            # 创建模拟数据进行筛选测试
            import pandas as pd
            test_data = pd.DataFrame({
                'symbol': ['000001', '000002', '600000'],
                'name': ['平安银行', '万科A', '浦发银行'],
                'market': ['深圳', '深圳', '上海'],
                'current_price': [15.5, 25.8, 12.3],
                'price_change': [2.5, 1.8, 3.2],
                'volume_ratio': [1.5, 2.1, 1.8],
                'turnover_rate': [2.5, 3.1, 2.8]
            })
            
            filtered_data = selector.apply_basic_filters(test_data)
            print(f"✅ 基础筛选测试成功，筛选后 {len(filtered_data)} 只股票")
        
        return True, selector
        
    except Exception as e:
        print(f"❌ 选股模块测试失败: {e}")
        traceback.print_exc()
        return False, None


def test_output_manager(db):
    """测试输出模块"""
    print("\n" + "=" * 50)
    print("测试输出模块")
    print("=" * 50)
    
    try:
        output_mgr = OutputManager(db)
        
        # 创建模拟选股结果
        import pandas as pd
        test_results = pd.DataFrame({
            'symbol': ['000001', '000002', '600000'],
            'name': ['平安银行', '万科A', '浦发银行'],
            'current_price': [15.5, 25.8, 12.3],
            'price_change': [2.5, 1.8, 3.2],
            'volume_ratio': [1.5, 2.1, 1.8],
            'turnover_rate': [2.5, 3.1, 2.8],
            'macd_signal': ['金叉', '看涨', '观望'],
            'rsi_signal': ['正常', '超卖反弹', '正常'],
            'ma_signal': ['突破', '多头排列', '观望'],
            'total_score': [85.5, 78.2, 72.1]
        })
        
        # 测试格式化结果
        formatted_results = output_mgr.format_selection_results(test_results)
        print("✅ 结果格式化成功")
        print(f"  格式化后列数: {len(formatted_results.columns)}")
        
        # 测试统计摘要
        summary = output_mgr.generate_summary_statistics(test_results)
        print("✅ 统计摘要生成成功")
        print(f"  统计项目数: {len(summary)}")
        
        # 测试Excel导出（不实际保存）
        try:
            wb = output_mgr.create_excel_workbook(test_results, summary)
            print("✅ Excel工作簿创建成功")
            print(f"  工作表数量: {len(wb.worksheets)}")
        except Exception as e:
            print(f"⚠️  Excel创建测试跳过: {e}")
        
        return True, output_mgr
        
    except Exception as e:
        print(f"❌ 输出模块测试失败: {e}")
        traceback.print_exc()
        return False, None


def test_integration():
    """集成测试"""
    print("\n" + "=" * 50)
    print("集成测试")
    print("=" * 50)
    
    try:
        # 设置测试模式
        config_manager.set('debug.test_mode', True)
        
        # 初始化所有模块
        db = DatabaseManager()
        fetcher = DataFetcher(db)
        tech = TechnicalIndicators(db)
        selector = StockSelector(db)
        output_mgr = OutputManager(db)
        
        print("✅ 所有模块初始化成功")
        
        # 测试模块间协作
        stock_list = selector.get_stock_basic_data()
        if not stock_list.empty:
            print(f"✅ 模块协作测试成功，获取到 {len(stock_list)} 只股票")
        else:
            print("⚠️  没有股票数据，可能需要先运行数据更新")
        
        return True
        
    except Exception as e:
        print(f"❌ 集成测试失败: {e}")
        traceback.print_exc()
        return False


def main():
    """主测试函数"""
    print("短线选股工具系统测试")
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    test_results = []
    
    # 1. 测试配置模块
    config_ok = test_config()
    test_results.append(("配置模块", config_ok))
    
    # 2. 测试数据库模块
    db_ok, db = test_database()
    test_results.append(("数据库模块", db_ok))
    
    if not db_ok:
        print("\n❌ 数据库测试失败，停止后续测试")
        return
    
    # 3. 测试数据获取模块
    fetcher_ok, fetcher = test_data_fetcher(db)
    test_results.append(("数据获取模块", fetcher_ok))
    
    # 4. 测试技术指标模块
    tech_ok, tech = test_technical_indicators(db)
    test_results.append(("技术指标模块", tech_ok))
    
    # 5. 测试选股模块
    selector_ok, selector = test_stock_selector(db)
    test_results.append(("选股模块", selector_ok))
    
    # 6. 测试输出模块
    output_ok, output_mgr = test_output_manager(db)
    test_results.append(("输出模块", output_ok))
    
    # 7. 集成测试
    integration_ok = test_integration()
    test_results.append(("集成测试", integration_ok))
    
    # 输出测试结果摘要
    print("\n" + "=" * 50)
    print("测试结果摘要")
    print("=" * 50)
    
    passed = 0
    total = len(test_results)
    
    for test_name, result in test_results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n总体结果: {passed}/{total} 项测试通过")
    
    if passed == total:
        print("🎉 所有测试通过！系统可以正常使用。")
        print("\n使用建议:")
        print("1. 首次使用请运行: python main.py --full --test-mode")
        print("2. 日常选股请运行: python main.py --select")
        print("3. 查看帮助请运行: python main.py --help")
    else:
        print("⚠️  部分测试失败，请检查相关模块。")
        print("注意: 数据获取功能需要网络连接才能正常工作。")


if __name__ == "__main__":
    main()