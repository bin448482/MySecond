"""
智能选股系统主程序
整合所有功能模块，提供统一的选股服务
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
import logging
import argparse
import sys
from datetime import datetime, timedelta
import schedule
import time
import json
from database import DatabaseManager
from enhanced_technical_indicators import EnhancedTechnicalIndicators
from comprehensive_scoring import ComprehensiveScoring
from stock_strategy_engine import StockStrategyEngine
from strategy_backtest import StrategyBacktest
from enhanced_output_manager import EnhancedOutputManager
from utils import config_manager, LoggerManager

# 设置日志
logger = logging.getLogger(__name__)


class SmartStockSelector:
    """智能选股系统主类"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        初始化智能选股系统
        
        Args:
            config_path: 配置文件路径
        """
        # 初始化配置和日志
        self.config = config_manager
        self.logger = LoggerManager.setup_logger(self.config)
        
        # 初始化核心组件
        self.db = DatabaseManager()
        self.tech_indicators = EnhancedTechnicalIndicators(self.db)
        self.scoring = ComprehensiveScoring(self.db)
        self.strategy_engine = StockStrategyEngine(self.db)
        self.backtest = StrategyBacktest(self.db)
        self.output_manager = EnhancedOutputManager(self.db)
        
        logger.info("智能选股系统初始化完成")
    
    def run_daily_selection(self, strategies: List[str] = None, 
                          max_stocks_per_strategy: int = 30) -> Dict[str, Any]:
        """
        执行每日选股
        
        Args:
            strategies: 策略列表，None表示使用所有策略
            max_stocks_per_strategy: 每个策略最大选股数
            
        Returns:
            选股结果字典
        """
        logger.info("开始执行每日选股...")
        
        try:
            # 获取可用策略
            if strategies is None:
                available_strategies = self.strategy_engine.get_available_strategies()
                strategies = list(available_strategies.keys())
            
            logger.info(f"使用策略: {strategies}")
            
            # 执行多策略选股
            strategy_results = self.strategy_engine.execute_multiple_strategies(
                strategies, max_stocks_per_strategy
            )
            
            # 获取策略交集
            intersection_stocks = self.strategy_engine.get_strategy_intersection(
                strategy_results, min_strategies=2
            )
            
            # 保存结果到数据库
            self.strategy_engine.save_strategy_results(strategy_results)
            
            # 生成报告
            report_file = self.output_manager.create_comprehensive_report(strategies)
            
            # 统计结果
            total_selections = sum(len(df) for df in strategy_results.values())
            intersection_count = len(intersection_stocks)
            
            result = {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'time': datetime.now().strftime('%H:%M:%S'),
                'strategies_used': strategies,
                'total_selections': total_selections,
                'intersection_count': intersection_count,
                'strategy_results': {k: len(v) for k, v in strategy_results.items()},
                'report_file': report_file,
                'top_intersection_stocks': intersection_stocks.head(10).to_dict('records') if not intersection_stocks.empty else [],
                'status': 'success'
            }
            
            logger.info(f"每日选股完成: 总计 {total_selections} 只股票，交集 {intersection_count} 只")
            return result
            
        except Exception as e:
            logger.error(f"每日选股执行失败: {e}")
            return {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'time': datetime.now().strftime('%H:%M:%S'),
                'status': 'error',
                'error': str(e)
            }
    
    def run_strategy_backtest(self, strategy_name: str, 
                            start_date: str, 
                            end_date: str) -> Dict[str, Any]:
        """
        运行策略回测
        
        Args:
            strategy_name: 策略名称
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            回测结果
        """
        logger.info(f"开始回测策略: {strategy_name}")
        
        try:
            # 执行回测
            backtest_result = self.backtest.backtest_strategy(strategy_name, start_date, end_date)
            
            # 生成回测报告
            report_file = self.output_manager.create_backtest_report(strategy_name, start_date, end_date)
            
            # 添加报告文件路径
            backtest_result['report_file'] = report_file
            
            logger.info(f"策略回测完成: {strategy_name}")
            return backtest_result
            
        except Exception as e:
            logger.error(f"策略回测失败: {e}")
            return {
                'strategy': strategy_name,
                'error': str(e),
                'status': 'failed'
            }
    
    def run_strategy_comparison(self, strategy_names: List[str],
                              start_date: str,
                              end_date: str) -> Dict[str, Any]:
        """
        运行策略比较
        
        Args:
            strategy_names: 策略名称列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            比较结果
        """
        logger.info(f"开始策略比较: {strategy_names}")
        
        try:
            # 执行策略比较
            comparison_result = self.backtest.compare_strategies(strategy_names, start_date, end_date)
            
            logger.info(f"策略比较完成")
            return comparison_result
            
        except Exception as e:
            logger.error(f"策略比较失败: {e}")
            return {
                'strategies': strategy_names,
                'error': str(e),
                'status': 'failed'
            }
    
    def get_stock_analysis(self, symbol: str) -> Dict[str, Any]:
        """
        获取单只股票的详细分析
        
        Args:
            symbol: 股票代码
            
        Returns:
            股票分析结果
        """
        try:
            logger.info(f"开始分析股票: {symbol}")
            
            # 获取技术指标
            indicators = self.tech_indicators.calculate_all_indicators(symbol)
            
            # 获取综合评分
            score_result = self.scoring.calculate_comprehensive_score(symbol)
            
            # 获取技术信号
            signals = self.tech_indicators.detect_comprehensive_signals(symbol)
            
            # 获取最新价格数据
            latest_data = self.db.get_stock_data(symbol, days=1)
            
            result = {
                'symbol': symbol,
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'latest_price': latest_data['close'].iloc[-1] if not latest_data.empty else 0,
                'comprehensive_score': score_result.get('comprehensive_score', 0),
                'technical_score': score_result.get('technical_score', 0),
                'momentum_score': score_result.get('momentum_score', 0),
                'volume_score': score_result.get('volume_score', 0),
                'volatility_score': score_result.get('volatility_score', 0),
                'technical_signals': signals,
                'latest_indicators': indicators.iloc[-1].to_dict() if not indicators.empty else {},
                'status': 'success'
            }
            
            logger.info(f"股票分析完成: {symbol}")
            return result
            
        except Exception as e:
            logger.error(f"股票分析失败 {symbol}: {e}")
            return {
                'symbol': symbol,
                'error': str(e),
                'status': 'failed'
            }
    
    def schedule_daily_tasks(self):
        """设置定时任务"""
        # 每日选股任务
        schedule.every().day.at("09:00").do(self._scheduled_daily_selection)
        
        # 每周回测任务
        schedule.every().monday.at("08:00").do(self._scheduled_weekly_backtest)
        
        logger.info("定时任务已设置")
    
    def _scheduled_daily_selection(self):
        """定时每日选股任务"""
        try:
            result = self.run_daily_selection()
            
            # 保存结果到文件
            result_file = f"output/daily_selection_{datetime.now().strftime('%Y%m%d')}.json"
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            
            logger.info(f"定时每日选股完成，结果已保存到: {result_file}")
            
        except Exception as e:
            logger.error(f"定时每日选股失败: {e}")
    
    def _scheduled_weekly_backtest(self):
        """定时每周回测任务"""
        try:
            # 回测最近一个月的数据
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            strategies = ['momentum_breakout', 'technical_reversal', 'volume_surge']
            
            for strategy in strategies:
                result = self.run_strategy_backtest(strategy, start_date, end_date)
                logger.info(f"定时回测完成: {strategy}")
            
        except Exception as e:
            logger.error(f"定时回测失败: {e}")
    
    def run_interactive_mode(self):
        """运行交互模式"""
        print("=" * 60)
        print("智能选股系统 - 交互模式")
        print("=" * 60)
        
        while True:
            print("\n请选择操作:")
            print("1. 执行每日选股")
            print("2. 单只股票分析")
            print("3. 策略回测")
            print("4. 策略比较")
            print("5. 查看可用策略")
            print("6. 启动定时任务")
            print("0. 退出")
            
            choice = input("\n请输入选项 (0-6): ").strip()
            
            try:
                if choice == '1':
                    self._interactive_daily_selection()
                elif choice == '2':
                    self._interactive_stock_analysis()
                elif choice == '3':
                    self._interactive_strategy_backtest()
                elif choice == '4':
                    self._interactive_strategy_comparison()
                elif choice == '5':
                    self._show_available_strategies()
                elif choice == '6':
                    self._start_scheduled_tasks()
                elif choice == '0':
                    print("感谢使用智能选股系统！")
                    break
                else:
                    print("无效选项，请重新输入")
                    
            except KeyboardInterrupt:
                print("\n\n操作已取消")
            except Exception as e:
                print(f"操作失败: {e}")
    
    def _interactive_daily_selection(self):
        """交互式每日选股"""
        print("\n=== 每日选股 ===")
        
        # 选择策略
        strategies = self._select_strategies()
        
        # 设置每策略最大选股数
        max_stocks = input("每个策略最大选股数 (默认30): ").strip()
        max_stocks = int(max_stocks) if max_stocks.isdigit() else 30
        
        print(f"\n开始执行选股，使用策略: {strategies}")
        print(f"每策略最大选股数: {max_stocks}")
        
        result = self.run_daily_selection(strategies, max_stocks)
        
        if result['status'] == 'success':
            print(f"\n选股完成!")
            print(f"总选股数: {result['total_selections']}")
            print(f"策略交集: {result['intersection_count']} 只")
            print(f"报告文件: {result['report_file']}")
            
            if result['top_intersection_stocks']:
                print("\n策略交集股票 (前5只):")
                for i, stock in enumerate(result['top_intersection_stocks'][:5], 1):
                    print(f"  {i}. {stock['symbol']} {stock['name']}: {stock['comprehensive_score']:.2f}分")
        else:
            print(f"选股失败: {result.get('error', 'Unknown error')}")
    
    def _interactive_stock_analysis(self):
        """交互式股票分析"""
        print("\n=== 股票分析 ===")
        
        symbol = input("请输入股票代码: ").strip().upper()
        
        if not symbol:
            print("股票代码不能为空")
            return
        
        print(f"\n开始分析股票: {symbol}")
        
        result = self.get_stock_analysis(symbol)
        
        if result['status'] == 'success':
            print(f"\n分析完成!")
            print(f"当前价格: ¥{result['latest_price']:.2f}")
            print(f"综合评分: {result['comprehensive_score']:.2f}")
            print(f"技术评分: {result['technical_score']:.2f}")
            print(f"动量评分: {result['momentum_score']:.2f}")
            print(f"成交量评分: {result['volume_score']:.2f}")
            print(f"波动率评分: {result['volatility_score']:.2f}")
            
            print("\n技术信号:")
            for signal_type, signal_value in result['technical_signals'].items():
                print(f"  {signal_type}: {signal_value}")
        else:
            print(f"分析失败: {result.get('error', 'Unknown error')}")
    
    def _interactive_strategy_backtest(self):
        """交互式策略回测"""
        print("\n=== 策略回测 ===")
        
        # 选择策略
        strategy = self._select_single_strategy()
        
        # 输入日期范围
        start_date = input("开始日期 (YYYY-MM-DD, 默认30天前): ").strip()
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        end_date = input("结束日期 (YYYY-MM-DD, 默认今天): ").strip()
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        print(f"\n开始回测策略: {strategy}")
        print(f"回测期间: {start_date} 到 {end_date}")
        
        result = self.run_strategy_backtest(strategy, start_date, end_date)
        
        if 'error' not in result:
            performance = result.get('performance', {})
            summary = performance.get('summary', {})
            
            print(f"\n回测完成!")
            print(f"测试次数: {result.get('test_dates', 0)}")
            print(f"总选股数: {result.get('total_selections', 0)}")
            
            if summary:
                print(f"平均收益率: {summary.get('primary_avg_return', 0):.2f}%")
                print(f"胜率: {summary.get('primary_positive_rate', 0):.1f}%")
                print(f"策略评级: {summary.get('strategy_rating', 'N/A')}")
            
            print(f"报告文件: {result.get('report_file', 'N/A')}")
        else:
            print(f"回测失败: {result.get('error', 'Unknown error')}")
    
    def _interactive_strategy_comparison(self):
        """交互式策略比较"""
        print("\n=== 策略比较 ===")
        
        # 选择多个策略
        strategies = self._select_strategies()
        
        if len(strategies) < 2:
            print("至少需要选择2个策略进行比较")
            return
        
        # 输入日期范围
        start_date = input("开始日期 (YYYY-MM-DD, 默认30天前): ").strip()
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        end_date = input("结束日期 (YYYY-MM-DD, 默认今天): ").strip()
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        print(f"\n开始比较策略: {strategies}")
        print(f"比较期间: {start_date} 到 {end_date}")
        
        result = self.run_strategy_comparison(strategies, start_date, end_date)
        
        if 'error' not in result:
            ranking = result.get('ranking', [])
            summary = result.get('summary', {})
            
            print(f"\n策略比较完成!")
            
            if ranking:
                print("\n策略排名:")
                for i, strategy_data in enumerate(ranking, 1):
                    print(f"  {i}. {strategy_data['strategy']}: "
                          f"{strategy_data['avg_return']:.2f}% "
                          f"(胜率: {strategy_data['positive_rate']:.1f}%)")
            
            if summary:
                print(f"\n最佳策略: {summary.get('best_strategy', 'N/A')}")
                print(f"最佳收益率: {summary.get('best_avg_return', 0):.2f}%")
        else:
            print(f"策略比较失败: {result.get('error', 'Unknown error')}")
    
    def _show_available_strategies(self):
        """显示可用策略"""
        print("\n=== 可用策略 ===")
        
        strategies = self.strategy_engine.get_available_strategies()
        
        for i, (name, config) in enumerate(strategies.items(), 1):
            print(f"{i}. {name}")
            print(f"   名称: {config.get('name', '')}")
            print(f"   描述: {config.get('description', '')}")
            print()
    
    def _select_strategies(self) -> List[str]:
        """选择策略"""
        strategies = self.strategy_engine.get_available_strategies()
        strategy_list = list(strategies.keys())
        
        print("\n可用策略:")
        for i, name in enumerate(strategy_list, 1):
            print(f"  {i}. {name}")
        
        print(f"  {len(strategy_list) + 1}. 全部策略")
        
        choice = input(f"\n请选择策略 (1-{len(strategy_list) + 1}, 多选用逗号分隔): ").strip()
        
        if not choice:
            return strategy_list  # 默认全部
        
        selected = []
        for c in choice.split(','):
            c = c.strip()
            if c.isdigit():
                idx = int(c) - 1
                if idx == len(strategy_list):  # 全部策略
                    return strategy_list
                elif 0 <= idx < len(strategy_list):
                    selected.append(strategy_list[idx])
        
        return selected if selected else strategy_list
    
    def _select_single_strategy(self) -> str:
        """选择单个策略"""
        strategies = self.strategy_engine.get_available_strategies()
        strategy_list = list(strategies.keys())
        
        print("\n可用策略:")
        for i, name in enumerate(strategy_list, 1):
            print(f"  {i}. {name}")
        
        choice = input(f"\n请选择策略 (1-{len(strategy_list)}): ").strip()
        
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(strategy_list):
                return strategy_list[idx]
        
        return strategy_list[0]  # 默认第一个
    
    def _start_scheduled_tasks(self):
        """启动定时任务"""
        print("\n=== 启动定时任务 ===")
        print("定时任务已设置:")
        print("- 每日09:00执行选股")
        print("- 每周一08:00执行回测")
        print("\n按 Ctrl+C 停止定时任务")
        
        self.schedule_daily_tasks()
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            print("\n定时任务已停止")


def create_argument_parser():
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(description='智能选股系统')
    
    parser.add_argument('--mode', choices=['interactive', 'daily', 'backtest', 'analysis'],
                       default='interactive', help='运行模式')
    
    parser.add_argument('--strategy', type=str, help='策略名称')
    parser.add_argument('--strategies', type=str, nargs='+', help='策略名称列表')
    parser.add_argument('--symbol', type=str, help='股票代码')
    parser.add_argument('--start-date', type=str, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--max-stocks', type=int, default=30, help='最大选股数')
    
    parser.add_argument('--config', type=str, default='config.yaml', help='配置文件路径')
    parser.add_argument('--schedule', action='store_true', help='启动定时任务')
    
    return parser


def main():
    """主函数"""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    try:
        # 初始化系统
        selector = SmartStockSelector(args.config)
        
        if args.mode == 'interactive':
            # 交互模式
            selector.run_interactive_mode()
            
        elif args.mode == 'daily':
            # 每日选股模式
            strategies = args.strategies if args.strategies else None
            result = selector.run_daily_selection(strategies, args.max_stocks)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            
        elif args.mode == 'backtest':
            # 回测模式
            if not args.strategy:
                print("回测模式需要指定策略名称 (--strategy)")
                sys.exit(1)
            
            start_date = args.start_date or (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            end_date = args.end_date or datetime.now().strftime('%Y-%m-%d')
            
            result = selector.run_strategy_backtest(args.strategy, start_date, end_date)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            
        elif args.mode == 'analysis':
            # 股票分析模式
            if not args.symbol:
                print("分析模式需要指定股票代码 (--symbol)")
                sys.exit(1)
            
            result = selector.get_stock_analysis(args.symbol)
            print(json.dumps(result, ensure_ascii=False, indent=2))
        
        if args.schedule:
            # 启动定时任务
            print("启动定时任务...")
            selector.schedule_daily_tasks()
            
            try:
                while True:
                    schedule.run_pending()
                    time.sleep(60)
            except KeyboardInterrupt:
                print("定时任务已停止")
                
    except Exception as e:
        logger.error(f"系统运行失败: {e}")
        print(f"系统运行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()