"""
策略回测系统
用于验证选股策略的历史表现和有效性
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime, timedelta
from database import DatabaseManager
from stock_strategy_engine import StockStrategyEngine
from utils import config_manager

logger = logging.getLogger(__name__)


class StrategyBacktest:
    """策略回测系统"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        初始化策略回测系统
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.strategy_engine = StockStrategyEngine(db_manager)
        self.config = config_manager
    
    def get_historical_data_range(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取指定时间范围的历史数据
        
        Args:
            symbol: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            历史数据DataFrame
        """
        try:
            conn = self.db.get_connection()
            query = '''
                SELECT * FROM daily_data 
                WHERE symbol = ? AND date BETWEEN ? AND ?
                ORDER BY date
            '''
            
            df = pd.read_sql_query(query, conn, params=(symbol, start_date, end_date))
            conn.close()
            
            return df
            
        except Exception as e:
            logger.error(f"获取历史数据失败 {symbol} ({start_date} to {end_date}): {e}")
            return pd.DataFrame()
    
    def calculate_returns(self, prices: pd.Series, holding_days: int = 5) -> pd.Series:
        """
        计算收益率
        
        Args:
            prices: 价格序列
            holding_days: 持有天数
            
        Returns:
            收益率序列
        """
        if len(prices) <= holding_days:
            return pd.Series(dtype=float)
        
        # 计算N日后的收益率
        future_prices = prices.shift(-holding_days)
        returns = (future_prices / prices - 1) * 100
        
        return returns.dropna()
    
    def simulate_strategy_on_date(self, strategy_name: str, test_date: str, 
                                 max_stocks: int = 20) -> pd.DataFrame:
        """
        模拟在指定日期执行策略
        
        Args:
            strategy_name: 策略名称
            test_date: 测试日期
            max_stocks: 最大选股数量
            
        Returns:
            选股结果DataFrame
        """
        try:
            # 临时修改数据库查询以获取历史数据
            # 这里需要模拟在test_date当天的数据状态
            
            # 获取该日期前的股票列表
            conn = self.db.get_connection()
            
            # 获取在test_date之前有数据的股票
            stock_query = '''
                SELECT DISTINCT symbol FROM daily_data 
                WHERE date <= ? 
                GROUP BY symbol 
                HAVING COUNT(*) >= 60
            '''
            
            available_stocks = pd.read_sql_query(stock_query, conn, params=(test_date,))
            conn.close()
            
            if available_stocks.empty:
                logger.warning(f"在日期 {test_date} 没有足够的历史数据")
                return pd.DataFrame()
            
            symbols = available_stocks['symbol'].tolist()
            
            # 为每只股票计算截至test_date的技术指标和评分
            # 这里简化处理，实际应该重新计算历史状态
            
            # 模拟执行策略（简化版）
            strategy_config = self.strategy_engine.strategies.get(strategy_name, {})
            if not strategy_config:
                logger.error(f"未知策略: {strategy_name}")
                return pd.DataFrame()
            
            # 简化的选股逻辑 - 随机选择一些股票作为示例
            # 实际应用中需要重新计算历史技术指标
            selected_symbols = np.random.choice(symbols, 
                                              min(max_stocks, len(symbols)), 
                                              replace=False)
            
            results = []
            for symbol in selected_symbols:
                # 获取test_date的价格数据
                price_data = self.get_historical_data_range(symbol, test_date, test_date)
                
                if not price_data.empty:
                    results.append({
                        'symbol': symbol,
                        'selection_date': test_date,
                        'selection_price': price_data['close'].iloc[0],
                        'strategy': strategy_name
                    })
            
            return pd.DataFrame(results)
            
        except Exception as e:
            logger.error(f"模拟策略执行失败 {strategy_name} on {test_date}: {e}")
            return pd.DataFrame()
    
    def calculate_strategy_performance(self, selections: pd.DataFrame, 
                                     holding_days: List[int] = [1, 3, 5, 10]) -> Dict[str, Any]:
        """
        计算策略表现
        
        Args:
            selections: 选股结果DataFrame
            holding_days: 持有天数列表
            
        Returns:
            策略表现字典
        """
        if selections.empty:
            return {'error': '没有选股数据'}
        
        performance = {
            'total_selections': len(selections),
            'holding_periods': {},
            'summary': {}
        }
        
        try:
            for days in holding_days:
                period_returns = []
                valid_trades = 0
                
                for _, row in selections.iterrows():
                    symbol = row['symbol']
                    selection_date = row['selection_date']
                    selection_price = row['selection_price']
                    
                    # 计算持有期结束日期
                    end_date = (pd.to_datetime(selection_date) + 
                               timedelta(days=days)).strftime('%Y-%m-%d')
                    
                    # 获取结束日期的价格
                    end_data = self.get_historical_data_range(symbol, end_date, end_date)
                    
                    if not end_data.empty:
                        end_price = end_data['close'].iloc[0]
                        return_pct = (end_price / selection_price - 1) * 100
                        period_returns.append(return_pct)
                        valid_trades += 1
                
                if period_returns:
                    returns_array = np.array(period_returns)
                    
                    performance['holding_periods'][f'{days}d'] = {
                        'valid_trades': valid_trades,
                        'avg_return': np.mean(returns_array),
                        'median_return': np.median(returns_array),
                        'std_return': np.std(returns_array),
                        'min_return': np.min(returns_array),
                        'max_return': np.max(returns_array),
                        'positive_rate': np.sum(returns_array > 0) / len(returns_array) * 100,
                        'returns_distribution': {
                            'q25': np.percentile(returns_array, 25),
                            'q75': np.percentile(returns_array, 75)
                        }
                    }
                else:
                    performance['holding_periods'][f'{days}d'] = {
                        'valid_trades': 0,
                        'error': '没有有效的交易数据'
                    }
            
            # 计算综合表现指标
            if performance['holding_periods']:
                # 使用5日持有期作为主要评估标准
                main_period = performance['holding_periods'].get('5d', {})
                
                if 'avg_return' in main_period:
                    avg_return = main_period['avg_return']
                    std_return = main_period['std_return']
                    positive_rate = main_period['positive_rate']
                    
                    # 计算夏普比率（简化版）
                    sharpe_ratio = avg_return / std_return if std_return > 0 else 0
                    
                    # 计算策略评级
                    if avg_return > 3 and positive_rate > 60:
                        rating = 'A'
                    elif avg_return > 1 and positive_rate > 50:
                        rating = 'B'
                    elif avg_return > 0 and positive_rate > 45:
                        rating = 'C'
                    else:
                        rating = 'D'
                    
                    performance['summary'] = {
                        'primary_avg_return': avg_return,
                        'primary_positive_rate': positive_rate,
                        'sharpe_ratio': sharpe_ratio,
                        'strategy_rating': rating,
                        'risk_level': 'High' if std_return > 8 else 'Medium' if std_return > 4 else 'Low'
                    }
            
        except Exception as e:
            logger.error(f"计算策略表现时出错: {e}")
            performance['error'] = str(e)
        
        return performance
    
    def backtest_strategy(self, strategy_name: str, 
                         start_date: str, 
                         end_date: str,
                         test_frequency: int = 5,
                         max_stocks_per_test: int = 20) -> Dict[str, Any]:
        """
        回测策略
        
        Args:
            strategy_name: 策略名称
            start_date: 回测开始日期
            end_date: 回测结束日期
            test_frequency: 测试频率（每N天测试一次）
            max_stocks_per_test: 每次测试的最大选股数
            
        Returns:
            回测结果字典
        """
        logger.info(f"开始回测策略 {strategy_name} ({start_date} to {end_date})")
        
        try:
            # 生成测试日期列表
            test_dates = pd.date_range(start=start_date, end=end_date, freq=f'{test_frequency}D')
            test_dates = [date.strftime('%Y-%m-%d') for date in test_dates]
            
            all_selections = []
            
            # 在每个测试日期执行策略
            for i, test_date in enumerate(test_dates):
                logger.info(f"回测进度: {i+1}/{len(test_dates)} - {test_date}")
                
                selections = self.simulate_strategy_on_date(
                    strategy_name, test_date, max_stocks_per_test
                )
                
                if not selections.empty:
                    all_selections.append(selections)
            
            if not all_selections:
                return {
                    'strategy': strategy_name,
                    'period': f"{start_date} to {end_date}",
                    'error': '没有生成任何选股结果'
                }
            
            # 合并所有选股结果
            combined_selections = pd.concat(all_selections, ignore_index=True)
            
            # 计算策略表现
            performance = self.calculate_strategy_performance(combined_selections)
            
            # 添加回测元信息
            backtest_result = {
                'strategy': strategy_name,
                'backtest_period': f"{start_date} to {end_date}",
                'test_dates': len(test_dates),
                'total_selections': len(combined_selections),
                'performance': performance,
                'selections_sample': combined_selections.head(10).to_dict('records'),
                'backtest_date': datetime.now().isoformat()
            }
            
            logger.info(f"策略 {strategy_name} 回测完成")
            return backtest_result
            
        except Exception as e:
            logger.error(f"回测策略 {strategy_name} 时出错: {e}")
            return {
                'strategy': strategy_name,
                'error': str(e)
            }
    
    def compare_strategies(self, strategy_names: List[str],
                          start_date: str,
                          end_date: str) -> Dict[str, Any]:
        """
        比较多个策略的表现
        
        Args:
            strategy_names: 策略名称列表
            start_date: 回测开始日期
            end_date: 回测结束日期
            
        Returns:
            策略比较结果
        """
        logger.info(f"开始比较策略: {strategy_names}")
        
        strategy_results = {}
        
        # 回测每个策略
        for strategy_name in strategy_names:
            result = self.backtest_strategy(strategy_name, start_date, end_date)
            strategy_results[strategy_name] = result
        
        # 生成比较报告
        comparison = {
            'comparison_period': f"{start_date} to {end_date}",
            'strategies_tested': len(strategy_names),
            'individual_results': strategy_results,
            'ranking': [],
            'summary': {}
        }
        
        try:
            # 提取关键指标进行排名
            ranking_data = []
            
            for strategy_name, result in strategy_results.items():
                if 'performance' in result and 'summary' in result['performance']:
                    summary = result['performance']['summary']
                    ranking_data.append({
                        'strategy': strategy_name,
                        'avg_return': summary.get('primary_avg_return', 0),
                        'positive_rate': summary.get('primary_positive_rate', 0),
                        'sharpe_ratio': summary.get('sharpe_ratio', 0),
                        'rating': summary.get('strategy_rating', 'N/A')
                    })
            
            if ranking_data:
                # 按平均收益率排序
                ranking_df = pd.DataFrame(ranking_data)
                ranking_df = ranking_df.sort_values('avg_return', ascending=False)
                comparison['ranking'] = ranking_df.to_dict('records')
                
                # 生成总结
                best_strategy = ranking_df.iloc[0]
                comparison['summary'] = {
                    'best_strategy': best_strategy['strategy'],
                    'best_avg_return': best_strategy['avg_return'],
                    'best_positive_rate': best_strategy['positive_rate'],
                    'strategies_with_positive_return': len(ranking_df[ranking_df['avg_return'] > 0]),
                    'average_return_all_strategies': ranking_df['avg_return'].mean()
                }
            
        except Exception as e:
            logger.error(f"生成策略比较报告时出错: {e}")
            comparison['error'] = str(e)
        
        return comparison
    
    def generate_backtest_report(self, backtest_result: Dict[str, Any], 
                               output_file: Optional[str] = None) -> str:
        """
        生成回测报告
        
        Args:
            backtest_result: 回测结果
            output_file: 输出文件路径
            
        Returns:
            报告内容字符串
        """
        try:
            strategy = backtest_result.get('strategy', 'Unknown')
            period = backtest_result.get('backtest_period', 'Unknown')
            
            report_lines = [
                f"策略回测报告",
                f"=" * 50,
                f"策略名称: {strategy}",
                f"回测期间: {period}",
                f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f"",
                f"回测概况:",
                f"  测试次数: {backtest_result.get('test_dates', 0)}",
                f"  总选股数: {backtest_result.get('total_selections', 0)}",
                f""
            ]
            
            # 添加表现数据
            performance = backtest_result.get('performance', {})
            
            if 'summary' in performance:
                summary = performance['summary']
                report_lines.extend([
                    f"策略表现总结:",
                    f"  主要收益率: {summary.get('primary_avg_return', 0):.2f}%",
                    f"  胜率: {summary.get('primary_positive_rate', 0):.1f}%",
                    f"  夏普比率: {summary.get('sharpe_ratio', 0):.2f}",
                    f"  策略评级: {summary.get('strategy_rating', 'N/A')}",
                    f"  风险水平: {summary.get('risk_level', 'N/A')}",
                    f""
                ])
            
            # 添加各持有期表现
            if 'holding_periods' in performance:
                report_lines.append("各持有期表现:")
                
                for period, data in performance['holding_periods'].items():
                    if 'avg_return' in data:
                        report_lines.extend([
                            f"  {period}:",
                            f"    平均收益: {data['avg_return']:.2f}%",
                            f"    胜率: {data['positive_rate']:.1f}%",
                            f"    最大收益: {data['max_return']:.2f}%",
                            f"    最大亏损: {data['min_return']:.2f}%",
                            f"    有效交易: {data['valid_trades']}笔",
                            f""
                        ])
            
            # 添加样本数据
            if 'selections_sample' in backtest_result:
                report_lines.append("选股样本 (前10只):")
                for i, selection in enumerate(backtest_result['selections_sample'], 1):
                    report_lines.append(
                        f"  {i}. {selection.get('symbol', 'N/A')} - "
                        f"{selection.get('selection_date', 'N/A')} - "
                        f"¥{selection.get('selection_price', 0):.2f}"
                    )
            
            report_content = "\n".join(report_lines)
            
            # 保存到文件
            if output_file:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(report_content)
                logger.info(f"回测报告已保存到: {output_file}")
            
            return report_content
            
        except Exception as e:
            logger.error(f"生成回测报告时出错: {e}")
            return f"生成报告失败: {e}"


def main():
    """测试策略回测系统"""
    from database import DatabaseManager
    
    # 初始化
    db = DatabaseManager()
    backtest = StrategyBacktest(db)
    
    print("=== 策略回测系统测试 ===")
    
    # 设置回测参数
    strategy_name = 'momentum_breakout'
    start_date = '2024-01-01'
    end_date = '2024-03-31'
    
    print(f"\n回测策略: {strategy_name}")
    print(f"回测期间: {start_date} to {end_date}")
    
    # 执行回测
    result = backtest.backtest_strategy(strategy_name, start_date, end_date)
    
    if 'error' not in result:
        print(f"\n回测完成:")
        print(f"  测试次数: {result.get('test_dates', 0)}")
        print(f"  总选股数: {result.get('total_selections', 0)}")
        
        # 显示表现总结
        performance = result.get('performance', {})
        if 'summary' in performance:
            summary = performance['summary']
            print(f"\n策略表现:")
            print(f"  平均收益率: {summary.get('primary_avg_return', 0):.2f}%")
            print(f"  胜率: {summary.get('primary_positive_rate', 0):.1f}%")
            print(f"  策略评级: {summary.get('strategy_rating', 'N/A')}")
        
        # 生成报告
        report = backtest.generate_backtest_report(result)
        print(f"\n回测报告预览:")
        print(report[:500] + "..." if len(report) > 500 else report)
        
    else:
        print(f"回测失败: {result.get('error', 'Unknown error')}")
    
    # 测试策略比较
    print(f"\n=== 策略比较测试 ===")
    
    strategies_to_compare = ['momentum_breakout', 'technical_reversal']
    comparison = backtest.compare_strategies(strategies_to_compare, start_date, end_date)
    
    if 'ranking' in comparison and comparison['ranking']:
        print(f"\n策略排名:")
        for i, strategy_data in enumerate(comparison['ranking'], 1):
            print(f"  {i}. {strategy_data['strategy']}: "
                  f"{strategy_data['avg_return']:.2f}% "
                  f"(胜率: {strategy_data['positive_rate']:.1f}%)")


if __name__ == "__main__":
    main()