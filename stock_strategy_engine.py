"""
选股策略引擎
整合技术指标、综合评分等多维度数据实现智能选股策略
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime, timedelta
from database import DatabaseManager
from enhanced_technical_indicators import EnhancedTechnicalIndicators
from comprehensive_scoring import ComprehensiveScoring
from utils import config_manager

logger = logging.getLogger(__name__)


class StockStrategyEngine:
    """选股策略引擎"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        初始化选股策略引擎
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.tech_indicators = EnhancedTechnicalIndicators(db_manager)
        self.scoring = ComprehensiveScoring(db_manager)
        self.config = config_manager
        
        # 预定义策略配置
        self.strategies = {
            'momentum_breakout': {
                'name': '动量突破策略',
                'description': '寻找技术指标向好、动量强劲的突破股票',
                'weights': {'technical': 0.35, 'momentum': 0.40, 'volume': 0.20, 'volatility': 0.05},
                'filters': {
                    'min_score': 70,
                    'min_volume_ratio': 1.5,
                    'min_price_change_5d': 2.0,
                    'max_rsi': 80,
                    'min_rsi': 30
                }
            },
            'technical_reversal': {
                'name': '技术反转策略',
                'description': '寻找超卖反弹、技术指标修复的股票',
                'weights': {'technical': 0.50, 'momentum': 0.20, 'volume': 0.15, 'volatility': 0.15},
                'filters': {
                    'min_score': 60,
                    'min_volume_ratio': 1.2,
                    'max_rsi': 35,
                    'min_rsi': 15,
                    'kdj_oversold': True
                }
            },
            'volume_surge': {
                'name': '放量突破策略',
                'description': '寻找成交量放大、价格突破的股票',
                'weights': {'technical': 0.30, 'momentum': 0.25, 'volume': 0.35, 'volatility': 0.10},
                'filters': {
                    'min_score': 65,
                    'min_volume_ratio': 2.0,
                    'min_turnover_rate': 3.0,
                    'price_above_ma20': True
                }
            },
            'balanced_growth': {
                'name': '均衡成长策略',
                'description': '寻找各项指标均衡、稳健成长的股票',
                'weights': {'technical': 0.40, 'momentum': 0.25, 'volume': 0.20, 'volatility': 0.15},
                'filters': {
                    'min_score': 75,
                    'min_volume_ratio': 1.0,
                    'min_price_change_5d': 1.0,
                    'max_volatility': 40,
                    'ma_alignment': True
                }
            }
        }
    
    def get_available_strategies(self) -> Dict[str, Dict]:
        """
        获取可用策略列表
        
        Returns:
            策略配置字典
        """
        return self.strategies
    
    def apply_basic_filters(self, stock_data: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
        """
        应用基础筛选条件
        
        Args:
            stock_data: 股票数据DataFrame
            filters: 筛选条件字典
            
        Returns:
            筛选后的股票数据
        """
        filtered_data = stock_data.copy()
        
        try:
            # 价格区间筛选
            if 'min_price' in filters:
                filtered_data = filtered_data[filtered_data['close'] >= filters['min_price']]
            
            if 'max_price' in filters:
                filtered_data = filtered_data[filtered_data['close'] <= filters['max_price']]
            
            # 成交量筛选
            if 'min_volume_ratio' in filters:
                filtered_data = filtered_data[filtered_data['volume_ratio'] >= filters['min_volume_ratio']]
            
            # 换手率筛选
            if 'min_turnover_rate' in filters and 'turnover_rate' in filtered_data.columns:
                filtered_data = filtered_data[filtered_data['turnover_rate'] >= filters['min_turnover_rate']]
            
            # 涨跌幅筛选
            if 'min_price_change_1d' in filters and 'price_change_1d' in filtered_data.columns:
                filtered_data = filtered_data[filtered_data['price_change_1d'] >= filters['min_price_change_1d']]
            
            if 'max_price_change_1d' in filters and 'price_change_1d' in filtered_data.columns:
                filtered_data = filtered_data[filtered_data['price_change_1d'] <= filters['max_price_change_1d']]
            
            logger.info(f"基础筛选完成，从 {len(stock_data)} 只股票筛选出 {len(filtered_data)} 只")
            
        except Exception as e:
            logger.error(f"应用基础筛选时出错: {e}")
        
        return filtered_data
    
    def apply_technical_filters(self, symbols: List[str], filters: Dict[str, Any]) -> List[str]:
        """
        应用技术指标筛选
        
        Args:
            symbols: 股票代码列表
            filters: 技术筛选条件
            
        Returns:
            筛选后的股票代码列表
        """
        filtered_symbols = []
        
        for symbol in symbols:
            try:
                # 获取技术指标数据
                indicators = self.tech_indicators.calculate_all_indicators(symbol)
                
                if indicators.empty:
                    continue
                
                latest = indicators.iloc[-1]
                include_stock = True
                
                # RSI筛选
                if 'min_rsi' in filters or 'max_rsi' in filters:
                    rsi = latest.get('rsi')
                    if pd.notna(rsi):
                        if 'min_rsi' in filters and rsi < filters['min_rsi']:
                            include_stock = False
                        if 'max_rsi' in filters and rsi > filters['max_rsi']:
                            include_stock = False
                
                # MACD筛选
                if 'macd_golden_cross' in filters and filters['macd_golden_cross']:
                    macd = latest.get('macd')
                    macd_signal = latest.get('macd_signal')
                    if pd.notna(macd) and pd.notna(macd_signal):
                        if macd <= macd_signal:
                            include_stock = False
                
                # KDJ超卖筛选
                if 'kdj_oversold' in filters and filters['kdj_oversold']:
                    kdj_k = latest.get('kdj_k')
                    kdj_d = latest.get('kdj_d')
                    if pd.notna(kdj_k) and pd.notna(kdj_d):
                        if kdj_k > 30 or kdj_d > 30:  # 不在超卖区域
                            include_stock = False
                
                # 价格相对均线位置
                if 'price_above_ma20' in filters and filters['price_above_ma20']:
                    close = latest.get('close')
                    ma20 = latest.get('ma20')
                    if pd.notna(close) and pd.notna(ma20):
                        if close <= ma20:
                            include_stock = False
                
                # 均线排列
                if 'ma_alignment' in filters and filters['ma_alignment']:
                    ma5 = latest.get('ma5')
                    ma10 = latest.get('ma10')
                    ma20 = latest.get('ma20')
                    if pd.notna(ma5) and pd.notna(ma10) and pd.notna(ma20):
                        if not (ma5 > ma10 > ma20):  # 不是多头排列
                            include_stock = False
                
                # 布林带位置
                if 'bb_position_range' in filters:
                    bb_pos = latest.get('bb_position')
                    if pd.notna(bb_pos):
                        min_pos, max_pos = filters['bb_position_range']
                        if bb_pos < min_pos or bb_pos > max_pos:
                            include_stock = False
                
                if include_stock:
                    filtered_symbols.append(symbol)
                    
            except Exception as e:
                logger.error(f"技术筛选股票 {symbol} 时出错: {e}")
                continue
        
        logger.info(f"技术筛选完成，从 {len(symbols)} 只股票筛选出 {len(filtered_symbols)} 只")
        return filtered_symbols
    
    def calculate_price_momentum(self, symbol: str, days: int = 5) -> float:
        """
        计算价格动量
        
        Args:
            symbol: 股票代码
            days: 计算天数
            
        Returns:
            价格变化百分比
        """
        try:
            hist_data = self.db.get_stock_data(symbol, days=days+5)
            
            if hist_data.empty or len(hist_data) < days+1:
                return 0.0
            
            hist_data = hist_data.sort_values('date')
            close_prices = hist_data['close']
            
            current_price = close_prices.iloc[-1]
            past_price = close_prices.iloc[-(days+1)]
            
            return (current_price / past_price - 1) * 100
            
        except Exception as e:
            logger.error(f"计算价格动量失败 {symbol}: {e}")
            return 0.0
    
    def execute_strategy(self, strategy_name: str, 
                        max_results: int = 50,
                        custom_filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        执行指定策略
        
        Args:
            strategy_name: 策略名称
            max_results: 最大返回结果数
            custom_filters: 自定义筛选条件
            
        Returns:
            选股结果DataFrame
        """
        if strategy_name not in self.strategies:
            raise ValueError(f"未知策略: {strategy_name}")
        
        strategy_config = self.strategies[strategy_name]
        logger.info(f"开始执行策略: {strategy_config['name']}")
        
        try:
            # 1. 获取股票列表
            stock_list = self.db.get_stock_list()
            if stock_list.empty:
                logger.warning("没有可用的股票数据")
                return pd.DataFrame()
            
            symbols = stock_list['symbol'].tolist()
            logger.info(f"获取到 {len(symbols)} 只股票")
            
            # 2. 应用技术指标筛选
            strategy_filters = strategy_config.get('filters', {})
            if custom_filters:
                strategy_filters.update(custom_filters)
            
            filtered_symbols = self.apply_technical_filters(symbols, strategy_filters)
            
            if not filtered_symbols:
                logger.warning("技术筛选后没有符合条件的股票")
                return pd.DataFrame()
            
            # 3. 计算综合评分
            logger.info(f"开始计算 {len(filtered_symbols)} 只股票的综合评分...")
            
            strategy_weights = strategy_config.get('weights', {})
            scores_df = self.scoring.batch_calculate_scores(filtered_symbols, strategy_weights)
            
            if scores_df.empty:
                logger.warning("评分计算后没有有效结果")
                return pd.DataFrame()
            
            # 4. 应用评分筛选
            min_score = strategy_filters.get('min_score', 0)
            scores_df = scores_df[scores_df['comprehensive_score'] >= min_score]
            
            # 5. 添加额外的市场数据
            enhanced_results = []
            
            for _, row in scores_df.head(max_results * 2).iterrows():  # 多取一些以备筛选
                symbol = row['symbol']
                
                try:
                    # 获取最新市场数据
                    latest_data = self.db.get_stock_data(symbol, days=1)
                    if latest_data.empty:
                        continue
                    
                    latest = latest_data.iloc[-1]
                    
                    # 计算额外指标
                    price_change_5d = self.calculate_price_momentum(symbol, 5)
                    
                    # 获取股票基本信息
                    stock_info = stock_list[stock_list['symbol'] == symbol]
                    stock_name = stock_info['name'].iloc[0] if not stock_info.empty else symbol
                    
                    result_row = {
                        'symbol': symbol,
                        'name': stock_name,
                        'close': latest['close'],
                        'comprehensive_score': row['comprehensive_score'],
                        'technical_score': row.get('technical_score', 0),
                        'momentum_score': row.get('momentum_score', 0),
                        'volume_score': row.get('volume_score', 0),
                        'volatility_score': row.get('volatility_score', 0),
                        'price_change_5d': price_change_5d,
                        'volume': latest.get('volume', 0),
                        'turnover_rate': latest.get('turnover_rate', 0),
                        'strategy': strategy_name,
                        'selection_date': datetime.now().strftime('%Y-%m-%d'),
                        'selection_time': datetime.now().strftime('%H:%M:%S')
                    }
                    
                    # 应用最终筛选条件
                    include_result = True
                    
                    # 5日涨跌幅筛选
                    if 'min_price_change_5d' in strategy_filters:
                        if price_change_5d < strategy_filters['min_price_change_5d']:
                            include_result = False
                    
                    # 波动率筛选
                    if 'max_volatility' in strategy_filters:
                        volatility_score = row.get('volatility_score', 0)
                        if volatility_score > strategy_filters['max_volatility']:
                            include_result = False
                    
                    if include_result:
                        enhanced_results.append(result_row)
                        
                except Exception as e:
                    logger.error(f"处理股票 {symbol} 时出错: {e}")
                    continue
            
            # 6. 转换为DataFrame并排序
            if enhanced_results:
                result_df = pd.DataFrame(enhanced_results)
                result_df = result_df.sort_values('comprehensive_score', ascending=False)
                result_df = result_df.head(max_results).reset_index(drop=True)
                
                logger.info(f"策略 {strategy_name} 执行完成，选出 {len(result_df)} 只股票")
                return result_df
            else:
                logger.warning(f"策略 {strategy_name} 执行完成，但没有符合条件的股票")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"执行策略 {strategy_name} 时出错: {e}")
            return pd.DataFrame()
    
    def execute_multiple_strategies(self, strategy_names: List[str], 
                                  max_results_per_strategy: int = 20) -> Dict[str, pd.DataFrame]:
        """
        执行多个策略
        
        Args:
            strategy_names: 策略名称列表
            max_results_per_strategy: 每个策略的最大结果数
            
        Returns:
            策略结果字典
        """
        results = {}
        
        for strategy_name in strategy_names:
            if strategy_name in self.strategies:
                logger.info(f"执行策略: {strategy_name}")
                strategy_result = self.execute_strategy(strategy_name, max_results_per_strategy)
                results[strategy_name] = strategy_result
            else:
                logger.warning(f"未知策略: {strategy_name}")
                results[strategy_name] = pd.DataFrame()
        
        return results
    
    def get_strategy_intersection(self, strategy_results: Dict[str, pd.DataFrame], 
                                min_strategies: int = 2) -> pd.DataFrame:
        """
        获取多个策略的交集股票
        
        Args:
            strategy_results: 策略结果字典
            min_strategies: 最少出现在几个策略中
            
        Returns:
            交集股票DataFrame
        """
        if not strategy_results:
            return pd.DataFrame()
        
        # 收集所有股票及其出现次数
        symbol_counts = {}
        symbol_data = {}
        
        for strategy_name, df in strategy_results.items():
            if df.empty:
                continue
                
            for _, row in df.iterrows():
                symbol = row['symbol']
                
                if symbol not in symbol_counts:
                    symbol_counts[symbol] = 0
                    symbol_data[symbol] = row.to_dict()
                
                symbol_counts[symbol] += 1
                
                # 更新为最高评分的数据
                if row['comprehensive_score'] > symbol_data[symbol]['comprehensive_score']:
                    symbol_data[symbol] = row.to_dict()
        
        # 筛选出现在足够多策略中的股票
        intersection_symbols = [
            symbol for symbol, count in symbol_counts.items() 
            if count >= min_strategies
        ]
        
        if intersection_symbols:
            intersection_data = [symbol_data[symbol] for symbol in intersection_symbols]
            intersection_df = pd.DataFrame(intersection_data)
            intersection_df['strategy_count'] = [symbol_counts[symbol] for symbol in intersection_symbols]
            intersection_df = intersection_df.sort_values(['strategy_count', 'comprehensive_score'], 
                                                        ascending=[False, False]).reset_index(drop=True)
            
            logger.info(f"找到 {len(intersection_df)} 只股票出现在至少 {min_strategies} 个策略中")
            return intersection_df
        else:
            logger.info(f"没有股票出现在至少 {min_strategies} 个策略中")
            return pd.DataFrame()
    
    def save_strategy_results(self, strategy_results: Dict[str, pd.DataFrame]) -> bool:
        """
        保存策略结果到数据库
        
        Args:
            strategy_results: 策略结果字典
            
        Returns:
            是否保存成功
        """
        try:
            total_saved = 0
            
            for strategy_name, df in strategy_results.items():
                if df.empty:
                    continue
                
                # 添加策略信息
                df_to_save = df.copy()
                df_to_save['strategy_name'] = strategy_name
                
                # 保存到数据库
                count = self.db.save_selection_results(df_to_save)
                total_saved += count
                
                logger.info(f"策略 {strategy_name} 结果已保存，共 {count} 条记录")
            
            logger.info(f"所有策略结果保存完成，共 {total_saved} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"保存策略结果时出错: {e}")
            return False


def main():
    """测试选股策略引擎"""
    from database import DatabaseManager
    
    # 初始化
    db = DatabaseManager()
    strategy_engine = StockStrategyEngine(db)
    
    print("=== 选股策略引擎测试 ===")
    
    # 1. 显示可用策略
    strategies = strategy_engine.get_available_strategies()
    print(f"\n可用策略 ({len(strategies)} 个):")
    for name, config in strategies.items():
        print(f"  {name}: {config['name']}")
        print(f"    {config['description']}")
    
    # 2. 执行单个策略
    test_strategy = 'momentum_breakout'
    print(f"\n执行策略: {test_strategy}")
    
    result = strategy_engine.execute_strategy(test_strategy, max_results=10)
    
    if not result.empty:
        print(f"选股结果 (前5只):")
        for i, row in result.head(5).iterrows():
            print(f"  {i+1}. {row['symbol']} {row['name']}: {row['comprehensive_score']:.2f}分")
    else:
        print("没有找到符合条件的股票")
    
    # 3. 执行多策略
    print(f"\n执行多策略组合...")
    
    multi_results = strategy_engine.execute_multiple_strategies(
        ['momentum_breakout', 'technical_reversal'], 
        max_results_per_strategy=10
    )
    
    for strategy_name, df in multi_results.items():
        print(f"  {strategy_name}: {len(df)} 只股票")
    
    # 4. 获取策略交集
    intersection = strategy_engine.get_strategy_intersection(multi_results, min_strategies=2)
    
    if not intersection.empty:
        print(f"\n策略交集股票 ({len(intersection)} 只):")
        for i, row in intersection.iterrows():
            print(f"  {i+1}. {row['symbol']} {row['name']}: {row['comprehensive_score']:.2f}分 "
                  f"(出现在{row['strategy_count']}个策略中)")


if __name__ == "__main__":
    main()