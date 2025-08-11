"""
选股逻辑模块
实现基于涨幅、量比、换手率和技术指标的多维度选股策略
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
import logging
from datetime import datetime, timedelta
from database import DatabaseManager
from technical_indicators import TechnicalIndicators
from utils import config_manager, calculate_technical_score, safe_divide

logger = logging.getLogger(__name__)


class StockSelector:
    """股票选择器类"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        初始化选股器
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.tech_indicators = TechnicalIndicators(db_manager)
        self.config = config_manager
    
    def get_stock_basic_data(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """
        获取股票基础数据
        
        Args:
            symbols: 股票代码列表，如果为None则获取所有股票
            
        Returns:
            股票基础数据DataFrame
        """
        try:
            # 获取股票列表
            stock_list = self.db.get_stock_list()
            
            if stock_list.empty:
                logger.warning("数据库中没有股票列表")
                return pd.DataFrame()
            
            # 筛选指定股票
            if symbols:
                stock_list = stock_list[stock_list['symbol'].isin(symbols)]
            
            return stock_list
            
        except Exception as e:
            logger.error(f"获取股票基础数据失败: {e}")
            return pd.DataFrame()
    
    def calculate_price_changes(self, symbols: List[str], days: int = 1) -> pd.DataFrame:
        """
        计算股票价格变化
        
        Args:
            symbols: 股票代码列表
            days: 计算天数
            
        Returns:
            包含价格变化信息的DataFrame
        """
        results = []
        
        for symbol in symbols:
            try:
                # 获取历史数据
                hist_data = self.db.get_stock_data(symbol, days + 10)  # 多获取一些数据确保计算准确
                
                if len(hist_data) < days + 1:
                    continue
                
                # 计算价格变化
                latest_price = hist_data.iloc[-1]['close']
                previous_price = hist_data.iloc[-(days + 1)]['close']
                
                price_change = ((latest_price - previous_price) / previous_price) * 100
                
                results.append({
                    'symbol': symbol,
                    'current_price': latest_price,
                    'previous_price': previous_price,
                    'price_change': price_change,
                    'date': hist_data.iloc[-1]['date']
                })
                
            except Exception as e:
                logger.error(f"计算股票 {symbol} 价格变化失败: {e}")
                continue
        
        return pd.DataFrame(results)
    
    def calculate_volume_metrics(self, symbols: List[str]) -> pd.DataFrame:
        """
        计算成交量指标
        
        Args:
            symbols: 股票代码列表
            
        Returns:
            包含成交量指标的DataFrame
        """
        results = []
        
        for symbol in symbols:
            try:
                # 获取历史数据
                hist_data = self.db.get_stock_data(symbol, days=30)
                
                if len(hist_data) < 10:
                    continue
                
                # 获取最新数据
                latest_data = hist_data.iloc[-1]
                
                # 计算量比（当日成交量/近5日平均成交量）
                recent_volumes = hist_data['volume'].tail(6).iloc[:-1]  # 排除当日
                avg_volume = recent_volumes.mean()
                volume_ratio = safe_divide(latest_data['volume'], avg_volume, 0)
                
                # 获取换手率
                turnover_rate = latest_data.get('turnover_rate', 0)
                
                results.append({
                    'symbol': symbol,
                    'volume': latest_data['volume'],
                    'avg_volume': avg_volume,
                    'volume_ratio': volume_ratio,
                    'turnover_rate': turnover_rate,
                    'date': latest_data['date']
                })
                
            except Exception as e:
                logger.error(f"计算股票 {symbol} 成交量指标失败: {e}")
                continue
        
        return pd.DataFrame(results)
    
    def apply_basic_filters(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """
        应用基础筛选条件
        
        Args:
            stock_data: 股票数据DataFrame
            
        Returns:
            筛选后的DataFrame
        """
        if stock_data.empty:
            return stock_data
        
        filtered_data = stock_data.copy()
        
        # 获取筛选配置
        filters_config = self.config.get('filters', {})
        
        # 市场筛选 - 只选择A股市场
        allowed_markets = filters_config.get('markets', ['上海', '深圳'])
        excluded_markets = filters_config.get('exclude_markets', ['其他'])
        
        if 'market' in filtered_data.columns:
            # 只保留允许的市场
            filtered_data = filtered_data[filtered_data['market'].isin(allowed_markets)]
            # 排除不需要的市场
            filtered_data = filtered_data[~filtered_data['market'].isin(excluded_markets)]
            logger.info(f"市场筛选: 只保留 {allowed_markets} 市场的股票")
        
        # ST股票筛选
        exclude_st = filters_config.get('exclude_st', True)
        if exclude_st and 'name' in filtered_data.columns:
            before_count = len(filtered_data)
            filtered_data = filtered_data[~filtered_data['name'].str.contains('ST|退|暂停', na=False)]
            logger.info(f"ST股票筛选: {before_count} -> {len(filtered_data)} 只股票")
        
        # 价格区间筛选
        price_range = filters_config.get('price_range', [3, 200])
        if 'current_price' in filtered_data.columns:
            filtered_data = filtered_data[
                (filtered_data['current_price'] >= price_range[0]) &
                (filtered_data['current_price'] <= price_range[1])
            ]
        
        # 涨幅筛选
        min_change = filters_config.get('min_price_change', 1.0)
        max_change = filters_config.get('max_price_change', 20.0)
        if 'price_change' in filtered_data.columns:
            filtered_data = filtered_data[
                (filtered_data['price_change'] >= min_change) &
                (filtered_data['price_change'] <= max_change)
            ]
        
        # 量比筛选
        min_volume_ratio = filters_config.get('min_volume_ratio', 1.2)
        if 'volume_ratio' in filtered_data.columns:
            filtered_data = filtered_data[
                filtered_data['volume_ratio'] >= min_volume_ratio
            ]
        
        # 换手率筛选
        min_turnover = filters_config.get('min_turnover_rate', 2.0)
        if 'turnover_rate' in filtered_data.columns:
            filtered_data = filtered_data[
                filtered_data['turnover_rate'] >= min_turnover
            ]
        
        logger.info(f"基础筛选: {len(stock_data)} -> {len(filtered_data)} 只股票")
        
        return filtered_data.reset_index(drop=True)
    
    def apply_technical_filters(self, symbols: List[str]) -> pd.DataFrame:
        """
        应用技术指标筛选
        
        Args:
            symbols: 股票代码列表
            
        Returns:
            包含技术指标信号的DataFrame
        """
        results = []
        
        for symbol in symbols:
            try:
                # 计算技术指标信号
                signals = self.tech_indicators.calculate_signals(symbol)
                
                # 获取最新技术指标值
                latest_indicators = self.tech_indicators.get_latest_indicators(symbol)
                
                results.append({
                    'symbol': symbol,
                    'macd_signal': signals.get('macd_signal', '观望'),
                    'rsi_signal': signals.get('rsi_signal', '正常'),
                    'ma_signal': signals.get('ma_signal', '观望'),
                    'macd': latest_indicators.get('macd', 0),
                    'rsi': latest_indicators.get('rsi', 50),
                    'volume_ratio_tech': latest_indicators.get('volume_ratio', 1)
                })
                
            except Exception as e:
                logger.error(f"计算股票 {symbol} 技术指标失败: {e}")
                continue
        
        return pd.DataFrame(results)
    
    def calculate_comprehensive_score(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """
        计算综合评分
        
        Args:
            stock_data: 股票数据DataFrame
            
        Returns:
            包含综合评分的DataFrame
        """
        if stock_data.empty:
            return stock_data
        
        data_with_score = stock_data.copy()
        
        # 获取策略配置
        strategy_config = self.config.get('strategy', {})
        
        # 价格动量权重
        price_weight = strategy_config.get('price_momentum', {}).get('weight', 0.3)
        
        # 成交量权重
        volume_weight = strategy_config.get('volume_strategy', {}).get('weight', 0.25)
        
        # 技术指标权重
        technical_weight = strategy_config.get('technical_strategy', {}).get('weight', 0.45)
        
        scores = []
        
        for _, row in data_with_score.iterrows():
            score = 0.0
            
            # 价格动量得分 (0-100)
            price_change = row.get('price_change', 0)
            price_score = min(max(price_change * 5, 0), 100)  # 涨幅*5，最高100分
            score += price_score * price_weight
            
            # 成交量得分 (0-100)
            volume_ratio = row.get('volume_ratio', 1)
            turnover_rate = row.get('turnover_rate', 0)
            
            volume_score = min(volume_ratio * 30, 60)  # 量比得分，最高60分
            turnover_score = min(turnover_rate * 2, 40)  # 换手率得分，最高40分
            total_volume_score = volume_score + turnover_score
            
            score += total_volume_score * volume_weight
            
            # 技术指标得分 (0-100)
            macd_signal = row.get('macd_signal', '观望')
            rsi_signal = row.get('rsi_signal', '正常')
            ma_signal = row.get('ma_signal', '观望')
            
            technical_score = calculate_technical_score(macd_signal, rsi_signal, ma_signal)
            score += technical_score * technical_weight
            
            scores.append(round(score, 2))
        
        data_with_score['total_score'] = scores
        
        # 按得分排序
        data_with_score = data_with_score.sort_values('total_score', ascending=False)
        
        return data_with_score.reset_index(drop=True)
    
    def select_stocks(self, max_results: Optional[int] = None) -> pd.DataFrame:
        """
        执行完整的选股流程
        
        Args:
            max_results: 最大返回结果数
            
        Returns:
            选股结果DataFrame
        """
        if max_results is None:
            max_results = self.config.get('output.max_results', 50)
        
        logger.info("开始执行选股流程...")
        
        # 1. 获取股票基础数据
        stock_list = self.get_stock_basic_data()
        if stock_list.empty:
            logger.warning("没有可用的股票数据")
            return pd.DataFrame()
        
        symbols = stock_list['symbol'].tolist()
        logger.info(f"获取到 {len(symbols)} 只股票")
        
        # 2. 计算价格变化
        logger.info("计算价格变化...")
        price_data = self.calculate_price_changes(symbols)
        
        if price_data.empty:
            logger.warning("没有价格变化数据")
            return pd.DataFrame()
        
        # 3. 计算成交量指标
        logger.info("计算成交量指标...")
        volume_data = self.calculate_volume_metrics(price_data['symbol'].tolist())
        
        # 4. 合并基础数据
        basic_data = price_data.merge(volume_data, on='symbol', how='inner', suffixes=('', '_vol'))
        basic_data = basic_data.merge(stock_list[['symbol', 'name', 'market']], on='symbol', how='left')
        
        # 5. 应用基础筛选
        logger.info("应用基础筛选条件...")
        filtered_data = self.apply_basic_filters(basic_data)
        
        if filtered_data.empty:
            logger.warning("基础筛选后没有符合条件的股票")
            return pd.DataFrame()
        
        # 6. 应用技术指标筛选
        logger.info("计算技术指标...")
        technical_data = self.apply_technical_filters(filtered_data['symbol'].tolist())
        
        # 7. 合并所有数据
        final_data = filtered_data.merge(technical_data, on='symbol', how='left')
        
        # 8. 计算综合评分
        logger.info("计算综合评分...")
        scored_data = self.calculate_comprehensive_score(final_data)
        
        # 9. 返回前N个结果
        result = scored_data.head(max_results)
        
        logger.info(f"选股完成，共选出 {len(result)} 只股票")
        
        return result
    
    def get_top_gainers(self, top_n: int = 100) -> pd.DataFrame:
        """
        获取涨幅榜前N名
        
        Args:
            top_n: 返回数量
            
        Returns:
            涨幅榜DataFrame
        """
        logger.info(f"获取涨幅榜前 {top_n} 名...")
        
        # 获取所有股票
        stock_list = self.get_stock_basic_data()
        if stock_list.empty:
            return pd.DataFrame()
        
        # 计算价格变化
        price_data = self.calculate_price_changes(stock_list['symbol'].tolist())
        
        if price_data.empty:
            return pd.DataFrame()
        
        # 合并股票名称
        result = price_data.merge(stock_list[['symbol', 'name']], on='symbol', how='left')
        
        # 按涨幅排序
        result = result.sort_values('price_change', ascending=False)
        
        return result.head(top_n).reset_index(drop=True)
    
    def get_high_volume_stocks(self, min_ratio: float = 2.0, top_n: int = 100) -> pd.DataFrame:
        """
        获取高量比股票
        
        Args:
            min_ratio: 最小量比
            top_n: 返回数量
            
        Returns:
            高量比股票DataFrame
        """
        logger.info(f"获取量比大于 {min_ratio} 的前 {top_n} 只股票...")
        
        # 获取所有股票
        stock_list = self.get_stock_basic_data()
        if stock_list.empty:
            return pd.DataFrame()
        
        # 计算成交量指标
        volume_data = self.calculate_volume_metrics(stock_list['symbol'].tolist())
        
        if volume_data.empty:
            return pd.DataFrame()
        
        # 筛选高量比股票
        high_volume = volume_data[volume_data['volume_ratio'] >= min_ratio]
        
        # 合并股票名称
        result = high_volume.merge(stock_list[['symbol', 'name']], on='symbol', how='left')
        
        # 按量比排序
        result = result.sort_values('volume_ratio', ascending=False)
        
        return result.head(top_n).reset_index(drop=True)
    
    def backtest_strategy(self, days: int = 30) -> Dict[str, float]:
        """
        简单的策略回测
        
        Args:
            days: 回测天数
            
        Returns:
            回测结果字典
        """
        logger.info(f"开始 {days} 天策略回测...")
        
        # 这里实现简单的回测逻辑
        # 实际应用中需要更复杂的回测框架
        
        return {
            'total_return': 0.0,
            'win_rate': 0.0,
            'max_drawdown': 0.0,
            'sharpe_ratio': 0.0
        }


def main():
    """测试选股功能"""
    from database import DatabaseManager
    
    # 初始化
    db = DatabaseManager()
    selector = StockSelector(db)
    
    print("开始选股测试...")
    
    # 执行选股
    results = selector.select_stocks(max_results=10)
    
    if not results.empty:
        print(f"\n选股结果 (前10名):")
        print("-" * 80)
        
        for _, row in results.iterrows():
            print(f"股票: {row['symbol']} {row.get('name', 'N/A')}")
            print(f"  价格: {row.get('current_price', 0):.2f} 涨幅: {row.get('price_change', 0):.2f}%")
            print(f"  量比: {row.get('volume_ratio', 0):.2f} 换手率: {row.get('turnover_rate', 0):.2f}%")
            print(f"  MACD: {row.get('macd_signal', 'N/A')} RSI: {row.get('rsi_signal', 'N/A')} MA: {row.get('ma_signal', 'N/A')}")
            print(f"  综合得分: {row.get('total_score', 0):.2f}")
            print("-" * 40)
    else:
        print("没有找到符合条件的股票")
    
    # 测试涨幅榜
    print("\n涨幅榜前5名:")
    gainers = selector.get_top_gainers(top_n=5)
    
    for _, row in gainers.iterrows():
        print(f"  {row['symbol']} {row.get('name', 'N/A')}: {row['price_change']:.2f}%")


if __name__ == "__main__":
    main()