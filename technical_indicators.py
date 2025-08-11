"""
技术指标计算模块
实现MACD、RSI、均线等技术指标的计算和信号识别
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional, List
import logging
from database import DatabaseManager
from utils import config_manager

logger = logging.getLogger(__name__)


class TechnicalIndicators:
    """技术指标计算类"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        初始化技术指标计算器
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.config = config_manager
    
    def calculate_ema(self, data: pd.Series, period: int) -> pd.Series:
        """
        计算指数移动平均线(EMA)
        
        Args:
            data: 价格数据序列
            period: 计算周期
            
        Returns:
            EMA序列
        """
        return data.ewm(span=period, adjust=False).mean()
    
    def calculate_sma(self, data: pd.Series, period: int) -> pd.Series:
        """
        计算简单移动平均线(SMA)
        
        Args:
            data: 价格数据序列
            period: 计算周期
            
        Returns:
            SMA序列
        """
        return data.rolling(window=period).mean()
    
    def calculate_macd(self, close_prices: pd.Series, 
                      fast_period: int = 12, 
                      slow_period: int = 26, 
                      signal_period: int = 9) -> pd.DataFrame:
        """
        计算MACD指标
        
        Args:
            close_prices: 收盘价序列
            fast_period: 快线周期
            slow_period: 慢线周期
            signal_period: 信号线周期
            
        Returns:
            包含MACD、信号线、柱状图的DataFrame
        """
        # 计算快慢EMA
        ema_fast = self.calculate_ema(close_prices, fast_period)
        ema_slow = self.calculate_ema(close_prices, slow_period)
        
        # 计算MACD线(DIF)
        macd_line = ema_fast - ema_slow
        
        # 计算信号线(DEA)
        signal_line = self.calculate_ema(macd_line, signal_period)
        
        # 计算MACD柱状图
        histogram = 2 * (macd_line - signal_line)
        
        result = pd.DataFrame({
            'macd': macd_line,
            'macd_signal': signal_line,
            'macd_histogram': histogram
        })
        
        return result
    
    def calculate_rsi(self, close_prices: pd.Series, period: int = 14) -> pd.Series:
        """
        计算RSI指标
        
        Args:
            close_prices: 收盘价序列
            period: 计算周期
            
        Returns:
            RSI序列
        """
        # 计算价格变化
        delta = close_prices.diff()
        
        # 分离上涨和下跌
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        # 计算平均涨跌幅
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        # 计算RS和RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def calculate_moving_averages(self, close_prices: pd.Series, 
                                periods: List[int] = None) -> pd.DataFrame:
        """
        计算多周期移动平均线
        
        Args:
            close_prices: 收盘价序列
            periods: 周期列表
            
        Returns:
            包含各周期均线的DataFrame
        """
        if periods is None:
            periods = self.config.get('technical.ma_periods', [5, 10, 20, 60])
        
        result = pd.DataFrame(index=close_prices.index)
        
        for period in periods:
            result[f'ma{period}'] = self.calculate_sma(close_prices, period)
        
        return result
    
    def calculate_volume_ratio(self, volume_data: pd.Series, period: int = 5) -> pd.Series:
        """
        计算量比
        
        Args:
            volume_data: 成交量序列
            period: 基准周期
            
        Returns:
            量比序列
        """
        # 计算平均成交量
        avg_volume = volume_data.rolling(window=period).mean()
        
        # 计算量比
        volume_ratio = volume_data / avg_volume
        
        return volume_ratio
    
    def detect_macd_signals(self, macd_data: pd.DataFrame) -> pd.Series:
        """
        识别MACD信号
        
        Args:
            macd_data: MACD数据DataFrame
            
        Returns:
            信号序列
        """
        signals = pd.Series(index=macd_data.index, dtype='object')
        signals[:] = '观望'
        
        macd = macd_data['macd']
        signal = macd_data['macd_signal']
        histogram = macd_data['macd_histogram']
        
        # 金叉：MACD线上穿信号线
        golden_cross = (macd > signal) & (macd.shift(1) <= signal.shift(1))
        signals[golden_cross] = '金叉'
        
        # 死叉：MACD线下穿信号线
        death_cross = (macd < signal) & (macd.shift(1) >= signal.shift(1))
        signals[death_cross] = '死叉'
        
        # 看涨：MACD线和信号线都在零轴上方
        bullish = (macd > 0) & (signal > 0) & (histogram > 0)
        signals[bullish & (signals == '观望')] = '看涨'
        
        # 看跌：MACD线和信号线都在零轴下方
        bearish = (macd < 0) & (signal < 0) & (histogram < 0)
        signals[bearish & (signals == '观望')] = '看跌'
        
        return signals
    
    def detect_rsi_signals(self, rsi_data: pd.Series, 
                          overbought: float = 70, 
                          oversold: float = 30) -> pd.Series:
        """
        识别RSI信号
        
        Args:
            rsi_data: RSI数据序列
            overbought: 超买阈值
            oversold: 超卖阈值
            
        Returns:
            信号序列
        """
        signals = pd.Series(index=rsi_data.index, dtype='object')
        signals[:] = '正常'
        
        # 超买
        signals[rsi_data > overbought] = '超买'
        
        # 超卖
        signals[rsi_data < oversold] = '超卖'
        
        # 超卖反弹：从超卖区域向上突破
        oversold_rebound = (rsi_data > oversold) & (rsi_data.shift(1) <= oversold)
        signals[oversold_rebound] = '超卖反弹'
        
        return signals
    
    def detect_ma_signals(self, price_data: pd.Series, ma_data: pd.DataFrame) -> pd.Series:
        """
        识别均线信号
        
        Args:
            price_data: 价格数据
            ma_data: 均线数据
            
        Returns:
            信号序列
        """
        signals = pd.Series(index=price_data.index, dtype='object')
        signals[:] = '观望'
        
        # 获取均线列
        ma_columns = [col for col in ma_data.columns if col.startswith('ma')]
        ma_columns.sort(key=lambda x: int(x[2:]))  # 按周期排序
        
        if len(ma_columns) < 2:
            return signals
        
        # 多头排列：短期均线在长期均线上方
        ma_short = ma_data[ma_columns[0]]
        ma_long = ma_data[ma_columns[-1]]
        
        bullish_alignment = ma_short > ma_long
        signals[bullish_alignment] = '多头排列'
        
        # 空头排列：短期均线在长期均线下方
        bearish_alignment = ma_short < ma_long
        signals[bearish_alignment] = '空头排列'
        
        # 突破：价格突破均线
        for ma_col in ma_columns:
            ma_line = ma_data[ma_col]
            breakthrough = (price_data > ma_line) & (price_data.shift(1) <= ma_line.shift(1))
            signals[breakthrough] = '突破'
        
        return signals
    
    def calculate_all_indicators(self, symbol: str) -> pd.DataFrame:
        """
        计算指定股票的所有技术指标
        
        Args:
            symbol: 股票代码
            
        Returns:
            包含所有技术指标的DataFrame
        """
        # 获取历史数据
        hist_data = self.db.get_stock_data(symbol, days=120)  # 获取更多数据以确保指标计算准确
        
        if hist_data.empty:
            logger.warning(f"股票 {symbol} 没有历史数据")
            return pd.DataFrame()
        
        # 确保数据按日期排序
        hist_data = hist_data.sort_values('date').reset_index(drop=True)
        
        # 提取价格和成交量数据
        close_prices = hist_data['close']
        volume_data = hist_data['volume']
        
        # 计算技术指标
        result = pd.DataFrame()
        result['date'] = hist_data['date']
        result['close'] = close_prices
        
        # MACD指标
        macd_config = self.config.get('technical.macd', {})
        macd_data = self.calculate_macd(
            close_prices,
            fast_period=macd_config.get('fast_period', 12),
            slow_period=macd_config.get('slow_period', 26),
            signal_period=macd_config.get('signal_period', 9)
        )
        result = pd.concat([result, macd_data], axis=1)
        
        # RSI指标
        rsi_config = self.config.get('technical.rsi', {})
        result['rsi'] = self.calculate_rsi(
            close_prices,
            period=rsi_config.get('period', 14)
        )
        
        # 移动平均线
        ma_periods = self.config.get('technical.ma_periods', [5, 10, 20, 60])
        ma_data = self.calculate_moving_averages(close_prices, ma_periods)
        result = pd.concat([result, ma_data], axis=1)
        
        # 量比
        volume_config = self.config.get('technical.volume_ratio', {})
        result['volume_ratio'] = self.calculate_volume_ratio(
            volume_data,
            period=volume_config.get('base_period', 5)
        )
        
        return result
    
    def calculate_signals(self, symbol: str) -> Dict[str, str]:
        """
        计算指定股票的技术指标信号
        
        Args:
            symbol: 股票代码
            
        Returns:
            信号字典
        """
        # 获取技术指标数据
        indicators = self.calculate_all_indicators(symbol)
        
        if indicators.empty:
            return {
                'macd_signal': '无数据',
                'rsi_signal': '无数据',
                'ma_signal': '无数据'
            }
        
        # 获取最新数据
        latest_data = indicators.iloc[-1]
        
        # 计算信号
        signals = {}
        
        # MACD信号
        macd_data = indicators[['macd', 'macd_signal', 'macd_histogram']].dropna()
        if not macd_data.empty:
            macd_signals = self.detect_macd_signals(macd_data)
            signals['macd_signal'] = macd_signals.iloc[-1] if not macd_signals.empty else '观望'
        else:
            signals['macd_signal'] = '无数据'
        
        # RSI信号
        if 'rsi' in indicators.columns:
            rsi_config = self.config.get('technical.rsi', {})
            rsi_signals = self.detect_rsi_signals(
                indicators['rsi'],
                overbought=rsi_config.get('overbought', 70),
                oversold=rsi_config.get('oversold', 30)
            )
            signals['rsi_signal'] = rsi_signals.iloc[-1] if not rsi_signals.empty else '正常'
        else:
            signals['rsi_signal'] = '无数据'
        
        # 均线信号
        ma_columns = [col for col in indicators.columns if col.startswith('ma')]
        if ma_columns and 'close' in indicators.columns:
            ma_data = indicators[ma_columns]
            ma_signals = self.detect_ma_signals(indicators['close'], ma_data)
            signals['ma_signal'] = ma_signals.iloc[-1] if not ma_signals.empty else '观望'
        else:
            signals['ma_signal'] = '无数据'
        
        return signals
    
    def batch_calculate_indicators(self, symbols: List[str]) -> Dict[str, int]:
        """
        批量计算技术指标并存储到数据库
        
        Args:
            symbols: 股票代码列表
            
        Returns:
            处理结果字典
        """
        results = {}
        total_symbols = len(symbols)
        
        logger.info(f"开始批量计算 {total_symbols} 只股票的技术指标...")
        
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"正在计算 {symbol} 技术指标 ({i}/{total_symbols})")
                
                # 计算技术指标
                indicators = self.calculate_all_indicators(symbol)
                
                if not indicators.empty:
                    # 存储到数据库
                    count = self.db.insert_technical_indicators(symbol, indicators)
                    results[symbol] = count
                else:
                    results[symbol] = 0
                    
            except Exception as e:
                logger.error(f"计算股票 {symbol} 技术指标失败: {e}")
                results[symbol] = 0
        
        # 统计结果
        total_updated = sum(results.values())
        success_count = sum(1 for count in results.values() if count > 0)
        
        logger.info(f"批量计算完成: 成功 {success_count}/{total_symbols} 只股票，共计算 {total_updated} 条指标记录")
        
        return results
    
    def get_latest_indicators(self, symbol: str) -> Dict[str, float]:
        """
        获取指定股票的最新技术指标值
        
        Args:
            symbol: 股票代码
            
        Returns:
            最新指标值字典
        """
        try:
            conn = self.db.get_connection()
            query = '''
                SELECT * FROM technical_indicators 
                WHERE symbol = ? 
                ORDER BY date DESC 
                LIMIT 1
            '''
            
            result = conn.execute(query, (symbol,)).fetchone()
            conn.close()
            
            if result:
                return {
                    'macd': result['macd'],
                    'macd_signal': result['macd_signal'],
                    'macd_histogram': result['macd_histogram'],
                    'rsi': result['rsi'],
                    'ma5': result['ma5'],
                    'ma10': result['ma10'],
                    'ma20': result['ma20'],
                    'ma60': result['ma60'],
                    'volume_ratio': result['volume_ratio']
                }
            else:
                return {}
                
        except Exception as e:
            logger.error(f"获取股票 {symbol} 最新技术指标失败: {e}")
            return {}


def main():
    """测试技术指标计算功能"""
    from database import DatabaseManager
    
    # 初始化
    db = DatabaseManager()
    tech_indicators = TechnicalIndicators(db)
    
    # 测试股票
    test_symbol = '000001'
    
    print(f"测试股票: {test_symbol}")
    
    # 计算技术指标
    print("1. 计算技术指标...")
    indicators = tech_indicators.calculate_all_indicators(test_symbol)
    
    if not indicators.empty:
        print(f"计算完成，共 {len(indicators)} 条记录")
        print("\n最新指标值:")
        latest = indicators.iloc[-1]
        print(f"  MACD: {latest.get('macd', 'N/A'):.4f}")
        print(f"  RSI: {latest.get('rsi', 'N/A'):.2f}")
        print(f"  MA5: {latest.get('ma5', 'N/A'):.2f}")
        print(f"  MA20: {latest.get('ma20', 'N/A'):.2f}")
        print(f"  量比: {latest.get('volume_ratio', 'N/A'):.2f}")
    
    # 计算信号
    print("\n2. 计算技术信号...")
    signals = tech_indicators.calculate_signals(test_symbol)
    
    for signal_type, signal_value in signals.items():
        print(f"  {signal_type}: {signal_value}")


if __name__ == "__main__":
    main()