"""
增强版技术指标计算模块
在原有基础上添加更多技术指标和优化算法
包括：BOLL、KDJ、CCI、威廉指标、动量指标等
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional, List
import logging
from database import DatabaseManager
from utils import config_manager

logger = logging.getLogger(__name__)


class EnhancedTechnicalIndicators:
    """增强版技术指标计算类"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        初始化增强版技术指标计算器
        
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
        计算RSI指标 - 使用威尔德平滑方法
        
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
        
        # 使用威尔德平滑方法计算平均涨跌幅
        alpha = 1.0 / period
        avg_gain = gain.ewm(alpha=alpha, adjust=False).mean()
        avg_loss = loss.ewm(alpha=alpha, adjust=False).mean()
        
        # 计算RS和RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def calculate_bollinger_bands(self, close_prices: pd.Series, 
                                 period: int = 20, 
                                 std_dev: float = 2.0) -> pd.DataFrame:
        """
        计算布林带指标
        
        Args:
            close_prices: 收盘价序列
            period: 计算周期
            std_dev: 标准差倍数
            
        Returns:
            包含上轨、中轨、下轨的DataFrame
        """
        # 计算中轨（移动平均线）
        middle_band = self.calculate_sma(close_prices, period)
        
        # 计算标准差
        std = close_prices.rolling(window=period).std()
        
        # 计算上下轨
        upper_band = middle_band + (std * std_dev)
        lower_band = middle_band - (std * std_dev)
        
        # 计算布林带宽度和位置
        bb_width = (upper_band - lower_band) / middle_band * 100
        bb_position = (close_prices - lower_band) / (upper_band - lower_band) * 100
        
        result = pd.DataFrame({
            'bb_upper': upper_band,
            'bb_middle': middle_band,
            'bb_lower': lower_band,
            'bb_width': bb_width,
            'bb_position': bb_position
        })
        
        return result
    
    def calculate_kdj(self, high_prices: pd.Series, 
                     low_prices: pd.Series, 
                     close_prices: pd.Series,
                     k_period: int = 9,
                     d_period: int = 3,
                     j_period: int = 3) -> pd.DataFrame:
        """
        计算KDJ指标
        
        Args:
            high_prices: 最高价序列
            low_prices: 最低价序列
            close_prices: 收盘价序列
            k_period: K值计算周期
            d_period: D值平滑周期
            j_period: J值平滑周期
            
        Returns:
            包含K、D、J值的DataFrame
        """
        # 计算最高价和最低价的滚动窗口
        lowest_low = low_prices.rolling(window=k_period).min()
        highest_high = high_prices.rolling(window=k_period).max()
        
        # 计算RSV（未成熟随机值）
        rsv = (close_prices - lowest_low) / (highest_high - lowest_low) * 100
        
        # 计算K值（RSV的移动平均）
        k_values = rsv.ewm(span=d_period, adjust=False).mean()
        
        # 计算D值（K值的移动平均）
        d_values = k_values.ewm(span=j_period, adjust=False).mean()
        
        # 计算J值
        j_values = 3 * k_values - 2 * d_values
        
        result = pd.DataFrame({
            'kdj_k': k_values,
            'kdj_d': d_values,
            'kdj_j': j_values,
            'kdj_rsv': rsv
        })
        
        return result
    
    def calculate_cci(self, high_prices: pd.Series,
                     low_prices: pd.Series,
                     close_prices: pd.Series,
                     period: int = 14) -> pd.Series:
        """
        计算CCI指标（顺势指标）
        
        Args:
            high_prices: 最高价序列
            low_prices: 最低价序列
            close_prices: 收盘价序列
            period: 计算周期
            
        Returns:
            CCI序列
        """
        # 计算典型价格
        typical_price = (high_prices + low_prices + close_prices) / 3
        
        # 计算典型价格的移动平均
        sma_tp = typical_price.rolling(window=period).mean()
        
        # 计算平均绝对偏差
        mad = typical_price.rolling(window=period).apply(
            lambda x: np.mean(np.abs(x - x.mean())), raw=True
        )
        
        # 计算CCI
        cci = (typical_price - sma_tp) / (0.015 * mad)
        
        return cci
    
    def calculate_williams_r(self, high_prices: pd.Series,
                           low_prices: pd.Series,
                           close_prices: pd.Series,
                           period: int = 14) -> pd.Series:
        """
        计算威廉指标(%R)
        
        Args:
            high_prices: 最高价序列
            low_prices: 最低价序列
            close_prices: 收盘价序列
            period: 计算周期
            
        Returns:
            威廉指标序列
        """
        # 计算周期内最高价和最低价
        highest_high = high_prices.rolling(window=period).max()
        lowest_low = low_prices.rolling(window=period).min()
        
        # 计算威廉指标
        williams_r = (highest_high - close_prices) / (highest_high - lowest_low) * (-100)
        
        return williams_r
    
    def calculate_momentum(self, close_prices: pd.Series, period: int = 10) -> pd.Series:
        """
        计算动量指标
        
        Args:
            close_prices: 收盘价序列
            period: 计算周期
            
        Returns:
            动量指标序列
        """
        momentum = close_prices / close_prices.shift(period) * 100
        return momentum
    
    def calculate_roc(self, close_prices: pd.Series, period: int = 12) -> pd.Series:
        """
        计算变动率指标(ROC)
        
        Args:
            close_prices: 收盘价序列
            period: 计算周期
            
        Returns:
            ROC序列
        """
        roc = (close_prices - close_prices.shift(period)) / close_prices.shift(period) * 100
        return roc
    
    def calculate_obv(self, close_prices: pd.Series, volume: pd.Series) -> pd.Series:
        """
        计算能量潮指标(OBV)
        
        Args:
            close_prices: 收盘价序列
            volume: 成交量序列
            
        Returns:
            OBV序列
        """
        # 计算价格变化方向
        price_change = close_prices.diff()
        
        # 根据价格变化方向调整成交量
        obv_volume = volume.copy()
        obv_volume[price_change < 0] = -obv_volume[price_change < 0]
        obv_volume[price_change == 0] = 0
        
        # 计算累积成交量
        obv = obv_volume.cumsum()
        
        return obv
    
    def calculate_atr(self, high_prices: pd.Series,
                     low_prices: pd.Series,
                     close_prices: pd.Series,
                     period: int = 14) -> pd.Series:
        """
        计算平均真实波幅(ATR)
        
        Args:
            high_prices: 最高价序列
            low_prices: 最低价序列
            close_prices: 收盘价序列
            period: 计算周期
            
        Returns:
            ATR序列
        """
        # 计算真实波幅
        tr1 = high_prices - low_prices
        tr2 = np.abs(high_prices - close_prices.shift(1))
        tr3 = np.abs(low_prices - close_prices.shift(1))
        
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        # 计算ATR（真实波幅的移动平均）
        atr = true_range.rolling(window=period).mean()
        
        return atr
    
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
    
    def calculate_all_indicators(self, symbol: str) -> pd.DataFrame:
        """
        计算指定股票的所有技术指标
        
        Args:
            symbol: 股票代码
            
        Returns:
            包含所有技术指标的DataFrame
        """
        # 获取历史数据
        hist_data = self.db.get_stock_data(symbol, days=120)
        
        if hist_data.empty:
            logger.warning(f"股票 {symbol} 没有历史数据")
            return pd.DataFrame()
        
        # 确保数据按日期排序
        hist_data = hist_data.sort_values('date').reset_index(drop=True)
        
        # 提取价格和成交量数据
        high_prices = hist_data['high']
        low_prices = hist_data['low']
        close_prices = hist_data['close']
        volume_data = hist_data['volume']
        
        # 初始化结果DataFrame
        result = pd.DataFrame()
        result['date'] = hist_data['date']
        result['symbol'] = symbol
        result['close'] = close_prices
        result['high'] = high_prices
        result['low'] = low_prices
        result['volume'] = volume_data
        
        try:
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
            for period in ma_periods:
                result[f'ma{period}'] = self.calculate_sma(close_prices, period)
            
            # 布林带
            bb_config = self.config.get('technical.bollinger', {})
            bb_data = self.calculate_bollinger_bands(
                close_prices,
                period=bb_config.get('period', 20),
                std_dev=bb_config.get('std_dev', 2.0)
            )
            result = pd.concat([result, bb_data], axis=1)
            
            # KDJ指标
            kdj_config = self.config.get('technical.kdj', {})
            kdj_data = self.calculate_kdj(
                high_prices, low_prices, close_prices,
                k_period=kdj_config.get('k_period', 9),
                d_period=kdj_config.get('d_period', 3),
                j_period=kdj_config.get('j_period', 3)
            )
            result = pd.concat([result, kdj_data], axis=1)
            
            # CCI指标
            cci_config = self.config.get('technical.cci', {})
            result['cci'] = self.calculate_cci(
                high_prices, low_prices, close_prices,
                period=cci_config.get('period', 14)
            )
            
            # 威廉指标
            wr_config = self.config.get('technical.williams_r', {})
            result['williams_r'] = self.calculate_williams_r(
                high_prices, low_prices, close_prices,
                period=wr_config.get('period', 14)
            )
            
            # 动量指标
            momentum_config = self.config.get('technical.momentum', {})
            result['momentum'] = self.calculate_momentum(
                close_prices,
                period=momentum_config.get('period', 10)
            )
            
            # ROC指标
            roc_config = self.config.get('technical.roc', {})
            result['roc'] = self.calculate_roc(
                close_prices,
                period=roc_config.get('period', 12)
            )
            
            # OBV指标
            result['obv'] = self.calculate_obv(close_prices, volume_data)
            
            # ATR指标
            atr_config = self.config.get('technical.atr', {})
            result['atr'] = self.calculate_atr(
                high_prices, low_prices, close_prices,
                period=atr_config.get('period', 14)
            )
            
            # 量比
            volume_config = self.config.get('technical.volume_ratio', {})
            result['volume_ratio'] = self.calculate_volume_ratio(
                volume_data,
                period=volume_config.get('base_period', 5)
            )
            
        except Exception as e:
            logger.error(f"计算股票 {symbol} 技术指标时出错: {e}")
            
        return result
    
    def detect_comprehensive_signals(self, symbol: str) -> Dict[str, str]:
        """
        综合技术指标信号检测
        
        Args:
            symbol: 股票代码
            
        Returns:
            综合信号字典
        """
        indicators = self.calculate_all_indicators(symbol)
        
        if indicators.empty:
            return {'综合信号': '无数据'}
        
        # 获取最新数据
        latest = indicators.iloc[-1]
        signals = {}
        
        # MACD信号
        if pd.notna(latest.get('macd')) and pd.notna(latest.get('macd_signal')):
            if latest['macd'] > latest['macd_signal']:
                if latest['macd'] > 0:
                    signals['MACD'] = '强势看涨'
                else:
                    signals['MACD'] = '弱势看涨'
            else:
                if latest['macd'] < 0:
                    signals['MACD'] = '强势看跌'
                else:
                    signals['MACD'] = '弱势看跌'
        else:
            signals['MACD'] = '无信号'
        
        # RSI信号
        rsi = latest.get('rsi')
        if pd.notna(rsi):
            if rsi > 80:
                signals['RSI'] = '严重超买'
            elif rsi > 70:
                signals['RSI'] = '超买'
            elif rsi < 20:
                signals['RSI'] = '严重超卖'
            elif rsi < 30:
                signals['RSI'] = '超卖'
            else:
                signals['RSI'] = '正常'
        else:
            signals['RSI'] = '无信号'
        
        # 布林带信号
        bb_pos = latest.get('bb_position')
        if pd.notna(bb_pos):
            if bb_pos > 80:
                signals['布林带'] = '接近上轨'
            elif bb_pos < 20:
                signals['布林带'] = '接近下轨'
            else:
                signals['布林带'] = '正常区间'
        else:
            signals['布林带'] = '无信号'
        
        # KDJ信号
        kdj_k = latest.get('kdj_k')
        kdj_d = latest.get('kdj_d')
        if pd.notna(kdj_k) and pd.notna(kdj_d):
            if kdj_k > 80 and kdj_d > 80:
                signals['KDJ'] = '超买'
            elif kdj_k < 20 and kdj_d < 20:
                signals['KDJ'] = '超卖'
            elif kdj_k > kdj_d:
                signals['KDJ'] = '看涨'
            else:
                signals['KDJ'] = '看跌'
        else:
            signals['KDJ'] = '无信号'
        
        # 综合评分
        bullish_signals = 0
        bearish_signals = 0
        
        for signal in signals.values():
            if '看涨' in signal or '超卖' in signal or '接近下轨' in signal:
                bullish_signals += 1
            elif '看跌' in signal or '超买' in signal or '接近上轨' in signal:
                bearish_signals += 1
        
        if bullish_signals > bearish_signals:
            signals['综合信号'] = '偏多'
        elif bearish_signals > bullish_signals:
            signals['综合信号'] = '偏空'
        else:
            signals['综合信号'] = '中性'
        
        return signals
    
    def calculate_technical_score(self, symbol: str) -> float:
        """
        计算技术指标综合评分
        
        Args:
            symbol: 股票代码
            
        Returns:
            技术指标评分 (0-100)
        """
        indicators = self.calculate_all_indicators(symbol)
        
        if indicators.empty:
            return 0.0
        
        latest = indicators.iloc[-1]
        score = 50.0  # 基础分数
        
        try:
            # MACD评分 (权重: 25%)
            macd = latest.get('macd', 0)
            macd_signal = latest.get('macd_signal', 0)
            if pd.notna(macd) and pd.notna(macd_signal):
                if macd > macd_signal and macd > 0:
                    score += 12.5
                elif macd > macd_signal:
                    score += 6.25
                elif macd < macd_signal and macd < 0:
                    score -= 12.5
                elif macd < macd_signal:
                    score -= 6.25
            
            # RSI评分 (权重: 20%)
            rsi = latest.get('rsi', 50)
            if pd.notna(rsi):
                if 30 <= rsi <= 70:
                    score += 10  # 正常区间
                elif 20 <= rsi < 30:
                    score += 15  # 超卖反弹机会
                elif rsi < 20:
                    score += 5   # 严重超卖
                elif 70 < rsi <= 80:
                    score -= 10  # 超买
                else:
                    score -= 15  # 严重超买
            
            # 布林带评分 (权重: 15%)
            bb_pos = latest.get('bb_position', 50)
            if pd.notna(bb_pos):
                if 20 <= bb_pos <= 80:
                    score += 7.5
                elif bb_pos < 20:
                    score += 10  # 接近下轨，反弹机会
                else:
                    score -= 10  # 接近上轨，回调风险
            
            # KDJ评分 (权重: 15%)
            kdj_k = latest.get('kdj_k', 50)
            kdj_d = latest.get('kdj_d', 50)
            if pd.notna(kdj_k) and pd.notna(kdj_d):
                if kdj_k > kdj_d and kdj_k < 80:
                    score += 7.5
                elif kdj_k < kdj_d and kdj_k > 20:
                    score -= 7.5
                elif kdj_k < 20 and kdj_d < 20:
                    score += 5  # 超卖
                elif kdj_k > 80 and kdj_d > 80:
                    score -= 10  # 超买
            
            # 均线评分 (权重: 15%)
            ma5 = latest.get('ma5')
            ma20 = latest.get('ma20')
            close = latest.get('close')
            if pd.notna(ma5) and pd.notna(ma20) and pd.notna(close):
                if close > ma5 > ma20:
                    score += 7.5  # 多头排列
                elif close < ma5 < ma20:
                    score -= 7.5  # 空头排列
            
            # 成交量评分 (权重: 10%)
            volume_ratio = latest.get('volume_ratio', 1)
            if pd.notna(volume_ratio):
                if volume_ratio > 2:
                    score += 5  # 放量
                elif volume_ratio < 0.5:
                    score -= 2.5  # 缩量
            
        except Exception as e:
            logger.error(f"计算技术评分时出错: {e}")
        
        # 确保评分在0-100范围内
        return max(0, min(100, score))


def main():
    """测试增强版技术指标功能"""
    from database import DatabaseManager
    
    # 初始化
    db = DatabaseManager()
    enhanced_tech = EnhancedTechnicalIndicators(db)
    
    # 测试股票
    test_symbol = '000001'
    
    print(f"测试股票: {test_symbol}")
    
    # 计算所有技术指标
    print("1. 计算增强版技术指标...")
    indicators = enhanced_tech.calculate_all_indicators(test_symbol)
    
    if not indicators.empty:
        print(f"计算完成，共 {len(indicators)} 条记录")
        print(f"指标数量: {len(indicators.columns)} 个")
        
        # 显示最新指标值
        latest = indicators.iloc[-1]
        print("\n最新指标值:")
        for col in indicators.columns:
            if col not in ['date', 'symbol', 'close', 'high', 'low', 'volume']:
                value = latest.get(col)
                if pd.notna(value):
                    print(f"  {col}: {value:.4f}")
    
    # 计算综合信号
    print("\n2. 计算综合技术信号...")
    signals = enhanced_tech.detect_comprehensive_signals(test_symbol)
    
    for signal_type, signal_value in signals.items():
        print(f"  {signal_type}: {signal_value}")
    
    # 计算技术评分
    print("\n3. 计算技术指标评分...")
    score = enhanced_tech.calculate_technical_score(test_symbol)
    print(f"  技术指标综合评分: {score:.2f}/100")


if __name__ == "__main__":
    main()