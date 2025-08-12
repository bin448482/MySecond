"""
综合评分算法模块
整合技术指标、基本面、市场表现等多维度数据进行股票评分
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime, timedelta
from database import DatabaseManager
from enhanced_technical_indicators import EnhancedTechnicalIndicators
from utils import config_manager

logger = logging.getLogger(__name__)


class ComprehensiveScoring:
    """综合评分系统"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        初始化综合评分系统
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.tech_indicators = EnhancedTechnicalIndicators(db_manager)
        self.config = config_manager
        
        # 默认权重配置
        self.default_weights = {
            'technical': 0.40,      # 技术指标权重 40%
            'momentum': 0.25,       # 动量指标权重 25%
            'volume': 0.20,         # 成交量指标权重 20%
            'volatility': 0.10,     # 波动率指标权重 10%
            'market_sentiment': 0.05 # 市场情绪权重 5%
        }
    
    def calculate_technical_score(self, symbol: str) -> Dict[str, float]:
        """
        计算技术指标评分
        
        Args:
            symbol: 股票代码
            
        Returns:
            技术指标评分字典
        """
        try:
            indicators = self.tech_indicators.calculate_all_indicators(symbol)
            
            if indicators.empty:
                return {'technical_score': 0.0, 'details': {}}
            
            latest = indicators.iloc[-1]
            scores = {}
            
            # MACD评分 (0-25分)
            macd = latest.get('macd', 0)
            macd_signal = latest.get('macd_signal', 0)
            macd_hist = latest.get('macd_histogram', 0)
            
            if pd.notna(macd) and pd.notna(macd_signal):
                if macd > macd_signal and macd_hist > 0:
                    if macd > 0:
                        scores['macd'] = 25  # 强势金叉
                    else:
                        scores['macd'] = 20  # 弱势金叉
                elif macd > macd_signal:
                    scores['macd'] = 15
                elif macd < macd_signal and macd_hist < 0:
                    if macd < 0:
                        scores['macd'] = 0   # 强势死叉
                    else:
                        scores['macd'] = 5   # 弱势死叉
                else:
                    scores['macd'] = 10
            else:
                scores['macd'] = 10
            
            # RSI评分 (0-20分)
            rsi = latest.get('rsi', 50)
            if pd.notna(rsi):
                if 40 <= rsi <= 60:
                    scores['rsi'] = 20      # 最佳区间
                elif 30 <= rsi < 40:
                    scores['rsi'] = 18      # 轻微超卖，有反弹机会
                elif 60 < rsi <= 70:
                    scores['rsi'] = 15      # 轻微超买
                elif 20 <= rsi < 30:
                    scores['rsi'] = 12      # 超卖
                elif 70 < rsi <= 80:
                    scores['rsi'] = 8       # 超买
                elif rsi < 20:
                    scores['rsi'] = 5       # 严重超卖
                else:
                    scores['rsi'] = 2       # 严重超买
            else:
                scores['rsi'] = 10
            
            # 布林带评分 (0-15分)
            bb_pos = latest.get('bb_position', 50)
            bb_width = latest.get('bb_width', 0)
            if pd.notna(bb_pos):
                if 30 <= bb_pos <= 70:
                    scores['bollinger'] = 15    # 正常区间
                elif 20 <= bb_pos < 30:
                    scores['bollinger'] = 12    # 接近下轨，反弹机会
                elif 70 < bb_pos <= 80:
                    scores['bollinger'] = 8     # 接近上轨
                elif bb_pos < 20:
                    scores['bollinger'] = 10    # 触及下轨，强反弹机会
                else:
                    scores['bollinger'] = 3     # 触及上轨，回调风险
            else:
                scores['bollinger'] = 7
            
            # KDJ评分 (0-15分)
            kdj_k = latest.get('kdj_k', 50)
            kdj_d = latest.get('kdj_d', 50)
            kdj_j = latest.get('kdj_j', 50)
            
            if pd.notna(kdj_k) and pd.notna(kdj_d):
                if kdj_k > kdj_d and 20 < kdj_k < 80:
                    scores['kdj'] = 15      # 金叉且不在极值区
                elif kdj_k > kdj_d:
                    scores['kdj'] = 10      # 金叉但在极值区
                elif kdj_k < kdj_d and 20 < kdj_k < 80:
                    scores['kdj'] = 5       # 死叉但不在极值区
                else:
                    scores['kdj'] = 2       # 死叉且在极值区
                
                # KDJ超卖反弹加分
                if kdj_k < 20 and kdj_d < 20 and kdj_j > kdj_k:
                    scores['kdj'] += 5
            else:
                scores['kdj'] = 7
            
            # 均线评分 (0-15分)
            ma5 = latest.get('ma5')
            ma10 = latest.get('ma10')
            ma20 = latest.get('ma20')
            ma60 = latest.get('ma60')
            close = latest.get('close')
            
            if all(pd.notna(x) for x in [ma5, ma10, ma20, ma60, close]):
                ma_score = 0
                # 价格位置评分
                if close > ma5:
                    ma_score += 4
                if close > ma10:
                    ma_score += 3
                if close > ma20:
                    ma_score += 3
                if close > ma60:
                    ma_score += 2
                
                # 均线排列评分
                if ma5 > ma10 > ma20 > ma60:
                    ma_score += 3  # 完美多头排列
                elif ma5 > ma10 > ma20:
                    ma_score += 2  # 短期多头排列
                elif ma5 < ma10 < ma20 < ma60:
                    ma_score -= 3  # 完美空头排列
                
                scores['moving_average'] = max(0, min(15, ma_score))
            else:
                scores['moving_average'] = 7
            
            # CCI评分 (0-10分)
            cci = latest.get('cci', 0)
            if pd.notna(cci):
                if -100 <= cci <= 100:
                    scores['cci'] = 10      # 正常区间
                elif -200 <= cci < -100:
                    scores['cci'] = 8       # 超卖
                elif 100 < cci <= 200:
                    scores['cci'] = 6       # 超买
                elif cci < -200:
                    scores['cci'] = 5       # 严重超卖
                else:
                    scores['cci'] = 2       # 严重超买
            else:
                scores['cci'] = 5
            
            # 计算总分
            total_score = sum(scores.values())
            
            return {
                'technical_score': total_score,
                'details': scores,
                'max_score': 100
            }
            
        except Exception as e:
            logger.error(f"计算技术指标评分失败 {symbol}: {e}")
            return {'technical_score': 0.0, 'details': {}}
    
    def calculate_momentum_score(self, symbol: str) -> Dict[str, float]:
        """
        计算动量指标评分
        
        Args:
            symbol: 股票代码
            
        Returns:
            动量评分字典
        """
        try:
            # 获取历史数据
            hist_data = self.db.get_stock_data(symbol, days=30)
            
            if hist_data.empty or len(hist_data) < 10:
                return {'momentum_score': 0.0, 'details': {}}
            
            hist_data = hist_data.sort_values('date')
            close_prices = hist_data['close']
            
            scores = {}
            
            # 价格动量评分 (0-30分)
            if len(close_prices) >= 5:
                # 5日涨跌幅
                price_change_5d = (close_prices.iloc[-1] / close_prices.iloc[-5] - 1) * 100
                if price_change_5d > 10:
                    scores['price_momentum_5d'] = 30
                elif price_change_5d > 5:
                    scores['price_momentum_5d'] = 25
                elif price_change_5d > 2:
                    scores['price_momentum_5d'] = 20
                elif price_change_5d > 0:
                    scores['price_momentum_5d'] = 15
                elif price_change_5d > -2:
                    scores['price_momentum_5d'] = 10
                elif price_change_5d > -5:
                    scores['price_momentum_5d'] = 5
                else:
                    scores['price_momentum_5d'] = 0
            else:
                scores['price_momentum_5d'] = 10
            
            # 价格趋势评分 (0-25分)
            if len(close_prices) >= 10:
                # 计算趋势强度
                recent_prices = close_prices.tail(10)
                trend_slope = np.polyfit(range(len(recent_prices)), recent_prices, 1)[0]
                
                if trend_slope > 0.5:
                    scores['trend_strength'] = 25
                elif trend_slope > 0.2:
                    scores['trend_strength'] = 20
                elif trend_slope > 0:
                    scores['trend_strength'] = 15
                elif trend_slope > -0.2:
                    scores['trend_strength'] = 10
                elif trend_slope > -0.5:
                    scores['trend_strength'] = 5
                else:
                    scores['trend_strength'] = 0
            else:
                scores['trend_strength'] = 10
            
            # 相对强度评分 (0-25分)
            if len(close_prices) >= 20:
                # 计算相对于自身历史的强度
                current_price = close_prices.iloc[-1]
                max_price_20d = close_prices.tail(20).max()
                min_price_20d = close_prices.tail(20).min()
                
                if max_price_20d > min_price_20d:
                    relative_position = (current_price - min_price_20d) / (max_price_20d - min_price_20d)
                    scores['relative_strength'] = relative_position * 25
                else:
                    scores['relative_strength'] = 12.5
            else:
                scores['relative_strength'] = 12.5
            
            # 突破评分 (0-20分)
            if len(close_prices) >= 20:
                # 检查是否突破近期高点
                current_price = close_prices.iloc[-1]
                recent_high = close_prices.tail(20).max()
                prev_high = close_prices.tail(21).iloc[:-1].max()
                
                if current_price >= recent_high and recent_high > prev_high:
                    scores['breakout'] = 20  # 创新高
                elif current_price >= recent_high * 0.98:
                    scores['breakout'] = 15  # 接近新高
                elif current_price >= recent_high * 0.95:
                    scores['breakout'] = 10  # 相对强势
                else:
                    scores['breakout'] = 5
            else:
                scores['breakout'] = 10
            
            total_score = sum(scores.values())
            
            return {
                'momentum_score': total_score,
                'details': scores,
                'max_score': 100
            }
            
        except Exception as e:
            logger.error(f"计算动量评分失败 {symbol}: {e}")
            return {'momentum_score': 0.0, 'details': {}}
    
    def calculate_volume_score(self, symbol: str) -> Dict[str, float]:
        """
        计算成交量评分
        
        Args:
            symbol: 股票代码
            
        Returns:
            成交量评分字典
        """
        try:
            hist_data = self.db.get_stock_data(symbol, days=30)
            
            if hist_data.empty or len(hist_data) < 10:
                return {'volume_score': 0.0, 'details': {}}
            
            hist_data = hist_data.sort_values('date')
            volume_data = hist_data['volume']
            close_prices = hist_data['close']
            
            scores = {}
            
            # 量比评分 (0-30分)
            if len(volume_data) >= 5:
                current_volume = volume_data.iloc[-1]
                avg_volume_5d = volume_data.tail(5).mean()
                
                if avg_volume_5d > 0:
                    volume_ratio = current_volume / avg_volume_5d
                    
                    if volume_ratio > 3:
                        scores['volume_ratio'] = 30    # 巨量
                    elif volume_ratio > 2:
                        scores['volume_ratio'] = 25    # 大量
                    elif volume_ratio > 1.5:
                        scores['volume_ratio'] = 20    # 放量
                    elif volume_ratio > 1:
                        scores['volume_ratio'] = 15    # 正常
                    elif volume_ratio > 0.7:
                        scores['volume_ratio'] = 10    # 缩量
                    else:
                        scores['volume_ratio'] = 5     # 地量
                else:
                    scores['volume_ratio'] = 10
            else:
                scores['volume_ratio'] = 10
            
            # 量价配合评分 (0-25分)
            if len(volume_data) >= 5 and len(close_prices) >= 5:
                # 计算价格和成交量的相关性
                price_change = close_prices.pct_change().tail(5)
                volume_change = volume_data.pct_change().tail(5)
                
                # 去除NaN值
                valid_data = pd.DataFrame({
                    'price': price_change,
                    'volume': volume_change
                }).dropna()
                
                if len(valid_data) >= 3:
                    correlation = valid_data['price'].corr(valid_data['volume'])
                    
                    if pd.notna(correlation):
                        if correlation > 0.5:
                            scores['price_volume_sync'] = 25    # 量价齐升
                        elif correlation > 0.2:
                            scores['price_volume_sync'] = 20    # 量价配合良好
                        elif correlation > -0.2:
                            scores['price_volume_sync'] = 15    # 量价配合一般
                        elif correlation > -0.5:
                            scores['price_volume_sync'] = 10    # 量价背离
                        else:
                            scores['price_volume_sync'] = 5     # 严重背离
                    else:
                        scores['price_volume_sync'] = 12
                else:
                    scores['price_volume_sync'] = 12
            else:
                scores['price_volume_sync'] = 12
            
            # 成交量趋势评分 (0-25分)
            if len(volume_data) >= 10:
                # 计算成交量趋势
                recent_volume = volume_data.tail(5).mean()
                earlier_volume = volume_data.tail(10).head(5).mean()
                
                if earlier_volume > 0:
                    volume_trend = (recent_volume / earlier_volume - 1) * 100
                    
                    if volume_trend > 50:
                        scores['volume_trend'] = 25    # 成交量大幅增长
                    elif volume_trend > 20:
                        scores['volume_trend'] = 20    # 成交量增长
                    elif volume_trend > 0:
                        scores['volume_trend'] = 15    # 成交量温和增长
                    elif volume_trend > -20:
                        scores['volume_trend'] = 10    # 成交量小幅下降
                    else:
                        scores['volume_trend'] = 5     # 成交量大幅下降
                else:
                    scores['volume_trend'] = 12
            else:
                scores['volume_trend'] = 12
            
            # 换手率评分 (0-20分)
            if 'turnover_rate' in hist_data.columns:
                current_turnover = hist_data['turnover_rate'].iloc[-1]
                
                if pd.notna(current_turnover):
                    if current_turnover > 10:
                        scores['turnover_rate'] = 20    # 高换手率
                    elif current_turnover > 5:
                        scores['turnover_rate'] = 18    # 较高换手率
                    elif current_turnover > 3:
                        scores['turnover_rate'] = 15    # 正常换手率
                    elif current_turnover > 1:
                        scores['turnover_rate'] = 12    # 较低换手率
                    else:
                        scores['turnover_rate'] = 8     # 低换手率
                else:
                    scores['turnover_rate'] = 10
            else:
                scores['turnover_rate'] = 10
            
            total_score = sum(scores.values())
            
            return {
                'volume_score': total_score,
                'details': scores,
                'max_score': 100
            }
            
        except Exception as e:
            logger.error(f"计算成交量评分失败 {symbol}: {e}")
            return {'volume_score': 0.0, 'details': {}}
    
    def calculate_volatility_score(self, symbol: str) -> Dict[str, float]:
        """
        计算波动率评分
        
        Args:
            symbol: 股票代码
            
        Returns:
            波动率评分字典
        """
        try:
            hist_data = self.db.get_stock_data(symbol, days=30)
            
            if hist_data.empty or len(hist_data) < 10:
                return {'volatility_score': 0.0, 'details': {}}
            
            hist_data = hist_data.sort_values('date')
            close_prices = hist_data['close']
            high_prices = hist_data['high']
            low_prices = hist_data['low']
            
            scores = {}
            
            # 历史波动率评分 (0-40分)
            if len(close_prices) >= 20:
                returns = close_prices.pct_change().dropna()
                volatility = returns.std() * np.sqrt(252) * 100  # 年化波动率
                
                # 短线交易偏好适中波动率
                if 15 <= volatility <= 35:
                    scores['historical_volatility'] = 40    # 最佳波动率区间
                elif 10 <= volatility < 15:
                    scores['historical_volatility'] = 35    # 较低波动率
                elif 35 < volatility <= 50:
                    scores['historical_volatility'] = 35    # 较高波动率
                elif 5 <= volatility < 10:
                    scores['historical_volatility'] = 25    # 低波动率
                elif 50 < volatility <= 70:
                    scores['historical_volatility'] = 25    # 高波动率
                elif volatility < 5:
                    scores['historical_volatility'] = 15    # 极低波动率
                else:
                    scores['historical_volatility'] = 10    # 极高波动率
            else:
                scores['historical_volatility'] = 20
            
            # ATR相对评分 (0-30分)
            if len(high_prices) >= 14:
                # 计算ATR
                tr1 = high_prices - low_prices
                tr2 = np.abs(high_prices - close_prices.shift(1))
                tr3 = np.abs(low_prices - close_prices.shift(1))
                
                true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                atr = true_range.rolling(window=14).mean()
                
                current_atr = atr.iloc[-1]
                current_price = close_prices.iloc[-1]
                
                if current_price > 0:
                    atr_percentage = (current_atr / current_price) * 100
                    
                    if 2 <= atr_percentage <= 5:
                        scores['atr_relative'] = 30    # 最佳ATR区间
                    elif 1 <= atr_percentage < 2:
                        scores['atr_relative'] = 25    # 较低ATR
                    elif 5 < atr_percentage <= 8:
                        scores['atr_relative'] = 25    # 较高ATR
                    elif 0.5 <= atr_percentage < 1:
                        scores['atr_relative'] = 20    # 低ATR
                    elif 8 < atr_percentage <= 12:
                        scores['atr_relative'] = 20    # 高ATR
                    else:
                        scores['atr_relative'] = 10    # 极端ATR
                else:
                    scores['atr_relative'] = 15
            else:
                scores['atr_relative'] = 15
            
            # 波动率趋势评分 (0-30分)
            if len(close_prices) >= 20:
                # 比较近期和历史波动率
                recent_returns = close_prices.tail(10).pct_change().dropna()
                earlier_returns = close_prices.tail(20).head(10).pct_change().dropna()
                
                if len(recent_returns) >= 5 and len(earlier_returns) >= 5:
                    recent_vol = recent_returns.std()
                    earlier_vol = earlier_returns.std()
                    
                    if earlier_vol > 0:
                        vol_change = (recent_vol / earlier_vol - 1) * 100
                        
                        if -20 <= vol_change <= 20:
                            scores['volatility_trend'] = 30    # 波动率稳定
                        elif -40 <= vol_change < -20:
                            scores['volatility_trend'] = 25    # 波动率下降
                        elif 20 < vol_change <= 40:
                            scores['volatility_trend'] = 25    # 波动率上升
                        elif vol_change < -40:
                            scores['volatility_trend'] = 20    # 波动率大幅下降
                        else:
                            scores['volatility_trend'] = 15    # 波动率大幅上升
                    else:
                        scores['volatility_trend'] = 15
                else:
                    scores['volatility_trend'] = 15
            else:
                scores['volatility_trend'] = 15
            
            total_score = sum(scores.values())
            
            return {
                'volatility_score': total_score,
                'details': scores,
                'max_score': 100
            }
            
        except Exception as e:
            logger.error(f"计算波动率评分失败 {symbol}: {e}")
            return {'volatility_score': 0.0, 'details': {}}
    
    def calculate_comprehensive_score(self, symbol: str, 
                                    custom_weights: Optional[Dict[str, float]] = None) -> Dict[str, float]:
        """
        计算综合评分
        
        Args:
            symbol: 股票代码
            custom_weights: 自定义权重配置
            
        Returns:
            综合评分结果
        """
        try:
            # 使用自定义权重或默认权重
            weights = custom_weights if custom_weights else self.default_weights
            
            # 计算各维度评分
            technical_result = self.calculate_technical_score(symbol)
            momentum_result = self.calculate_momentum_score(symbol)
            volume_result = self.calculate_volume_score(symbol)
            volatility_result = self.calculate_volatility_score(symbol)
            
            # 提取评分
            technical_score = technical_result.get('technical_score', 0)
            momentum_score = momentum_result.get('momentum_score', 0)
            volume_score = volume_result.get('volume_score', 0)
            volatility_score = volatility_result.get('volatility_score', 0)
            
            # 计算加权综合评分
            comprehensive_score = (
                technical_score * weights.get('technical', 0.4) +
                momentum_score * weights.get('momentum', 0.25) +
                volume_score * weights.get('volume', 0.2) +
                volatility_score * weights.get('volatility', 0.1)
            )
            
            # 市场情绪调整（简化版，可以后续扩展）
            market_sentiment_adjustment = weights.get('market_sentiment', 0.05) * 50  # 中性调整
            comprehensive_score += market_sentiment_adjustment
            
            return {
                'symbol': symbol,
                'comprehensive_score': round(comprehensive_score, 2),
                'technical_score': round(technical_score, 2),
                'momentum_score': round(momentum_score, 2),
                'volume_score': round(volume_score, 2),
                'volatility_score': round(volatility_score, 2),
                'weights_used': weights,
                'details': {
                    'technical': technical_result.get('details', {}),
                    'momentum': momentum_result.get('details', {}),
                    'volume': volume_result.get('details', {}),
                    'volatility': volatility_result.get('details', {})
                },
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"计算综合评分失败 {symbol}: {e}")
            return {
                'symbol': symbol,
                'comprehensive_score': 0.0,
                'error': str(e)
            }
    
    def batch_calculate_scores(self, symbols: List[str], 
                             custom_weights: Optional[Dict[str, float]] = None) -> pd.DataFrame:
        """
        批量计算股票评分
        
        Args:
            symbols: 股票代码列表
            custom_weights: 自定义权重配置
            
        Returns:
            评分结果DataFrame
        """
        results = []
        total_symbols = len(symbols)
        
        logger.info(f"开始批量计算 {total_symbols} 只股票的综合评分...")
        
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"正在计算 {symbol} 综合评分 ({i}/{total_symbols})")
                
                score_result = self.calculate_comprehensive_score(symbol, custom_weights)
                results.append(score_result)
                
            except Exception as e:
                logger.error(f"计算股票 {symbol} 评分失败: {e}")
                results.append({
                    'symbol': symbol,
                    'comprehensive_score': 0.0,
                    'error': str(e)
                })
        
        # 转换为DataFrame
        df = pd.DataFrame(results)
        
        # 按综合评分排序
        if 'comprehensive_score' in df.columns:
            df = df.sort_values('comprehensive_score', ascending=False).reset_index(drop=True)
        
        logger.info(f"批量评分完成，共处理 {len(df)} 只股票")
        
        return df


def main():
    """测试综合评分功能"""
    from database import DatabaseManager
    
    # 初始化
    db = DatabaseManager()
    scoring = ComprehensiveScoring(db)
    
    # 测试股票
    test_symbols = ['000001', '000002', '600000']
    
    print("=== 综合评分系统测试 ===")
    
    # 单只股票评分测试
    test_symbol = test_symbols[0]
    print(f"\n1. 单只股票评分测试: {test_symbol}")
    
    result = scoring.calculate_comprehensive_score(test_symbol)
    
    print(f"综合评分: {result.get('comprehensive_score', 0):.2f}")
    print(f"技术指标评分: {result.get('technical_score', 0):.2f}")
    print(f"动量评分: {result.get('momentum_score', 0):.2f}")
    print(f"成交量评分: {result.get('volume_score', 0):.2f}")
    print(f"波动率评分: {result.get('volatility_score', 0):.2f}")
    
    # 批量评分测试
    print(f"\n2. 批量评分测试: {test_symbols}")
    
    batch_results = scoring.batch_calculate_scores(test_symbols)
    
    if not batch_results.empty:
        print("\n评分排名:")
        for i, row in batch_results.iterrows():
            print(f"  {i+1}. {row['symbol']}: {row.get('comprehensive_score', 0):.2f}分")


if __name__ == "__main__":
    main()