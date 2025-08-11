"""
增强版数据获取模块
实现动态延迟、指数退避、网络状态监控等功能
提高网络请求的稳定性和成功率
"""

import akshare as ak
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Tuple
import time
import logging
import random
import math
from database import DatabaseManager
import warnings
from dataclasses import dataclass
from enum import Enum

# 忽略警告信息
warnings.filterwarnings('ignore')

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NetworkStatus(Enum):
    """网络状态枚举"""
    EXCELLENT = "excellent"  # 成功率 > 95%
    GOOD = "good"           # 成功率 80-95%
    POOR = "poor"           # 成功率 60-80%
    BAD = "bad"             # 成功率 < 60%


@dataclass
class RequestMetrics:
    """请求指标数据类"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_time: float = 0.0
    last_success_time: Optional[float] = None
    consecutive_failures: int = 0
    
    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.total_requests == 0:
            return 1.0
        return self.successful_requests / self.total_requests
    
    @property
    def average_response_time(self) -> float:
        """平均响应时间"""
        if self.successful_requests == 0:
            return 0.0
        return self.total_time / self.successful_requests


class FixedDelayManager:
    """固定延迟管理器 - 专为东方财富API优化"""
    
    def __init__(self,
                 min_delay: float = 0.3,
                 max_delay: float = 1.0,
                 retry_delay: float = 2.0,
                 enterprise_mode: bool = False):
        """
        初始化固定延迟管理器
        
        Args:
            min_delay: 最小延迟时间（秒）
            max_delay: 最大延迟时间（秒）
            retry_delay: 重试延迟时间（秒）
            enterprise_mode: 是否启用企业网络模式
        """
        # 企业网络模式使用更保守的延迟设置
        if enterprise_mode:
            self.min_delay = max(min_delay, 2.0)
            self.max_delay = max(max_delay, 5.0)
            self.retry_delay = max(retry_delay, 10.0)
            logger.info(f"启用企业网络模式: {self.min_delay}-{self.max_delay}秒延迟")
        else:
            self.min_delay = min_delay
            self.max_delay = max_delay
            self.retry_delay = retry_delay
            logger.info(f"初始化固定延迟管理器: {min_delay}-{max_delay}秒延迟")
        
        self.metrics = RequestMetrics()
        self.enterprise_mode = enterprise_mode
    
    def get_delay(self, is_retry: bool = False, retry_count: int = 0) -> float:
        """
        获取延迟时间
        
        Args:
            is_retry: 是否为重试请求
            retry_count: 重试次数
            
        Returns:
            延迟时间（秒）
        """
        if is_retry:
            # 重试时使用固定延迟，避免过于激进
            delay = self.retry_delay * (1 + retry_count * 0.5)  # 递增延迟
        else:
            # 正常请求使用随机延迟，避免请求同步
            delay = random.uniform(self.min_delay, self.max_delay)
        
        return delay
    
    def record_request(self, success: bool, response_time: float = 0.0):
        """
        记录请求结果
        
        Args:
            success: 请求是否成功
            response_time: 响应时间
        """
        self.metrics.total_requests += 1
        
        if success:
            self.metrics.successful_requests += 1
            self.metrics.total_time += response_time
            self.metrics.last_success_time = time.time()
            self.metrics.consecutive_failures = 0
        else:
            self.metrics.failed_requests += 1
            self.metrics.consecutive_failures += 1
    
    def get_network_status(self) -> NetworkStatus:
        """获取当前网络状态"""
        success_rate = self.metrics.success_rate
        
        if success_rate > 0.95:
            return NetworkStatus.EXCELLENT
        elif success_rate > 0.80:
            return NetworkStatus.GOOD
        elif success_rate > 0.60:
            return NetworkStatus.POOR
        else:
            return NetworkStatus.BAD
    
    def should_pause(self) -> bool:
        """判断是否应该暂停请求"""
        # 连续失败超过15次，建议暂停（比之前更宽松）
        if self.metrics.consecutive_failures > 15:
            return True
        
        # 成功率过低且请求数量足够多，建议暂停
        if self.metrics.total_requests > 30 and self.metrics.success_rate < 0.2:
            return True
        
        return False


class EnhancedDataFetcher:
    """增强版数据获取类 - 优化东方财富数据源"""
    
    def __init__(self, db_manager: DatabaseManager, enterprise_mode: bool = False):
        """
        初始化增强版数据获取器
        
        Args:
            db_manager: 数据库管理器实例
            enterprise_mode: 是否启用企业网络模式
        """
        self.db = db_manager
        self.enterprise_mode = enterprise_mode
        self.delay_manager = FixedDelayManager(enterprise_mode=enterprise_mode)
        
        # 重试配置 - 企业模式使用更保守的设置
        if enterprise_mode:
            self.max_retry_times = 2  # 减少重试次数
            self.retry_delays = [5, 15]  # 增加重试延迟
            self.request_timeout = 60  # 增加超时时间
            self.batch_pause_threshold = 0.3  # 提高暂停阈值
            self.segment_days = 15  # 减少分段天数
            logger.info("增强版数据获取器初始化完成 - 企业网络模式")
        else:
            self.max_retry_times = 3
            self.retry_delays = [2, 5, 10]
            self.request_timeout = 30
            self.batch_pause_threshold = 0.2
            self.segment_days = 30
            logger.info("增强版数据获取器初始化完成 - 使用东方财富优先数据源")
    
    def get_stock_list(self) -> pd.DataFrame:
        """
        获取A股股票列表（带重试机制）
        
        Returns:
            股票列表DataFrame
        """
        logger.info("开始获取A股股票列表...")
        
        for attempt in range(self.max_retry_times):
            start_time = time.time()
            
            try:
                # 应用延迟
                if attempt > 0:
                    delay = self.delay_manager.get_delay(is_retry=True, retry_count=attempt)
                    logger.info(f"重试前等待 {delay:.2f} 秒...")
                    time.sleep(delay)
                
                # 获取股票列表
                stock_list = ak.stock_zh_a_spot_em()
                response_time = time.time() - start_time
                
                if stock_list.empty:
                    logger.warning("获取到的股票列表为空")
                    self.delay_manager.record_request(False)
                    continue
                
                # 数据清洗和格式化
                stock_list = self._clean_stock_list(stock_list)
                
                # 记录成功请求
                self.delay_manager.record_request(True, response_time)
                
                logger.info(f"成功获取股票列表，共 {len(stock_list)} 只股票")
                logger.info(f"网络状态: {self.delay_manager.get_network_status().value}")
                
                return stock_list
                
            except Exception as e:
                response_time = time.time() - start_time
                self.delay_manager.record_request(False, response_time)
                
                logger.error(f"获取股票列表失败 (尝试 {attempt + 1}/{self.max_retry_times}): {e}")
                
                # 检查是否应该暂停
                if self.delay_manager.should_pause():
                    logger.warning("网络状态过差，建议稍后重试")
                    break
        
        logger.error("获取股票列表最终失败")
        return pd.DataFrame()
    
    def get_stock_history(self, symbol: str, period: str = "daily", 
                         start_date: Optional[str] = None, 
                         end_date: Optional[str] = None) -> pd.DataFrame:
        """
        获取股票历史数据（带智能重试）
        
        Args:
            symbol: 股票代码
            period: 数据周期
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            历史数据DataFrame
        """
        # 设置默认日期范围
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')
        
        for attempt in range(self.max_retry_times):
            start_time = time.time()
            
            try:
                # 应用延迟策略
                if attempt > 0:
                    delay = self.delay_manager.get_delay(is_retry=True, retry_count=attempt)
                    logger.debug(f"股票 {symbol} 重试前等待 {delay:.2f} 秒...")
                    time.sleep(delay)
                else:
                    # 正常请求延迟
                    delay = self.delay_manager.get_delay()
                    time.sleep(delay)
                
                # 尝试多个数据源获取历史数据
                hist_data = self._get_stock_history_multi_source(
                    symbol=symbol,
                    period=period,
                    start_date=start_date,
                    end_date=end_date
                )
                
                response_time = time.time() - start_time
                
                if hist_data.empty:
                    logger.debug(f"股票 {symbol} 没有历史数据")
                    self.delay_manager.record_request(False, response_time)
                    return pd.DataFrame()
                
                # 数据清洗
                hist_data = self._clean_history_data(hist_data)
                
                # 记录成功请求
                self.delay_manager.record_request(True, response_time)
                
                logger.debug(f"成功获取股票 {symbol} 历史数据，共 {len(hist_data)} 条记录")
                return hist_data
                
            except Exception as e:
                response_time = time.time() - start_time
                self.delay_manager.record_request(False, response_time)
                
                # 详细错误日志
                import traceback
                logger.error(f"股票 {symbol} 获取数据异常: {type(e).__name__}: {str(e)}")
                logger.error(f"错误堆栈: {traceback.format_exc()}")
                
                # 根据错误类型决定是否继续重试
                if self._is_permanent_error(e):
                    logger.debug(f"股票 {symbol} 遇到永久性错误，停止重试: {e}")
                    break
                
                logger.debug(f"获取股票 {symbol} 历史数据失败 (尝试 {attempt + 1}/{self.max_retry_times}): {e}")
                
                # 检查网络状态
                if self.delay_manager.should_pause():
                    logger.warning(f"网络状态过差，跳过股票 {symbol}")
                    break
        
        logger.debug(f"股票 {symbol} 历史数据获取最终失败")
        return pd.DataFrame()
    
    def _is_permanent_error(self, error: Exception) -> bool:
        """
        判断是否为永久性错误（不需要重试）
        
        Args:
            error: 异常对象
            
        Returns:
            是否为永久性错误
        """
        error_str = str(error).lower()
        
        # 常见的永久性错误
        permanent_errors = [
            'not found',
            '404',
            'invalid symbol',
            'delisted',
            'suspended'
        ]
        
        return any(pe in error_str for pe in permanent_errors)
    
    def update_stock_data_with_fixed_delay(self, symbol: str, days: int = 60) -> int:
        """
        使用固定延迟更新单只股票数据 - 支持数据分段获取
        
        Args:
            symbol: 股票代码
            days: 获取天数
            
        Returns:
            更新的记录数
        """
        try:
            # 检查数据库中最后更新日期
            last_date = self.db.get_last_update_date(symbol)
            
            # 确定开始日期
            if last_date:
                start_date = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y%m%d')
            else:
                start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
            
            end_date = datetime.now().strftime('%Y%m%d')
            
            # 如果开始日期大于等于结束日期，说明数据已是最新
            if start_date >= end_date:
                logger.debug(f"股票 {symbol} 数据已是最新")
                return 0
            
            # 计算日期差，决定是否需要分段获取
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            date_diff = (end_dt - start_dt).days
            
            total_records = 0
            
            if date_diff > self.segment_days:
                # 需要分段获取
                logger.debug(f"股票 {symbol} 需要分段获取数据，总天数: {date_diff}")
                
                # 分成两段
                mid_date = start_dt + timedelta(days=self.segment_days)
                mid_date_str = mid_date.strftime('%Y%m%d')
                
                # 获取第一段数据
                logger.debug(f"获取第一段数据: {start_date} 到 {mid_date_str}")
                hist_data1 = self.get_stock_history(symbol, start_date=start_date, end_date=mid_date_str)
                if not hist_data1.empty:
                    total_records += self.db.insert_daily_data(symbol, hist_data1)
                
                # 延迟后获取第二段数据
                delay = self.delay_manager.get_delay()
                logger.debug(f"分段间延迟 {delay:.2f} 秒")
                time.sleep(delay)
                
                # 获取第二段数据
                second_start = (mid_date + timedelta(days=1)).strftime('%Y%m%d')
                logger.debug(f"获取第二段数据: {second_start} 到 {end_date}")
                hist_data2 = self.get_stock_history(symbol, start_date=second_start, end_date=end_date)
                if not hist_data2.empty:
                    total_records += self.db.insert_daily_data(symbol, hist_data2)
                
            else:
                # 单次获取
                hist_data = self.get_stock_history(symbol, start_date=start_date, end_date=end_date)
                if not hist_data.empty:
                    total_records = self.db.insert_daily_data(symbol, hist_data)
            
            return total_records
            
        except Exception as e:
            import traceback
            logger.error(f"更新股票 {symbol} 数据失败: {type(e).__name__}: {str(e)}")
            logger.error(f"详细错误堆栈: {traceback.format_exc()}")
            return 0
    
    # 保持向后兼容性
    def update_stock_data_with_adaptive_delay(self, symbol: str, days: int = 60) -> int:
        """向后兼容方法"""
        return self.update_stock_data_with_fixed_delay(symbol, days)
    
    def batch_update_with_monitoring(self, symbols: Optional[List[str]] = None, 
                                   days: int = 60, max_stocks: int = 100) -> Dict[str, int]:
        """
        带监控的批量更新股票数据
        
        Args:
            symbols: 股票代码列表
            days: 获取天数
            max_stocks: 最大处理股票数量
            
        Returns:
            更新结果字典
        """
        if symbols is None:
            stock_list = self.db.get_stock_list()
            if stock_list.empty:
                logger.warning("数据库中没有股票列表，请先更新股票列表")
                return {}
            symbols = stock_list['symbol'].tolist()[:max_stocks]
        
        results = {}
        total_stocks = len(symbols)
        
        logger.info(f"开始批量更新 {total_stocks} 只股票的历史数据...")
        logger.info(f"初始网络状态: {self.delay_manager.get_network_status().value}")
        
        for i, symbol in enumerate(symbols, 1):
            try:
                # 检查是否需要暂停
                if self.delay_manager.should_pause():
                    logger.warning(f"网络状态过差，暂停批处理。已处理 {i-1}/{total_stocks}")
                    break
                
                logger.info(f"正在更新 {symbol} ({i}/{total_stocks})")
                updated_count = self.update_stock_data_with_fixed_delay(symbol, days)
                results[symbol] = updated_count
                
                # 每处理50只股票显示一次状态
                if i % 50 == 0:
                    status = self.delay_manager.get_network_status()
                    success_rate = self.delay_manager.metrics.success_rate
                    logger.info(f"进度: {i}/{total_stocks}, 网络状态: {status.value}, 成功率: {success_rate:.2%}")
                
            except Exception as e:
                logger.error(f"更新股票 {symbol} 失败: {e}")
                results[symbol] = 0
        
        # 统计结果
        total_updated = sum(results.values())
        success_count = sum(1 for count in results.values() if count > 0)
        final_success_rate = self.delay_manager.metrics.success_rate
        
        logger.info(f"批量更新完成: 成功 {success_count}/{len(results)} 只股票")
        logger.info(f"共更新 {total_updated} 条记录，最终成功率: {final_success_rate:.2%}")
        
        return results
    
    def _clean_stock_list(self, stock_list: pd.DataFrame) -> pd.DataFrame:
        """清洗股票列表数据"""
        # 重命名列
        column_mapping = {
            '代码': 'symbol',
            '名称': 'name',
            '最新价': 'price',
            '涨跌幅': 'change_pct',
            '涨跌额': 'change_amount',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '最高': 'high',
            '最低': 'low',
            '今开': 'open',
            '昨收': 'pre_close',
            '换手率': 'turnover_rate',
            '市盈率-动态': 'pe_ratio',
            '市净率': 'pb_ratio',
            '总市值': 'total_market_cap',
            '流通市值': 'circulating_market_cap'
        }
        
        # 选择需要的列并重命名
        available_columns = [col for col in column_mapping.keys() if col in stock_list.columns]
        stock_list = stock_list[available_columns].copy()
        stock_list.rename(columns=column_mapping, inplace=True)
        
        # 过滤掉ST、*ST等特殊股票
        if 'name' in stock_list.columns:
            stock_list = stock_list[~stock_list['name'].str.contains('ST|退', na=False)]
        
        # 过滤掉价格异常的股票
        if 'price' in stock_list.columns:
            stock_list = stock_list[
                (stock_list['price'] > 1) & 
                (stock_list['price'] < 1000)
            ]
        
        # 添加市场信息
        if 'symbol' in stock_list.columns:
            stock_list['market'] = stock_list['symbol'].apply(self._get_market_info)
        
        # 重置索引
        stock_list.reset_index(drop=True, inplace=True)
        
        return stock_list
    
    def _get_market_info(self, symbol: str) -> str:
        """根据股票代码判断所属市场"""
        if symbol.startswith('00') or symbol.startswith('30'):
            return '深圳'
        elif symbol.startswith('60') or symbol.startswith('68'):
            return '上海'
        else:
            return '其他'
    
    def _clean_history_data(self, hist_data: pd.DataFrame) -> pd.DataFrame:
        """清洗历史数据"""
        # 重命名列
        column_mapping = {
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '涨跌幅': 'change_pct',
            '涨跌额': 'change_amount',
            '换手率': 'turnover_rate'
        }
        
        # 选择需要的列并重命名
        available_columns = [col for col in column_mapping.keys() if col in hist_data.columns]
        hist_data = hist_data[available_columns].copy()
        hist_data.rename(columns=column_mapping, inplace=True)
        
        # 数据类型转换
        if 'date' in hist_data.columns:
            hist_data['date'] = pd.to_datetime(hist_data['date']).dt.strftime('%Y-%m-%d')
        
        # 数值列转换
        numeric_columns = ['open', 'close', 'high', 'low', 'volume', 'amount', 'turnover_rate']
        for col in numeric_columns:
            if col in hist_data.columns:
                hist_data[col] = pd.to_numeric(hist_data[col], errors='coerce')
        
        # 去除空值行
        hist_data.dropna(subset=['date', 'close'], inplace=True)
        
        # 按日期排序
        hist_data.sort_values('date', inplace=True)
        hist_data.reset_index(drop=True, inplace=True)
        
        return hist_data
    
    def _get_stock_history_multi_source(self, symbol: str, period: str = "daily",
                                      start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        使用多个数据源获取股票历史数据 - 东方财富优先
        
        Args:
            symbol: 股票代码
            period: 数据周期
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            历史数据DataFrame
        """
        # 重新排序数据源，东方财富优先
        data_sources = [
            self._get_from_eastmoney, # 东方财富 - 主要数据源
            self._get_from_simple,    # 简化版本 - 备用
            self._get_from_tencent    # 腾讯数据源 - 最后备用
        ]
        
        for i, get_data_func in enumerate(data_sources):
            try:
                logger.debug(f"尝试数据源 {i+1}: {get_data_func.__name__}")
                hist_data = get_data_func(symbol, period, start_date, end_date)
                
                if not hist_data.empty:
                    logger.debug(f"数据源 {i+1} 成功获取 {len(hist_data)} 条记录")
                    return hist_data
                else:
                    logger.debug(f"数据源 {i+1} 返回空数据")
                    
            except Exception as e:
                logger.debug(f"数据源 {i+1} 失败: {e}")
                continue
        
        logger.warning(f"所有数据源都失败，股票 {symbol}")
        return pd.DataFrame()
    
    def _get_from_tencent(self, symbol: str, period: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从腾讯数据源获取数据"""
        try:
            # 使用akshare的腾讯数据源
            hist_data = ak.stock_zh_a_hist_tx(symbol=symbol)
            
            if not hist_data.empty:
                # 确保有日期列
                if 'date' in hist_data.columns:
                    # 过滤日期范围
                    hist_data['date'] = pd.to_datetime(hist_data['date'])
                    start_dt = pd.to_datetime(start_date, format='%Y%m%d')
                    end_dt = pd.to_datetime(end_date, format='%Y%m%d')
                    
                    hist_data = hist_data[
                        (hist_data['date'] >= start_dt) &
                        (hist_data['date'] <= end_dt)
                    ]
                    
                    # 转换日期格式
                    hist_data['date'] = hist_data['date'].dt.strftime('%Y-%m-%d')
                
            return hist_data
            
        except Exception as e:
            logger.debug(f"腾讯数据源异常: {e}")
            return pd.DataFrame()
    
    def _get_from_simple(self, symbol: str, period: str, start_date: str, end_date: str) -> pd.DataFrame:
        """简化版数据获取 - 只获取最近的数据"""
        try:
            # 尝试获取最近30天的数据，不指定具体日期范围
            from datetime import datetime, timedelta
            recent_start = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
            recent_end = datetime.now().strftime('%Y%m%d')
            
            hist_data = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=recent_start,
                end_date=recent_end,
                adjust="qfq"
            )
            
            # 如果成功获取到数据，再过滤到用户要求的日期范围
            if not hist_data.empty and 'date' in hist_data.columns:
                hist_data['date'] = pd.to_datetime(hist_data['date'])
                start_dt = pd.to_datetime(start_date, format='%Y%m%d')
                end_dt = pd.to_datetime(end_date, format='%Y%m%d')
                
                hist_data = hist_data[
                    (hist_data['date'] >= start_dt) &
                    (hist_data['date'] <= end_dt)
                ]
                
                # 转换日期格式
                hist_data['date'] = hist_data['date'].dt.strftime('%Y-%m-%d')
            
            return hist_data
            
        except Exception as e:
            logger.debug(f"简化数据源异常: {e}")
            return pd.DataFrame()
    
    def _get_from_eastmoney(self, symbol: str, period: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从东方财富获取数据（原始方法）"""
        try:
            # 原始的东方财富数据源
            hist_data = ak.stock_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            return hist_data
            
        except Exception as e:
            logger.debug(f"东方财富数据源异常: {e}")
            return pd.DataFrame()
    
    def get_metrics_summary(self) -> Dict[str, any]:
        """获取请求指标摘要"""
        metrics = self.delay_manager.metrics
        return {
            'total_requests': metrics.total_requests,
            'success_rate': f"{metrics.success_rate:.2%}",
            'average_response_time': f"{metrics.average_response_time:.2f}s",
            'network_status': self.delay_manager.get_network_status().value,
            'consecutive_failures': metrics.consecutive_failures,
            'delay_range': f"{self.delay_manager.min_delay:.1f}-{self.delay_manager.max_delay:.1f}s"
        }


if __name__ == "__main__":
    """测试增强版数据获取功能"""
    from database import DatabaseManager
    
    # 初始化
    db = DatabaseManager()
    fetcher = EnhancedDataFetcher(db)
    
    # 测试获取股票列表
    print("测试获取股票列表...")
    stock_count = len(fetcher.get_stock_list())
    print(f"获取股票数量: {stock_count}")
    
    # 测试获取历史数据
    print("\n测试获取历史数据...")
    test_symbols = ['000001', '000002', '600000']
    results = fetcher.batch_update_with_monitoring(test_symbols, days=30)
    
    for symbol, count in results.items():
        print(f"  {symbol}: {count} 条记录")
    
    # 显示指标摘要
    print("\n请求指标摘要:")
    summary = fetcher.get_metrics_summary()
    for key, value in summary.items():
        print(f"  {key}: {value}")