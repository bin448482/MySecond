"""
工具函数模块
提供通用的工具函数和配置管理
"""

import yaml
import os
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np


class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
                return config
        except FileNotFoundError:
            logging.error(f"配置文件 {self.config_path} 不存在")
            return {}
        except yaml.YAMLError as e:
            logging.error(f"配置文件格式错误: {e}")
            return {}
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置值
        
        Args:
            key: 配置键，支持点号分隔的嵌套键
            default: 默认值
            
        Returns:
            配置值
        """
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key: str, value: Any):
        """
        设置配置值
        
        Args:
            key: 配置键
            value: 配置值
        """
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def save(self):
        """保存配置到文件"""
        try:
            with open(self.config_path, 'w', encoding='utf-8') as file:
                yaml.dump(self.config, file, default_flow_style=False, 
                         allow_unicode=True, indent=2)
        except Exception as e:
            logging.error(f"保存配置文件失败: {e}")


class LoggerManager:
    """日志管理器"""
    
    @staticmethod
    def setup_logger(config: ConfigManager) -> logging.Logger:
        """
        设置日志记录器
        
        Args:
            config: 配置管理器
            
        Returns:
            配置好的日志记录器
        """
        # 获取日志配置
        log_level = config.get('logging.level', 'INFO')
        file_enabled = config.get('logging.file_enabled', True)
        file_path = config.get('logging.file_path', 'logs/stock_selector.log')
        
        # 创建日志目录
        if file_enabled:
            log_dir = os.path.dirname(file_path)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)
        
        # 配置日志格式
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # 创建根日志记录器
        logger = logging.getLogger()
        logger.setLevel(getattr(logging, log_level.upper()))
        
        # 清除现有处理器
        logger.handlers.clear()
        
        # 添加控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # 添加文件处理器
        if file_enabled:
            from logging.handlers import RotatingFileHandler
            max_size = config.get('logging.max_file_size', 10) * 1024 * 1024  # MB to bytes
            backup_count = config.get('logging.backup_count', 5)
            
            file_handler = RotatingFileHandler(
                file_path, maxBytes=max_size, backupCount=backup_count, encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger


def ensure_dir(directory: str):
    """
    确保目录存在
    
    Args:
        directory: 目录路径
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"创建目录: {directory}")


def format_number(value: float, decimal_places: int = 2) -> str:
    """
    格式化数字显示
    
    Args:
        value: 数值
        decimal_places: 小数位数
        
    Returns:
        格式化后的字符串
    """
    if pd.isna(value):
        return "N/A"
    
    if abs(value) >= 1e8:
        return f"{value/1e8:.{decimal_places}f}亿"
    elif abs(value) >= 1e4:
        return f"{value/1e4:.{decimal_places}f}万"
    else:
        return f"{value:.{decimal_places}f}"


def format_percentage(value: float, decimal_places: int = 2) -> str:
    """
    格式化百分比显示
    
    Args:
        value: 数值
        decimal_places: 小数位数
        
    Returns:
        格式化后的百分比字符串
    """
    if pd.isna(value):
        return "N/A"
    return f"{value:.{decimal_places}f}%"


def calculate_change_percentage(current: float, previous: float) -> float:
    """
    计算变化百分比
    
    Args:
        current: 当前值
        previous: 之前值
        
    Returns:
        变化百分比
    """
    if pd.isna(current) or pd.isna(previous) or previous == 0:
        return np.nan
    
    return ((current - previous) / previous) * 100


def is_trading_day(check_date: Optional[date] = None) -> bool:
    """
    判断是否为交易日（简单版本，仅排除周末）
    
    Args:
        check_date: 检查日期，默认为今天
        
    Returns:
        是否为交易日
    """
    if check_date is None:
        check_date = date.today()
    
    # 周末不是交易日
    return check_date.weekday() < 5


def get_trading_dates(start_date: date, end_date: date) -> list:
    """
    获取指定日期范围内的交易日列表（简单版本）
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        交易日列表
    """
    trading_dates = []
    current_date = start_date
    
    while current_date <= end_date:
        if is_trading_day(current_date):
            trading_dates.append(current_date)
        current_date = pd.Timestamp(current_date) + pd.Timedelta(days=1)
        current_date = current_date.date()
    
    return trading_dates


def validate_stock_symbol(symbol: str) -> bool:
    """
    验证股票代码格式
    
    Args:
        symbol: 股票代码
        
    Returns:
        是否为有效的股票代码
    """
    if not symbol or len(symbol) != 6:
        return False
    
    if not symbol.isdigit():
        return False
    
    # 检查是否为A股代码
    if symbol.startswith(('00', '30', '60', '68')):
        return True
    
    return False


def clean_dataframe(df: pd.DataFrame, 
                   numeric_columns: Optional[list] = None,
                   date_columns: Optional[list] = None) -> pd.DataFrame:
    """
    清洗DataFrame数据
    
    Args:
        df: 原始DataFrame
        numeric_columns: 数值列名列表
        date_columns: 日期列名列表
        
    Returns:
        清洗后的DataFrame
    """
    if df.empty:
        return df
    
    df_clean = df.copy()
    
    # 处理数值列
    if numeric_columns:
        for col in numeric_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # 处理日期列
    if date_columns:
        for col in date_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
    
    return df_clean


def calculate_technical_score(macd_signal: str, rsi_signal: str, ma_signal: str,
                            weights: Optional[Dict[str, float]] = None) -> float:
    """
    计算技术指标综合得分
    
    Args:
        macd_signal: MACD信号
        rsi_signal: RSI信号
        ma_signal: 均线信号
        weights: 权重字典
        
    Returns:
        综合得分
    """
    if weights is None:
        weights = {'macd': 0.4, 'rsi': 0.3, 'ma': 0.3}
    
    score = 0.0
    
    # MACD得分
    if macd_signal == '金叉':
        score += weights.get('macd', 0.4) * 100
    elif macd_signal == '看涨':
        score += weights.get('macd', 0.4) * 60
    elif macd_signal == '死叉':
        score += weights.get('macd', 0.4) * 20
    else:
        score += weights.get('macd', 0.4) * 40
    
    # RSI得分
    if rsi_signal == '超卖反弹':
        score += weights.get('rsi', 0.3) * 100
    elif rsi_signal == '正常':
        score += weights.get('rsi', 0.3) * 60
    elif rsi_signal == '超买':
        score += weights.get('rsi', 0.3) * 20
    else:
        score += weights.get('rsi', 0.3) * 40
    
    # 均线得分
    if ma_signal == '多头排列':
        score += weights.get('ma', 0.3) * 100
    elif ma_signal == '突破':
        score += weights.get('ma', 0.3) * 80
    elif ma_signal == '空头排列':
        score += weights.get('ma', 0.3) * 20
    else:
        score += weights.get('ma', 0.3) * 40
    
    return round(score, 2)


def get_current_timestamp() -> str:
    """
    获取当前时间戳字符串
    
    Returns:
        时间戳字符串
    """
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_date_string(date_obj: Optional[date] = None, format_str: str = '%Y-%m-%d') -> str:
    """
    获取日期字符串
    
    Args:
        date_obj: 日期对象，默认为今天
        format_str: 格式字符串
        
    Returns:
        日期字符串
    """
    if date_obj is None:
        date_obj = date.today()
    
    return date_obj.strftime(format_str)


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    安全除法，避免除零错误
    
    Args:
        numerator: 分子
        denominator: 分母
        default: 默认值
        
    Returns:
        除法结果
    """
    if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
        return default
    
    return numerator / denominator


def filter_outliers(data: pd.Series, threshold: float = 3.0) -> pd.Series:
    """
    过滤异常值
    
    Args:
        data: 数据序列
        threshold: 标准差倍数阈值
        
    Returns:
        过滤后的数据序列
    """
    if data.empty:
        return data
    
    mean = data.mean()
    std = data.std()
    
    if pd.isna(mean) or pd.isna(std) or std == 0:
        return data
    
    # 计算上下界
    lower_bound = mean - threshold * std
    upper_bound = mean + threshold * std
    
    # 过滤异常值
    return data[(data >= lower_bound) & (data <= upper_bound)]


# 全局配置管理器实例
config_manager = ConfigManager()

# 设置日志
logger = LoggerManager.setup_logger(config_manager)


if __name__ == "__main__":
    # 测试工具函数
    print("测试配置管理器:")
    print(f"数据库路径: {config_manager.get('database.path')}")
    print(f"最大结果数: {config_manager.get('output.max_results')}")
    
    print("\n测试格式化函数:")
    print(f"格式化数字: {format_number(123456789.12)}")
    print(f"格式化百分比: {format_percentage(12.345)}")
    
    print("\n测试股票代码验证:")
    test_symbols = ['000001', '600000', '300001', '123456', 'ABCDEF']
    for symbol in test_symbols:
        print(f"{symbol}: {validate_stock_symbol(symbol)}")
    
    print(f"\n当前时间戳: {get_current_timestamp()}")
    print(f"今天是否为交易日: {is_trading_day()}")