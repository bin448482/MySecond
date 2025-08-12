"""
数据库管理模块
负责SQLite数据库的创建、连接和基本操作
"""

import sqlite3
import pandas as pd
from datetime import datetime, date
from typing import Optional, List, Dict, Any
import os
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """SQLite数据库管理类"""
    
    def __init__(self, db_path: str = "data/stock_data.db"):
        """
        初始化数据库管理器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self._ensure_data_dir()
        self.create_tables()
    
    def _ensure_data_dir(self):
        """确保数据目录存在"""
        data_dir = os.path.dirname(self.db_path)
        if data_dir and not os.path.exists(data_dir):
            os.makedirs(data_dir)
            logger.info(f"创建数据目录: {data_dir}")
    
    def get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # 使查询结果可以通过列名访问
        return conn
    
    def create_tables(self):
        """创建所有必要的数据表"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # 股票基本信息表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stock_info (
                    symbol TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    industry TEXT,
                    market TEXT,
                    list_date DATE,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 日线数据表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    date DATE NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    amount REAL,
                    turnover_rate REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, date)
                )
            ''')
            
            # 技术指标表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS technical_indicators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    date DATE NOT NULL,
                    macd REAL,
                    macd_signal REAL,
                    macd_histogram REAL,
                    rsi REAL,
                    ma5 REAL,
                    ma10 REAL,
                    ma20 REAL,
                    ma60 REAL,
                    volume_ratio REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, date)
                )
            ''')
            
            # 选股结果表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS selection_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    name TEXT,
                    date DATE NOT NULL,
                    price_change REAL,
                    volume_ratio REAL,
                    turnover_rate REAL,
                    macd_signal TEXT,
                    rsi_signal TEXT,
                    ma_signal TEXT,
                    total_score REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建索引以提高查询性能
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_data_symbol_date ON daily_data(symbol, date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_technical_indicators_symbol_date ON technical_indicators(symbol, date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_selection_results_date ON selection_results(date)')
            
            conn.commit()
            logger.info("数据库表创建成功")
            
        except Exception as e:
            logger.error(f"创建数据库表失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def insert_stock_info(self, stock_data: pd.DataFrame) -> int:
        """
        插入股票基本信息
        
        Args:
            stock_data: 包含股票信息的DataFrame
            
        Returns:
            插入的记录数
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # 准备插入数据
            records = []
            for _, row in stock_data.iterrows():
                records.append((
                    row.get('symbol', ''),
                    row.get('name', ''),
                    row.get('industry', ''),
                    row.get('market', ''),
                    row.get('list_date', None)
                ))
            
            # 使用INSERT OR REPLACE来处理重复数据
            cursor.executemany('''
                INSERT OR REPLACE INTO stock_info 
                (symbol, name, industry, market, list_date, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', records)
            
            conn.commit()
            inserted_count = cursor.rowcount
            logger.info(f"插入股票信息 {inserted_count} 条")
            return inserted_count
            
        except Exception as e:
            logger.error(f"插入股票信息失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def insert_daily_data(self, symbol: str, data: pd.DataFrame) -> int:
        """
        插入日线数据
        
        Args:
            symbol: 股票代码
            data: 日线数据DataFrame
            
        Returns:
            插入的记录数
        """
        if data.empty:
            return 0
            
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            records = []
            for _, row in data.iterrows():
                records.append((
                    symbol,
                    row.get('date', ''),
                    row.get('open', 0),
                    row.get('high', 0),
                    row.get('low', 0),
                    row.get('close', 0),
                    row.get('volume', 0),
                    row.get('amount', 0),
                    row.get('turnover_rate', 0)
                ))
            
            cursor.executemany('''
                INSERT OR REPLACE INTO daily_data 
                (symbol, date, open, high, low, close, volume, amount, turnover_rate, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', records)
            
            conn.commit()
            inserted_count = cursor.rowcount
            logger.info(f"插入 {symbol} 日线数据 {inserted_count} 条")
            return inserted_count
            
        except Exception as e:
            logger.error(f"插入日线数据失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def get_stock_list(self) -> pd.DataFrame:
        """
        获取股票列表
        
        Returns:
            股票列表DataFrame
        """
        conn = self.get_connection()
        try:
            query = "SELECT * FROM stock_info ORDER BY symbol"
            df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def get_stock_list_exclude_incomplete(self, report_file: str = "data/completeness_report.json") -> pd.DataFrame:
        """
        根据完整性报告获取数据完整的股票列表（排除不完整的股票）
        
        Args:
            report_file: 完整性报告文件路径
        
        Returns:
            数据完整的股票列表DataFrame
        """
        import json
        import os
        
        try:
            # 检查报告文件是否存在
            if not os.path.exists(report_file):
                logger.warning(f"完整性报告文件不存在: {report_file}，返回所有股票")
                return self.get_stock_list()
            
            # 读取完整性报告
            with open(report_file, 'r', encoding='utf-8') as f:
                report = json.load(f)
            
            # 获取数据完整的股票代码
            detailed_results = report.get('detailed_results', {})
            complete_symbols = []
            
            for symbol, stock_data in detailed_results.items():
                status = stock_data.get('status', '')
                if status == 'complete':
                    complete_symbols.append(symbol)
            
            if not complete_symbols:
                logger.warning("报告中没有找到数据完整的股票")
                return pd.DataFrame()
            
            # 从数据库获取这些股票的信息
            conn = self.get_connection()
            try:
                placeholders = ','.join(['?' for _ in complete_symbols])
                query = f"SELECT * FROM stock_info WHERE symbol IN ({placeholders}) ORDER BY symbol"
                df = pd.read_sql_query(query, conn, params=complete_symbols)
                logger.info(f"根据完整性报告获取到 {len(df)} 只数据完整的股票")
                return df
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"根据完整性报告获取股票列表失败: {e}")
            # 出错时返回所有股票
            return self.get_stock_list()
    
    def get_stock_data(self, symbol: str, days: int = 60) -> pd.DataFrame:
        """
        获取指定股票的历史数据
        
        Args:
            symbol: 股票代码
            days: 获取天数
            
        Returns:
            历史数据DataFrame
        """
        conn = self.get_connection()
        try:
            query = '''
                SELECT * FROM daily_data 
                WHERE symbol = ? 
                ORDER BY date DESC 
                LIMIT ?
            '''
            df = pd.read_sql_query(query, conn, params=(symbol, days))
            # 按日期正序排列
            df = df.sort_values('date').reset_index(drop=True)
            return df
        except Exception as e:
            logger.error(f"获取股票数据失败: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def get_last_update_date(self, symbol: str) -> Optional[str]:
        """
        获取指定股票的最后更新日期
        
        Args:
            symbol: 股票代码
            
        Returns:
            最后更新日期字符串，如果没有数据则返回None
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT MAX(date) as last_date 
                FROM daily_data 
                WHERE symbol = ?
            ''', (symbol,))
            
            result = cursor.fetchone()
            return result['last_date'] if result and result['last_date'] else None
            
        except Exception as e:
            logger.error(f"获取最后更新日期失败: {e}")
            return None
        finally:
            conn.close()
    
    def insert_technical_indicators(self, symbol: str, indicators_data: pd.DataFrame) -> int:
        """
        插入技术指标数据
        
        Args:
            symbol: 股票代码
            indicators_data: 技术指标数据DataFrame
            
        Returns:
            插入的记录数
        """
        if indicators_data.empty:
            return 0
            
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            records = []
            for _, row in indicators_data.iterrows():
                records.append((
                    symbol,
                    row.get('date', ''),
                    row.get('macd', None),
                    row.get('macd_signal', None),
                    row.get('macd_histogram', None),
                    row.get('rsi', None),
                    row.get('ma5', None),
                    row.get('ma10', None),
                    row.get('ma20', None),
                    row.get('ma60', None),
                    row.get('volume_ratio', None)
                ))
            
            cursor.executemany('''
                INSERT OR REPLACE INTO technical_indicators 
                (symbol, date, macd, macd_signal, macd_histogram, rsi, 
                 ma5, ma10, ma20, ma60, volume_ratio, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', records)
            
            conn.commit()
            inserted_count = cursor.rowcount
            logger.info(f"插入 {symbol} 技术指标数据 {inserted_count} 条")
            return inserted_count
            
        except Exception as e:
            logger.error(f"插入技术指标数据失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def save_selection_results(self, results: pd.DataFrame) -> int:
        """
        保存选股结果
        
        Args:
            results: 选股结果DataFrame
            
        Returns:
            插入的记录数
        """
        if results.empty:
            return 0
            
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            records = []
            current_date = date.today().isoformat()
            
            for _, row in results.iterrows():
                records.append((
                    row.get('symbol', ''),
                    row.get('name', ''),
                    current_date,
                    row.get('price_change', 0),
                    row.get('volume_ratio', 0),
                    row.get('turnover_rate', 0),
                    row.get('macd_signal', ''),
                    row.get('rsi_signal', ''),
                    row.get('ma_signal', ''),
                    row.get('total_score', 0)
                ))
            
            cursor.executemany('''
                INSERT INTO selection_results 
                (symbol, name, date, price_change, volume_ratio, turnover_rate,
                 macd_signal, rsi_signal, ma_signal, total_score, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', records)
            
            conn.commit()
            inserted_count = cursor.rowcount
            logger.info(f"保存选股结果 {inserted_count} 条")
            return inserted_count
            
        except Exception as e:
            logger.error(f"保存选股结果失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def get_database_stats(self) -> Dict[str, int]:
        """
        获取数据库统计信息
        
        Returns:
            包含各表记录数的字典
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        stats = {}
        tables = ['stock_info', 'daily_data', 'technical_indicators', 'selection_results']
        
        try:
            for table in tables:
                cursor.execute(f'SELECT COUNT(*) as count FROM {table}')
                result = cursor.fetchone()
                stats[table] = result['count'] if result else 0
            
            return stats
            
        except Exception as e:
            logger.error(f"获取数据库统计信息失败: {e}")
            return {}
        finally:
            conn.close()
    
    def clear_all_data(self) -> bool:
        """
        清除所有数据表中的数据
        
        Returns:
            清除是否成功
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # 清除所有表的数据
            tables = ['selection_results', 'technical_indicators', 'daily_data', 'stock_info']
            
            for table in tables:
                cursor.execute(f'DELETE FROM {table}')
                logger.info(f"清除表 {table} 的所有数据")
            
            # 重置自增ID
            cursor.execute("DELETE FROM sqlite_sequence")
            
            conn.commit()
            logger.info("所有数据清除成功")
            return True
            
        except Exception as e:
            logger.error(f"清除数据失败: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def clear_stock_info(self) -> bool:
        """
        仅清除股票基本信息表
        
        Returns:
            清除是否成功
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('DELETE FROM stock_info')
            conn.commit()
            
            cleared_count = cursor.rowcount
            logger.info(f"清除股票信息表，共删除 {cleared_count} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"清除股票信息表失败: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def clear_daily_data(self) -> bool:
        """
        仅清除日线数据表
        
        Returns:
            清除是否成功
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('DELETE FROM daily_data')
            conn.commit()
            
            cleared_count = cursor.rowcount
            logger.info(f"清除日线数据表，共删除 {cleared_count} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"清除日线数据表失败: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def backup_database(self, backup_path: str = None) -> bool:
        """
        备份数据库
        
        Args:
            backup_path: 备份文件路径
            
        Returns:
            备份是否成功
        """
        if backup_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f"data/backup_stock_data_{timestamp}.db"
        
        try:
            import shutil
            shutil.copy2(self.db_path, backup_path)
            logger.info(f"数据库备份成功: {backup_path}")
            return True
        except Exception as e:
            logger.error(f"数据库备份失败: {e}")
            return False
    
    def get_stock_count_by_market(self) -> Dict[str, int]:
        """
        按市场统计股票数量
        
        Returns:
            市场股票数量字典
        """
        conn = self.get_connection()
        try:
            query = """
                SELECT market, COUNT(*) as count
                FROM stock_info
                GROUP BY market
                ORDER BY count DESC
            """
            df = pd.read_sql_query(query, conn)
            return dict(zip(df['market'], df['count']))
        except Exception as e:
            logger.error(f"获取市场统计失败: {e}")
            return {}
        finally:
            conn.close()


if __name__ == "__main__":
    # 测试数据库管理器
    db = DatabaseManager()
    stats = db.get_database_stats()
    print("数据库统计信息:")
    for table, count in stats.items():
        print(f"  {table}: {count} 条记录")