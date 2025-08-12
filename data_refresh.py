#!/usr/bin/env python3
"""
数据刷新工具
根据data_completeness_checker.py生成的报告，有针对性地刷新缺失60天数据的股票

使用方法:
  python data_refresh.py smart-refresh [--test-mode] [--max-stocks N] [--yes]
  python data_refresh.py full-refresh [--test-mode] [--max-stocks N] [--yes]
  python data_refresh.py cleanup [--progress-file PATH] [--yes]
  python data_refresh.py check [--target-days N] [--output-file PATH]
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta, date
import logging
import os
import sys
import time
import json
import argparse
from collections import defaultdict

# 导入项目模块
from database import DatabaseManager
from enhanced_data_fetcher import EnhancedDataFetcher

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataRefreshManager:
    """数据刷新管理器"""
    
    def __init__(self):
        """初始化管理器"""
        self.db = DatabaseManager()
        self.data_fetcher = EnhancedDataFetcher(self.db)
        self.issues = {
            'missing_days': {},      # 缺失天数的股票
            'duplicate_days': {},    # 重复天数的股票
            'incomplete_stocks': [], # 数据不完整的股票
            'complete_stocks': []    # 数据完整的股票
        }
    
    def backup_database(self) -> str:
        """
        备份数据库
        
        Returns:
            备份文件路径
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = f"data/backup_before_refresh_{timestamp}.db"
        
        try:
            import shutil
            shutil.copy2(self.db.db_path, backup_path)
            logger.info(f"数据库已备份至: {backup_path}")
            return backup_path
        except Exception as e:
            logger.error(f"数据库备份失败: {e}")
            return None
    
    def get_incomplete_stocks_from_report(self, report_file: str = "data/completeness_report.json") -> dict:
        """
        从完整性报告中获取需要刷新的股票
        
        Args:
            report_file: 报告文件路径
            
        Returns:
            包含需要刷新股票信息的字典
        """
        if not os.path.exists(report_file):
            logger.warning(f"报告文件不存在: {report_file}")
            return {}
        
        try:
            with open(report_file, 'r', encoding='utf-8') as f:
                report = json.load(f)
            
            # 提取需要刷新的股票
            incomplete_stocks = {
                'missing_data': [],
                'duplicate_data': [],
                'missing_and_duplicate': []
            }
            
            detailed_results = report.get('detailed_results', {})
            
            for symbol, stock_data in detailed_results.items():
                status = stock_data.get('status', '')
                if status in ['missing_data', 'duplicate_data', 'missing_and_duplicate']:
                    incomplete_stocks[status].append({
                        'symbol': symbol,
                        'missing_days': len(stock_data.get('missing_days', [])),
                        'duplicate_days': len(stock_data.get('duplicate_days', [])),
                        'completeness_rate': stock_data.get('completeness_rate', 0)
                    })
            
            total_incomplete = sum(len(stocks) for stocks in incomplete_stocks.values())
            logger.info(f"从报告中找到 {total_incomplete} 只需要刷新的股票")
            logger.info(f"  缺失数据: {len(incomplete_stocks['missing_data'])} 只")
            logger.info(f"  重复数据: {len(incomplete_stocks['duplicate_data'])} 只")
            logger.info(f"  缺失+重复: {len(incomplete_stocks['missing_and_duplicate'])} 只")
            
            return incomplete_stocks
            
        except Exception as e:
            logger.error(f"读取报告文件失败: {e}")
            return {}
    
    def get_stock_symbols(self) -> list:
        """
        获取所有股票代码
        
        Returns:
            股票代码列表
        """
        conn = self.db.get_connection()
        try:
            # 从stock_info表获取股票代码
            query = "SELECT DISTINCT symbol FROM stock_info ORDER BY symbol"
            df = pd.read_sql_query(query, conn)
            symbols = df['symbol'].tolist()
            logger.info(f"获取到 {len(symbols)} 只股票代码")
            return symbols
        except Exception as e:
            logger.error(f"获取股票代码失败: {e}")
            return []
        finally:
            conn.close()
    
    def clear_stock_data(self, symbols: list) -> bool:
        """
        清除指定股票的数据
        
        Args:
            symbols: 要清除的股票代码列表
            
        Returns:
            清除是否成功
        """
        if not symbols:
            return True
            
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            # 获取清除前的记录数
            placeholders = ','.join(['?' for _ in symbols])
            cursor.execute(f"SELECT COUNT(*) FROM daily_data WHERE symbol IN ({placeholders})", symbols)
            before_count = cursor.fetchone()[0]
            
            # 清除指定股票的日线数据
            cursor.execute(f"DELETE FROM daily_data WHERE symbol IN ({placeholders})", symbols)
            
            # 清除指定股票的技术指标数据
            cursor.execute(f"DELETE FROM technical_indicators WHERE symbol IN ({placeholders})", symbols)
            
            conn.commit()
            
            logger.info(f"已清除 {len(symbols)} 只股票的 {before_count} 条数据记录")
            return True
            
        except Exception as e:
            logger.error(f"清除股票数据失败: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def clear_all_daily_data(self) -> bool:
        """
        清除所有日线数据表
        
        Returns:
            清除是否成功
        """
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            # 获取清除前的记录数
            cursor.execute("SELECT COUNT(*) FROM daily_data")
            before_count = cursor.fetchone()[0]
            
            # 清除日线数据
            cursor.execute("DELETE FROM daily_data")
            
            # 清除技术指标数据（依赖于日线数据）
            cursor.execute("DELETE FROM technical_indicators")
            
            conn.commit()
            
            logger.info(f"已清除 {before_count} 条日线数据记录")
            logger.info("已清除所有技术指标数据")
            return True
            
        except Exception as e:
            logger.error(f"清除数据失败: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def refresh_stock_data(self, symbols: list, days: int = 60, max_stocks: int = None) -> dict:
        """
        重新获取股票数据
        
        Args:
            symbols: 股票代码列表
            days: 获取天数
            max_stocks: 最大处理股票数（用于测试）
            
        Returns:
            刷新结果统计
        """
        if max_stocks:
            symbols = symbols[:max_stocks]
            logger.info(f"限制处理股票数量为: {max_stocks}")
        
        total_symbols = len(symbols)
        success_count = 0
        failed_count = 0
        total_records = 0
        
        logger.info(f"开始重新获取 {total_symbols} 只股票的数据...")
        
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"[{i}/{total_symbols}] 获取 {symbol} 的数据...")
                
                # 获取股票数据 - 使用固定延迟管理器
                records_added = self.data_fetcher.update_stock_data_with_fixed_delay(symbol, days=days)
                
                if records_added > 0:
                    success_count += 1
                    total_records += records_added
                    logger.info(f"  ✅ {symbol}: 成功获取 {records_added} 条记录")
                else:
                    failed_count += 1
                    logger.warning(f"  ❌ {symbol}: 获取失败或无新数据")
                
                # 每处理10只股票显示进度
                if i % 10 == 0:
                    logger.info(f"进度: {i}/{total_symbols} ({i/total_symbols*100:.1f}%), 成功: {success_count}, 失败: {failed_count}")
                
                # enhanced_data_fetcher已经处理了延迟管理，无需额外延迟
                # 但可以检查网络状态，决定是否需要暂停
                if self.data_fetcher.delay_manager.should_pause():
                    logger.warning(f"网络状态过差，暂停处理。已处理 {i}/{total_symbols}")
                    break
                
            except Exception as e:
                failed_count += 1
                logger.error(f"  ❌ {symbol}: 获取出错 - {e}")
        
        results = {
            'total_symbols': total_symbols,
            'success_count': success_count,
            'failed_count': failed_count,
            'total_records': total_records,
            'success_rate': success_count / total_symbols * 100 if total_symbols > 0 else 0
        }
        
        # 获取网络状态和指标摘要
        metrics_summary = self.data_fetcher.get_metrics_summary()
        
        logger.info(f"数据刷新完成:")
        logger.info(f"  总股票数: {total_symbols}")
        logger.info(f"  成功: {success_count} ({results['success_rate']:.1f}%)")
        logger.info(f"  失败: {failed_count}")
        logger.info(f"  总记录数: {total_records}")
        logger.info(f"  网络状态: {metrics_summary['network_status']}")
        logger.info(f"  请求成功率: {metrics_summary['success_rate']}")
        logger.info(f"  平均响应时间: {metrics_summary['average_response_time']}")
        
        return results
    
    def get_trading_days(self, start_date: date, end_date: date) -> set:
        """
        获取交易日集合（排除周末）
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            交易日期集合
        """
        trading_days = set()
        current_date = start_date
        
        while current_date <= end_date:
            # 排除周末（周六=5, 周日=6）
            if current_date.weekday() < 5:
                trading_days.add(current_date)
            current_date += timedelta(days=1)
        
        return trading_days
    
    def check_stock_completeness(self, symbol: str, target_days: int = 60) -> dict:
        """
        检查单只股票的数据完备性
        
        Args:
            symbol: 股票代码
            target_days: 目标天数
            
        Returns:
            检查结果字典
        """
        conn = self.db.get_connection()
        
        try:
            # 获取股票的所有数据日期
            query = """
                SELECT date, COUNT(*) as count
                FROM daily_data 
                WHERE symbol = ? 
                GROUP BY date
                ORDER BY date DESC
            """
            
            df = pd.read_sql_query(query, conn, params=(symbol,))
            
            if df.empty:
                return {
                    'symbol': symbol,
                    'status': 'no_data',
                    'total_records': 0,
                    'missing_days': [],
                    'duplicate_days': [],
                    'data_range': None
                }
            
            # 转换日期
            df['date'] = pd.to_datetime(df['date']).dt.date
            actual_dates = set(df['date'].tolist())
            
            # 找出重复的日期
            duplicate_dates = df[df['count'] > 1]['date'].tolist()
            
            # 计算目标日期范围（最近60个交易日，排除今天因为数据要下午4点后才更新）
            end_date = datetime.now().date() - timedelta(days=1)  # 从昨天开始计算
            start_date = end_date - timedelta(days=target_days + 20)  # 多加20天以确保覆盖60个交易日
            
            # 获取理论交易日
            expected_trading_days = self.get_trading_days(start_date, end_date)
            
            # 只取最近的60个交易日
            expected_trading_days = sorted(expected_trading_days, reverse=True)[:target_days]
            expected_trading_days_set = set(expected_trading_days)
            
            # 找出缺失的日期
            missing_dates = expected_trading_days_set - actual_dates
            
            # 计算实际拥有的目标期间内的数据
            actual_target_dates = actual_dates & expected_trading_days_set
            
            result = {
                'symbol': symbol,
                'total_records': len(df),
                'target_period_records': len(actual_target_dates),
                'expected_records': len(expected_trading_days_set),
                'missing_days': sorted(missing_dates, reverse=True),
                'duplicate_days': duplicate_dates,
                'data_range': {
                    'start': df['date'].min().isoformat() if not df.empty else None,
                    'end': df['date'].max().isoformat() if not df.empty else None
                },
                'completeness_rate': len(actual_target_dates) / len(expected_trading_days_set) * 100 if expected_trading_days_set else 0
            }
            
            # 判断状态
            if len(missing_dates) == 0 and len(duplicate_dates) == 0:
                result['status'] = 'complete'
            elif len(missing_dates) > 0 and len(duplicate_dates) > 0:
                result['status'] = 'missing_and_duplicate'
            elif len(missing_dates) > 0:
                result['status'] = 'missing_data'
            elif len(duplicate_dates) > 0:
                result['status'] = 'duplicate_data'
            else:
                result['status'] = 'unknown'
            
            return result
            
        except Exception as e:
            logger.error(f"检查股票 {symbol} 完备性失败: {e}")
            return {
                'symbol': symbol,
                'status': 'error',
                'error': str(e)
            }
        finally:
            conn.close()
    
    def check_all_stocks_completeness(self, target_days: int = 60) -> dict:
        """
        检查所有股票的数据完备性
        
        Args:
            target_days: 目标天数
            
        Returns:
            完整的检查结果
        """
        logger.info(f"开始检查所有股票的数据完备性（目标天数: {target_days}）...")
        
        conn = self.db.get_connection()
        
        try:
            # 获取所有有数据的股票
            stocks_query = """
                SELECT DISTINCT symbol 
                FROM daily_data 
                ORDER BY symbol
            """
            
            stocks_df = pd.read_sql_query(stocks_query, conn)
            total_stocks = len(stocks_df)
            
            logger.info(f"找到 {total_stocks} 只有数据的股票")
            
            # 重置问题统计
            self.issues = {
                'missing_days': {},
                'duplicate_days': {},
                'incomplete_stocks': [],
                'complete_stocks': []
            }
            
            # 统计结果
            results = {
                'check_time': datetime.now().isoformat(),
                'target_days': target_days,
                'total_stocks': total_stocks,
                'summary': {
                    'complete': 0,
                    'missing_data': 0,
                    'duplicate_data': 0,
                    'missing_and_duplicate': 0,
                    'no_data': 0,
                    'error': 0
                },
                'stocks': {}
            }
            
            # 逐个检查股票
            for i, row in stocks_df.iterrows():
                symbol = row['symbol']
                
                if (i + 1) % 100 == 0:
                    logger.info(f"已检查 {i + 1}/{total_stocks} 只股票...")
                
                stock_result = self.check_stock_completeness(symbol, target_days)
                results['stocks'][symbol] = stock_result
                
                # 更新统计
                status = stock_result.get('status', 'error')
                if status in results['summary']:
                    results['summary'][status] += 1
                
                # 记录问题股票
                if status == 'missing_data' or status == 'missing_and_duplicate':
                    if stock_result['missing_days']:
                        self.issues['missing_days'][symbol] = stock_result['missing_days']
                
                if status == 'duplicate_data' or status == 'missing_and_duplicate':
                    if stock_result['duplicate_days']:
                        self.issues['duplicate_days'][symbol] = stock_result['duplicate_days']
                
                if status == 'complete':
                    self.issues['complete_stocks'].append(symbol)
                else:
                    self.issues['incomplete_stocks'].append(symbol)
            
            logger.info("数据完备性检查完成")
            return results
            
        except Exception as e:
            logger.error(f"检查所有股票完备性失败: {e}")
            return {}
        finally:
            conn.close()
    
    def generate_completeness_report(self, results: dict, output_file: str = "data/completeness_report.json") -> dict:
        """
        生成完备性报告
        
        Args:
            results: 检查结果
            output_file: 输出文件路径
            
        Returns:
            报告字典
        """
        # 确保目录存在
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # 生成详细报告
        report = {
            'metadata': {
                'check_time': results.get('check_time'),
                'target_days': results.get('target_days'),
                'total_stocks': results.get('total_stocks')
            },
            'summary': results.get('summary', {}),
            'issues': self.issues,
            'detailed_results': results.get('stocks', {})
        }
        
        # 保存报告
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"完备性报告已生成: {output_file}")
        
        # 打印摘要
        self.print_completeness_summary(results)
        
        return report
    
    def print_completeness_summary(self, results: dict):
        """打印完整性检查摘要"""
        print("\n" + "=" * 80)
        print("数据完备性检查报告")
        print("=" * 80)
        
        summary = results.get('summary', {})
        total = results.get('total_stocks', 0)
        
        print(f"检查时间: {results.get('check_time', 'N/A')}")
        print(f"目标天数: {results.get('target_days', 60)} 天")
        print(f"总股票数: {total:,} 只")
        
        print(f"\n检查结果统计:")
        print("-" * 40)
        if total > 0:
            print(f"✅ 数据完整: {summary.get('complete', 0):,} 只 ({summary.get('complete', 0)/total*100:.1f}%)")
            print(f"❌ 缺失数据: {summary.get('missing_data', 0):,} 只 ({summary.get('missing_data', 0)/total*100:.1f}%)")
            print(f"🔄 重复数据: {summary.get('duplicate_data', 0):,} 只 ({summary.get('duplicate_data', 0)/total*100:.1f}%)")
            print(f"⚠️  缺失+重复: {summary.get('missing_and_duplicate', 0):,} 只 ({summary.get('missing_and_duplicate', 0)/total*100:.1f}%)")
            print(f"❓ 无数据: {summary.get('no_data', 0):,} 只 ({summary.get('no_data', 0)/total*100:.1f}%)")
            print(f"💥 检查错误: {summary.get('error', 0):,} 只 ({summary.get('error', 0)/total*100:.1f}%)")
        else:
            print("❌ 数据库中没有任何股票数据")
            print("💡 请先运行数据获取程序来填充数据")
        
        # 显示问题详情
        if self.issues['missing_days']:
            print(f"\n缺失数据的股票示例 (前10只):")
            print("-" * 40)
            count = 0
            for symbol, missing_days in self.issues['missing_days'].items():
                if count >= 10:
                    break
                print(f"{symbol}: 缺失 {len(missing_days)} 天")
                count += 1
            
            if len(self.issues['missing_days']) > 10:
                print(f"... 还有 {len(self.issues['missing_days']) - 10} 只股票有缺失数据")
        
        if self.issues['duplicate_days']:
            print(f"\n重复数据的股票示例 (前10只):")
            print("-" * 40)
            count = 0
            for symbol, duplicate_days in self.issues['duplicate_days'].items():
                if count >= 10:
                    break
                print(f"{symbol}: 重复 {len(duplicate_days)} 天")
                count += 1
            
            if len(self.issues['duplicate_days']) > 10:
                print(f"... 还有 {len(self.issues['duplicate_days']) - 10} 只股票有重复数据")
        
        print("\n" + "=" * 80)
    
    def check_and_cleanup_failed_symbols(self, progress_file: str = "data/enhanced_batch_progress.json") -> dict:
        """
        检查失败股票的数据完整性，如果数据完整则从失败列表中清除
        
        Args:
            progress_file: 批处理进度文件路径
            
        Returns:
            清理结果统计
        """
        if not os.path.exists(progress_file):
            logger.warning(f"进度文件不存在: {progress_file}")
            return {}
        
        try:
            # 读取进度文件
            with open(progress_file, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
            
            failed_symbols = progress_data.get('failed_symbols', [])
            if not failed_symbols:
                logger.info("没有失败的股票需要检查")
                return {'total_failed': 0, 'checked': 0, 'cleaned': 0, 'still_failed': 0}
            
            logger.info(f"开始检查 {len(failed_symbols)} 只失败股票的数据完整性...")
            
            # 检查每只股票的数据完整性
            cleaned_symbols = []
            still_failed_symbols = []
            checked_count = 0
            
            for symbol in failed_symbols:
                try:
                    checked_count += 1
                    
                    # 检查股票数据完整性
                    is_complete = self._check_stock_data_completeness(symbol)
                    
                    if is_complete:
                        cleaned_symbols.append(symbol)
                        logger.info(f"✅ {symbol}: 数据完整，从失败列表中清除")
                    else:
                        still_failed_symbols.append(symbol)
                        logger.debug(f"❌ {symbol}: 数据仍不完整")
                    
                    # 每检查50只股票显示进度
                    if checked_count % 50 == 0:
                        logger.info(f"进度: {checked_count}/{len(failed_symbols)}, 已清理: {len(cleaned_symbols)}")
                        
                except Exception as e:
                    logger.error(f"检查股票 {symbol} 时出错: {e}")
                    still_failed_symbols.append(symbol)
            
            # 更新进度文件
            if cleaned_symbols:
                progress_data['failed_symbols'] = still_failed_symbols
                progress_data['failed_count'] = len(still_failed_symbols)
                progress_data['last_cleanup'] = datetime.now().isoformat()
                
                # 备份原文件
                backup_file = f"{progress_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                with open(backup_file, 'w', encoding='utf-8') as f:
                    json.dump(progress_data, f, indent=2, ensure_ascii=False)
                logger.info(f"原进度文件已备份到: {backup_file}")
                
                # 写入更新后的文件
                with open(progress_file, 'w', encoding='utf-8') as f:
                    json.dump(progress_data, f, indent=2, ensure_ascii=False)
                
                logger.info(f"进度文件已更新，清除了 {len(cleaned_symbols)} 只股票")
            
            results = {
                'total_failed': len(failed_symbols),
                'checked': checked_count,
                'cleaned': len(cleaned_symbols),
                'still_failed': len(still_failed_symbols),
                'cleaned_symbols': cleaned_symbols,
                'still_failed_symbols': still_failed_symbols
            }
            
            logger.info("失败股票数据完整性检查完成:")
            logger.info(f"  总失败股票: {results['total_failed']} 只")
            logger.info(f"  已检查: {results['checked']} 只")
            logger.info(f"  数据完整(已清理): {results['cleaned']} 只")
            logger.info(f"  仍然失败: {results['still_failed']} 只")
            
            return results
            
        except Exception as e:
            logger.error(f"检查失败股票数据完整性时出错: {e}")
            return {}
    
    def _check_stock_data_completeness(self, symbol: str, days: int = 60) -> bool:
        """
        检查单只股票的数据完整性 - 使用与data_completeness_checker.py相同的严格标准
        
        Args:
            symbol: 股票代码
            days: 检查天数
            
        Returns:
            数据是否完整（必须满足：无缺失天数 AND 无重复天数）
        """
        try:
            conn = self.db.get_connection()
            
            # 获取股票的所有数据日期和计数
            query = """
                SELECT date, COUNT(*) as count
                FROM daily_data
                WHERE symbol = ?
                GROUP BY date
                ORDER BY date DESC
            """
            
            import pandas as pd
            from datetime import date, timedelta
            
            df = pd.read_sql_query(query, conn, params=(symbol,))
            conn.close()
            
            if df.empty:
                logger.debug(f"股票 {symbol} 没有历史数据")
                return False
            
            # 转换日期
            df['date'] = pd.to_datetime(df['date']).dt.date
            actual_dates = set(df['date'].tolist())
            
            # 找出重复的日期
            duplicate_dates = df[df['count'] > 1]['date'].tolist()
            if duplicate_dates:
                logger.debug(f"股票 {symbol} 有重复数据: {len(duplicate_dates)} 天")
                return False
            
            # 计算目标日期范围（最近60个交易日，排除今天因为数据要下午4点后才更新）
            end_date = datetime.now().date() - timedelta(days=1)  # 从昨天开始计算
            start_date = end_date - timedelta(days=days + 20)  # 多加20天以确保覆盖60个交易日
            
            # 获取理论交易日（排除周末）
            expected_trading_days = set()
            current_date = start_date
            
            while current_date <= end_date:
                # 排除周末（周六=5, 周日=6）
                if current_date.weekday() < 5:
                    expected_trading_days.add(current_date)
                current_date += timedelta(days=1)
            
            # 只取最近的60个交易日
            expected_trading_days = sorted(expected_trading_days, reverse=True)[:days]
            expected_trading_days_set = set(expected_trading_days)
            
            # 找出缺失的日期
            missing_dates = expected_trading_days_set - actual_dates
            
            if missing_dates:
                logger.debug(f"股票 {symbol} 缺失数据: {len(missing_dates)} 天")
                return False
            
            # 只有同时满足：无缺失数据 AND 无重复数据，才算完整
            logger.debug(f"股票 {symbol} 数据完整性检查通过: 无缺失无重复")
            return True
            
        except Exception as e:
            logger.error(f"检查股票 {symbol} 数据完整性时出错: {e}")
            return False


def cmd_smart_refresh(args, manager):
    """执行智能刷新命令"""
    print("\n" + "=" * 60)
    print("智能刷新模式")
    print("=" * 60)
    
    # 检查是否存在报告文件
    report_file = args.report_file or "data/completeness_report.json"
    if not os.path.exists(report_file):
        print(f"❌ 未找到完整性报告文件: {report_file}")
        print("💡 请先运行: python data_refresh.py check")
        return False
    
    # 从报告获取需要刷新的股票
    incomplete_stocks = manager.get_incomplete_stocks_from_report(report_file)
    if not incomplete_stocks:
        print("❌ 无法读取报告或没有需要刷新的股票")
        return False
    
    # 合并所有需要刷新的股票
    all_incomplete = []
    for category, stocks in incomplete_stocks.items():
        all_incomplete.extend([stock['symbol'] for stock in stocks])
    
    # 去重
    symbols_to_refresh = list(set(all_incomplete))
    
    if not symbols_to_refresh:
        print("✅ 所有股票数据都是完整的，无需刷新")
        return True
    
    print(f"需要刷新的股票数量: {len(symbols_to_refresh)} 只")
    
    # 处理测试模式和最大股票数限制
    max_stocks = args.max_stocks
    if args.test_mode and not max_stocks:
        max_stocks = 20
    
    if max_stocks:
        print(f"限制处理股票数量为: {max_stocks}")
    
    # 确认操作
    if not args.yes:
        response = input(f"\n⚠️  将刷新 {len(symbols_to_refresh)} 只股票的数据，是否继续？(y/n): ").lower().strip()
        if response != 'y':
            print("操作已取消")
            return False
    
    try:
        # 1. 备份数据库
        print("\n1. 备份数据库...")
        backup_path = manager.backup_database()
        if not backup_path:
            print("❌ 备份失败，操作终止")
            return False
        
        # 2. 清除需要刷新的股票数据
        print(f"\n2. 清除 {len(symbols_to_refresh)} 只股票的现有数据...")
        if not manager.clear_stock_data(symbols_to_refresh):
            print("❌ 清除数据失败，操作终止")
            return False
        
        # 3. 重新获取数据
        print("\n3. 重新获取股票数据...")
        refresh_results = manager.refresh_stock_data(symbols_to_refresh, days=60, max_stocks=max_stocks)
        
        # 4. 显示结果
        print("\n" + "=" * 80)
        print("智能数据刷新完成")
        print("=" * 80)
        print(f"处理股票: {refresh_results['total_symbols']} 只")
        print(f"成功获取: {refresh_results['success_count']} 只 ({refresh_results['success_rate']:.1f}%)")
        print(f"获取失败: {refresh_results['failed_count']} 只")
        print(f"总记录数: {refresh_results['total_records']} 条")
        print(f"备份文件: {backup_path}")
        print("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"智能刷新过程中出错: {e}")
        print(f"\n❌ 操作失败: {e}")
        print(f"💡 可以从备份文件恢复: {backup_path if 'backup_path' in locals() else '未创建备份'}")
        return False


def cmd_full_refresh(args, manager):
    """执行全量刷新命令"""
    print("\n" + "=" * 60)
    print("全量数据刷新模式")
    print("=" * 60)
    print("操作流程:")
    print("1. 备份当前数据库")
    print("2. 删除所有现有日线数据")
    print("3. 重新获取所有股票数据")
    
    # 处理测试模式和最大股票数限制
    max_stocks = args.max_stocks
    if args.test_mode and not max_stocks:
        max_stocks = 50
    
    if max_stocks:
        print(f"限制处理股票数量为: {max_stocks}")
    
    # 确认操作
    if not args.yes:
        response = input("\n⚠️  此操作将删除所有现有日线数据，是否继续？(y/n): ").lower().strip()
        if response != 'y':
            print("操作已取消")
            return False
    
    try:
        # 1. 备份数据库
        print("\n1. 备份数据库...")
        backup_path = manager.backup_database()
        if not backup_path:
            print("❌ 备份失败，操作终止")
            return False
        
        # 2. 获取股票代码
        print("\n2. 获取股票代码...")
        symbols = manager.get_stock_symbols()
        if not symbols:
            print("❌ 未找到股票代码，操作终止")
            return False
        
        # 3. 清除现有数据
        print("\n3. 清除所有现有日线数据...")
        if not manager.clear_all_daily_data():
            print("❌ 清除数据失败，操作终止")
            return False
        
        # 4. 重新获取数据
        print("\n4. 重新获取股票数据...")
        refresh_results = manager.refresh_stock_data(symbols, days=60, max_stocks=max_stocks)
        
        # 5. 显示结果
        print("\n" + "=" * 80)
        print("全量数据刷新完成")
        print("=" * 80)
        print(f"处理股票: {refresh_results['total_symbols']} 只")
        print(f"成功获取: {refresh_results['success_count']} 只 ({refresh_results['success_rate']:.1f}%)")
        print(f"获取失败: {refresh_results['failed_count']} 只")
        print(f"总记录数: {refresh_results['total_records']} 条")
        print(f"备份文件: {backup_path}")
        
        print("\n💡 现在可以运行以下命令检查数据完整性:")
        print("python data_refresh.py check")
        print("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"全量刷新过程中出错: {e}")
        print(f"\n❌ 操作失败: {e}")
        print(f"💡 可以从备份文件恢复: {backup_path if 'backup_path' in locals() else '未创建备份'}")
        return False


def cmd_cleanup(args, manager):
    """执行失败股票清理命令"""
    print("\n" + "=" * 60)
    print("失败股票数据完整性检查模式")
    print("=" * 60)
    print("操作说明:")
    print("1. 检查enhanced_batch_progress.json中失败股票的数据完整性")
    print("2. 如果数据实际完整，则从失败列表中清除")
    print("3. 更新进度文件并备份原文件")
    
    # 检查进度文件是否存在
    progress_file = args.progress_file or "data/enhanced_batch_progress.json"
    if not os.path.exists(progress_file):
        print(f"❌ 未找到进度文件: {progress_file}")
        print("💡 请先运行批量数据获取程序生成进度文件")
        return False
    
    # 显示当前失败股票数量
    try:
        with open(progress_file, 'r', encoding='utf-8') as f:
            progress_data = json.load(f)
        failed_count = len(progress_data.get('failed_symbols', []))
        print(f"\n当前失败股票数量: {failed_count} 只")
        
        if failed_count == 0:
            print("✅ 没有失败的股票需要检查")
            return True
            
    except Exception as e:
        print(f"❌ 读取进度文件失败: {e}")
        return False
    
    # 确认操作
    if not args.yes:
        response = input(f"\n⚠️  将检查 {failed_count} 只失败股票的数据完整性，是否继续？(y/n): ").lower().strip()
        if response != 'y':
            print("操作已取消")
            return False
    
    try:
        # 执行失败股票清理
        print("\n开始检查失败股票的数据完整性...")
        cleanup_results = manager.check_and_cleanup_failed_symbols(progress_file)
        
        # 显示结果
        print("\n" + "=" * 80)
        print("失败股票数据完整性检查完成")
        print("=" * 80)
        print(f"总失败股票: {cleanup_results.get('total_failed', 0)} 只")
        print(f"已检查: {cleanup_results.get('checked', 0)} 只")
        print(f"数据完整(已清理): {cleanup_results.get('cleaned', 0)} 只")
        print(f"仍然失败: {cleanup_results.get('still_failed', 0)} 只")
        
        if cleanup_results.get('cleaned', 0) > 0:
            print(f"\n✅ 成功清理了 {cleanup_results['cleaned']} 只股票")
            print("已清理的股票代码:")
            cleaned_symbols = cleanup_results.get('cleaned_symbols', [])
            for i, symbol in enumerate(cleaned_symbols):
                if i % 10 == 0:
                    print()
                print(f"{symbol:>8}", end=" ")
            print()
            
            print(f"\n💡 进度文件已更新，原文件已备份")
        else:
            print("\n📝 没有发现数据完整的失败股票")
        
        print("=" * 80)
        return True
        
    except Exception as e:
        logger.error(f"失败股票清理过程中出错: {e}")
        print(f"\n❌ 操作失败: {e}")
        return False


def cmd_check(args, manager):
    """执行数据完整性检查命令"""
    print("\n" + "=" * 60)
    print("数据完整性检查模式")
    print("=" * 60)
    print("操作说明:")
    print("1. 检查所有股票的数据完整性（最近60个交易日）")
    print("2. 生成详细的完整性报告")
    print("3. 显示缺失数据和重复数据的统计")
    
    target_days = args.target_days or 60
    output_file = args.output_file or "data/completeness_report.json"
    
    print(f"目标天数: {target_days}")
    print(f"输出文件: {output_file}")
    
    try:
        # 执行完整性检查
        print("\n开始检查所有股票的数据完整性...")
        results = manager.check_all_stocks_completeness(target_days=target_days)
        
        if results:
            # 生成报告
            report = manager.generate_completeness_report(results, output_file)
            print(f"\n📊 详细报告已保存至: {output_file}")
            
            # 显示建议
            summary = results.get('summary', {})
            incomplete_count = summary.get('missing_data', 0) + summary.get('duplicate_data', 0) + summary.get('missing_and_duplicate', 0)
            
            if incomplete_count > 0:
                print(f"\n💡 发现 {incomplete_count} 只股票数据不完整，建议运行:")
                print("   python data_refresh.py smart-refresh")
            else:
                print(f"\n✅ 所有股票数据完整，无需刷新")
            
            return True
        else:
            print("❌ 检查失败，请检查数据库连接")
            return False
            
    except Exception as e:
        logger.error(f"数据完整性检查过程中出错: {e}")
        print(f"\n❌ 操作失败: {e}")
        return False


def create_parser():
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description='数据刷新工具 - 根据完整性检查报告，有针对性地刷新缺失数据的股票',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 智能刷新（推荐）
  python data_refresh.py smart-refresh
  python data_refresh.py smart-refresh --test-mode --yes
  
  # 全量刷新
  python data_refresh.py full-refresh --max-stocks 100
  
  # 清理失败股票列表
  python data_refresh.py cleanup --yes
  
  # 数据完整性检查
  python data_refresh.py check --target-days 60
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # smart-refresh 子命令
    smart_parser = subparsers.add_parser(
        'smart-refresh',
        help='基于报告的智能刷新（推荐）',
        description='根据完整性报告，只刷新有问题的股票数据'
    )
    smart_parser.add_argument('--test-mode', action='store_true',
                             help='测试模式（限制处理股票数量）')
    smart_parser.add_argument('--max-stocks', type=int,
                             help='最大处理股票数量')
    smart_parser.add_argument('--report-file',
                             help='完整性报告文件路径（默认: data/completeness_report.json）')
    smart_parser.add_argument('--yes', action='store_true',
                             help='跳过确认提示，直接执行')
    
    # full-refresh 子命令
    full_parser = subparsers.add_parser(
        'full-refresh',
        help='全量数据刷新',
        description='删除所有现有数据，重新获取所有股票数据'
    )
    full_parser.add_argument('--test-mode', action='store_true',
                            help='测试模式（限制处理股票数量）')
    full_parser.add_argument('--max-stocks', type=int,
                            help='最大处理股票数量')
    full_parser.add_argument('--yes', action='store_true',
                            help='跳过确认提示，直接执行')
    
    # cleanup 子命令
    cleanup_parser = subparsers.add_parser(
        'cleanup',
        help='检查并清理失败股票列表',
        description='检查失败股票的数据完整性，如果完整则从失败列表中清除'
    )
    cleanup_parser.add_argument('--progress-file',
                               help='批处理进度文件路径（默认: data/enhanced_batch_progress.json）')
    cleanup_parser.add_argument('--yes', action='store_true',
                               help='跳过确认提示，直接执行')
    
    # check 子命令
    check_parser = subparsers.add_parser(
        'check',
        help='数据完整性检查',
        description='检查所有股票的数据完整性并生成报告'
    )
    check_parser.add_argument('--target-days', type=int, default=60,
                             help='检查的目标天数（默认: 60）')
    check_parser.add_argument('--output-file',
                             help='输出报告文件路径（默认: data/completeness_report.json）')
    
    return parser


def main():
    """主函数 - 处理命令行参数并执行相应操作"""
    parser = create_parser()
    args = parser.parse_args()
    
    # 如果没有提供命令，显示帮助信息
    if not args.command:
        parser.print_help()
        return
    
    print("=" * 80)
    print("智能数据刷新工具")
    print("根据完整性检查报告，有针对性地刷新缺失数据的股票")
    print("=" * 80)
    
    # 创建数据刷新管理器
    manager = DataRefreshManager()
    
    # 根据命令执行相应操作
    success = False
    try:
        if args.command == 'smart-refresh':
            success = cmd_smart_refresh(args, manager)
        elif args.command == 'full-refresh':
            success = cmd_full_refresh(args, manager)
        elif args.command == 'cleanup':
            success = cmd_cleanup(args, manager)
        elif args.command == 'check':
            success = cmd_check(args, manager)
        else:
            print(f"❌ 未知命令: {args.command}")
            parser.print_help()
            return
    except KeyboardInterrupt:
        print("\n\n⚠️  操作被用户中断")
        return
    except Exception as e:
        logger.error(f"执行命令 {args.command} 时出错: {e}")
        print(f"\n❌ 执行失败: {e}")
        return
    
    # 根据执行结果设置退出码
    if success:
        print(f"\n✅ 命令 '{args.command}' 执行成功")
        sys.exit(0)
    else:
        print(f"\n❌ 命令 '{args.command}' 执行失败")
        sys.exit(1)


if __name__ == "__main__":
    main()