"""
增强版短线选股工具主程序
集成动态延迟、指数退避、网络状态监控等功能
提供更稳定的数据获取和断点续传机制
"""

import argparse
import sys
import time
import json
import os
from datetime import datetime, timedelta
from typing import Optional, List
import logging
import pandas as pd

# 导入自定义模块
from database import DatabaseManager
from enhanced_data_fetcher import EnhancedDataFetcher, NetworkStatus
from utils import config_manager, logger


class EnhancedStockSelectorApp:
    """增强版短线选股工具主应用类"""
    
    def __init__(self, enterprise_mode: bool = False):
        """初始化应用"""
        self.config = config_manager
        self.db = DatabaseManager()
        self.data_fetcher = EnhancedDataFetcher(self.db, enterprise_mode=enterprise_mode)
        self.enterprise_mode = enterprise_mode
        
        mode_info = "企业网络模式" if enterprise_mode else "标准模式"
        logger.info(f"增强版短线选股工具初始化完成 - {mode_info}")
    
    def update_all_stocks_historical_data_enhanced(self, days: int = 60, resume: bool = True) -> dict:
        """
        增强版批量更新所有股票历史数据
        支持动态延迟、网络状态监控、智能暂停等功能
        
        Args:
            days: 历史数据天数
            resume: 是否从上次中断处继续
            
        Returns:
            处理结果统计
        """
        progress_file = "data/enhanced_batch_progress.json"
        logger.info(f"开始增强版批量更新所有股票近{days}天的历史数据...")
        
        try:
            # 获取所有股票列表
            stock_list = self.db.get_stock_list()
            if stock_list.empty:
                logger.error("没有股票列表，请先更新股票列表")
                return {}
            
            total_stocks = len(stock_list)
            all_symbols = stock_list['symbol'].tolist()
            
            # 加载之前的进度
            progress_data = {}
            start_index = 0
            
            if resume:
                progress_data = self.load_enhanced_progress(progress_file)
                if progress_data:
                    start_index = progress_data.get('last_processed_index', -1) + 1
                    logger.info(f"检测到之前的进度，从第 {start_index + 1} 只股票继续处理")
                    logger.info(f"之前已处理: {progress_data.get('success_count', 0)} 成功, {progress_data.get('failed_count', 0)} 失败")
            
            # 初始化进度数据
            if not progress_data:
                progress_data = {
                    'total_stocks': total_stocks,
                    'success_count': 0,
                    'failed_count': 0,
                    'total_records': 0,
                    'last_processed_index': -1,
                    'failed_symbols': [],
                    'paused_symbols': [],  # 因网络问题暂停的股票
                    'start_time': datetime.now().isoformat(),
                    'days': days,
                    'network_pauses': 0,  # 网络暂停次数
                    'total_pause_time': 0.0  # 总暂停时间
                }
            
            symbols_to_process = all_symbols[start_index:]
            logger.info(f"总股票数: {total_stocks:,}, 待处理: {len(symbols_to_process):,}")
            
            total_start_time = time.time()
            last_network_check = time.time()
            
            # 逐只股票处理
            for i, symbol in enumerate(symbols_to_process):
                current_index = start_index + i
                
                # 检查网络状态（每50只股票检查一次）
                if i > 0 and i % 50 == 0:
                    network_status = self.data_fetcher.delay_manager.get_network_status()
                    metrics = self.data_fetcher.get_metrics_summary()
                    
                    logger.info(f"网络状态检查 - 状态: {network_status.value}, 成功率: {metrics['success_rate']}")
                    
                    # 如果网络状态很差，暂停一段时间
                    if network_status == NetworkStatus.BAD:
                        pause_time = 60  # 暂停1分钟
                        logger.warning(f"网络状态差，暂停 {pause_time} 秒...")
                        time.sleep(pause_time)
                        progress_data['network_pauses'] += 1
                        progress_data['total_pause_time'] += pause_time
                
                logger.info(f"处理股票 {current_index + 1}/{total_stocks}: {symbol}")
                
                # 更新单只股票的历史数据
                try:
                    updated_count = self.data_fetcher.update_stock_data_with_fixed_delay(symbol, days)
                    
                    if updated_count > 0:
                        progress_data['success_count'] += 1
                        progress_data['total_records'] += updated_count
                        logger.info(f"  ✅ 成功更新 {updated_count} 条记录")
                    else:
                        # 检查是否因为网络问题失败
                        if self.data_fetcher.delay_manager.should_pause():
                            progress_data['paused_symbols'].append(symbol)
                            logger.warning(f"  ⏸️ 因网络问题暂停")
                        else:
                            progress_data['failed_count'] += 1
                            progress_data['failed_symbols'].append(symbol)
                            logger.warning(f"  ❌ 更新失败")
                    
                except Exception as e:
                    progress_data['failed_count'] += 1
                    progress_data['failed_symbols'].append(symbol)
                    logger.error(f"  ❌ 更新失败: {e}")
                    logger.error(f"  详细错误信息: {type(e).__name__}: {str(e)}")
                    # 打印更详细的错误堆栈
                    import traceback
                    logger.error(f"  错误堆栈: {traceback.format_exc()}")
                
                # 更新进度
                progress_data['last_processed_index'] = current_index
                progress_data['last_update'] = datetime.now().isoformat()
                
                # 添加网络指标到进度数据
                metrics = self.data_fetcher.get_metrics_summary()
                progress_data['current_network_status'] = metrics['network_status']
                progress_data['current_success_rate'] = metrics['success_rate']
                
                # 每处理10只股票保存一次进度
                if (i + 1) % 10 == 0:
                    self.save_enhanced_progress(progress_data, progress_file)
                
                # 显示进度
                processed_count = current_index + 1
                progress_pct = (processed_count / total_stocks) * 100
                elapsed_time = time.time() - total_start_time
                
                if processed_count > start_index:
                    avg_time_per_stock = elapsed_time / (processed_count - start_index)
                    remaining_stocks = total_stocks - processed_count
                    eta_seconds = remaining_stocks * avg_time_per_stock
                    
                    if (i + 1) % 10 == 0:  # 每10只股票显示一次进度
                        logger.info(f"进度: {progress_pct:.1f}% ({processed_count}/{total_stocks}) | "
                                   f"成功: {progress_data['success_count']} | 失败: {progress_data['failed_count']} | "
                                   f"暂停: {len(progress_data['paused_symbols'])} | "
                                   f"记录数: {progress_data['total_records']:,} | "
                                   f"网络: {metrics['network_status']} | "
                                   f"预计剩余: {eta_seconds/60:.1f}分钟")
                
                # 检查是否需要因网络问题长时间暂停
                if self.data_fetcher.delay_manager.should_pause():
                    logger.warning("网络状态持续不佳，建议暂停批处理")
                    logger.info("您可以稍后重新运行命令继续处理")
                    break
            
            # 处理失败和暂停的股票（重试一次）
            retry_symbols = progress_data['failed_symbols'] + progress_data['paused_symbols']
            if retry_symbols:
                logger.info(f"重试 {len(retry_symbols)} 只失败/暂停的股票...")
                
                # 等待一段时间让网络状态恢复
                time.sleep(30)
                
                retry_success = 0
                retry_symbols_copy = retry_symbols.copy()
                
                for symbol in retry_symbols_copy:
                    try:
                        updated_count = self.data_fetcher.update_stock_data_with_fixed_delay(symbol, days)
                        if updated_count > 0:
                            retry_success += 1
                            progress_data['success_count'] += 1
                            progress_data['total_records'] += updated_count
                            
                            # 从失败列表中移除
                            if symbol in progress_data['failed_symbols']:
                                progress_data['failed_symbols'].remove(symbol)
                                progress_data['failed_count'] -= 1
                            if symbol in progress_data['paused_symbols']:
                                progress_data['paused_symbols'].remove(symbol)
                            
                            logger.info(f"  ✅ 重试成功: {symbol} ({updated_count} 条记录)")
                        else:
                            logger.warning(f"  ❌ 重试仍失败: {symbol}")
                    except Exception as e:
                        logger.error(f"  ❌ 重试失败: {symbol} - {e}")
                
                logger.info(f"重试完成，成功恢复 {retry_success} 只股票")
            
            # 最终统计
            total_elapsed_time = time.time() - total_start_time
            progress_data['end_time'] = datetime.now().isoformat()
            progress_data['total_elapsed_time'] = total_elapsed_time
            
            # 获取最终网络指标
            final_metrics = self.data_fetcher.get_metrics_summary()
            progress_data['final_metrics'] = final_metrics
            
            logger.info(f"增强版批量更新完成!")
            logger.info(f"总股票数: {total_stocks:,}")
            logger.info(f"成功更新: {progress_data['success_count']:,} 只股票")
            logger.info(f"失败: {progress_data['failed_count']:,} 只股票")
            logger.info(f"因网络暂停: {len(progress_data['paused_symbols'])} 只股票")
            logger.info(f"总记录数: {progress_data['total_records']:,} 条")
            logger.info(f"总耗时: {total_elapsed_time/60:.1f} 分钟")
            logger.info(f"网络暂停次数: {progress_data['network_pauses']}")
            logger.info(f"最终网络状态: {final_metrics['network_status']}")
            logger.info(f"最终成功率: {final_metrics['success_rate']}")
            
            if total_elapsed_time > 0:
                logger.info(f"平均速度: {total_stocks/(total_elapsed_time/60):.1f} 只股票/分钟")
            
            # 保存最终进度
            self.save_enhanced_progress(progress_data, progress_file)
            
            return progress_data
            
        except Exception as e:
            logger.error(f"增强版批量更新历史数据失败: {e}")
            return {}
    
    def save_enhanced_progress(self, progress_data: dict, progress_file: str = "data/enhanced_batch_progress.json"):
        """保存增强版批处理进度"""
        os.makedirs(os.path.dirname(progress_file), exist_ok=True)
        
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=2)
    
    def load_enhanced_progress(self, progress_file: str = "data/enhanced_batch_progress.json") -> dict:
        """加载增强版批处理进度"""
        if not os.path.exists(progress_file):
            return {}
        
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载增强版进度文件失败: {e}")
            return {}
    
    def show_enhanced_progress_status(self):
        """显示增强版进度状态"""
        progress_file = "data/enhanced_batch_progress.json"
        progress_data = self.load_enhanced_progress(progress_file)
        
        if not progress_data:
            print("没有找到增强版进度文件")
            return
        
        print("\n" + "="*60)
        print("增强版批处理进度状态")
        print("="*60)
        
        total_stocks = progress_data.get('total_stocks', 0)
        success_count = progress_data.get('success_count', 0)
        failed_count = progress_data.get('failed_count', 0)
        paused_count = len(progress_data.get('paused_symbols', []))
        processed_count = success_count + failed_count + paused_count
        
        print(f"总股票数: {total_stocks:,}")
        print(f"已处理: {processed_count:,} ({processed_count/total_stocks*100:.1f}%)")
        print(f"成功: {success_count:,}")
        print(f"失败: {failed_count:,}")
        print(f"网络暂停: {paused_count:,}")
        print(f"总记录数: {progress_data.get('total_records', 0):,}")
        
        if 'start_time' in progress_data:
            start_time = datetime.fromisoformat(progress_data['start_time'])
            print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if 'last_update' in progress_data:
            last_update = datetime.fromisoformat(progress_data['last_update'])
            print(f"最后更新: {last_update.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if 'network_pauses' in progress_data:
            print(f"网络暂停次数: {progress_data['network_pauses']}")
        
        if 'current_network_status' in progress_data:
            print(f"当前网络状态: {progress_data['current_network_status']}")
        
        if 'current_success_rate' in progress_data:
            print(f"当前成功率: {progress_data['current_success_rate']}")
        
        print("="*60 + "\n")
    
    def create_failed_stocks_recovery_plan(self):
        """创建失败股票恢复计划"""
        progress_file = "data/enhanced_batch_progress.json"
        progress_data = self.load_enhanced_progress(progress_file)
        
        if not progress_data:
            print("没有找到进度文件")
            return
        
        failed_symbols = progress_data.get('failed_symbols', [])
        paused_symbols = progress_data.get('paused_symbols', [])
        
        if not failed_symbols and not paused_symbols:
            print("没有需要恢复的股票")
            return
        
        recovery_plan = {
            'failed_symbols': failed_symbols,
            'paused_symbols': paused_symbols,
            'total_to_recover': len(failed_symbols) + len(paused_symbols),
            'created_time': datetime.now().isoformat(),
            'recovery_strategy': {
                'batch_size': 10,  # 小批量处理
                'delay_multiplier': 2.0,  # 增加延迟
                'max_retries': 3
            }
        }
        
        recovery_file = "data/stock_recovery_plan.json"
        with open(recovery_file, 'w', encoding='utf-8') as f:
            json.dump(recovery_plan, f, ensure_ascii=False, indent=2)
        
        print(f"已创建股票恢复计划: {recovery_file}")
        print(f"需要恢复的股票数: {recovery_plan['total_to_recover']}")
        print(f"失败股票: {len(failed_symbols)}")
        print(f"暂停股票: {len(paused_symbols)}")
        
    def test_api_connectivity(self, test_symbols: List[str] = None):
        """
        测试API连接性和数据获取功能
        
        Args:
            test_symbols: 测试用的股票代码列表，如果为None则使用默认测试股票
        """
        print("\n" + "="*60)
        print("API连接性测试")
        print("="*60)
        
        # 默认测试股票代码
        if not test_symbols:
            test_symbols = ['000001', '000002']
        
        print(f"📡 测试股票代码: {', '.join(test_symbols)}")
        print(f"🔍 测试项目: 网络连接、数据获取、API响应")
        
        results = {
            'total_tested': len(test_symbols),
            'success_count': 0,
            'failed_count': 0,
            'api_status': 'unknown',
            'network_status': 'unknown',
            'test_details': []
        }
        
        try:
            # 获取网络状态
            network_status = self.data_fetcher.delay_manager.get_network_status()
            results['network_status'] = network_status.value
            print(f"🌐 当前网络状态: {network_status.value}")
            
            print("\n开始API测试...")
            print("-" * 40)
            
            for i, symbol in enumerate(test_symbols, 1):
                print(f"\n[{i}/{len(test_symbols)}] 测试股票: {symbol}")
                
                test_detail = {
                    'symbol': symbol,
                    'success': False,
                    'records_count': 0,
                    'error': None,
                    'response_time': 0
                }
                
                try:
                    start_time = time.time()
                    
                    # 获取API调用前的最新数据日期
                    last_date_before = self.db.get_last_update_date(symbol)
                    
                    # 测试获取最近1天的数据（尝试获取今天的数据）
                    updated_count = self.data_fetcher.update_stock_data_with_fixed_delay(symbol, days=1)
                    
                    response_time = time.time() - start_time
                    test_detail['response_time'] = response_time
                    
                    # 获取API调用后的最新数据日期
                    last_date_after = self.db.get_last_update_date(symbol)
                    
                    if updated_count >= 0:  # API调用成功
                        test_detail['success'] = True
                        test_detail['records_count'] = updated_count
                        results['success_count'] += 1
                        
                        # 分析API调用结果
                        from datetime import date
                        today = date.today().isoformat()
                        
                        if updated_count > 0:
                            print(f"  ✅ API成功 - 新增 {updated_count} 条记录 ({response_time:.2f}秒)")
                            print(f"  📊 数据更新: {last_date_before} → {last_date_after}")
                        else:
                            print(f"  ✅ API成功 - 无新数据 ({response_time:.2f}秒)")
                            print(f"  📊 最新数据日期: {last_date_after or '无数据'}")
                        
                        # 检查是否有今天的数据
                        if last_date_after == today:
                            print(f"  🎯 ✅ 已获取今日数据: {today}")
                        elif last_date_after:
                            print(f"  🎯 ⚠️ 最新数据: {last_date_after} (今日: {today})")
                            print(f"      可能原因: 市场未开盘、数据源未更新或非交易日")
                        else:
                            print(f"  🎯 ❌ 数据库中无此股票数据")
                    else:
                        test_detail['error'] = "API返回负值"
                        results['failed_count'] += 1
                        print(f"  ❌ 失败 - API返回异常值: {updated_count}")
                        
                except Exception as e:
                    test_detail['error'] = str(e)
                    results['failed_count'] += 1
                    print(f"  ❌ 失败 - {e}")
                
                results['test_details'].append(test_detail)
                
                # 短暂延迟避免请求过快
                time.sleep(1)
            
            # 计算API状态
            success_rate = results['success_count'] / results['total_tested']
            if success_rate >= 0.8:
                results['api_status'] = 'good'
            elif success_rate >= 0.5:
                results['api_status'] = 'fair'
            else:
                results['api_status'] = 'poor'
            
            # 显示测试总结
            print("\n" + "="*60)
            print("API测试总结")
            print("="*60)
            print(f"📊 测试股票数: {results['total_tested']}")
            print(f"✅ 成功: {results['success_count']}")
            print(f"❌ 失败: {results['failed_count']}")
            print(f"📈 成功率: {success_rate:.1%}")
            print(f"🌐 网络状态: {results['network_status']}")
            print(f"🔌 API状态: {results['api_status']}")
            
            # 显示响应时间统计
            response_times = [detail['response_time'] for detail in results['test_details'] if detail['success']]
            if response_times:
                avg_response_time = sum(response_times) / len(response_times)
                max_response_time = max(response_times)
                min_response_time = min(response_times)
                print(f"⏱️ 平均响应时间: {avg_response_time:.2f}秒")
                print(f"⏱️ 最快响应: {min_response_time:.2f}秒")
                print(f"⏱️ 最慢响应: {max_response_time:.2f}秒")
            
            # 给出建议
            print("\n💡 建议:")
            if results['api_status'] == 'good':
                print("  ✅ API工作正常，可以进行批量数据更新")
            elif results['api_status'] == 'fair':
                print("  ⚠️ API部分正常，建议检查网络连接或稍后重试")
            else:
                print("  ❌ API状态不佳，建议检查网络连接和API配置")
                print("  💡 可以尝试使用企业模式: --enterprise-mode")
            
            # 显示失败详情
            failed_tests = [detail for detail in results['test_details'] if not detail['success']]
            if failed_tests:
                print(f"\n❌ 失败详情:")
                for detail in failed_tests:
                    print(f"  {detail['symbol']}: {detail['error']}")
            
            print("="*60)
            
            return results
            
        except Exception as e:
            print(f"❌ API测试过程中发生错误: {e}")
            import traceback
            print(f"详细错误: {traceback.format_exc()}")
            return results



def create_enhanced_argument_parser():
    """创建增强版命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="增强版短线选股工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
增强版功能:
  python enhanced_main.py --update-all-enhanced --days 60    # 增强版批量更新
  python enhanced_main.py --show-progress                    # 显示进度状态
  python enhanced_main.py --create-recovery-plan             # 创建恢复计划
  python enhanced_main.py --test-network                     # 测试网络状态
  python enhanced_main.py --test-api                         # 测试API连接性
  python enhanced_main.py --diagnose-network                 # 完整网络诊断
  
企业网络模式:
  python enhanced_main.py --update-all-enhanced --enterprise-mode --days 60  # 企业模式更新
  python enhanced_main.py --test-network --enterprise-mode                   # 企业模式测试

数据刷新工具 (data_refresh.py):
  python data_refresh.py smart-refresh                       # 智能刷新（基于报告）
  python data_refresh.py smart-refresh --test-mode --yes     # 测试模式智能刷新
  python data_refresh.py full-refresh                        # 全量数据刷新
  python data_refresh.py full-refresh --max-stocks 100      # 限制股票数量的全量刷新
  python data_refresh.py cleanup                             # 清理失败股票列表
  python data_refresh.py cleanup --yes                       # 跳过确认的清理操作
  python data_refresh.py check                               # 数据完整性检查
  python data_refresh.py check --target-days 60             # 自定义天数的完整性检查
        """
    )
    
    # 增强版功能选项
    parser.add_argument('--update-all-enhanced', action='store_true',
                       help='使用增强版算法更新所有股票历史数据')
    parser.add_argument('--show-progress', action='store_true',
                       help='显示增强版批处理进度状态')
    parser.add_argument('--create-recovery-plan', action='store_true',
                       help='为失败的股票创建恢复计划')
    parser.add_argument('--test-network', action='store_true',
                       help='测试网络状态和延迟策略')
    parser.add_argument('--diagnose-network', action='store_true',
                       help='运行完整的网络诊断')
    parser.add_argument('--test-api', action='store_true',
                       help='测试API连接性和数据获取功能')
    parser.add_argument('--enterprise-mode', action='store_true',
                       help='启用企业网络模式（更保守的延迟和重试策略）')
    
    # 参数选项
    parser.add_argument('--days', type=int, default=60,
                       help='历史数据天数 (默认: 60)')
    parser.add_argument('--no-resume', action='store_true',
                       help='不使用断点续传，从头开始')
    
    return parser


def main():
    """增强版主函数"""
    parser = create_enhanced_argument_parser()
    args = parser.parse_args()
    
    # 如果没有提供任何参数，显示帮助信息
    if len(sys.argv) == 1:
        parser.print_help()
        return
    
    try:
        # 初始化增强版应用
        app = EnhancedStockSelectorApp(enterprise_mode=args.enterprise_mode)
        
        if args.enterprise_mode:
            print("🏢 企业网络模式已启用")
            print("   - 使用更保守的延迟策略")
            print("   - 减少重试次数")
            print("   - 增加请求超时时间")
            print("   - 适合企业防火墙环境")
        
        # 显示进度状态
        if args.show_progress:
            app.show_enhanced_progress_status()
            return
        
        # 创建恢复计划
        if args.create_recovery_plan:
            app.create_failed_stocks_recovery_plan()
            return
        
        # 测试网络状态
        if args.test_network:
            print("测试网络状态...")
            test_symbols = ['000001', '000002', '600000']
            results = app.data_fetcher.batch_update_with_monitoring(test_symbols, days=5)
            
            print("\n测试结果:")
            for symbol, count in results.items():
                print(f"  {symbol}: {count} 条记录")
            
            print("\n网络指标:")
            metrics = app.data_fetcher.get_metrics_summary()
            for key, value in metrics.items():
                print(f"  {key}: {value}")
            return
        
        # 网络诊断
        if args.diagnose_network:
            print("运行网络诊断...")
            try:
                from network_diagnostic import NetworkDiagnostic
                diagnostic = NetworkDiagnostic()
                diagnostic.run_full_diagnostic()
                diagnostic.generate_report()
                print("网络诊断完成，详细报告已保存")
            except ImportError:
                print("❌ 网络诊断模块未找到，请确保 network_diagnostic.py 存在")
            except Exception as e:
                print(f"❌ 网络诊断失败: {e}")
            return
        
        # API连接性测试
        if args.test_api:
            print("🔍 开始API连接性测试...")
            results = app.test_api_connectivity()
            return
        
        # 增强版批量更新
        if args.update_all_enhanced:
            print(f"\n🚀 开始增强版批量更新所有股票近{args.days}天的历史数据...")
            print("✨ 新功能: 动态延迟、指数退避、网络状态监控")
            print("⚠️  这是一个长时间运行的任务，支持智能断点续传")
            print("   如果中途中断，可以重新运行相同命令从中断处继续")
            
            resume = not args.no_resume
            result = app.update_all_stocks_historical_data_enhanced(
                days=args.days,
                resume=resume
            )
            
            if result:
                success_count = result.get('success_count', 0)
                failed_count = result.get('failed_count', 0)
                paused_count = len(result.get('paused_symbols', []))
                total_records = result.get('total_records', 0)
                elapsed_time = result.get('total_elapsed_time', 0)
                network_pauses = result.get('network_pauses', 0)
                
                print(f"\n✅ 增强版批量更新完成！")
                print(f"成功更新: {success_count:,} 只股票")
                print(f"失败: {failed_count:,} 只股票")
                print(f"网络暂停: {paused_count:,} 只股票")
                print(f"总记录数: {total_records:,} 条")
                print(f"总耗时: {elapsed_time/60:.1f} 分钟")
                print(f"网络暂停次数: {network_pauses}")
                
                if 'final_metrics' in result:
                    final_metrics = result['final_metrics']
                    print(f"最终网络状态: {final_metrics['network_status']}")
                    print(f"最终成功率: {final_metrics['success_rate']}")
                
                if failed_count > 0 or paused_count > 0:
                    print(f"\n⚠️  有 {failed_count + paused_count} 只股票需要恢复")
                    print("   可以运行 --create-recovery-plan 创建恢复计划")
                
                # 显示最终统计
                print("\n📊 使用 --show-progress 查看详细进度")
            else:
                print("\n❌ 增强版批量更新失败！")
        
        else:
            print("请指定要执行的操作，使用 --help 查看帮助信息")
    
    except KeyboardInterrupt:
        logger.info("用户中断操作")
        print("\n操作已取消")
    
    except Exception as e:
        logger.error(f"程序执行出错: {e}")
        print(f"\n❌ 程序执行出错: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()