#!/usr/bin/env python3
"""
查看选股结果脚本
"""

from database import DatabaseManager
import pandas as pd

def view_selection_results():
    """查看最新的选股结果"""
    try:
        # 连接数据库
        db = DatabaseManager()
        
        # 查询最新的选股结果
        conn = db.get_connection()
        query = '''
            SELECT symbol, name, date, price_change, volume_ratio, turnover_rate, 
                   macd_signal, rsi_signal, ma_signal, total_score, created_at
            FROM selection_results 
            ORDER BY created_at DESC, total_score DESC
            LIMIT 10
        '''
        
        results = pd.read_sql_query(query, conn)
        conn.close()
        
        if not results.empty:
            print('=' * 60)
            print('最新选股结果')
            print('=' * 60)
            print(f'选股时间: {results.iloc[0]["created_at"]}')
            print(f'共选出: {len(results)} 只股票')
            print()
            
            for i, row in results.iterrows():
                print(f'{i+1}. 股票代码: {row["symbol"]}')
                if pd.notna(row['name']) and row['name']:
                    print(f'   股票名称: {row["name"]}')
                else:
                    print('   股票名称: 未知')
                
                if pd.notna(row['total_score']):
                    print(f'   综合评分: {row["total_score"]:.2f}分')
                
                if pd.notna(row['price_change']):
                    print(f'   涨跌幅: {row["price_change"]:.2f}%')
                
                if pd.notna(row['volume_ratio']):
                    print(f'   量比: {row["volume_ratio"]:.2f}')
                
                if pd.notna(row['turnover_rate']):
                    print(f'   换手率: {row["turnover_rate"]:.2f}%')
                
                if pd.notna(row['macd_signal']):
                    print(f'   MACD信号: {row["macd_signal"]}')
                
                if pd.notna(row['rsi_signal']):
                    print(f'   RSI信号: {row["rsi_signal"]}')
                
                if pd.notna(row['ma_signal']):
                    print(f'   均线信号: {row["ma_signal"]}')
                
                print('-' * 40)
        else:
            print('数据库中暂无选股结果')
            print('请先运行选股程序：python smart_stock_selector.py --mode interactive')
            
    except Exception as e:
        print(f'查询选股结果时出错: {e}')

def view_stock_info():
    """查看股票基本信息"""
    try:
        db = DatabaseManager()
        conn = db.get_connection()
        
        # 查询最新选股结果中的股票代码
        query = '''
            SELECT DISTINCT symbol FROM selection_results 
            ORDER BY created_at DESC
            LIMIT 5
        '''
        
        symbols = pd.read_sql_query(query, conn)
        
        if not symbols.empty:
            print('\n' + '=' * 60)
            print('选中股票的基本信息')
            print('=' * 60)
            
            for symbol in symbols['symbol']:
                # 查询股票基本信息
                info_query = '''
                    SELECT symbol, name, industry, market, list_date
                    FROM stock_info 
                    WHERE symbol = ?
                '''
                
                info = pd.read_sql_query(info_query, conn, params=(symbol,))
                
                if not info.empty:
                    row = info.iloc[0]
                    print(f'股票代码: {row["symbol"]}')
                    print(f'股票名称: {row["name"] if pd.notna(row["name"]) else "未知"}')
                    print(f'所属行业: {row["industry"] if pd.notna(row["industry"]) else "未知"}')
                    print(f'所属市场: {row["market"] if pd.notna(row["market"]) else "未知"}')
                    print(f'上市日期: {row["list_date"] if pd.notna(row["list_date"]) else "未知"}')
                    print('-' * 40)
        
        conn.close()
        
    except Exception as e:
        print(f'查询股票信息时出错: {e}')

if __name__ == "__main__":
    view_selection_results()
    view_stock_info()