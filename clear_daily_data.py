#!/usr/bin/env python3
"""
清空日线数据表的脚本
"""

import sys
import os
from database import DatabaseManager

def main():
    """清空日线数据表"""
    try:
        print("正在清空日线数据表...")
        
        # 初始化数据库管理器
        db = DatabaseManager()
        
        # 获取清空前的统计信息
        stats_before = db.get_database_stats()
        daily_data_count_before = stats_before.get('daily_data', 0)
        
        print(f"清空前日线数据表记录数: {daily_data_count_before:,}")
        
        # 清空日线数据表
        success = db.clear_daily_data()
        
        if success:
            # 获取清空后的统计信息
            stats_after = db.get_database_stats()
            daily_data_count_after = stats_after.get('daily_data', 0)
            
            print(f"✅ 日线数据表清空成功!")
            print(f"清空后日线数据表记录数: {daily_data_count_after:,}")
            print(f"共删除 {daily_data_count_before:,} 条记录")
        else:
            print("❌ 日线数据表清空失败!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ 清空日线数据表时发生错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()