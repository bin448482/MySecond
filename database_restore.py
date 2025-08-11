#!/usr/bin/env python3
"""
数据库恢复脚本
从备份文件恢复数据库到指定位置
"""

import os
import shutil
import sqlite3
import logging
from datetime import datetime
from typing import Optional, Dict, Any
import hashlib

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/database_restore.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DatabaseRestorer:
    """数据库恢复管理类"""
    
    def __init__(self, backup_path: str, target_path: str = "data/stock_data.db"):
        """
        初始化数据库恢复器
        
        Args:
            backup_path: 备份文件路径
            target_path: 目标数据库文件路径
        """
        self.backup_path = backup_path
        self.target_path = target_path
        self.original_backup_path = None
        
        # 确保日志目录存在
        os.makedirs('logs', exist_ok=True)
    
    def validate_backup_file(self) -> bool:
        """
        验证备份文件的完整性
        
        Returns:
            验证是否成功
        """
        logger.info(f"开始验证备份文件: {self.backup_path}")
        
        # 检查文件是否存在
        if not os.path.exists(self.backup_path):
            logger.error(f"备份文件不存在: {self.backup_path}")
            return False
        
        # 检查文件大小
        file_size = os.path.getsize(self.backup_path)
        if file_size == 0:
            logger.error(f"备份文件为空: {self.backup_path}")
            return False
        
        logger.info(f"备份文件大小: {file_size:,} 字节")
        
        # 尝试连接数据库验证完整性
        try:
            conn = sqlite3.connect(self.backup_path)
            cursor = conn.cursor()
            
            # 检查数据库完整性
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            
            if result[0] != 'ok':
                logger.error(f"数据库完整性检查失败: {result[0]}")
                return False
            
            # 获取表列表
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            table_names = [table[0] for table in tables]
            
            logger.info(f"备份数据库包含表: {', '.join(table_names)}")
            
            # 获取每个表的记录数
            table_stats = {}
            for table_name in table_names:
                if table_name != 'sqlite_sequence':
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    table_stats[table_name] = count
            
            logger.info("备份数据库表统计:")
            for table, count in table_stats.items():
                logger.info(f"  {table}: {count:,} 条记录")
            
            conn.close()
            logger.info("备份文件验证成功")
            return True
            
        except Exception as e:
            logger.error(f"验证备份文件时出错: {e}")
            return False
    
    def backup_current_database(self) -> bool:
        """
        备份当前数据库文件
        
        Returns:
            备份是否成功
        """
        if not os.path.exists(self.target_path):
            logger.info("目标数据库文件不存在，无需备份")
            return True
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_current_path = f"data/backup_current_{timestamp}.db"
        
        try:
            shutil.copy2(self.target_path, backup_current_path)
            self.original_backup_path = backup_current_path
            logger.info(f"当前数据库已备份到: {backup_current_path}")
            return True
        except Exception as e:
            logger.error(f"备份当前数据库失败: {e}")
            return False
    
    def calculate_file_hash(self, file_path: str) -> str:
        """
        计算文件的MD5哈希值
        
        Args:
            file_path: 文件路径
            
        Returns:
            MD5哈希值
        """
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"计算文件哈希失败: {e}")
            return ""
    
    def restore_database(self) -> bool:
        """
        执行数据库恢复
        
        Returns:
            恢复是否成功
        """
        logger.info("开始数据库恢复过程")
        
        try:
            # 计算备份文件哈希
            backup_hash = self.calculate_file_hash(self.backup_path)
            logger.info(f"备份文件哈希: {backup_hash}")
            
            # 复制备份文件到目标位置
            shutil.copy2(self.backup_path, self.target_path)
            logger.info(f"已将备份文件复制到: {self.target_path}")
            
            # 验证复制后的文件哈希
            target_hash = self.calculate_file_hash(self.target_path)
            logger.info(f"目标文件哈希: {target_hash}")
            
            if backup_hash != target_hash:
                logger.error("文件哈希不匹配，恢复可能失败")
                return False
            
            # 验证恢复后的数据库
            if not self.validate_restored_database():
                logger.error("恢复后的数据库验证失败")
                return False
            
            logger.info("数据库恢复成功完成")
            return True
            
        except Exception as e:
            logger.error(f"数据库恢复失败: {e}")
            return False
    
    def validate_restored_database(self) -> bool:
        """
        验证恢复后的数据库
        
        Returns:
            验证是否成功
        """
        logger.info("验证恢复后的数据库")
        
        try:
            conn = sqlite3.connect(self.target_path)
            cursor = conn.cursor()
            
            # 检查数据库完整性
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            
            if result[0] != 'ok':
                logger.error(f"恢复后数据库完整性检查失败: {result[0]}")
                return False
            
            # 获取表统计信息
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            logger.info("恢复后数据库表统计:")
            for table in tables:
                table_name = table[0]
                if table_name != 'sqlite_sequence':
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    logger.info(f"  {table_name}: {count:,} 条记录")
            
            conn.close()
            logger.info("恢复后数据库验证成功")
            return True
            
        except Exception as e:
            logger.error(f"验证恢复后数据库时出错: {e}")
            return False
    
    def rollback_restore(self) -> bool:
        """
        回滚恢复操作
        
        Returns:
            回滚是否成功
        """
        if not self.original_backup_path or not os.path.exists(self.original_backup_path):
            logger.error("无法回滚，原始备份文件不存在")
            return False
        
        try:
            shutil.copy2(self.original_backup_path, self.target_path)
            logger.info("数据库恢复已回滚")
            return True
        except Exception as e:
            logger.error(f"回滚失败: {e}")
            return False
    
    def get_database_info(self, db_path: str) -> Dict[str, Any]:
        """
        获取数据库信息
        
        Args:
            db_path: 数据库文件路径
            
        Returns:
            数据库信息字典
        """
        info = {
            'file_path': db_path,
            'file_size': 0,
            'last_modified': None,
            'tables': {},
            'total_records': 0
        }
        
        try:
            if os.path.exists(db_path):
                info['file_size'] = os.path.getsize(db_path)
                info['last_modified'] = datetime.fromtimestamp(os.path.getmtime(db_path))
                
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                
                for table in tables:
                    table_name = table[0]
                    if table_name != 'sqlite_sequence':
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        count = cursor.fetchone()[0]
                        info['tables'][table_name] = count
                        info['total_records'] += count
                
                conn.close()
        except Exception as e:
            logger.error(f"获取数据库信息失败: {e}")
        
        return info


def main():
    """主函数"""
    backup_file = "data/backup_before_refresh_20250811_163427.db"
    target_file = "data/stock_data.db"
    
    logger.info("=" * 60)
    logger.info("数据库恢复程序启动")
    logger.info("=" * 60)
    
    # 创建恢复器实例
    restorer = DatabaseRestorer(backup_file, target_file)
    
    # 显示当前数据库信息
    logger.info("当前数据库信息:")
    current_info = restorer.get_database_info(target_file)
    logger.info(f"  文件大小: {current_info['file_size']:,} 字节")
    logger.info(f"  最后修改: {current_info['last_modified']}")
    logger.info(f"  总记录数: {current_info['total_records']:,}")
    
    # 显示备份数据库信息
    logger.info("备份数据库信息:")
    backup_info = restorer.get_database_info(backup_file)
    logger.info(f"  文件大小: {backup_info['file_size']:,} 字节")
    logger.info(f"  最后修改: {backup_info['last_modified']}")
    logger.info(f"  总记录数: {backup_info['total_records']:,}")
    
    # 验证备份文件
    if not restorer.validate_backup_file():
        logger.error("备份文件验证失败，终止恢复过程")
        return False
    
    # 备份当前数据库
    if not restorer.backup_current_database():
        logger.error("备份当前数据库失败，终止恢复过程")
        return False
    
    # 执行恢复
    if restorer.restore_database():
        logger.info("数据库恢复成功完成！")
        return True
    else:
        logger.error("数据库恢复失败")
        # 尝试回滚
        if restorer.rollback_restore():
            logger.info("已回滚到原始状态")
        return False


if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ 数据库恢复成功！")
    else:
        print("\n❌ 数据库恢复失败！")