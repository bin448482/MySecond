#!/usr/bin/env python3
"""
网络环境适配器
针对企业网络环境的特殊处理
"""

import requests
import time
import random
from typing import Dict, Any, Optional
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class EnterpriseNetworkAdapter:
    """企业网络环境适配器"""
    
    def __init__(self):
        """初始化适配器"""
        self.session = self._create_robust_session()
        self.request_count = 0
        self.last_request_time = 0
        
        # 企业网络友好的配置
        self.min_delay = 2.0  # 增加最小延迟
        self.max_delay = 5.0  # 增加最大延迟
        self.burst_limit = 5  # 连续请求限制
        self.burst_delay = 30  # 突发后的延迟
    
    def _create_robust_session(self) -> requests.Session:
        """创建健壮的请求会话"""
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 设置企业网络友好的headers
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        return session
    
    def adaptive_delay(self):
        """自适应延迟"""
        current_time = time.time()
        
        # 检查是否需要突发延迟
        if self.request_count > 0 and self.request_count % self.burst_limit == 0:
            logger.info(f"达到突发限制，延迟 {self.burst_delay} 秒...")
            time.sleep(self.burst_delay)
        
        # 计算基础延迟
        base_delay = random.uniform(self.min_delay, self.max_delay)
        
        # 如果请求过于频繁，增加延迟
        if self.last_request_time > 0:
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.min_delay:
                additional_delay = self.min_delay - time_since_last
                base_delay += additional_delay
        
        logger.debug(f"延迟 {base_delay:.2f} 秒")
        time.sleep(base_delay)
        
        self.last_request_time = time.time()
        self.request_count += 1
    
    def safe_request(self, url: str, **kwargs) -> Optional[requests.Response]:
        """安全的请求方法"""
        self.adaptive_delay()
        
        try:
            # 设置较长的超时时间
            kwargs.setdefault('timeout', 30)
            
            # 禁用SSL验证（如果企业网络有SSL拦截）
            kwargs.setdefault('verify', False)
            
            response = self.session.get(url, **kwargs)
            
            # 检查响应状态
            if response.status_code == 403:
                logger.warning(f"访问被拒绝 (403): {url}")
                return None
            elif response.status_code == 429:
                logger.warning(f"请求过于频繁 (429): {url}")
                time.sleep(60)  # 等待1分钟
                return None
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.ConnectionError as e:
            logger.error(f"连接错误: {e}")
            return None
        except requests.exceptions.Timeout as e:
            logger.error(f"请求超时: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"请求异常: {e}")
            return None
    
    def test_connectivity(self) -> Dict[str, Any]:
        """测试连通性"""
        test_urls = [
            'https://www.baidu.com',
            'https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=1',
            'https://hq.sinajs.cn/list=sh000001'
        ]
        
        results = {}
        
        for url in test_urls:
            logger.info(f"测试连接: {url}")
            response = self.safe_request(url)
            
            results[url] = {
                'success': response is not None,
                'status_code': response.status_code if response else None,
                'response_time': getattr(response, 'elapsed', None)
            }
        
        return results


def create_enterprise_friendly_akshare_patch():
    """创建企业网络友好的akshare补丁"""
    
    # 这个函数可以用来修改akshare的默认行为
    # 使其更适合企业网络环境
    
    import akshare as ak
    
    # 保存原始函数
    original_stock_zh_a_hist = ak.stock_zh_a_hist
    
    def patched_stock_zh_a_hist(*args, **kwargs):
        """企业网络友好的股票历史数据获取"""
        adapter = EnterpriseNetworkAdapter()
        
        try:
            # 添加延迟
            adapter.adaptive_delay()
            
            # 调用原始函数
            return original_stock_zh_a_hist(*args, **kwargs)
            
        except Exception as e:
            logger.error(f"获取历史数据失败: {e}")
            # 可以在这里实现备用数据源
            raise e
    
    # 替换函数
    ak.stock_zh_a_hist = patched_stock_zh_a_hist
    
    logger.info("已应用企业网络友好补丁")


if __name__ == "__main__":
    # 测试适配器
    adapter = EnterpriseNetworkAdapter()
    results = adapter.test_connectivity()
    
    print("企业网络适配器测试结果:")
    for url, result in results.items():
        status = "✅ 成功" if result['success'] else "❌ 失败"
        print(f"{url}: {status}")