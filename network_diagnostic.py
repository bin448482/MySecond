#!/usr/bin/env python3
"""
网络诊断工具
用于诊断为什么手机热点能成功但直接联网不行的问题
"""

import requests
import socket
import ssl
import dns.resolver
import time
import json
from datetime import datetime
from typing import Dict, List, Any
import logging
import akshare as ak
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# 禁用SSL警告
urllib3.disable_warnings(InsecureRequestWarning)

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class NetworkDiagnostic:
    """网络诊断器"""
    
    def __init__(self):
        """初始化诊断器"""
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'tests': {}
        }
        
        # 常用的股票数据API域名
        self.test_domains = [
            'push2.eastmoney.com',
            'datacenter-web.eastmoney.com',
            'quote.eastmoney.com',
            'api.finance.sina.com.cn',
            'hq.sinajs.cn',
            'qt.gtimg.cn',
            'web.ifzq.gtimg.cn'
        ]
        
        # 测试URL
        self.test_urls = [
            'https://push2.eastmoney.com/api/qt/clist/get',
            'https://datacenter-web.eastmoney.com/api/data/v1/get',
            'https://api.finance.sina.com.cn/suggest/',
            'https://hq.sinajs.cn/list=sh000001'
        ]
    
    def test_basic_connectivity(self) -> Dict[str, Any]:
        """测试基本网络连通性"""
        logger.info("测试基本网络连通性...")
        
        results = {
            'internet_access': False,
            'dns_resolution': {},
            'ping_results': {},
            'errors': []
        }
        
        try:
            # 测试基本互联网访问
            response = requests.get('https://www.baidu.com', timeout=10)
            results['internet_access'] = response.status_code == 200
            logger.info(f"基本互联网访问: {'✅ 正常' if results['internet_access'] else '❌ 异常'}")
        except Exception as e:
            results['errors'].append(f"互联网访问测试失败: {e}")
            logger.error(f"互联网访问测试失败: {e}")
        
        # 测试DNS解析
        for domain in self.test_domains:
            try:
                answers = dns.resolver.resolve(domain, 'A')
                ips = [str(answer) for answer in answers]
                results['dns_resolution'][domain] = {
                    'success': True,
                    'ips': ips
                }
                logger.info(f"DNS解析 {domain}: ✅ {ips}")
            except Exception as e:
                results['dns_resolution'][domain] = {
                    'success': False,
                    'error': str(e)
                }
                logger.error(f"DNS解析 {domain}: ❌ {e}")
        
        return results
    
    def test_ssl_connectivity(self) -> Dict[str, Any]:
        """测试SSL连接"""
        logger.info("测试SSL连接...")
        
        results = {
            'ssl_tests': {},
            'certificate_info': {}
        }
        
        for domain in self.test_domains:
            try:
                # 创建SSL上下文
                context = ssl.create_default_context()
                
                # 测试SSL连接
                with socket.create_connection((domain, 443), timeout=10) as sock:
                    with context.wrap_socket(sock, server_hostname=domain) as ssock:
                        cert = ssock.getpeercert()
                        
                        results['ssl_tests'][domain] = {
                            'success': True,
                            'protocol': ssock.version(),
                            'cipher': ssock.cipher()
                        }
                        
                        results['certificate_info'][domain] = {
                            'subject': dict(x[0] for x in cert['subject']),
                            'issuer': dict(x[0] for x in cert['issuer']),
                            'version': cert['version'],
                            'not_after': cert['notAfter']
                        }
                        
                        logger.info(f"SSL连接 {domain}: ✅ 正常")
                        
            except Exception as e:
                results['ssl_tests'][domain] = {
                    'success': False,
                    'error': str(e)
                }
                logger.error(f"SSL连接 {domain}: ❌ {e}")
        
        return results
    
    def test_http_requests(self) -> Dict[str, Any]:
        """测试HTTP请求"""
        logger.info("测试HTTP请求...")
        
        results = {
            'url_tests': {},
            'user_agent_tests': {},
            'proxy_tests': {}
        }
        
        # 不同的User-Agent
        user_agents = {
            'default': None,
            'chrome': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'firefox': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'mobile': 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
        }
        
        # 测试不同URL
        for url in self.test_urls:
            results['url_tests'][url] = {}
            
            for ua_name, ua_string in user_agents.items():
                try:
                    headers = {}
                    if ua_string:
                        headers['User-Agent'] = ua_string
                    
                    start_time = time.time()
                    response = requests.get(url, headers=headers, timeout=15, verify=False)
                    response_time = time.time() - start_time
                    
                    results['url_tests'][url][ua_name] = {
                        'success': True,
                        'status_code': response.status_code,
                        'response_time': response_time,
                        'content_length': len(response.content),
                        'headers': dict(response.headers)
                    }
                    
                    logger.info(f"HTTP请求 {url} ({ua_name}): ✅ {response.status_code} ({response_time:.2f}s)")
                    
                except Exception as e:
                    results['url_tests'][url][ua_name] = {
                        'success': False,
                        'error': str(e)
                    }
                    logger.error(f"HTTP请求 {url} ({ua_name}): ❌ {e}")
        
        return results
    
    def test_akshare_functions(self) -> Dict[str, Any]:
        """测试akshare具体功能"""
        logger.info("测试akshare具体功能...")
        
        results = {
            'akshare_tests': {}
        }
        
        # 测试不同的akshare函数
        test_functions = [
            ('stock_zh_a_spot_em', lambda: ak.stock_zh_a_spot_em()),
            ('stock_zh_a_hist', lambda: ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20240101", end_date="20240110", adjust="qfq")),
            ('stock_individual_info_em', lambda: ak.stock_individual_info_em(symbol="000001"))
        ]
        
        for func_name, func in test_functions:
            try:
                start_time = time.time()
                result = func()
                response_time = time.time() - start_time
                
                results['akshare_tests'][func_name] = {
                    'success': True,
                    'response_time': response_time,
                    'data_shape': result.shape if hasattr(result, 'shape') else None,
                    'data_type': str(type(result))
                }
                
                logger.info(f"akshare {func_name}: ✅ 成功 ({response_time:.2f}s)")
                
            except Exception as e:
                results['akshare_tests'][func_name] = {
                    'success': False,
                    'error': str(e),
                    'error_type': type(e).__name__
                }
                logger.error(f"akshare {func_name}: ❌ {e}")
        
        return results
    
    def test_proxy_detection(self) -> Dict[str, Any]:
        """检测代理设置"""
        logger.info("检测代理设置...")
        
        results = {
            'system_proxy': {},
            'environment_proxy': {},
            'requests_proxy': {}
        }
        
        # 检查环境变量中的代理设置
        import os
        proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']
        
        for var in proxy_vars:
            value = os.environ.get(var)
            if value:
                results['environment_proxy'][var] = value
                logger.info(f"环境变量代理 {var}: {value}")
        
        # 检查requests的代理设置
        try:
            session = requests.Session()
            results['requests_proxy'] = {
                'proxies': session.proxies,
                'trust_env': session.trust_env
            }
        except Exception as e:
            results['requests_proxy']['error'] = str(e)
        
        return results
    
    def get_network_info(self) -> Dict[str, Any]:
        """获取网络信息"""
        logger.info("获取网络信息...")
        
        results = {
            'local_ip': None,
            'public_ip': None,
            'dns_servers': [],
            'network_interfaces': {}
        }
        
        try:
            # 获取本地IP
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                results['local_ip'] = s.getsockname()[0]
        except Exception as e:
            logger.error(f"获取本地IP失败: {e}")
        
        try:
            # 获取公网IP
            response = requests.get('https://httpbin.org/ip', timeout=10)
            if response.status_code == 200:
                results['public_ip'] = response.json().get('origin')
        except Exception as e:
            logger.error(f"获取公网IP失败: {e}")
        
        try:
            # 获取DNS服务器
            import subprocess
            result = subprocess.run(['nslookup', 'baidu.com'], capture_output=True, text=True, timeout=10)
            # 简单解析DNS服务器信息
            if 'Server:' in result.stdout:
                lines = result.stdout.split('\n')
                for line in lines:
                    if 'Server:' in line:
                        dns_server = line.split('Server:')[1].strip()
                        if dns_server:
                            results['dns_servers'].append(dns_server)
        except Exception as e:
            logger.error(f"获取DNS服务器失败: {e}")
        
        return results
    
    def run_full_diagnostic(self) -> Dict[str, Any]:
        """运行完整诊断"""
        logger.info("开始网络诊断...")
        
        # 运行所有测试
        self.results['tests']['basic_connectivity'] = self.test_basic_connectivity()
        self.results['tests']['ssl_connectivity'] = self.test_ssl_connectivity()
        self.results['tests']['http_requests'] = self.test_http_requests()
        self.results['tests']['akshare_functions'] = self.test_akshare_functions()
        self.results['tests']['proxy_detection'] = self.test_proxy_detection()
        self.results['tests']['network_info'] = self.get_network_info()
        
        return self.results
    
    def generate_report(self, output_file: str = "data/network_diagnostic_report.json"):
        """生成诊断报告"""
        import os
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"诊断报告已生成: {output_file}")
        
        # 打印摘要
        self.print_summary()
    
    def print_summary(self):
        """打印诊断摘要"""
        print("\n" + "="*80)
        print("网络诊断摘要")
        print("="*80)
        
        # 基本连通性
        basic = self.results['tests'].get('basic_connectivity', {})
        print(f"基本互联网访问: {'✅ 正常' if basic.get('internet_access') else '❌ 异常'}")
        
        # DNS解析
        dns_results = basic.get('dns_resolution', {})
        dns_success = sum(1 for result in dns_results.values() if result.get('success'))
        print(f"DNS解析: {dns_success}/{len(dns_results)} 个域名成功")
        
        # SSL连接
        ssl_results = self.results['tests'].get('ssl_connectivity', {}).get('ssl_tests', {})
        ssl_success = sum(1 for result in ssl_results.values() if result.get('success'))
        print(f"SSL连接: {ssl_success}/{len(ssl_results)} 个域名成功")
        
        # akshare测试
        akshare_results = self.results['tests'].get('akshare_functions', {}).get('akshare_tests', {})
        akshare_success = sum(1 for result in akshare_results.values() if result.get('success'))
        print(f"akshare功能: {akshare_success}/{len(akshare_results)} 个函数成功")
        
        # 网络信息
        network_info = self.results['tests'].get('network_info', {})
        print(f"本地IP: {network_info.get('local_ip', 'N/A')}")
        print(f"公网IP: {network_info.get('public_ip', 'N/A')}")
        
        # 代理检测
        proxy_info = self.results['tests'].get('proxy_detection', {})
        env_proxies = proxy_info.get('environment_proxy', {})
        if env_proxies:
            print(f"检测到代理设置: {list(env_proxies.keys())}")
        else:
            print("未检测到代理设置")
        
        print("="*80)
        
        # 问题分析
        self.analyze_issues()
    
    def analyze_issues(self):
        """分析可能的问题"""
        print("\n问题分析:")
        print("-"*40)
        
        issues = []
        
        # 检查DNS问题
        dns_results = self.results['tests'].get('basic_connectivity', {}).get('dns_resolution', {})
        failed_dns = [domain for domain, result in dns_results.items() if not result.get('success')]
        if failed_dns:
            issues.append(f"DNS解析失败: {', '.join(failed_dns)}")
        
        # 检查SSL问题
        ssl_results = self.results['tests'].get('ssl_connectivity', {}).get('ssl_tests', {})
        failed_ssl = [domain for domain, result in ssl_results.items() if not result.get('success')]
        if failed_ssl:
            issues.append(f"SSL连接失败: {', '.join(failed_ssl)}")
        
        # 检查akshare问题
        akshare_results = self.results['tests'].get('akshare_functions', {}).get('akshare_tests', {})
        failed_akshare = [func for func, result in akshare_results.items() if not result.get('success')]
        if failed_akshare:
            issues.append(f"akshare功能失败: {', '.join(failed_akshare)}")
        
        # 检查代理问题
        proxy_info = self.results['tests'].get('proxy_detection', {})
        env_proxies = proxy_info.get('environment_proxy', {})
        if env_proxies:
            issues.append("检测到代理设置，可能影响连接")
        
        if issues:
            for i, issue in enumerate(issues, 1):
                print(f"{i}. {issue}")
        else:
            print("未发现明显问题")
        
        # 建议
        print("\n建议解决方案:")
        print("-"*40)
        if failed_dns:
            print("1. 尝试更换DNS服务器 (如8.8.8.8, 114.114.114.114)")
        if failed_ssl:
            print("2. 检查防火墙和SSL拦截设置")
        if env_proxies:
            print("3. 临时禁用代理设置测试")
        if failed_akshare:
            print("4. 尝试使用手机热点验证网络环境差异")
        
        print("5. 联系网络管理员检查企业网络限制")


def main():
    """主函数"""
    print("网络诊断工具")
    print("用于诊断akshare数据获取的网络问题")
    print("="*60)
    
    diagnostic = NetworkDiagnostic()
    
    try:
        # 运行完整诊断
        results = diagnostic.run_full_diagnostic()
        
        # 生成报告
        diagnostic.generate_report()
        
    except KeyboardInterrupt:
        print("\n诊断被用户中断")
    except Exception as e:
        logger.error(f"诊断过程出错: {e}")
        print(f"诊断失败: {e}")


if __name__ == "__main__":
    main()