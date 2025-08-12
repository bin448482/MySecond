# 智能选股系统 (Smart Stock Selector)

一个基于Python的A股智能选股系统，集成了增强技术指标、综合评分算法、多策略选股引擎、回测验证和智能报告生成功能。

## 🎯 系统概述

### 核心特性
- **🔍 增强技术指标**: 20+种技术指标（MACD、RSI、布林带、KDJ、CCI、威廉指标等）
- **📊 综合评分算法**: 多维度智能评分（技术、动量、成交量、波动率）
- **🚀 多策略引擎**: 4种内置策略（动量突破、技术反转、放量突破、均衡成长）
- **📈 回测验证系统**: 历史数据验证策略有效性
- **📋 智能报告**: 专业Excel报告和可视化图表
- **⏰ 定时任务**: 自动化每日选股和定期回测
- **🎛️ 交互界面**: 友好的命令行交互模式

### 系统架构
```
智能选股系统
├── 数据层 (SQLite数据库)
├── 增强技术指标层 (enhanced_technical_indicators.py)
├── 综合评分算法层 (comprehensive_scoring.py)
├── 多策略引擎层 (stock_strategy_engine.py)
├── 回测验证层 (strategy_backtest.py)
├── 智能输出层 (enhanced_output_manager.py)
└── 主程序层 (smart_stock_selector.py)
```

## 🚀 快速开始

### 环境要求
- Python 3.8+
- Windows/Linux/macOS
- 8GB+ RAM (推荐)

### 安装步骤

1. **克隆项目**
```bash
git clone <repository-url>
cd smart-stock-selector
```

2. **创建虚拟环境**
```bash
python -m venv stock_env
# Windows
stock_env\Scripts\activate
# Linux/macOS
source stock_env/bin/activate
```

3. **安装依赖**
```bash
pip install -r requirements.txt
```

4. **初始化数据库**
```bash
python database.py
```

5. **获取股票数据**
```bash
python data_refresh.py --smart-refresh
```

6. **启动系统**
```bash
python smart_stock_selector.py
```

### 第一次使用
1. 启动交互模式：`python smart_stock_selector.py --mode interactive`
2. 选择"1. 执行每日选股"
3. 选择策略（建议选择"全部策略"）
4. 等待选股完成，查看生成的Excel报告

## 📊 功能模块

### 1. 增强技术指标系统
**文件**: `enhanced_technical_indicators.py`

**支持指标**:
- **趋势指标**: MACD, EMA, SMA, 移动平均线(MA5/10/20/60)
- **震荡指标**: RSI, KDJ, CCI, 威廉%R, 动量指标, ROC
- **通道指标**: 布林带(上轨/中轨/下轨/宽度/位置)
- **成交量指标**: 量比, OBV(能量潮)
- **波动率指标**: ATR(平均真实波幅)

**技术信号**:
- MACD金叉/死叉、看涨/看跌
- RSI超买/超卖/超卖反弹
- 布林带位置信号
- KDJ金叉/死叉/超买超卖
- 均线多头/空头排列

### 2. 综合评分算法
**文件**: `comprehensive_scoring.py`

**评分维度**:
- **技术指标评分** (40%): MACD、RSI、布林带、KDJ、均线、CCI
- **动量评分** (25%): 价格趋势、相对强度、突破信号、价格动量
- **成交量评分** (20%): 量比、量价配合、成交量趋势、换手率
- **波动率评分** (10%): 历史波动率、ATR相对值、波动率趋势
- **市场情绪** (5%): 市场环境调整因子

**评分范围**: 0-100分，分数越高表示短线机会越大

### 3. 多策略选股引擎
**文件**: `stock_strategy_engine.py`

#### 内置策略

| 策略名称 | 适用场景 | 特点 | 权重配置 |
|---------|---------|------|---------|
| **动量突破** (momentum_breakout) | 强势上涨行情 | 技术指标向好、动量强劲 | 技术35% + 动量40% + 成交量20% + 波动率5% |
| **技术反转** (technical_reversal) | 超跌反弹行情 | 超卖反弹、技术修复 | 技术50% + 动量20% + 成交量15% + 波动率15% |
| **放量突破** (volume_surge) | 成交量放大行情 | 成交量放大、价格突破 | 技术30% + 动量25% + 成交量35% + 波动率10% |
| **均衡成长** (balanced_growth) | 稳健上涨行情 | 各项指标均衡、稳健成长 | 技术40% + 动量25% + 成交量20% + 波动率15% |

#### 策略功能
- 多策略并行执行
- 策略交集分析
- 自定义权重配置
- 动态筛选条件

### 4. 回测验证系统
**文件**: `strategy_backtest.py`

**回测功能**:
- 历史策略表现验证
- 多持有期收益分析(1日/3日/5日/10日)
- 策略对比评估
- 风险收益指标计算

**关键指标**:
- 平均收益率、胜率、夏普比率
- 最大收益、最大亏损、标准差
- 策略评级 (A/B/C/D)
- 风险水平评估

### 5. 智能输出管理
**文件**: `enhanced_output_manager.py`

**报告类型**:
- **选股报告**: 当日选股结果和技术指标详情
- **回测报告**: 策略历史表现分析
- **综合报告**: 多策略对比和市场概况

**Excel报告结构**:
- 选股结果工作表（排名、评分、技术指标）
- 技术指标详情工作表（前20只股票详细指标）
- 策略配置工作表（权重和筛选条件）
- 市场概况工作表（数据统计和市场分布）

## 💻 使用方法

### 命令行模式

#### 1. 交互模式（推荐新手）
```bash
python smart_stock_selector.py --mode interactive
```

#### 2. 每日选股
```bash
# 使用所有策略
python smart_stock_selector.py --mode daily

# 使用指定策略
python smart_stock_selector.py --mode daily --strategies momentum_breakout technical_reversal

# 设置最大选股数
python smart_stock_selector.py --mode daily --max-stocks 50
```

#### 3. 策略回测
```bash
python smart_stock_selector.py --mode backtest --strategy momentum_breakout --start-date 2024-01-01 --end-date 2024-03-31
```

#### 4. 股票分析
```bash
python smart_stock_selector.py --mode analysis --symbol 000001
```

#### 5. 定时任务
```bash
# 启动定时任务（每日09:00选股，每周一08:00回测）
python smart_stock_selector.py --schedule
```

### 程序化调用

```python
from smart_stock_selector import SmartStockSelector

# 初始化系统
selector = SmartStockSelector()

# 执行每日选股
result = selector.run_daily_selection(['momentum_breakout'], max_stocks_per_strategy=30)

# 分析单只股票
analysis = selector.get_stock_analysis('000001')

# 策略回测
backtest = selector.run_strategy_backtest('momentum_breakout', '2024-01-01', '2024-03-31')
```

## 📈 策略使用指南

### 策略选择建议

| 市场环境 | 推荐策略 | 风险等级 | 说明 |
|---------|---------|---------|------|
| 强势上涨 | momentum_breakout | 高风险高收益 | 追涨强势股，适合牛市 |
| 震荡调整 | technical_reversal | 低风险稳健 | 抄底反弹，适合震荡市 |
| 放量突破 | volume_surge | 高风险高收益 | 关注成交量异动 |
| 稳健上涨 | balanced_growth | 中等风险收益 | 均衡配置，适合慢牛 |

### 策略组合推荐

#### 激进型组合
```python
strategies = ['momentum_breakout', 'volume_surge']
```

#### 稳健型组合
```python
strategies = ['balanced_growth', 'technical_reversal']
```

#### 全面型组合
```python
strategies = ['momentum_breakout', 'technical_reversal', 'volume_surge', 'balanced_growth']
```

## ⚙️ 配置说明

### 配置文件 (config.yaml)

```yaml
# 数据库配置
database:
  path: "data/stock_data.db"
  backup_enabled: true

# 技术指标配置
technical:
  macd:
    fast_period: 12
    slow_period: 26
    signal_period: 9
  rsi:
    period: 14
    overbought: 70
    oversold: 30
  ma_periods: [5, 10, 20, 60]
  bollinger:
    period: 20
    std_dev: 2.0
  kdj:
    k_period: 9
    d_period: 3
    j_period: 3

# 筛选条件配置
filters:
  price_range: [5, 100]
  min_volume_ratio: 1.5
  min_turnover_rate: 3.0
  min_price_change: 1.0

# 输出配置
output:
  max_results: 50
  output_dir: "output"
```

### 自定义策略权重

```python
# 自定义权重示例
custom_weights = {
    'technical': 0.5,    # 技术指标权重50%
    'momentum': 0.3,     # 动量权重30%
    'volume': 0.15,      # 成交量权重15%
    'volatility': 0.05   # 波动率权重5%
}

result = selector.scoring.calculate_comprehensive_score('000001', custom_weights)
```

## 📋 输出解读

### 综合评分等级
- **90-100分**: 极强信号，重点关注 ⭐⭐⭐⭐⭐
- **80-89分**: 强信号，优先考虑 ⭐⭐⭐⭐
- **70-79分**: 中等信号，可以关注 ⭐⭐⭐
- **60-69分**: 弱信号，谨慎考虑 ⭐⭐
- **60分以下**: 信号较弱，不建议 ⭐

### 技术信号解读
- **MACD**: 金叉看涨，死叉看跌，零轴上方强势
- **RSI**: 超卖反弹机会，超买回调风险，30-70正常
- **布林带**: 下轨反弹机会，上轨回调风险
- **KDJ**: 低位金叉买入，高位死叉卖出
- **均线**: 多头排列看涨，空头排列看跌

### 回测结果解读
- **平均收益率**: 策略的盈利能力
- **胜率**: 成功交易的比例
- **夏普比率**: 风险调整后收益（>1为优秀）
- **策略评级**: A级最佳，D级最差

## 🔧 常见问题

### Q1: 系统提示"没有股票数据"怎么办？
**A**: 需要先获取股票数据
```bash
python data_refresh.py --smart-refresh
```

### Q2: 选股结果为空怎么办？
**A**: 可能是筛选条件过于严格，可以：
1. 降低评分阈值
2. 调整技术指标参数
3. 检查数据是否最新

### Q3: 如何更新股票数据？
**A**: 
```bash
# 智能更新（推荐）
python data_refresh.py --smart-refresh

# 全量更新
python data_refresh.py --full-refresh
```

### Q4: 如何自定义策略？
**A**: 在`stock_strategy_engine.py`中添加新策略配置：
```python
'custom_strategy': {
    'name': '自定义策略',
    'description': '策略描述',
    'weights': {'technical': 0.4, 'momentum': 0.3, 'volume': 0.2, 'volatility': 0.1},
    'filters': {'min_score': 70, 'min_volume_ratio': 1.5}
}
```

### Q5: 如何设置定时任务？
**A**: 
```bash
# 启动定时任务
python smart_stock_selector.py --schedule

# 或在交互模式中选择"6. 启动定时任务"
```

## 📁 项目结构

```
smart-stock-selector/
├── smart_stock_selector.py           # 主程序入口
├── config.yaml                       # 配置文件
├── database.py                       # 数据库管理
├── enhanced_technical_indicators.py  # 增强技术指标
├── comprehensive_scoring.py          # 综合评分算法
├── stock_strategy_engine.py          # 多策略选股引擎
├── strategy_backtest.py              # 回测验证系统
├── enhanced_output_manager.py        # 智能输出管理
├── data_refresh.py                   # 数据刷新管理
├── utils.py                          # 工具函数
├── requirements.txt                  # 依赖包
├── README.md                         # 说明文档
├── claude.md                         # 技术文档
├── data/                             # 数据目录
│   ├── stock_data.db                 # SQLite数据库
│   ├── completeness_report.json     # 数据完整性报告
│   └── enhanced_batch_progress.json # 批处理进度
├── logs/                             # 日志目录
└── output/                           # 输出目录
    ├── strategy_selection_*.xlsx     # 策略选股报告
    ├── backtest_report_*.xlsx        # 回测报告
    └── comprehensive_report_*.xlsx   # 综合报告
```

## 🎯 使用建议

### 新手用户
1. 从交互模式开始：`python smart_stock_selector.py --mode interactive`
2. 先使用单一策略熟悉系统
3. 查看生成的Excel报告了解输出格式

### 进阶用户
1. 自定义策略权重和筛选条件
2. 使用多策略组合提高选股效果
3. 定期进行策略回测验证

### 专业用户
1. 修改源代码添加新的技术指标
2. 开发自定义策略
3. 集成到自动化交易系统

## 📊 性能优化

### 系统要求
- **内存**: 8GB+ (处理大量股票数据)
- **存储**: SSD推荐 (提高数据库I/O性能)
- **网络**: 稳定网络连接 (数据获取)

### 优化建议
1. 定期清理历史数据
2. 使用智能刷新而非全量更新
3. 根据需要调整选股数量
4. 定期备份数据库文件

## 📈 更新日志

### v2.0.0 (2024-08-12) - 重大更新
- ✨ 新增增强技术指标系统（20+种指标）
- ✨ 新增综合评分算法（多维度智能评分）
- ✨ 新增多策略选股引擎（4种内置策略）
- ✨ 新增回测验证系统（历史表现验证）
- ✨ 新增智能输出管理（专业Excel报告）
- ✨ 新增主程序整合（交互模式+命令行）
- ✨ 新增定时任务功能（自动化运行）
- 🔧 优化数据库性能和稳定性
- 🔧 改进错误处理和日志系统

### v1.0.0 (2024-01-01)
- 初始版本发布
- 实现基础选股功能
- 支持Excel导出
- 集成SQLite数据库

## ⚠️ 重要提醒

### 投资风险
1. **本工具仅供学习和研究使用，不构成投资建议**
2. **股市有风险，投资需谨慎**
3. **技术分析有局限性，需结合基本面分析**
4. **历史表现不代表未来收益**

### 使用建议
1. 建议结合多种分析方法
2. 设置合理的止损止盈
3. 控制仓位和风险
4. 持续学习和改进策略

## 📞 技术支持

### 日志文件
系统运行日志保存在`logs/`目录下，出现问题时请查看日志文件。

### 数据备份
数据库文件位于`data/stock_data.db`，建议定期备份。

### 联系方式
- 提交GitHub Issue
- 查看技术文档：`claude.md`

## 📄 许可证

本项目采用MIT许可证，详见LICENSE文件。

---

**免责声明**: 本工具仅供学习和研究使用，不构成投资建议。使用者应当根据自己的判断做出投资决策，并承担相应的投资风险。股市有风险，投资需谨慎。