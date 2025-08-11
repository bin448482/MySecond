# 短线选股工具

一个基于Python的A股短线选股工具，集成了数据获取、技术指标计算、多维度筛选和Excel导出功能。

## 功能特性

### 🎯 核心功能
- **数据获取**: 自动获取A股股票列表和历史数据
- **技术指标**: 计算MACD、RSI、均线等技术指标
- **多维筛选**: 基于涨幅、量比、换手率的综合筛选
- **智能评分**: 综合技术指标的智能评分系统
- **Excel导出**: 专业格式的Excel报告生成

### 📊 选股策略
1. **涨幅榜选股**: 筛选当日或近期涨幅较大的股票
2. **成交量筛选**: 基于量比和换手率的活跃度筛选
3. **技术指标筛选**: MACD金叉、RSI超卖反弹、均线突破
4. **综合评分**: 多维度加权评分排序

### 💾 数据管理
- **SQLite数据库**: 轻量级本地数据存储
- **增量更新**: 智能的数据增量更新机制
- **历史记录**: 完整的选股历史记录保存

## 安装说明

### 环境要求
- Python 3.8+
- Windows/Linux/macOS

### 安装步骤

1. **克隆项目**
```bash
git clone <repository-url>
cd short-term-stock-selector
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

## 快速开始

### 1. 首次运行（完整流程）
```bash
python main.py --full
```
这将执行：
- 获取A股股票列表
- 下载历史数据
- 计算技术指标
- 执行选股
- 生成Excel报告

### 2. 仅执行选股
```bash
python main.py --select
```

### 3. 查看涨幅榜
```bash
python main.py --gainers 20
```

### 4. 查看高量比股票
```bash
python main.py --high-volume 2.0 15
```

## 详细使用说明

### 命令行参数

#### 主要功能
- `--full`: 运行完整流程（推荐首次使用）
- `--select`: 仅执行选股
- `--update-data`: 仅更新股票数据
- `--calculate-indicators`: 仅计算技术指标

#### 查询功能
- `--stats`: 显示数据库统计信息
- `--gainers N`: 显示涨幅榜前N名
- `--high-volume RATIO N`: 显示量比大于RATIO的前N只股票

#### 参数选项
- `--max-results N`: 最大选股结果数（默认50）
- `--max-stocks N`: 最大处理股票数（默认100）
- `--days N`: 历史数据天数（默认60）
- `--test-mode`: 测试模式（处理少量股票）

### 配置文件

编辑 `config.yaml` 文件可以自定义各种参数：

```yaml
# 筛选条件
filters:
  price_range: [3, 200]        # 价格区间
  min_volume_ratio: 1.2        # 最小量比
  min_turnover_rate: 2.0       # 最小换手率
  min_price_change: 1.0        # 最小涨幅

# 技术指标参数
technical:
  macd:
    fast_period: 12
    slow_period: 26
    signal_period: 9
  rsi:
    period: 14
    overbought: 70
    oversold: 30

# 输出设置
output:
  max_results: 50
  excel_filename: "selected_stocks_{date}.xlsx"
```

## 项目结构

```
short-term-stock-selector/
├── main.py                 # 主程序入口
├── config.yaml            # 配置文件
├── database.py            # 数据库管理
├── data_fetcher.py        # 数据获取
├── technical_indicators.py # 技术指标计算
├── stock_selector.py      # 选股逻辑
├── output_manager.py      # 输出管理
├── utils.py               # 工具函数
├── requirements.txt       # 依赖包
├── README.md              # 说明文档
├── claude.md              # 技术文档
├── data/                  # 数据目录
│   └── stock_data.db      # SQLite数据库
├── logs/                  # 日志目录
└── output/                # 输出目录
    └── selected_stocks_*.xlsx
```

## 使用示例

### 示例1：日常选股
```bash
# 更新数据并选股
python main.py --full --max-results 30

# 仅基于现有数据选股
python main.py --select --max-results 20
```

### 示例2：市场分析
```bash
# 查看今日涨幅榜
python main.py --gainers 50

# 查看高量比股票
python main.py --high-volume 2.5 30

# 查看数据库统计
python main.py --stats
```

### 示例3：测试模式
```bash
# 测试模式（处理少量股票）
python main.py --full --test-mode
```

## 输出说明

### Excel报告包含三个工作表：

1. **选股结果**: 主要的选股结果，包含：
   - 股票代码和名称
   - 当前价格和涨跌幅
   - 量比和换手率
   - 技术指标信号
   - 综合得分

2. **统计摘要**: 本次选股的统计信息：
   - 基础统计数据
   - 技术指标分布
   - 市场概况

3. **技术指标**: 详细的技术指标数值：
   - MACD、RSI等指标值
   - 各种技术信号

### 控制台输出
程序运行时会在控制台显示：
- 实时进度信息
- 选股结果摘要
- 错误和警告信息

## 技术指标说明

### MACD (指数平滑移动平均线)
- **金叉**: MACD线上穿信号线，看涨信号
- **死叉**: MACD线下穿信号线，看跌信号
- **看涨**: MACD和信号线都在零轴上方
- **看跌**: MACD和信号线都在零轴下方

### RSI (相对强弱指标)
- **超买**: RSI > 70，可能回调
- **超卖**: RSI < 30，可能反弹
- **超卖反弹**: 从超卖区域向上突破
- **正常**: 30 ≤ RSI ≤ 70

### 均线系统
- **多头排列**: 短期均线在长期均线上方
- **空头排列**: 短期均线在长期均线下方
- **突破**: 价格突破均线

### 成交量指标
- **量比**: 当日成交量/近5日平均成交量
- **换手率**: 成交量/流通股本 × 100%

## 评分算法

综合得分 = 价格动量得分 × 30% + 成交量得分 × 25% + 技术指标得分 × 45%

- **价格动量得分**: 基于涨跌幅计算
- **成交量得分**: 基于量比和换手率
- **技术指标得分**: 基于MACD、RSI、均线信号

## 注意事项

### ⚠️ 重要提醒
1. **投资风险**: 本工具仅供参考，投资有风险，入市需谨慎
2. **数据延迟**: 数据可能存在延迟，请以实时行情为准
3. **策略局限**: 技术分析有局限性，需结合基本面分析

### 🔧 使用建议
1. **首次使用**: 建议先使用测试模式熟悉功能
2. **定期更新**: 建议每日更新数据以获得最新结果
3. **参数调整**: 根据市场情况调整筛选参数
4. **结果验证**: 建议人工验证选股结果

### 🐛 常见问题

**Q: 首次运行很慢怎么办？**
A: 首次运行需要下载大量数据，建议使用 `--test-mode` 先测试。

**Q: 没有找到符合条件的股票？**
A: 可以适当放宽筛选条件，或检查数据是否为最新。

**Q: Excel文件打不开？**
A: 确保安装了openpyxl包，检查输出目录权限。

**Q: 数据获取失败？**
A: 检查网络连接，akshare可能需要稳定的网络环境。

## 更新日志

### v1.0.0 (2024-01-01)
- 初始版本发布
- 实现基础选股功能
- 支持Excel导出
- 集成SQLite数据库

## 贡献指南

欢迎提交Issue和Pull Request来改进这个项目！

## 许可证

本项目采用MIT许可证，详见LICENSE文件。

## 联系方式

如有问题或建议，请通过以下方式联系：
- 提交GitHub Issue
- 发送邮件至：[your-email@example.com]

---

**免责声明**: 本工具仅供学习和研究使用，不构成投资建议。使用者应当根据自己的判断做出投资决策，并承担相应的投资风险。