# 数据刷新工具命令行使用说明

## 概述

`data_refresh.py` 已从交互式菜单界面改为命令行界面，支持通过命令行参数直接执行各种数据刷新操作。

## 基本语法

```bash
python data_refresh.py <command> [options]
```

## 可用命令

### 1. smart-refresh - 智能刷新（推荐）

根据完整性报告，只刷新有问题的股票数据。

```bash
python data_refresh.py smart-refresh [options]
```

**选项：**
- `--test-mode`: 启用测试模式（限制处理股票数量为20只）
- `--max-stocks N`: 设置最大处理股票数量
- `--report-file PATH`: 指定完整性报告文件路径（默认：data/completeness_report.json）
- `--yes`: 跳过确认提示，直接执行

**使用示例：**
```bash
# 基本智能刷新
python data_refresh.py smart-refresh

# 测试模式，跳过确认
python data_refresh.py smart-refresh --test-mode --yes

# 限制处理100只股票
python data_refresh.py smart-refresh --max-stocks 100

# 使用自定义报告文件
python data_refresh.py smart-refresh --report-file custom_report.json
```

### 2. full-refresh - 全量数据刷新

删除所有现有数据，重新获取所有股票数据。

```bash
python data_refresh.py full-refresh [options]
```

**选项：**
- `--test-mode`: 启用测试模式（限制处理股票数量为50只）
- `--max-stocks N`: 设置最大处理股票数量
- `--yes`: 跳过确认提示，直接执行

**使用示例：**
```bash
# 基本全量刷新
python data_refresh.py full-refresh

# 测试模式
python data_refresh.py full-refresh --test-mode

# 限制处理200只股票，跳过确认
python data_refresh.py full-refresh --max-stocks 200 --yes
```

### 3. cleanup - 清理失败股票列表

检查失败股票的数据完整性，如果完整则从失败列表中清除。

```bash
python data_refresh.py cleanup [options]
```

**选项：**
- `--progress-file PATH`: 指定批处理进度文件路径（默认：data/enhanced_batch_progress.json）
- `--yes`: 跳过确认提示，直接执行

**使用示例：**
```bash
# 基本清理操作
python data_refresh.py cleanup

# 跳过确认提示
python data_refresh.py cleanup --yes

# 使用自定义进度文件
python data_refresh.py cleanup --progress-file custom_progress.json
```

### 4. check - 数据完整性检查

检查所有股票的数据完整性并生成报告。

```bash
python data_refresh.py check [options]
```

**选项：**
- `--target-days N`: 设置检查的目标天数（默认：60）
- `--output-file PATH`: 指定输出报告文件路径（默认：data/completeness_report.json）

**使用示例：**
```bash
# 基本完整性检查
python data_refresh.py check

# 检查最近30天的数据
python data_refresh.py check --target-days 30

# 自定义输出文件
python data_refresh.py check --output-file custom_report.json

# 检查90天数据并保存到指定文件
python data_refresh.py check --target-days 90 --output-file 90day_report.json
```

## 帮助信息

### 查看总体帮助
```bash
python data_refresh.py --help
```

### 查看特定命令帮助
```bash
python data_refresh.py smart-refresh --help
python data_refresh.py full-refresh --help
python data_refresh.py cleanup --help
python data_refresh.py check --help
```

## 典型工作流程

### 1. 日常数据维护
```bash
# 1. 检查数据完整性
python data_refresh.py check

# 2. 根据报告智能刷新
python data_refresh.py smart-refresh --yes
```

### 2. 测试和开发
```bash
# 测试模式下的智能刷新
python data_refresh.py smart-refresh --test-mode --yes

# 限制处理数量的全量刷新
python data_refresh.py full-refresh --max-stocks 10 --yes
```

### 3. 批量处理后的清理
```bash
# 清理失败股票列表
python data_refresh.py cleanup --yes

# 重新检查数据完整性
python data_refresh.py check
```

## 注意事项

1. **备份**: 所有修改数据的操作（smart-refresh、full-refresh）都会自动创建数据库备份
2. **确认提示**: 使用 `--yes` 参数可以跳过所有确认提示，适合自动化脚本
3. **测试模式**: 建议在生产环境使用前先用 `--test-mode` 参数测试
4. **日志**: 所有操作都会记录详细日志，便于问题排查
5. **退出码**: 成功执行返回0，失败返回1，便于脚本集成

## 从旧版本迁移

如果你之前使用交互式菜单：

| 旧操作 | 新命令 |
|--------|--------|
| 选择 "1" (智能刷新) | `python data_refresh.py smart-refresh` |
| 选择 "2" (全量刷新) | `python data_refresh.py full-refresh` |
| 选择 "3" (清理失败股票) | `python data_refresh.py cleanup` |
| 选择 "4" (完整性检查) | `python data_refresh.py check` |

所有功能保持不变，只是调用方式改为命令行参数。