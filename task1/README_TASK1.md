# 泰迪杯竞赛 B 题 - 任务一实施指南

## 📋 任务概述
从PDF财务报告中提取数据，存储到MySQL数据库的4张表中，并进行数据校验。

## 🗂️ 文件说明

### 1. `task1_database_schema.sql`
MySQL建表脚本，包含4张财务报表表的结构定义。

**使用方法：**
```bash
# 在MySQL中执行
mysql -u root -p < task1_database_schema.sql
```

### 2. `task1_pdf_extractor.py`
PDF数据提取脚本，自动从财务报告PDF中提取结构化数据。

**使用方法：**
```bash
python task1_pdf_extractor.py
```

**输出文件：**
- `extracted_data.json` - JSON格式的提取数据
- `extracted_data.xlsx` - Excel格式的提取数据

### 3. `task1_import_to_mysql.py`
数据校验与MySQL导入脚本。

**使用方法：**
```bash
python task1_import_to_mysql.py
```

## 🚀 快速开始

### 步骤1: 安装依赖
```bash
pip install pdfplumber pandas pymysql openpyxl
```

### 步骤2: 创建数据库
```bash
mysql -u root -p
```
然后执行SQL文件中的内容。

### 步骤3: 提取PDF数据
```bash
python task1_pdf_extractor.py
```

### 步骤4: 校验并导入MySQL
```bash
python task1_import_to_mysql.py
```

## 📊 数据库表结构

| 表名 | 说明 | 字段数 |
|------|------|--------|
| `core_performance_indicators_sheet` | 核心业绩指标表 | 20 |
| `balance_sheet` | 资产负债表 | 21 |
| `cash_flow_sheet` | 现金流量表 | 18 |
| `income_sheet` | 利润表 | 21 |

## ⚠️ 注意事项

1. **PDF格式差异**：上交所和深交所的报告格式不同，脚本已做适配
2. **单位统一**：部分数据可能需要单位转换（元→万元）
3. **数据校验**：导入前会进行一致性校验
4. **重复数据处理**：同一股票代码+报告期+年份的组合会去重

## 🔍 数据校验规则

1. 必填字段检查
2. 股票代码格式验证
3. 报告期格式验证 (FY/Q1/HY/Q3)
4. 年份范围验证 (2020-2026)
5. 异常值检测
6. 跨表一致性检查

## 📝 数据来源

- **金花股份 (600080)**：上海证券交易所报告
- **华润三九 (000999)**：深圳证券交易所报告

## 🎯 下一步

完成任务一后，可以进行：
- **任务二**：搭建"智能问数"助手
- **任务三**：增强助手可靠性（融合研报数据）
