"""
泰迪杯任务二配置文件
"""
import os

# 项目根目录
PROJECT_ROOT = r"C:\Users\34084\Desktop\teddy_cup"

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "340841",
    "database": "teddy_cup_financial",
    "charset": "utf8mb4"
}

# 研报数据路径
#RESEARCH_REPORT_DIR = os.path.join(PROJECT_ROOT, "示例数据", "附件5：研报数据")

# 研报PDF目录
#STOCK_REPORTS_DIR = os.path.join(RESEARCH_REPORT_DIR, "个股研报")
#INDUSTRY_REPORTS_DIR = os.path.join(RESEARCH_REPORT_DIR, "行业研报")

# 模型配置
MODEL_CONFIG = {
    # 本地GGUF模型 (使用llama-server加载)
    "model_name": "Qwen3.5-0.8B-Q4_K_M.gguf",
    "llama_server_url": "http://127.0.0.1:8080/v1/chat/completions",
    # 备用: 使用在线模型
    "online_model": "Qwen/Qwen2.5-Coder-7B-Instruct",
}
#(如：2024年度, 2024年半年度, 2024年一季度, 2024年三季度)
# 数据库Schema描述
DATABASE_SCHEMA = """
## 数据库表结构

### 1. 核心业绩指标表 (core_performance_indicators_sheet)
- stock_code (varchar): 股票代码
- stock_abbr (varchar): 股票简称
- report_period (varchar): 报告期【数据库仅存储英文缩写，禁止使用中文】
  固定枚举值：
  FY = 年度数据
  HY = 半年报数据
  Q1 = 一季报数据
  Q3 = 三季报数据
- report_year (int): 报告期-年份 (如：2022,2023,2024,2025)
- eps (decimal): 每股收益(元)
- total_operating_revenue (decimal): 营业收入(万元)
- net_profit_10k_yuan (decimal): 净利润(万元)

### 2. 资产负债表 (balance_sheet)
- stock_code (varchar): 股票代码
- stock_abbr (varchar): 股票简称
- report_period (varchar): 报告期
- asset_total_assets (decimal): 资产总计(万元)
- liability_total_liabilities (decimal): 负债合计(万元)
- equity_total_equity (decimal): 所有者权益合计(万元)

### 3. 现金流量表 (cash_flow_sheet)
- stock_code (varchar): 股票代码
- stock_abbr (varchar): 股票简称
- report_period (varchar): 报告期
- operating_cf_net_amount (decimal): 经营活动产生的现金流量净额(万元)
- investing_cf_net_amount (decimal): 投资活动产生的现金流量净额(万元)
- financing_cf_net_amount (decimal): 筹资活动产生的现金流量净额(万元)

### 4. 利润表 (income_sheet)
- stock_code (varchar): 股票代码
- stock_abbr (varchar): 股票简称
- report_period (varchar): 报告期
- total_operating_revenue (decimal): 营业收入(万元)
- operating_profit (decimal): 营业利润(万元)
- net_profit (decimal): 净利润(万元)

## 公司列表
- 金花股份(600080)
- 华润三九(000999)

## 报告期说明
报告期格式: YYYY+报告类型
- 年度: YYYY年度 (如: 2024年度)
- 半年度: YYYY年半年度
- 一季度: YYYY年一季度
- 三季度: YYYY年三季度
"""

# SQL生成提示词模板
SQL_GENERATION_PROMPT = """
你是一个专业的SQL查询生成助手。请根据用户的问题，生成对应的MySQL查询语句。

## 数据库Schema
{schema}

## 用户问题
{question}

## 重要提示
1. 只生成SQL语句，不要有任何其他说明文字
2. 使用MySQL语法
3. 注意处理中文字符，使用varchar类型字段的值需要用引号包裹
4. 报告期字段格式: '2024年度', '2024年半年度', '2024年一季度', '2024年三季度'
5. 公司名称格式: '金花股份', '华润三九'

## SQL语句
"""
