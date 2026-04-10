"""
任务三配置
"""
import os

# 项目根目录
PROJECT_ROOT = r"C:\Users\34084\Desktop\teddy_cup"
TASK3_ROOT = os.path.join(PROJECT_ROOT, "task3")

# 研报数据路径
RESEARCH_REPORT_DIR = os.path.join(PROJECT_ROOT, "示例数据", "附件5：研报数据")

# 知识库路径
KNOWLEDGE_BASE_DIR = os.path.join(TASK3_ROOT, "knowledge_base")
VECTOR_STORE_PATH = os.path.join(KNOWLEDGE_BASE_DIR, "vector_store.pkl")

# 研报Excel信息
STOCK_REPORT_INFO = os.path.join(RESEARCH_REPORT_DIR, "个股_研报信息.xlsx")
INDUSTRY_REPORT_INFO = os.path.join(RESEARCH_REPORT_DIR, "行业_研报信息.xlsx")

# 研报PDF目录
STOCK_REPORTS_DIR = os.path.join(RESEARCH_REPORT_DIR, "个股研报")
INDUSTRY_REPORTS_DIR = os.path.join(RESEARCH_REPORT_DIR, "行业研报")

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "340841",
    "database": "teddy_cup_financial",
    "charset": "utf8mb4"
}

# 支持的公司
COMPANIES = ["金花股份", "华润三九"]
STOCK_CODES = {"金花股份": "600080", "华润三九": "000999"}

# 向量嵌入配置（使用轻量方案）
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DEVICE = "cpu"

# LLM配置
LLM_CONFIG = {
    "model_path": r"C:\Users\34084\Desktop\teddy_cup\models\Qwen3.5\Qwen3.5-0.8B-Q4_K_M.gguf",
    "llama_server_url": "http://127.0.0.1:8080/v1/chat/completions",
    "timeout": 120,
}

# 数据库Schema（用于LLM生成SQL）
DATABASE_SCHEMA = """
## 数据库表结构

### core_performance_indicators_sheet (核心业绩指标表)
- stock_abbr: 股票简称 (金花股份, 华润三九)
- report_year: 报告年份 (2022-2025)
- report_period: 报告期 (FY=年度, HY=半年度, Q1=一季度, Q3=三季度)
- total_operating_revenue: 营业收入(元)
- net_profit_10k_yuan: 净利润(万元)
- eps: 每股收益

### balance_sheet (资产负债表)
- stock_abbr: 股票简称
- report_year: 报告年份
- report_period: 报告期
- asset_total_assets: 总资产(万元)
- liability_total_liabilities: 总负债(万元)
- equity_total_equity: 总权益(万元)

### cash_flow_sheet (现金流量表)
- stock_abbr: 股票简称
- report_year: 报告年份
- report_period: 报告期
- operating_cf_net_amount: 经营活动现金流净额(万元)
- investing_cf_net_amount: 投资活动现金流净额(万元)
- financing_cf_net_amount: 筹资活动现金流净额(万元)

### income_sheet (利润表)
- stock_abbr: 股票简称
- report_year: 报告年份
- report_period: 报告期
- total_operating_revenue: 营业收入(万元)
- operating_profit: 营业利润(万元)
- net_profit: 净利润(万元)

## 重要规则
1. 表之间的关联字段是 stock_abbr, report_year, report_period
2. 查询某年数据时，如果用户没有指定报告期(年度/季度等)，不要添加 report_period 条件
3. 财务指标单位不同，注意转换（营业收入在核心业绩表是元，在利润表是万元）
4. 公司名称: '金花股份', '华润三九'
"""