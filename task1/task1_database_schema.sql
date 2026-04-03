-- ============================================================
-- 泰迪杯竞赛 B 题 - 任务一：构建结构化财报数据库
-- MySQL 建表脚本
-- ============================================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS teddy_cup_financial DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE teddy_cup_financial;

-- ============================================================
-- 1. 核心业绩指标表 (core_performance_indicators_sheet)
-- ============================================================
DROP TABLE IF EXISTS core_performance_indicators_sheet;
CREATE TABLE core_performance_indicators_sheet (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    serial_number INT COMMENT '序号 - 数据排序标识',
    stock_code VARCHAR(20) COMMENT '股票代码',
    stock_abbr VARCHAR(50) COMMENT '股票简称',
    eps DECIMAL(10,4) COMMENT '每股收益(元)',
    total_operating_revenue DECIMAL(20,2) COMMENT '营业总收入(万元)',
    operating_revenue_yoy_growth DECIMAL(10,4) COMMENT '营业总收入-同比增长(%)',
    operating_revenue_qoq_growth DECIMAL(10,4) COMMENT '营业总收入-季度环比增长(%)',
    net_profit_10k_yuan DECIMAL(20,2) COMMENT '净利润(万元)',
    net_profit_yoy_growth DECIMAL(10,4) COMMENT '净利润-同比增长(%)',
    net_profit_qoq_growth DECIMAL(10,4) COMMENT '净利润-季度环比增长(%)',
    net_asset_per_share DECIMAL(10,4) COMMENT '每股净资产(元)',
    roe DECIMAL(10,4) COMMENT '净资产收益率(%)',
    operating_cf_per_share DECIMAL(10,4) COMMENT '每股经营现金流量(元)',
    net_profit_excl_non_recurring DECIMAL(20,2) COMMENT '扣非净利润（万元）',
    net_profit_excl_non_recurring_yoy DECIMAL(10,4) COMMENT '扣非净利润同比增长（%）',
    gross_profit_margin DECIMAL(10,4) COMMENT '销售毛利率(%)',
    net_profit_margin DECIMAL(10,4) COMMENT '销售净利率（%）',
    roe_weighted_excl_non_recurring DECIMAL(10,4) COMMENT '加权平均净资产收益率（扣非）（%）',
    report_period VARCHAR(20) COMMENT '报告期 (FY=年报, Q1=一季度, HY=半年度, Q3=三季度)',
    report_year INT COMMENT '报告期-年份',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_stock_code (stock_code),
    INDEX idx_report_period (report_period),
    INDEX idx_report_year (report_year),
    UNIQUE KEY uk_stock_report (stock_code, report_period, report_year)
) COMMENT='核心业绩指标表';

-- ============================================================
-- 2. 资产负债表 (balance_sheet)
-- ============================================================
DROP TABLE IF EXISTS balance_sheet;
CREATE TABLE balance_sheet (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    serial_number INT COMMENT '序号',
    stock_code VARCHAR(20) COMMENT '股票代码',
    stock_abbr VARCHAR(50) COMMENT '股票简称',
    asset_cash_and_cash_equivalents DECIMAL(20,2) COMMENT '资产-货币资金(万元)',
    asset_accounts_receivable DECIMAL(20,2) COMMENT '资产-应收账款(万元)',
    asset_inventory DECIMAL(20,2) COMMENT '资产-存货(万元)',
    asset_trading_financial_assets DECIMAL(20,2) COMMENT '资产-交易性金融资产（万元）',
    asset_construction_in_progress DECIMAL(20,2) COMMENT '资产-在建工程（万元）',
    asset_total_assets DECIMAL(20,2) COMMENT '资产-总资产(万元)',
    asset_total_assets_yoy_growth DECIMAL(10,4) COMMENT '资产-总资产同比(%)',
    liability_accounts_payable DECIMAL(20,2) COMMENT '负债-应付账款(万元)',
    liability_advance_from_customers DECIMAL(20,2) COMMENT '负债-预收账款(万元)',
    liability_total_liabilities DECIMAL(20,2) COMMENT '负债-总负债(万元)',
    liability_total_liabilities_yoy_growth DECIMAL(10,4) COMMENT '负债-总负债同比(%)',
    liability_contract_liabilities DECIMAL(20,2) COMMENT '负债-合同负债（万元）',
    liability_short_term_loans DECIMAL(20,2) COMMENT '负债-短期借款（万元）',
    asset_liability_ratio DECIMAL(10,4) COMMENT '资产负债率(%)',
    equity_unappropriated_profit DECIMAL(20,2) COMMENT '股东权益-未分配利润（万元）',
    equity_total_equity DECIMAL(20,2) COMMENT '股东权益合计(万元)',
    report_period VARCHAR(20) COMMENT '报告期',
    report_year INT COMMENT '报告期-年份',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_stock_code (stock_code),
    INDEX idx_report_period (report_period),
    INDEX idx_report_year (report_year),
    UNIQUE KEY uk_stock_report (stock_code, report_period, report_year)
) COMMENT='资产负债表';

-- ============================================================
-- 3. 现金流量表 (cash_flow_sheet)
-- ============================================================
DROP TABLE IF EXISTS cash_flow_sheet;
CREATE TABLE cash_flow_sheet (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    serial_number INT COMMENT '序号',
    stock_code VARCHAR(20) COMMENT '股票代码',
    stock_abbr VARCHAR(50) COMMENT '股票简称',
    net_cash_flow DECIMAL(20,2) COMMENT '净现金流(元)',
    net_cash_flow_yoy_growth DECIMAL(10,4) COMMENT '净现金流-同比增长(%)',
    operating_cf_net_amount DECIMAL(20,2) COMMENT '经营性现金流-现金流量净额(万元)',
    operating_cf_ratio_of_net_cf DECIMAL(10,4) COMMENT '经营性现金流-净现金流占比(%)',
    operating_cf_cash_from_sales DECIMAL(20,2) COMMENT '经营性现金流-销售商品收到的现金（万元）',
    investing_cf_net_amount DECIMAL(20,2) COMMENT '投资性现金流-现金流量净额(万元)',
    investing_cf_ratio_of_net_cf DECIMAL(10,4) COMMENT '投资性现金流-净现金流占比(%)',
    investing_cf_cash_for_investments DECIMAL(20,2) COMMENT '投资性现金流-投资支付的现金（万元）',
    investing_cf_cash_from_investment_recovery DECIMAL(20,2) COMMENT '投资性现金流-收回投资收到的现金（万元）',
    financing_cf_cash_from_borrowing DECIMAL(20,2) COMMENT '融资性现金流-取得借款收到的现金（万元）',
    financing_cf_cash_for_debt_repayment DECIMAL(20,2) COMMENT '融资性现金流-偿还债务支付的现金（万元）',
    financing_cf_net_amount DECIMAL(20,2) COMMENT '融资性现金流-现金流量净额(万元)',
    financing_cf_ratio_of_net_cf DECIMAL(10,4) COMMENT '融资性现金流-净现金流占比(%)',
    report_period VARCHAR(20) COMMENT '报告期',
    report_year INT COMMENT '报告期-年份',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_stock_code (stock_code),
    INDEX idx_report_period (report_period),
    INDEX idx_report_year (report_year),
    UNIQUE KEY uk_stock_report (stock_code, report_period, report_year)
) COMMENT='现金流量表';

-- ============================================================
-- 4. 利润表 (income_sheet)
-- ============================================================
DROP TABLE IF EXISTS income_sheet;
CREATE TABLE income_sheet (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    serial_number INT COMMENT '序号',
    stock_code VARCHAR(20) COMMENT '股票代码',
    stock_abbr VARCHAR(50) COMMENT '股票简称',
    net_profit DECIMAL(20,2) COMMENT '净利润(万元)',
    net_profit_yoy_growth DECIMAL(10,4) COMMENT '净利润同比(%)',
    other_income DECIMAL(20,2) COMMENT '其他收益（万元）',
    total_operating_revenue DECIMAL(20,2) COMMENT '营业总收入(万元)',
    operating_revenue_yoy_growth DECIMAL(10,4) COMMENT '营业总收入同比(%)',
    operating_expense_cost_of_sales DECIMAL(20,2) COMMENT '营业总支出-营业成本(万元)',
    operating_expense_selling_expenses DECIMAL(20,2) COMMENT '营业总支出-销售费用(万元)',
    operating_expense_administrative_expenses DECIMAL(20,2) COMMENT '营业总支出-管理费用(万元)',
    operating_expense_financial_expenses DECIMAL(20,2) COMMENT '营业总支出-财务费用(万元)',
    operating_expense_rnd_expenses DECIMAL(20,2) COMMENT '营业总支出-研发费用（万元）',
    operating_expense_taxes_and_surcharges DECIMAL(20,2) COMMENT '营业总支出-税金及附加（万元）',
    total_operating_expenses DECIMAL(20,2) COMMENT '营业总支出(万元)',
    operating_profit DECIMAL(20,2) COMMENT '营业利润(万元)',
    total_profit DECIMAL(20,2) COMMENT '利润总额(万元)',
    asset_impairment_loss DECIMAL(20,2) COMMENT '资产减值损失（万元）',
    credit_impairment_loss DECIMAL(20,2) COMMENT '信用减值损失（万元）',
    report_period VARCHAR(20) COMMENT '报告期',
    report_year INT COMMENT '报告期-年份',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_stock_code (stock_code),
    INDEX idx_report_period (report_period),
    INDEX idx_report_year (report_year),
    UNIQUE KEY uk_stock_report (stock_code, report_period, report_year)
) COMMENT='利润表';

-- ============================================================
-- 5. 公司基本信息表 (company_info) - 可选，用于辅助查询
-- ============================================================
DROP TABLE IF EXISTS company_info;
CREATE TABLE company_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    serial_number INT COMMENT '序号',
    stock_code VARCHAR(20) UNIQUE COMMENT '股票代码',
    short_name VARCHAR(50) COMMENT 'A股简称',
    company_name VARCHAR(200) COMMENT '公司名称',
    english_name VARCHAR(200) COMMENT '英文名称',
    industry VARCHAR(100) COMMENT '所属证监会行业',
    exchange VARCHAR(50) COMMENT '上市交易所',
    security_category VARCHAR(50) COMMENT '证券类别',
    region VARCHAR(100) COMMENT '注册区域',
    registered_capital VARCHAR(20) COMMENT '注册资本',
    employee_count INT COMMENT '雇员人数',
    manager_count INT COMMENT '管理人员人数',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) COMMENT='公司基本信息表';

-- ============================================================
-- 插入公司基本信息
-- ============================================================
INSERT INTO company_info (serial_number, stock_code, short_name, company_name, english_name, industry, exchange, security_category, region, registered_capital, employee_count, manager_count) VALUES
(1, '000999', '华润三九', '华润三九医药股份有限公司', 'China Resources Sanjiu Medical & Pharmaceutical Co., Ltd.', '制造业-医药制造业', '深圳证券交易所', '深交所主板A股', '广东省深圳市', '16.64亿', 20031, 22),
(2, '600080', '金花股份', '金花企业(集团)股份有限公司', 'Ginwa Enterprise (Group) Inc.', '制造业-医药制造业', '上海证券交易所', '上交所主板A股', '陕西省西安市', '3.733亿', 588, 12);
