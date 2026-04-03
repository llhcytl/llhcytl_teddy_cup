"""
泰迪杯竞赛 B 题 - 任务一：PDF财务数据提取脚本（改进版v3）
使用表格提取而非正则表达式，支持复杂表格结构
"""

import pdfplumber
import pandas as pd
import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
import sys

# 确保输出中文正常
sys.stdout.reconfigure(encoding='utf-8')


class FinancialDataExtractor:
    """财务数据提取器 - 使用表格提取"""

    def __init__(self, pdf_base_path: str):
        self.pdf_base_path = Path(pdf_base_path)
        self.company_info = {
            "000999": {"name": "华润三九", "abbr": "华润三九"},
            "600080": {"name": "金花股份", "abbr": "金花股份"}
        }

    def parse_report_period(self, filename: str) -> Dict[str, Any]:
        """从文件名解析报告期信息"""
        result = {"year": None, "period": None, "stock_code": None}

        # 上交所格式
        match_sh = re.match(r'(\d{6})_(\d{4})(\d{2})(\d{2})_', filename)
        if match_sh:
            result["stock_code"] = match_sh.group(1)
            result["year"] = int(match_sh.group(2))
            month = int(match_sh.group(3))

            if month in [3, 4]:
                result["period"] = "Q1"
            elif month == 6:
                result["period"] = "HY"
            elif month in [8, 10]:
                result["period"] = "Q3"
            elif month == 12:
                result["period"] = "FY"
            else:
                result["period"] = "Q3"
            return result

        # 深交所格式
        match_sz = re.search(r'(\d{4})年(.+?)报告', filename)
        if match_sz:
            result["year"] = int(match_sz.group(1))
            period_text = match_sz.group(2)

            if "一季度" in period_text:
                result["period"] = "Q1"
            elif "半年度" in period_text or "中期" in period_text:
                result["period"] = "HY"
            elif "三季度" in period_text:
                result["period"] = "Q3"
            elif "年度" in period_text or "年报" in period_text:
                result["period"] = "FY"

            if "华润三九" in filename or "999" in filename:
                result["stock_code"] = "000999"
            elif "金花" in filename:
                result["stock_code"] = "600080"

        return result

    def extract_tables_from_pdf(self, pdf_path: str) -> List[List[List]]:
        """从PDF中提取所有表格"""
        tables = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_tables = page.extract_tables()
                    if page_tables:
                        tables.extend(page_tables)
        except Exception as e:
            print(f"提取表格失败 {pdf_path}: {e}")
        return tables

    def clean_number(self, value: Any) -> Optional[float]:
        """清理数字字符串，转换为float"""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            cleaned = value.replace(',', '').replace(' ', '').strip()
            cleaned = cleaned.replace('元', '').replace('%', '')
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None

    def find_value_in_row(self, row: List, keyword: str, max_distance: int = 5) -> Optional[float]:
        """
        在行中查找关键字后的数值

        Args:
            row: 表格行
            keyword: 要查找的关键字
            max_distance: 关键字后最大查找距离

        Returns:
            找到的数值，未找到返回None
        """
        for i, cell in enumerate(row):
            if cell and keyword in str(cell):
                # 在关键字后的单元格中查找数值
                for j in range(i + 1, min(len(row), i + 1 + max_distance)):
                    val = self.clean_number(row[j])
                    if val is not None:
                        return val
        return None

    def find_value_in_table(self, table: List[List], keyword: str) -> Optional[float]:
        """
        在整个表格中查找关键字关联的数值

        Args:
            table: 表格数据
            keyword: 要查找的关键字

        Returns:
            找到的数值，未找到返回None
        """
        for row in table:
            val = self.find_value_in_row(row, keyword)
            if val is not None:
                return val
        return None

    def extract_financial_indicators(self, pdf_path: str, report_info: Dict) -> Dict:
        """提取核心业绩指标 - 使用表格提取"""
        data = {
            "serial_number": 1,
            "stock_code": report_info.get("stock_code"),
            "stock_abbr": self.company_info.get(report_info.get("stock_code", ""), {}).get("abbr"),
            "report_period": report_info.get("period"),
            "report_year": report_info.get("year")
        }

        tables = self.extract_tables_from_pdf(pdf_path)

        # 查找核心业绩指标表格
        for table in tables:
            if not table or len(table) < 5:
                continue

            # 展平表格文本进行关键字匹配
            table_text = str(table)
            if not any(kw in table_text for kw in ['每股收益', '营业收入', '净资产收益']):
                continue

            # 提取各项指标
            data['eps'] = self.find_value_in_table(table, '基本每股收益')
            data['total_operating_revenue'] = self.find_value_in_table(table, '营业收入')
            data['net_profit_10k_yuan'] = self.find_value_in_table(table, '归属于上市公司股东的净利润')
            data['net_asset_per_share'] = self.find_value_in_table(table, '每股净资产')
            data['roe'] = self.find_value_in_table(table, '加权平均净资产收益率')
            data['operating_cf_per_share'] = self.find_value_in_table(table, '每股经营现金流')
            data['gross_profit_margin'] = self.find_value_in_table(table, '销售毛利率')

            # 如果找到了有效数据，跳出
            found_count = sum(1 for k, v in data.items() if v is not None and k not in
                            ['serial_number', 'stock_code', 'stock_abbr', 'report_period', 'report_year'])
            if found_count >= 2:
                break

        return data

    def extract_income_statement(self, pdf_path: str, report_info: Dict) -> Dict:
        """提取利润表数据 - 使用表格提取"""
        data = {
            "serial_number": 1,
            "stock_code": report_info.get("stock_code"),
            "stock_abbr": self.company_info.get(report_info.get("stock_code", ""), {}).get("abbr"),
            "report_period": report_info.get("period"),
            "report_year": report_info.get("year")
        }

        tables = self.extract_tables_from_pdf(pdf_path)

        for table in tables:
            if not table or len(table) < 5:
                continue

            table_text = str(table)
            if '营业收入' not in table_text:
                continue

            # 查找利润表特有的字段
            data['total_operating_revenue'] = self.find_value_in_table(table, '营业收入')
            data['operating_profit'] = self.find_value_in_table(table, '营业利润')
            data['total_profit'] = self.find_value_in_table(table, '利润总额')
            data['net_profit'] = self.find_value_in_table(table, '净利润')

            found_count = sum(1 for k, v in data.items() if v is not None and k not in
                            ['serial_number', 'stock_code', 'stock_abbr', 'report_period', 'report_year'])
            if found_count >= 1:
                break

        return data

    def extract_balance_sheet(self, pdf_path: str, report_info: Dict) -> Dict:
        """提取资产负债表数据 - 使用表格提取"""
        data = {
            "serial_number": 1,
            "stock_code": report_info.get("stock_code"),
            "stock_abbr": self.company_info.get(report_info.get("stock_code", ""), {}).get("abbr"),
            "report_period": report_info.get("period"),
            "report_year": report_info.get("year")
        }

        tables = self.extract_tables_from_pdf(pdf_path)

        for table in tables:
            if not table or len(table) < 5:
                continue

            table_text = str(table)
            if not any(kw in table_text for kw in ['总资产', '总负债', '股东权益', '货币资金']):
                continue

            data['asset_total_assets'] = self.find_value_in_table(table, '总资产')
            data['liability_total_liabilities'] = self.find_value_in_table(table, '总负债')
            data['equity_total_equity'] = self.find_value_in_table(table, '股东权益')
            data['asset_cash_and_cash_equivalents'] = self.find_value_in_table(table, '货币资金')

            found_count = sum(1 for k, v in data.items() if v is not None and k not in
                            ['serial_number', 'stock_code', 'stock_abbr', 'report_period', 'report_year'])
            if found_count >= 1:
                break

        return data

    def extract_cash_flow_statement(self, pdf_path: str, report_info: Dict) -> Dict:
        """提取现金流量表数据 - 使用表格提取"""
        data = {
            "serial_number": 1,
            "stock_code": report_info.get("stock_code"),
            "stock_abbr": self.company_info.get(report_info.get("stock_code", ""), {}).get("abbr"),
            "report_period": report_info.get("period"),
            "report_year": report_info.get("year")
        }

        tables = self.extract_tables_from_pdf(pdf_path)

        for table in tables:
            if not table or len(table) < 5:
                continue

            table_text = str(table)
            if '经营活动' not in table_text or '现金流量' not in table_text:
                continue

            data['operating_cf_net_amount'] = self.find_value_in_table(table, '经营活动')
            data['investing_cf_net_amount'] = self.find_value_in_table(table, '投资活动')
            data['financing_cf_net_amount'] = self.find_value_in_table(table, '筹资活动')

            found_count = sum(1 for k, v in data.items() if v is not None and k not in
                            ['serial_number', 'stock_code', 'stock_abbr', 'report_period', 'report_year'])
            if found_count >= 1:
                break

        return data

    def process_all_pdfs(self) -> Dict[str, List[Dict]]:
        """处理所有PDF文件"""
        results = {
            "core_performance_indicators_sheet": [],
            "balance_sheet": [],
            "cash_flow_sheet": [],
            "income_sheet": []
        }

        # 处理上交所报告
        sh_report_path = self.pdf_base_path / "附件2：财务报告" / "reports-上交所"
        if sh_report_path.exists():
            for pdf_file in sorted(sh_report_path.glob("*.pdf")):
                print(f"处理: {pdf_file.name}")
                report_info = self.parse_report_period(pdf_file.name)

                if report_info["year"] and report_info["period"]:
                    results["core_performance_indicators_sheet"].append(
                        self.extract_financial_indicators(str(pdf_file), report_info)
                    )
                    results["balance_sheet"].append(
                        self.extract_balance_sheet(str(pdf_file), report_info)
                    )
                    results["cash_flow_sheet"].append(
                        self.extract_cash_flow_statement(str(pdf_file), report_info)
                    )
                    results["income_sheet"].append(
                        self.extract_income_statement(str(pdf_file), report_info)
                    )

        # 处理深交所报告
        sz_report_path = self.pdf_base_path / "附件2：财务报告" / "reports-深交所"
        if sz_report_path.exists():
            for pdf_file in sorted(sz_report_path.glob("*.pdf")):
                print(f"处理: {pdf_file.name}")
                report_info = self.parse_report_period(pdf_file.name)

                if report_info["year"] and report_info["period"]:
                    results["core_performance_indicators_sheet"].append(
                        self.extract_financial_indicators(str(pdf_file), report_info)
                    )
                    results["balance_sheet"].append(
                        self.extract_balance_sheet(str(pdf_file), report_info)
                    )
                    results["cash_flow_sheet"].append(
                        self.extract_cash_flow_statement(str(pdf_file), report_info)
                    )
                    results["income_sheet"].append(
                        self.extract_income_statement(str(pdf_file), report_info)
                    )

        return results


def main():
    """主函数"""
    base_path = r"C:\Users\34084\Desktop\泰迪杯\示例数据"

    print("="*60)
    print("泰迪杯竞赛 B 题 - 任务一：PDF财务数据提取（表格提取v3）")
    print("="*60)

    extractor = FinancialDataExtractor(base_path)

    # 处理所有PDF
    print("\n开始处理PDF文件...")
    sys.stdout.flush()

    results = extractor.process_all_pdfs()
    sys.stdout.flush()

    # 保存结果到JSON
    output_path = Path(base_path) / "extracted_data_v3.json"
    print(f"\n保存JSON到: {output_path}")
    sys.stdout.flush()
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"提取完成！结果已保存")

    # 打印统计信息
    for table_name, data in results.items():
        print(f"{table_name}: {len(data)} 条记录")

    # 保存为Excel格式便于查看
    excel_output = Path(base_path) / "extracted_data_v3.xlsx"
    print(f"\n保存Excel到: {excel_output}")
    sys.stdout.flush()
    with pd.ExcelWriter(excel_output, engine='openpyxl') as writer:
        for table_name, data in results.items():
            if data:
                df = pd.DataFrame(data)
                df.to_excel(writer, sheet_name=table_name[:31], index=False)

    print(f"完成！")


if __name__ == "__main__":
    main()
