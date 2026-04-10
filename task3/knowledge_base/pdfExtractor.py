"""
研报PDF文本提取器
"""
import os
import sys
import pdfplumber
from typing import List, Dict, Any
# 引用task3的config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import STOCK_REPORTS_DIR, INDUSTRY_REPORTS_DIR # type: ignore


class PDFTextExtractor:
    """PDF文本提取器"""

    def __init__(self):
        self.stock_reports_dir = STOCK_REPORTS_DIR
        self.industry_reports_dir = INDUSTRY_REPORTS_DIR

    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """从PDF提取文本"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = ""
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
                return text
        except Exception as e:
            print(f"[ERROR] 提取PDF失败 {pdf_path}: {e}")
            return ""

    def extract_tables_from_pdf(self, pdf_path: str) -> List[List[List[str]]]:
        """从PDF提取表格"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                all_tables = []
                for page in pdf.pages:
                    tables = page.extract_tables()
                    if tables:
                        all_tables.extend(tables)
                return all_tables
        except Exception as e:
            print(f"[ERROR] 提取表格失败 {pdf_path}: {e}")
            return []

    def load_stock_reports(self) -> List[Dict[str, Any]]:
        """加载个股研报"""
        reports = []
        if not os.path.exists(self.stock_reports_dir):
            print(f"[WARN] 目录不存在: {self.stock_reports_dir}")
            return reports

        for filename in os.listdir(self.stock_reports_dir):
            if filename.endswith('.pdf'):
                pdf_path = os.path.join(self.stock_reports_dir, filename)
                text = self.extract_text_from_pdf(pdf_path)
                tables = self.extract_tables_from_pdf(pdf_path)
                reports.append({
                    "filename": filename,
                    "path": pdf_path,
                    "text": text,
                    "tables": tables,
                    "type": "stock"
                })
                print(f"[OK] 提取个股研报: {filename[:30]}... (字数: {len(text)})")
        return reports

    def load_industry_reports(self) -> List[Dict[str, Any]]:
        """加载行业研报"""
        reports = []
        if not os.path.exists(self.industry_reports_dir):
            print(f"[WARN] {self.stock_reports_dir}")
            return reports

        for filename in os.listdir(self.industry_reports_dir):
            if filename.endswith('.pdf'):
                pdf_path = os.path.join(self.industry_reports_dir, filename)
                text = self.extract_text_from_pdf(pdf_path)
                tables = self.extract_tables_from_pdf(pdf_path)
                reports.append({
                    "filename": filename,
                    "path": pdf_path,
                    "text": text,
                    "tables": tables,
                    "type": "industry"
                })
                print(f"[OK] 提取行业研报: {filename[:30]}... (字数: {len(text)})")
        return reports

    def load_all_reports(self) -> List[Dict[str, Any]]:
        """加载所有研报"""
        reports = []
        reports.extend(self.load_stock_reports())
        reports.extend(self.load_industry_reports())
        print(f"\n[INFO] 共加载 {len(reports)} 份研报")
        return reports


# 测试
if __name__ == "__main__":
    extractor = PDFTextExtractor()
    reports = extractor.load_all_reports()

    print("\n" + "=" * 60)
    print(f"共提取 {len(reports)} 份研报")

    # 打印第一份研报的摘要
    if reports:
        r = reports[0]
        print(f"\n研报: {r['filename']}")
        print(f"类型: {r['type']}")
        print(f"字数: {len(r['text'])}")
        print(f"表格数: {len(r['tables'])}")
        print(f"\n前500字:\n{r['text'][:500]}")