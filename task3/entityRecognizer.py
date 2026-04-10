"""
统一实体识别器
集中管理公司名、财务指标、年份等实体的识别逻辑
"""
import re
from typing import Optional, List, Dict, Any


class EntityRecognizer:
    """统一的实体识别类"""

    # 公司名称映射（标准名 -> 别名列表）
    COMPANIES: Dict[str, List[str]] = {
        "华润三九": ["华润三九", "华润", "000999"],
        "金花股份": ["金花股份", "金花", "600080"],
        "全部": ["两家公司", "全部", "所有公司", "两家"]
    }

    # 财务指标映射（标准名 -> 别名列表）
    FIELDS: Dict[str, List[str]] = {
        "营业收入": ["营业收入", "主营收", "销售额", "营收", "营业总收入"],
        "净利润": ["净利润", "净利", "盈利", "净收益"],
        "总资产": ["总资产", "资产总额", "资产总计"],
        "每股收益": ["每股收益", "EPS", "eps", "每股盈利"],
        "总负债": ["总负债", "负债合计", "负债总额"],
        "总权益": ["总权益", "权益合计", "股东权益", "所有者权益"],
        "经营现金流": ["经营现金流", "经营活动现金流", "经营性现金流"],
        "营业利润": ["营业利润", "运营利润"],
    }

    # 财务指标到数据库字段的映射
    FIELD_TO_DB_COLUMN: Dict[str, str] = {
        "营业收入": "total_operating_revenue",
        "净利润": "net_profit_10k_yuan",
        "总资产": "asset_total_assets",
        "每股收益": "eps",
        "总负债": "liability_total_liabilities",
        "总权益": "equity_total_equity",
        "经营现金流": "operating_cf_net_amount",
        "营业利润": "operating_profit",
    }

    # 时间范围关键词
    TIME_KEYWORDS = {
        "近一年": 1,
        "近两年": 2,
        "近三年": 3,
        "近年": 3,
        "近几年": 3,
        "近年来": 3,
    }

    @classmethod
    def extract_company(cls, text: str) -> Optional[str]:
        """
        提取公司名

        Args:
            text: 输入文本

        Returns:
            标准公司名或None
        """
        # 优先匹配"全部"
        if any(alias in text for alias in cls.COMPANIES.get("全部", [])):
            return "全部"

        # 按标准名顺序匹配（避免"华润"匹配到"华润三九"）
        for standard_name, aliases in cls.COMPANIES.items():
            if standard_name == "全部":
                continue
            for alias in aliases:
                if alias in text:
                    return standard_name

        return None

    @classmethod
    def extract_companies(cls, text: str) -> List[str]:
        """
        提取所有提到的公司

        Args:
            text: 输入文本

        Returns:
            公司名列表
        """
        companies = set()
        for standard_name, aliases in cls.COMPANIES.items():
            for alias in aliases:
                if alias in text:
                    companies.add(standard_name)
                    break

        return list(companies) if companies else []

    @classmethod
    def extract_field(cls, text: str) -> Optional[str]:
        """
        提取财务指标

        Args:
            text: 输入文本

        Returns:
            标准指标名或None
        """
        # 按别名长度降序匹配（优先匹配更具体的词）
        field_items = sorted(cls.FIELDS.items(), key=lambda x: -len(x[0]))

        for standard_name, aliases in field_items:
            for alias in aliases:
                if alias in text:
                    return standard_name

        return None

    @classmethod
    def extract_db_column(cls, text: str) -> Optional[str]:
        """
        直接提取数据库列名

        Args:
            text: 输入文本

        Returns:
            数据库列名或None
        """
        field_name = cls.extract_field(text)
        if field_name:
            return cls.FIELD_TO_DB_COLUMN.get(field_name)
        return None

    @classmethod
    def extract_year_range(cls, text: str) -> str:
        """
        提取年份范围

        Args:
            text: 输入文本

        Returns:
            年份范围描述 (如: "2022-2024年", "近三年")
        """
        # 1. 检测时间关键词
        for keyword, years in cls.TIME_KEYWORDS.items():
            if keyword in text:
                return f"近{years}年"

        # 2. 提取具体年份
        years = re.findall(r'20(22|23|24|25)', text)
        if years:
            years_int = [int(y) for y in years]
            if len(years_int) == 1:
                return f"{years_int[0]}年"
            else:
                return f"{min(years_int)}-{max(years_int)}年"

        return "近三年"

    @classmethod
    def extract_years(cls, text: str) -> List[int]:
        """
        提取所有年份

        Args:
            text: 输入文本

        Returns:
            年份列表
        """
        years = re.findall(r'20(22|23|24|25)', text)
        return [int(y) for y in years] if years else []

    @classmethod
    def extract_report_period(cls, text: str) -> Optional[str]:
        """
        提取报告期

        Args:
            text: 输入文本

        Returns:
            报告期代码 (FY/HY/Q1/Q3) 或 None
        """
        period_mapping = {
            "年度": "FY",
            "年报": "FY",
            "半年度": "HY",
            "半年报": "HY",
            "中期": "HY",
            "一季度": "Q1",
            "一季报": "Q1",
            "Q1": "Q1",
            "三季度": "Q3",
            "三季报": "Q3",
            "Q3": "Q3",
        }

        for chinese, code in period_mapping.items():
            if chinese in text:
                return code

        return None

    @classmethod
    def detect_intent(cls, text: str) -> str:
        """
        检测问题意图类型

        Args:
            text: 输入文本

        Returns:
            意图类型
        """
        # 归因分析
        if any(kw in text for kw in ["原因", "为什么", "为何", "什么原因"]):
            return "causality"

        # 知识检索
        if "哪些" in text and len(text) < 30:
            return "knowledge"

        # 可视化
        if any(kw in text for kw in ["可视化", "绘图", "图表", "画图", "展示"]):
            return "visualization"

        # 对比
        if any(kw in text for kw in ["对比", "比较", "差异"]):
            return "comparison"

        # 排名/TopN
        if any(kw in text for kw in ["排名", "top", "最高", "最大", "最低", "最小", "前"]):
            return "ranking"

        # 趋势
        if any(kw in text for kw in ["趋势", "变化", "走势", "增长", "下降"]):
            return "trend"

        # 默认数据查询
        return "data_query"

    @classmethod
    def extract_all_entities(cls, text: str) -> Dict[str, Any]:
        """
        一次性提取所有实体

        Args:
            text: 输入文本

        Returns:
            包含所有实体的字典
        """
        return {
            "companies": cls.extract_companies(text),
            "company": cls.extract_company(text),
            "field": cls.extract_field(text),
            "db_column": cls.extract_db_column(text),
            "year_range": cls.extract_year_range(text),
            "years": cls.extract_years(text),
            "report_period": cls.extract_report_period(text),
            "intent": cls.detect_intent(text),
        }


# 测试
if __name__ == "__main__":
    test_questions = [
        "华润三九近三年的营业收入是多少？",
        "金花股份2024年的净利润",
        "两家公司2024年的总资产对比",
        "华润三九营业收入上升的原因是什么",
        "国家医保目录新增的中药产品有哪些",
    ]

    print("=" * 60)
    print("实体识别器测试")
    print("=" * 60)

    for q in test_questions:
        print(f"\n问题: {q}")
        entities = EntityRecognizer.extract_all_entities(q)
        print(f"  公司: {entities['company']}")
        print(f"  指标: {entities['field']}")
        print(f"  年份: {entities['year_range']}")
        print(f"  报告期: {entities['report_period']}")
        print(f"  意图: {entities['intent']}")
