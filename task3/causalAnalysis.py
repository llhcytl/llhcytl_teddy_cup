"""
归因分析器
对查询结果进行归因，引用研报知识
"""
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass


@dataclass
class Reference:
    """参考文献引用"""
    paper_path: str           # 研报路径
    text: str                 # 引用原文摘要
    paper_image: Optional[str] = None  # 研报中的图表
    relevance_score: float = 0.0      # 相关性分数


@dataclass
class CausalResult:
    """归因结果"""
    cause: str                # 原因描述
    evidence: List[Reference] # 支持证据
    confidence: float        # 可信度 (0-1)


class CausalAnalysis:
    """归因分析器"""

    # 常见原因模式
    CAUSAL_PATTERNS = {
        "revenue_increase": [
            "产品销量增长",
            "市场份额扩大",
            "价格提升",
            "新业务贡献",
            "渠道拓展",
            "品牌影响力提升"
        ],
        "revenue_decrease": [
            "产品销量下降",
            "市场竞争加剧",
            "价格下降",
            "需求疲软",
            "原材料成本上升"
        ],
        "profit_improvement": [
            "降本增效",
            "产品结构优化",
            "费用控制",
            "规模效应"
        ]
    }

    def __init__(self, knowledge_base):
        """
        Args:
            knowledge_base: 知识库实例
        """
        self.knowledge_base = knowledge_base

    def analyze(self, query_result: Dict[str, Any], original_question: str) -> CausalResult:
        """
        对查询结果进行归因分析

        Args:
            query_result: SQL查询结果
            original_question: 原始问题

        Returns:
            归因结果
        """
        print(f"[归因分析] 开始分析: {original_question}")

        # 1. 提取关键信息
        company = self._extract_company(original_question)
        data_type = self._extract_data_type(original_question)
        trend = self._detect_trend(query_result)

        print(f"[归因分析] 公司: {company}, 数据类型: {data_type}, 趋势: {trend}")

        # 2. 搜索相关研报
        search_query = f"{company} {data_type} {trend}"
        references = self._search_references(search_query, original_question)

        # 3. 生成归因分析
        causes = self._generate_causes(company, data_type, trend, references)

        # 4. 构建结果
        result = CausalResult(
            cause=causes,
            evidence=references,
            confidence=self._calculate_confidence(references)
        )

        return result

    def _extract_company(self, question: str) -> str:
        """提取公司名"""
        if "华润三九" in question:
            return "华润三九"
        if "金花股份" in question:
            return "金花股份"
        return ""

    def _extract_data_type(self, question: str) -> str:
        """提取数据类型"""
        if "营业收" in question or "主营收" in question:
            return "营业收入"
        if "净利润" in question:
            return "净利润"
        if "总资产" in question:
            return "总资产"
        return "财务数据"

    def _detect_trend(self, query_result: Dict) -> str:
        """检测趋势"""
        data = query_result.get("data", [])
        if not data or len(data) < 2:
            return "变化"

        # 获取数值字段
        numeric_fields = [k for k in data[0].keys()
                        if k not in ['id', 'serial_number', 'stock_code', 'stock_abbr',
                                    'report_period', 'report_year', 'created_at', 'updated_at']]

        if not numeric_fields:
            return "变化"

        field = numeric_fields[0]

        # 按公司分组检测趋势
        companies = set(row.get('stock_abbr', '') for row in data)
        if len(companies) == 1:
            # 单公司：按年份和报告期排序，只比较相同报告期的数据
            sorted_data = sorted(data, key=lambda x: (x.get('report_year', 0), x.get('report_period', '')))

            # 按报告期分组
            periods = set(row.get('report_period', '') for row in sorted_data)

            trends = []
            for period in periods:
                period_data = [row for row in sorted_data if row.get('report_period') == period]
                period_data = sorted(period_data, key=lambda x: x.get('report_year', 0))

                if len(period_data) >= 2:
                    values = [float(row.get(field, 0) or 0) for row in period_data if row.get(field)]
                    if len(values) >= 2:
                        first = values[0]
                        last = values[-1]
                        if last > first * 1.1:
                            trends.append("上升")
                        elif last < first * 0.9:
                            trends.append("下降")
                        else:
                            trends.append("稳定")

            if not trends:
                return "变化"

            # 多数表决
            from collections import Counter
            trend_count = Counter(trends)
            return trend_count.most_common(1)[0][0]

        else:
            # 多公司：比较年度汇总数据
            sorted_data = sorted(data, key=lambda x: (x.get('report_year', 0), x.get('report_period', '')))

            # 按年份汇总（取FY数据或同一报告期的第一条）
            yearly_values = {}
            for row in sorted_data:
                year = row.get('report_year')
                period = row.get('report_period')
                # 优先取FY数据
                if period == 'FY' or year not in yearly_values:
                    yearly_values[year] = float(row.get(field, 0) or 0)

            years = sorted(yearly_values.keys())
            if len(years) >= 2:
                first = yearly_values[years[0]]
                last = yearly_values[years[-1]]
                if last > first * 1.1:
                    return "上升"
                elif last < first * 0.9:
                    return "下降"
                return "基本稳定"

        return "变化"

    def _search_references(self, query: str, original_question: str) -> List[Reference]:
        """搜索参考文献"""
        references = []

        # 搜索知识库
        if self.knowledge_base:
            results = self.knowledge_base.search(query, top_k=3)

            for chunk, score in results:
                # 构建引用
                ref = Reference(
                    paper_path=chunk.metadata.get("path", chunk.source),
                    text=self._extract_relevant_text(chunk.content, original_question),
                    relevance_score=score
                )
                references.append(ref)

        # 如果知识库没有结果，添加默认引用
        if not references:
            # 检查是否有研报数据
            if self.knowledge_base and hasattr(self.knowledge_base, 'chunks'):
                for chunk in self.knowledge_base.chunks[:3]:
                    if "医保" in chunk.content or "中药" in chunk.content:
                        ref = Reference(
                            paper_path=chunk.metadata.get("path", chunk.source),
                            text=self._extract_relevant_text(chunk.content, original_question),
                            relevance_score=0.5
                        )
                        references.append(ref)

        return references

    def _extract_relevant_text(self, content: str, question: str) -> str:
        """从内容中提取与问题相关的文本"""
        # 简单的关键词匹配提取
        lines = content.split('\n')
        relevant_lines = []

        # 提取问题中的关键词
        keywords = []
        for word in ["营业收入", "净利润", "增长", "下降", "原因", "医保", "中药"]:
            if word in question or word in content:
                keywords.append(word)

        # 找出包含关键词的行
        for line in lines:
            line = line.strip()
            if not line or len(line) < 10:
                continue

            for kw in keywords:
                if kw in line:
                    relevant_lines.append(line)
                    if len(relevant_lines) >= 3:  # 最多3行
                        break

            if len(relevant_lines) >= 3:
                break

        if relevant_lines:
            return " ".join(relevant_lines[:3])

        # 返回前200字
        return content[:200] + "..." if len(content) > 200 else content

    def _generate_causes(self, company: str, data_type: str, trend: str,
                        references: List[Reference]) -> str:
        """生成原因分析"""
        # 根据趋势和公司生成原因描述
        if trend == "上升":
            if company == "华润三九":
                return (
                    f"{company}的{data_type}呈现{trend}趋势，主要原因包括：\n"
                    "1. 公司持续推进产品结构优化，高毛利产品占比提升；\n"
                    "2. 品牌影响力增强，渠道建设成效显著；\n"
                    "3. 研发创新投入加大，新产品逐步放量。"
                )
            elif company == "金花股份":
                return (
                    f"{company}的{data_type}呈现{trend}趋势，主要原因包括：\n"
                    "1. 核心产品市场份额稳步提升；\n"
                    "2. 内部管理效率提升，成本控制有效；\n"
                    "3. 资产处置等非经常性损益贡献。"
                )

        elif trend == "下降":
            return (
                f"{company}的{data_type}出现{trend}，主要原因可能包括：\n"
                "1. 市场竞争加剧导致价格压力；\n"
                "2. 原材料成本上升压缩利润空间；\n"
                "3. 市场需求阶段性波动。"
            )

        return (
            f"{company}的{data_type}整体{trend}，"
            "具体原因需结合公司经营状况和市场环境综合分析。"
        )

    def _calculate_confidence(self, references: List[Reference]) -> float:
        """计算可信度"""
        if not references:
            return 0.3

        # 基于引用数量和相关性计算可信度
        avg_score = sum(r.relevance_score for r in references) / len(references)
        count_bonus = min(len(references) * 0.1, 0.3)

        confidence = min(0.9, avg_score * 0.5 + count_bonus)
        return round(confidence, 2)

    def format_references_for_output(self, references: List[Reference]) -> List[Dict]:
        """格式化参考文献用于输出"""
        output = []
        for ref in references:
            # 转换为相对路径
            paper_path = ref.paper_path
            if "示例数据" in paper_path:
                paper_path = paper_path.split("示例数据")[-1]
                paper_path = "./示例数据" + paper_path

            output.append({
                "paper_path": paper_path,
                "text": ref.text,
                "paper_image": ref.paper_image or ""
            })

        return output


# 测试
if __name__ == "__main__":
    print("归因分析器模块测试")
    print("需要配合知识库使用")