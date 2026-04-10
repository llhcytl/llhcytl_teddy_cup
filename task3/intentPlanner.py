"""
多意图自主规划器
将复杂查询拆分为可执行的子任务序列
"""
import re
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum


class IntentType(Enum):
    """意图类型"""
    DATA_QUERY = "data_query"           # 数据查询
    VISUALIZATION = "visualization"     # 可视化
    COMPARISON = "comparison"           # 对比分析
    TREND = "trend"                    # 趋势分析
    RANKING = "ranking"                # 排名
    CAUSALITY = "causality"            # 归因分析
    KNOWLEDGE = "knowledge"            # 知识问答
    TOP_N = "top_n"                    # TopN查询


@dataclass
class SubTask:
    """子任务"""
    task_id: str
    task_type: IntentType
    description: str
    sql_template: Optional[str] = None
    params: Dict[str, Any] = None
    depends_on: List[str] = None  # 依赖的子任务ID
    result_field: Optional[str] = None  # 结果字段名

    def __post_init__(self):
        if self.params is None:
            self.params = {}
        if self.depends_on is None:
            self.depends_on = []


class IntentPlanner:
    """多意图规划器"""

    # 意图关键词映射
    INTENT_KEYWORDS = {
        IntentType.DATA_QUERY: ["是多少", "多少", "查询", "获取"],
        IntentType.VISUALIZATION: ["可视化", "绘图", "图表", "画图", "展示"],
        IntentType.COMPARISON: ["对比", "比较", "差异"],
        IntentType.TREND: ["趋势", "变化", "走势", "近年来", "近几年"],
        IntentType.RANKING: ["排名", "top", "最高", "最大", "最小", "最低"],
        IntentType.CAUSALITY: ["原因", "为什么", "为何", "分析", "解释"],
        IntentType.KNOWLEDGE: ["什么是", "有哪些", "哪些是", "说明"],
        IntentType.TOP_N: ["top10", "top5", "前10", "前5", "前三"],
    }

    # 多意图连接词
    INTENT_CONNECTORS = ["？", "？", "，", ",", "。", "；", ";"]

    def __init__(self):
        self.task_counter = 0

    def parse_multi_intent(self, question: str) -> List[SubTask]:
        """
        解析多意图问题

        Args:
            question: 用户问题

        Returns:
            子任务列表
        """
        self.task_counter = 0

        # 检测问题类型
        question_type = self._detect_question_type(question)
        print(f"[意图规划] 问题类型: {question_type}")

        # 根据问题类型拆分
        if question_type == "multi_intent":
            return self._split_multi_intent(question)
        elif question_type == "causality":
            return self._parse_causality_question(question)
        elif question_type == "fuzzy":
            return self._parse_fuzzy_question(question)
        else:
            return [self._create_single_task(question)]

    def _detect_question_type(self, question: str) -> str:
        """检测问题类型"""
        # 多意图：包含多个问号或多个完整句子
        question_marks = question.count('？') + question.count('?')
        if question_marks > 1:
            return "multi_intent"

        # 检查是否包含多个完整的查询意图
        segments = self._split_by_connectors(question)
        if len(segments) > 2:
            return "multi_intent"

        # 归因分析
        causality_keywords = ["原因", "为什么", "为何", "什么原因"]
        if any(kw in question for kw in causality_keywords):
            return "causality"

        # 意图模糊
        if len(question) < 15 and "哪些" in question:
            return "fuzzy"

        return "single"

    def _split_by_connectors(self, text: str) -> List[str]:
        """按连接词分割"""
        # 按中英文标点和连接词分割
        pattern = r'[？?。，,\s]+'
        segments = re.split(pattern, text)
        return [s.strip() for s in segments if s.strip()]

    def _split_multi_intent(self, question: str) -> List[SubTask]:
        """拆分多意图问题"""
        tasks = []

        # 尝试按问号分割
        parts = re.split(r'[？?]', question)
        parts = [p.strip() for p in parts if p.strip()]

        for part in parts:
            if not part:
                continue

            # 判断每部分的意图类型
            intent = self._detect_intent_type(part)
            task = self._create_task_from_intent(part, intent)
            tasks.append(task)

        # 如果按问号分割太少，尝试其他方式
        if len(tasks) < 2 and len(question) > 30:
            # 尝试按连接词分割
            segments = self._split_by_connectors(question)
            if len(segments) >= 2:
                tasks = []
                for seg in segments:
                    if len(seg) > 5:  # 过滤短片段
                        intent = self._detect_intent_type(seg)
                        task = self._create_task_from_intent(seg, intent)
                        tasks.append(task)

        # 去重
        seen = set()
        unique_tasks = []
        for t in tasks:
            if t.description not in seen:
                seen.add(t.description)
                unique_tasks.append(t)

        print(f"[意图规划] 拆分 {len(unique_tasks)} 个子任务")
        return unique_tasks

    def _parse_causality_question(self, question: str) -> List[SubTask]:
        """解析归因问题"""
        tasks = []

        # 归因问题通常包含两部分：查询+原因分析
        # 例如："华润三九近三年的主营业务收入上升的原因是什么"

        # 1. 首先创建数据查询任务
        # 提取公司名
        company = self._extract_company(question)
        year_range = self._extract_year_range(question)

        query_desc = f"查询{company}{year_range}主营业务收入数据"
        query_task = SubTask(
            task_id=self._next_id(),
            task_type=IntentType.DATA_QUERY,
            description=query_desc,
            params={"company": company, "years": year_range, "field": "主营业务收入"}
        )
        tasks.append(query_task)

        # 2. 创建归因分析任务（依赖于查询任务）
        causal_task = SubTask(
            task_id=self._next_id(),
            task_type=IntentType.CAUSALITY,
            description="分析主营业务收入变化的原因",
            params={"company": company, "original_question": question},
            depends_on=[query_task.task_id]
        )
        tasks.append(causal_task)

        return tasks

    def _parse_fuzzy_question(self, question: str) -> List[SubTask]:
        """解析模糊意图问题"""
        # 模糊问题需要知识库检索

        if "哪些" in question:
            # 知识检索任务
            return [SubTask(
                task_id=self._next_id(),
                task_type=IntentType.KNOWLEDGE,
                description=question,
                params={"original_question": question}
            )]

        return [self._create_single_task(question)]

    def _detect_intent_type(self, text: str) -> IntentType:
        """检测意图类型"""
        text_lower = text.lower()

        # 检查每个意图类型的关键词
        for intent_type, keywords in self.INTENT_KEYWORDS.items():
            for kw in keywords:
                if kw in text:
                    return intent_type

        # 默认数据查询
        return IntentType.DATA_QUERY

    def _create_single_task(self, question: str) -> SubTask:
        """创建单个任务"""
        intent = self._detect_intent_type(question)
        return self._create_task_from_intent(question, intent)

    def _create_task_from_intent(self, question: str, intent: IntentType) -> SubTask:
        """根据意图创建任务"""
        task_id = self._next_id()

        # 提取参数
        company = self._extract_company(question)
        years = self._extract_year_range(question)
        field = self._extract_field(question)

        # 根据意图类型构建描述
        desc = question
        if intent == IntentType.DATA_QUERY:
            desc = f"查询{company or ''}{years}的数据"
        elif intent == IntentType.VISUALIZATION:
            desc = f"可视化展示{company or ''}{years}数据"
        elif intent == IntentType.TREND:
            desc = f"分析{company or ''}的趋势变化"
        elif intent == IntentType.RANKING:
            desc = f"排名分析{years}"

        return SubTask(
            task_id=task_id,
            task_type=intent,
            description=desc,
            params={
                "company": company,
                "years": years,
                "field": field,
                "original_question": question
            }
        )

    def _extract_company(self, text: str) -> Optional[str]:
        """提取公司名"""
        if "华润三九" in text:
            return "华润三九"
        if "金花股份" in text:
            return "金花股份"
        if "两家公司" in text or "全部" in text:
            return "全部"
        return None

    def _extract_year_range(self, text: str) -> str:
        """提取年份范围"""
        years = re.findall(r'20\d{2}', text)
        if not years:
            return "近三年"
        return f"{years[0]}-{years[-1]}年"

    def _extract_field(self, text: str) -> Optional[str]:
        """提取字段"""
        fields = {
            "营业收入": "营业收入",
            "净利润": "净利润",
            "总资产": "总资产",
            "主营业务收入": "主营业务收入",
            "利润": "利润",
            "每股收益": "每股收益",
            "销售额": "营业收入",
        }
        for name, field in fields.items():
            if name in text:
                return field
        return None

    def _next_id(self) -> str:
        """生成下一个任务ID"""
        self.task_counter += 1
        return f"task_{self.task_counter}"

    def plan_execution_order(self, tasks: List[SubTask]) -> List[List[str]]:
        """
        规划执行顺序（考虑依赖关系）

        Returns:
            分批执行的task_id列表
        """
        if not tasks:
            return []

        # 简单的拓扑排序
        ready = []  # 可以立即执行的任务
        waiting = {t.task_id: t for t in tasks}  # 等待中的任务
        executed = []  # 已执行

        # 初始化：找出没有依赖的任务
        for t in tasks:
            if not t.depends_on:
                ready.append(t.task_id)

        execution_order = []
        while ready or waiting:
            if not ready:
                # 理论上不应该发生（循环依赖）
                print("[WARN] 存在循环依赖")
                break

            # 执行一批任务
            batch = ready[:]
            execution_order.append(batch)
            ready.clear()

            # 标记这批任务为已执行
            for tid in batch:
                if tid in waiting:
                    del waiting[tid]

            # 检查等待中的任务是否可以执行
            still_waiting = {}
            for tid, t in waiting.items():
                # 检查依赖是否都已执行
                deps_done = all(dep in executed or dep not in waiting for dep in t.depends_on)
                if deps_done:
                    ready.append(tid)
                else:
                    still_waiting[tid] = t

            waiting = still_waiting
            executed.extend(batch)

        return execution_order


# 测试
if __name__ == "__main__":
    planner = IntentPlanner()

    test_questions = [
        # 多意图
        "2024年利润最高的top10企业是哪些？这些企业的利润、销售额年同比是多少？年同比上涨幅度最大的是哪家企业？",
        # 归因分析
        "华润三九近三年的主营业务收入情况做可视化绘图，主营业务收入上升的原因是什么",
        # 模糊意图
        "国家医保目录新增的中药产品有哪些",
        # 单意图
        "金花股份2024年的营业收入是多少？",
    ]

    print("=" * 60)
    print("多意图规划器测试")
    print("=" * 60)

    for q in test_questions:
        print(f"\n问题: {q}")
        tasks = planner.parse_multi_intent(q)
        print(f"拆分为 {len(tasks)} 个子任务:")
        for t in tasks:
            print(f"  [{t.task_id}] {t.task_type.value}: {t.description}")
            if t.depends_on:
                print(f"       依赖: {t.depends_on}")