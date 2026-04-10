"""
增强版智能问数助手 (任务三)
集成知识库、多意图规划、归因分析、LLM大模型
"""
import sys
import os
import json
import re
import pymysql
from typing import Dict, Any, List, Optional

# 添加路径
sys.path.insert(0, os.path.dirname(__file__))

from config import DB_CONFIG, KNOWLEDGE_BASE_DIR, VECTOR_STORE_PATH
from knowledge_base.pdfExtractor import PDFTextExtractor
from knowledge_base.vectorStore import VectorStore, KnowledgeBaseBuilder
from intentPlanner import IntentPlanner, IntentType, SubTask
from causalAnalysis import CausalAnalysis, Reference
from llm import LLM, IntentPlannerLLM, CausalAnalysisLLM, AnswerSynthesizerLLM


class EnhancedFinancialAssistant:
    """增强版财报智能问数助手"""

    def __init__(self, use_knowledge_base: bool = True, use_llm: bool = True):
        """初始化"""
        self.db_config = DB_CONFIG
        self.connection = None

        # 知识库
        self.use_knowledge_base = use_knowledge_base
        self.knowledge_base = None
        self._init_knowledge_base()

        # 意图规划器（规则+LLM）
        self.intent_planner = IntentPlanner()
        self.use_llm = use_llm
        self.llm = None
        self.intent_planner_llm = None
        self.causal_analyzer_llm = None
        self.answer_synthesizer = None
        self._init_llm()

        # 归因分析器（规则）
        self.causal_analyzer = CausalAnalysis(self.knowledge_base)

        # 结果目录
        self.result_dir = os.path.join(os.path.dirname(__file__), "result")
        os.makedirs(self.result_dir, exist_ok=True)

        print("=" * 60)
        print("     增强版智能问数助手 (任务三)")
        print("=" * 60)
        print("\n功能:")
        print("  - 知识库检索 (研报数据)")
        print("  - 多意图规划")
        print("  - 归因分析")
        if self.llm:
            print("  - LLM大模型支持 (Qwen3.5-0.8B)")
        print("=" * 60)

    def _init_llm(self):
        """初始化LLM"""
        if not self.use_llm:
            print("[INFO] LLM已禁用")
            return

        try:
            self.llm = LLM()
            if self.llm.check_connection():
                self.intent_planner_llm = IntentPlannerLLM(self.llm)
                self.causal_analyzer_llm = CausalAnalysisLLM(self.llm)
                self.answer_synthesizer = AnswerSynthesizerLLM(self.llm)
                print("[OK] LLM初始化成功 (Qwen3.5-0.8B)")
            else:
                print("[WARN] LLM未连接 (llama-server未运行)")
                print("[WARN] 将使用规则方法进行意图规划和归因分析")
                self.llm = None
        except Exception as e:
            print(f"[WARN] LLM初始化失败: {e}")
            self.llm = None

    def _init_knowledge_base(self):
        """初始化知识库"""
        if not self.use_knowledge_base:
            print("[INFO] 知识库已禁用")
            return

        self.knowledge_base = VectorStore(VECTOR_STORE_PATH)

        # 尝试加载已有知识库
        if os.path.exists(VECTOR_STORE_PATH):
            self.knowledge_base.load()
        else:
            print("[INFO] 正在构建知识库...")
            self._build_knowledge_base()

    def _build_knowledge_base(self):
        """构建知识库"""
        try:
            # 提取研报
            extractor = PDFTextExtractor()
            reports = extractor.load_all_reports()

            if not reports:
                print("[WARN] 未找到研报数据")
                return

            # 构建知识库
            self.knowledge_base = VectorStore(VECTOR_STORE_PATH)
            builder = KnowledgeBaseBuilder(self.knowledge_base)
            builder.add_reports_to_knowledge_base(reports)

            # 构建索引
            self.knowledge_base.build_index()

            # 保存
            self.knowledge_base.save()

            print(f"[OK] 知识库构建完成: {len(self.knowledge_base.chunks)} 个知识块")

        except Exception as e:
            print(f"[ERROR] 知识库构建失败: {e}")
            self.knowledge_base = None

    def _connect_db(self) -> bool:
        """连接数据库"""
        if self.connection:
            return True

        try:
            self.connection = pymysql.connect(**self.db_config)
            return True
        except Exception as e:
            print(f"[ERROR] 数据库连接失败: {e}")
            return False

    def query(self, question: str) -> Dict[str, Any]:
        """
        处理用户查询

        Args:
            question: 用户问题

        Returns:
            {
                "success": bool,
                "question": str,          # 保存问题
                "answer": {
                    "content": str,        # 回答内容
                    "image": List[str],    # 图片路径
                    "references": List[Dict]  # 参考文献
                },
                "sql": str,                # SQL语句
                "data": List[Dict],       # 查询数据
                "tasks": List[SubTask]    # 子任务列表
            }
        """
        print(f"\n{'='*60}")
        print(f"问题: {question}")
        print(f"{'='*60}")

        # 保存原始问题
        original_question = question

        # 1. 意图规划（优先使用LLM）
        if self.intent_planner_llm:
            print("[意图规划] 使用 LLM 进行意图解析...")
            llm_result = self.intent_planner_llm.parse(question)
            print(f"[意图规划] LLM结果: {llm_result}")

            # 将LLM结果转换为SubTask列表
            tasks = self._convert_llm_intents_to_tasks(llm_result, question)
        else:
            print("[意图规划] 使用规则方法进行意图解析...")
            tasks = self.intent_planner.parse_multi_intent(question)

        # 2. 执行子任务
        all_results = []
        all_sqls = []
        all_images = []

        for task in tasks:
            result = self._execute_task(task)
            if result:
                all_results.append(result)
                if result.get("sql"):
                    all_sqls.append(result["sql"])
                if result.get("image"):
                    all_images.extend(result.get("image", []))

        # 3. 获取查询数据用于归因分析
        query_data = all_results[0].get("data", []) if all_results else []

        # 4. 归因分析（如果需要）
        references = []
        causal_analysis_result = None
        if any(t.task_type == IntentType.CAUSALITY for t in tasks):
            # 使用LLM进行归因分析
            if self.causal_analyzer_llm and query_data:
                print("[归因分析] 使用 LLM 进行归因分析...")
                # 获取参考文献
                kb_references = []
                if self.knowledge_base:
                    kb_results = self.knowledge_base.search(
                        f"{self._extract_company(question)} {self._extract_field(question)}",
                        top_k=3
                    )
                    for chunk, score in kb_results:
                        kb_references.append({
                            "paper_path": chunk.metadata.get("path", chunk.source),
                            "text": chunk.content[:200],
                            "relevance_score": score
                        })

                causal_analysis_result = self.causal_analyzer_llm.analyze(
                    question, query_data, kb_references
                )
                print(f"[归因分析] LLM结果: {causal_analysis_result.get('trend', '未知')} - {causal_analysis_result.get('summary', '')[:50]}")

                # 提取参考文献
                for ref in kb_references:
                    references.append(ref)
            else:
                # 使用规则归因分析
                causal_result = None
                for r in all_results:
                    if r.get("causal_analysis"):
                        causal_result = r["causal_analysis"]
                        break

                if causal_result:
                    references = self.causal_analyzer.format_references_for_output(
                        causal_result.evidence
                    )

        # 5. 整合结果（优先使用LLM）
        if self.answer_synthesizer and query_data:
            print("[结果整合] 使用 LLM 进行回答整合...")
            integrated_answer = self.answer_synthesizer.synthesize(
                question, query_data, causal_analysis_result, references
            )
        else:
            integrated_answer = self._integrate_results(all_results, question)

        # 6. 构建最终响应
        final_result = {
            "success": bool(all_results),
            "question": original_question,
            "answer": {
                "content": integrated_answer,
                "image": all_images,
                "references": references
            },
            "sql": "; ".join(all_sqls) if all_sqls else "",
            "data": query_data,
            "tasks": tasks
        }

        return final_result

    def _execute_task(self, task: SubTask) -> Optional[Dict[str, Any]]:
        """执行单个子任务"""
        print(f"\n[执行] {task.task_id}: {task.description}")

        if task.task_type == IntentType.DATA_QUERY:
            return self._execute_data_query(task)
        elif task.task_type == IntentType.VISUALIZATION:
            return self._execute_visualization(task)
        elif task.task_type == IntentType.CAUSALITY:
            return self._execute_causality(task)
        elif task.task_type == IntentType.KNOWLEDGE:
            return self._execute_knowledge_query(task)
        elif task.task_type == IntentType.TOP_N:
            return self._execute_top_n_query(task)
        elif task.task_type == IntentType.COMPARISON:
            return self._execute_comparison(task)
        else:
            return self._execute_data_query(task)

    def _execute_data_query(self, task: SubTask) -> Dict[str, Any]:
        """执行数据查询任务"""
        params = task.params

        # 构建SQL
        sql = self._build_sql(
            company=params.get("company"),
            field=params.get("field"),
            years=params.get("years")
        )

        # 执行查询
        data = self._execute_sql(sql)

        return {
            "task_id": task.task_id,
            "type": "data_query",
            "sql": sql,
            "data": data,
            "description": task.description
        }

    def _execute_visualization(self, task: SubTask) -> Dict[str, Any]:
        """执行可视化任务"""
        # 先执行数据查询
        params = task.params
        sql = self._build_sql(
            company=params.get("company"),
            field=params.get("field"),
            years=params.get("years")
        )

        data = self._execute_sql(sql)

        # 生成图表
        image_path = self._generate_chart(data, task.description)

        return {
            "task_id": task.task_id,
            "type": "visualization",
            "sql": sql,
            "data": data,
            "image": [image_path] if image_path else [],
            "description": task.description
        }

    def _execute_causality(self, task: SubTask) -> Dict[str, Any]:
        """执行归因分析任务"""
        params = task.params

        # 先查询数据
        sql = self._build_sql(
            company=params.get("company"),
            field=params.get("field"),
            years=params.get("years")
        )

        data = self._execute_sql(sql)

        # 进行归因分析 - 使用原始问题
        query_result = {"success": True, "data": data}
        original_question = params.get("original_question", task.description)
        causal_result = self.causal_analyzer.analyze(
            query_result,
            original_question
        )

        return {
            "task_id": task.task_id,
            "type": "causality",
            "causal_analysis": causal_result,
            "data": data,
            "description": task.description
        }

    def _execute_knowledge_query(self, task: SubTask) -> Dict[str, Any]:
        """执行知识查询任务"""
        if not self.knowledge_base:
            return {
                "task_id": task.task_id,
                "type": "knowledge",
                "content": "知识库未初始化",
                "references": []
            }

        # 搜索知识库
        results = self.knowledge_base.search(task.params.get("original_question", task.description), top_k=3)

        # 整合内容
        contents = []
        references = []
        for chunk, score in results:
            contents.append(chunk.content)
            references.append(Reference(
                paper_path=chunk.metadata.get("path", chunk.source),
                text=chunk.content[:300],
                relevance_score=score
            ))

        return {
            "task_id": task.task_id,
            "type": "knowledge",
            "content": "\n\n".join(contents) if contents else "未找到相关信息",
            "references": references,
            "description": task.description
        }

    def _execute_top_n_query(self, task: SubTask) -> Dict[str, Any]:
        """执行TopN查询"""
        params = task.params

        # 构建SQL（需要排序和LIMIT）
        sql = self._build_top_n_sql(
            company=params.get("company"),
            field=params.get("field"),
            years=params.get("years"),
            n=10
        )

        data = self._execute_sql(sql)

        return {
            "task_id": task.task_id,
            "type": "top_n",
            "sql": sql,
            "data": data,
            "description": task.description
        }

    def _execute_comparison(self, task: SubTask) -> Dict[str, Any]:
        """执行对比分析任务"""
        params = task.params

        # 查询两家公司数据进行对比
        sql = self._build_sql(
            company="全部",  # 对比需要查询所有公司
            field=params.get("field"),
            years=params.get("years")
        )

        data = self._execute_sql(sql)

        return {
            "task_id": task.task_id,
            "type": "comparison",
            "sql": sql,
            "data": data,
            "description": task.description
        }

    # 字段名映射
    FIELD_MAPPING = {
        "营业收入": "total_operating_revenue",
        "净利润": "net_profit_10k_yuan",
        "主营业务收入": "total_operating_revenue",
        "利润": "net_profit_10k_yuan",
        "每股收益": "eps",
        "总资产": "asset_total_assets",
        "经营现金流": "operating_cf_net_amount",
        "营业收入同比": "operating_revenue_yoy_growth",
        "净利润同比": "net_profit_yoy_growth",
    }

    def _build_sql(self, company: str = None, field: str = None, years: str = None, table: str = "core_performance_indicators_sheet") -> str:
        """构建SQL查询"""
        # 确定字段
        if field:
            actual_field = self.FIELD_MAPPING.get(field, field)
            select_fields = ["stock_abbr", "report_year", "report_period", actual_field]
        else:
            select_fields = ["stock_abbr", "report_year", "report_period", "total_operating_revenue", "net_profit_10k_yuan"]

        field_str = ", ".join(select_fields)

        # 构建WHERE条件
        conditions = []

        if company and company != "全部":
            conditions.append(f"stock_abbr = '{company}'")

        # 年份条件
        if years:
            year_nums = re.findall(r'20\d{2}', years)
            if year_nums:
                min_year = min(year_nums)
                max_year = max(year_nums)
                if min_year == max_year:
                    conditions.append(f"report_year = {min_year}")
                else:
                    conditions.append(f"report_year >= {min_year} AND report_year <= {max_year}")

        where_clause = ""
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)

        sql = f"SELECT {field_str} FROM {table}{where_clause} ORDER BY stock_abbr, report_year, report_period;"

        return sql

    def _build_top_n_sql(self, company: str = None, field: str = None, years: str = None, n: int = 10) -> str:
        """构建TopN SQL"""
        import re

        order_field = self.FIELD_MAPPING.get(field, "net_profit_10k_yuan")

        # 构建WHERE条件
        conditions = ["report_period = 'FY'"]  # 只取全年数据

        # 年份条件
        if years:
            year_nums = re.findall(r'20\d{2}', years)
            if year_nums:
                # 取最近一年或指定年份
                target_year = max(year_nums)
                conditions.append(f"report_year = {target_year}")
            else:
                conditions.append("report_year = 2024")
        else:
            conditions.append("report_year = 2024")

        where_clause = " WHERE " + " AND ".join(conditions)

        sql = f"""
        SELECT stock_abbr, report_year, report_period, {order_field}
        FROM core_performance_indicators_sheet
        {where_clause}
        ORDER BY {order_field} DESC
        LIMIT {n};
        """.strip()

        return sql

    def _execute_sql(self, sql: str) -> List[Dict]:
        """执行SQL查询"""
        if not self._connect_db():
            return []

        try:
            with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(sql)
                return cursor.fetchall()
        except Exception as e:
            print(f"[ERROR] SQL执行失败: {e}")
            return []

    def _generate_chart(self, data: List[Dict], title: str) -> Optional[str]:
        """生成图表"""
        if not data:
            return None

        try:
            import matplotlib.pyplot as plt
            import matplotlib
            import base64
            import io

            matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei']
            matplotlib.rcParams['axes.unicode_minus'] = False

            # 简单的柱状图
            fig, ax = plt.subplots(figsize=(10, 6))

            labels = [f"{row.get('stock_abbr', '')}-{row.get('report_period', '')}" for row in data]

            # 获取数值字段
            numeric_fields = [k for k in data[0].keys()
                            if k not in ['stock_code', 'stock_abbr', 'report_period', 'report_year']]
            if numeric_fields:
                values = [float(row.get(numeric_fields[0], 0)) for row in data]
                ax.bar(range(len(labels)), values)
                ax.set_xticks(range(len(labels)))
                ax.set_xticklabels(labels, rotation=45, ha='right')

            ax.set_title(title)
            plt.tight_layout()

            # 保存
            filename = f"chart_{len(os.listdir(self.result_dir)) + 1}.png"
            filepath = os.path.join(self.result_dir, filename)
            plt.savefig(filepath, dpi=100)
            plt.close()

            print(f"[OK] 图表已保存: {filepath}")
            return filepath

        except Exception as e:
            print(f"[WARN] 图表生成失败: {e}")
            return None

    def _convert_llm_intents_to_tasks(self, llm_result: Dict, original_question: str) -> List[SubTask]:
        """将LLM意图解析结果转换为SubTask列表"""
        tasks = []
        intents = llm_result.get("intents", [])

        for i, intent in enumerate(intents):
            intent_type_str = intent.get("type", "data_query")
            params = intent.get("params", {})

            # 映射意图类型字符串到IntentType枚举
            type_mapping = {
                "data_query": IntentType.DATA_QUERY,
                "visualization": IntentType.VISUALIZATION,
                "comparison": IntentType.COMPARISON,
                "causality": IntentType.CAUSALITY,
                "knowledge": IntentType.KNOWLEDGE,
                "top_n": IntentType.TOP_N,
            }
            intent_type = type_mapping.get(intent_type_str, IntentType.DATA_QUERY)

            task = SubTask(
                task_id=f"task_{i+1}",
                task_type=intent_type,
                description=intent.get("description", original_question),
                params={
                    "company": params.get("company"),
                    "field": params.get("field"),
                    "years": params.get("years"),
                    "original_question": original_question
                }
            )
            tasks.append(task)

        return tasks

    def _extract_company(self, text: str) -> str:
        """提取公司名"""
        if "华润三九" in text:
            return "华润三九"
        if "金花股份" in text:
            return "金花股份"
        if "两家公司" in text or "全部" in text:
            return "全部"
        return ""

    def _extract_field(self, text: str) -> str:
        """提取字段"""
        if "营业收" in text or "主营收" in text:
            return "营业收入"
        if "净利润" in text:
            return "净利润"
        if "总资产" in text:
            return "总资产"
        if "每股收益" in text or "EPS" in text:
            return "每股收益"
        return "财务数据"

    def _integrate_results(self, results: List[Dict], original_question: str) -> str:
        """整合子任务结果"""
        if not results:
            return "未找到相关信息"

        parts = []

        for result in results:
            if result.get("type") == "data_query":
                data = result.get("data", [])
                if data:
                    parts.append(self._format_data_result(data, result.get("description", "")))

            elif result.get("type") == "causality":
                if result.get("causal_analysis"):
                    parts.append(result["causal_analysis"].cause)

            elif result.get("type") == "knowledge":
                content = result.get("content", "")
                if content:
                    parts.append(content[:500])  # 限制长度

        return "\n\n".join(parts) if parts else "未找到相关信息"

    def _format_data_result(self, data: List[Dict], description: str) -> str:
        """格式化数据结果"""
        if not data:
            return "无数据"

        lines = [f"查询结果 ({len(data)}条记录):"]

        # 只显示前几条
        for i, row in enumerate(data[:5], 1):
            row_str = ", ".join(f"{k}={v}" for k, v in row.items() if v is not None)
            lines.append(f"  [{i}] {row_str}")

        if len(data) > 5:
            lines.append(f"  ... 还有 {len(data) - 5} 条记录")

        return "\n".join(lines)

    def format_output(self, result: Dict[str, Any], question_id: str = "") -> Dict[str, Any]:
        """
        格式化输出（符合任务三要求的格式）

        输出格式:
        {
            "Q": "问题",
            "A": {
                "content": "回答",
                "image": ["图片路径"],
                "references": [{"paper_path": "", "text": "", "paper_image": ""}]
            }
        }
        """
        answer = result.get("answer", {})

        output = {
            "Q": result.get("question", ""),
            "A": {
                "content": answer.get("content", ""),
                "image": answer.get("image", []),
                "references": answer.get("references", [])
            }
        }

        return output

    def close(self):
        """关闭连接"""
        if self.connection:
            self.connection.close()
            print("[OK] 数据库连接已关闭")


# 测试
if __name__ == "__main__":
    assistant = EnhancedFinancialAssistant()

    test_questions = [
        "金花股份2024年的营业收入是多少？",
        "国家医保目录新增的中药产品有哪些",
        "华润三九近三年的主营业务收入上升的原因是什么",
    ]

    print("\n" + "=" * 60)
    print("增强版助手测试")
    print("=" * 60)

    for i, q in enumerate(test_questions, 1):
        result = assistant.query(q)
        print(f"\n结果 {i}:")
        print(f"  回答: {result['answer']['content'][:200]}...")
        print(f"  图片: {result['answer']['image']}")
        print(f"  参考文献: {len(result['answer']['references'])}条")

    assistant.close()