"""
LLM模块 - 为task3提供大模型能力
用于意图规划、归因分析、回答整合

使用 llama-server HTTP API 方式调用本地 GGUF 模型
"""
import os
import sys
import json
import re
import requests
from typing import Dict, Any, List, Optional

# 模型配置
MODEL_CONFIG = {
    "model_path": r"C:\Users\34084\Desktop\teddy_cup\models\Qwen3.5\Qwen3.5-0.8B-Q4_K_M.gguf",
    "llama_server_url": "http://127.0.0.1:8080/v1/chat/completions",
    "timeout": 120,
}


class LLM:
    """LLM大模型封装 - 使用 llama-server HTTP API"""

    def __init__(self, model_path: str = None, device: str = "auto"):
        """
        初始化LLM

        Args:
            model_path: 模型路径（GGUF文件）
            device: 设备（暂未使用，llama-server决定）
        """
        self.model_path = model_path or MODEL_CONFIG["model_path"]
        self.api_url = MODEL_CONFIG["llama_server_url"]
        self.timeout = MODEL_CONFIG["timeout"]
        self._checked = False

    def check_connection(self) -> bool:
        """检查 llama-server 是否运行"""
        try:
            response = requests.get(
                self.api_url.replace("/v1/chat/completions", "/v1/models"),
                timeout=5
            )
            return response.status_code == 200
        except:
            return False

    def load(self) -> bool:
        """检查模型是否可用"""
        if self._checked:
            return True

        if not self.check_connection():
            print("[LLM] 警告: llama-server 未运行或无法连接")
            print(f"[LLM] 请先启动 llama-server:")
            print(f"[LLM]   cd {os.path.dirname(self.model_path)}")
            print(f"[LLM]   ..\\..\\llama-server.exe --model Qwen3.5-0.8B-Q4_K_M.gguf --host 127.0.0.1 --port 8080")
            self._checked = True
            return False

        self._checked = True
        return True

    def generate(self, prompt: str, system: str = None, max_tokens: int = 512,
                 temperature: float = 0.1) -> Optional[str]:
        """
        生成文本

        Args:
            prompt: 用户提示
            system: 系统提示（可选）
            max_tokens: 最大生成长度
            temperature: 温度参数

        Returns:
            生成的文本，失败返回None
        """
        if not self._checked:
            if not self.load():
                return None

        try:
            messages = []
            if system:
                messages.append({"role": "system", "content": system})
            messages.append({"role": "user", "content": prompt})

            payload = {
                "model": "local",
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "stream": False
            }

            response = requests.post(
                self.api_url,
                json=payload,
                timeout=self.timeout
            )

            if response.status_code != 200:
                print(f"[LLM] API错误: {response.status_code}")
                return None

            result = response.json()
            return result.get("choices", [{}])[0].get("message", {}).get("content", "").strip()

        except requests.exceptions.Timeout:
            print("[LLM] 请求超时")
            return None
        except Exception as e:
            print(f"[LLM] 生成失败: {e}")
            return None

    def extract_json(self, text: str) -> Optional[Dict]:
        """
        增强的JSON提取 - 支持json5和常见错误修复
        """
        if not text:
            return None

        # 尝试导入json5（更宽松的JSON解析）
        try:
            import json5
            parsers = [json.loads, json5.loads]
        except ImportError:
            parsers = [json.loads]

        # 方法1: 尝试直接解析
        for parser in parsers:
            try:
                return parser(text)
            except:
                pass

        # 方法2: 提取markdown代码块
        patterns = [
            r'```json\s*([\s\S]*?)\s*```',
            r'```JSON\s*([\s\S]*?)\s*```',
            r'```\s*([\s\S]*?)\s*```'
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                json_text = match.group(1).strip()
                for parser in parsers:
                    try:
                        return parser(json_text)
                    except:
                        pass

        # 方法3: 查找JSON对象（增强版 - 支持嵌套）
        # 使用更健壮的正则表达式匹配JSON对象
        json_patterns = [
            r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}',  # 简单嵌套
            r'\{[\s\S]*?\}',  # 宽松匹配
        ]
        for pattern in json_patterns:
            match = re.search(pattern, text, re.DOTALL)
            if match:
                json_text = match.group(0)
                for parser in parsers:
                    try:
                        return parser(json_text)
                    except:
                        pass

        # 方法4: 修复常见JSON错误后重试
        try:
            cleaned = self._clean_json_text(text)
            for parser in parsers:
                try:
                    return parser(cleaned)
                except:
                    pass
        except:
            pass

        # 方法5: 尝试从LLM thinking模式中提取（Qwen3.5可能用）
        if "```" in text:
            parts = text.split("```")
            for i, part in enumerate(parts):
                if i % 2 == 1:  # 代码块内容
                    for parser in parsers:
                        try:
                            return parser(part.strip())
                        except:
                            pass

        return None

    def _clean_json_text(self, text: str) -> str:
        """
        清理常见JSON格式问题

        修复的问题：
        1. 移除注释
        2. 移除尾随逗号
        3. 修复引号问题
        4. 移除控制字符
        """
        import re

        # 移除单行注释 //...
        text = re.sub(r'//[^\n]*', '', text)

        # 移除多行注释 /* ... */
        text = re.sub(r'/\*[\s\S]*?\*/', '', text)

        # 移除尾随逗号 (在 } 或 ] 前的逗号)
        text = re.sub(r',\s*([}\]])', r'\1', text)

        # 修复单引号字符串为双引号
        # 简单处理：替换键名的单引号
        text = re.sub(r"'([^']+)'(\s*:)", r'"\1"\2', text)

        # 移除控制字符（除了换行、制表符等）
        text = ''.join(char for char in text if char.isprintable() or char in '\n\r\t')

        return text.strip()


class IntentPlannerLLM:
    """基于LLM的意图规划器"""

    SYSTEM_PROMPT = """你是一个专业的金融问答助手，负责分析用户问题并规划执行步骤。

## 你的任务
1. 判断问题类型（单意图/多意图/归因分析）
2. 拆分子任务
3. 输出结构化的执行计划

## 问题类型
- data_query: 数据查询
- visualization: 可视化
- comparison: 对比分析
- causality: 归因分析
- knowledge: 知识检索

## 输出格式（JSON）
{
    "question_type": "single/multi/causality/fuzzy",
    "intents": [
        {
            "type": "data_query/visualization/causality/knowledge/comparison",
            "description": "任务描述",
            "params": {
                "company": "公司名或null",
                "field": "字段名或null",
                "years": "年份范围或null"
            }
        }
    ]
}

只输出JSON，不要有其他内容。"""

    def __init__(self, llm: LLM):
        self.llm = llm

    def parse(self, question: str) -> Dict[str, Any]:
        """解析问题"""
        prompt = f"""分析以下问题，输出结构化的执行计划：

问题: {question}

请判断问题类型并拆分子任务。"""

        response = self.llm.generate(
            prompt=prompt,
            system=self.SYSTEM_PROMPT,
            max_tokens=512,
            temperature=0.1
        )

        if not response:
            return self._fallback_result(question)

        result = self.llm.extract_json(response)
        if not result:
            return self._fallback_result(question)

        return result

    def _fallback_result(self, question: str) -> Dict[str, Any]:
        """回退到规则结果"""
        has_cause = any(kw in question for kw in ["原因", "为什么", "为何"])
        has_visual = any(kw in question for kw in ["可视化", "绘图", "图表", "展示"])
        has_which = "哪些" in question and len(question) < 20
        has_compare = any(kw in question for kw in ["对比", "比较", "差异"])

        intents = []

        if has_cause:
            intents.append({
                "type": "causality",
                "description": "分析原因",
                "params": {"company": self._extract_company(question), "field": None, "years": None}
            })
            if has_visual:
                intents.insert(0, {
                    "type": "visualization",
                    "description": "数据可视化",
                    "params": {"company": self._extract_company(question), "field": None, "years": None}
                })
        elif has_which:
            intents.append({
                "type": "knowledge",
                "description": "知识检索",
                "params": {"company": None, "field": None, "years": None}
            })
        elif has_compare:
            intents.append({
                "type": "comparison",
                "description": "对比分析",
                "params": {"company": self._extract_company(question), "field": None, "years": None}
            })
        elif has_visual:
            intents.append({
                "type": "visualization",
                "description": "数据可视化",
                "params": {"company": self._extract_company(question), "field": None, "years": None}
            })
        else:
            intents.append({
                "type": "data_query",
                "description": "数据查询",
                "params": {"company": self._extract_company(question), "field": None, "years": None}
            })

        return {
            "question_type": "causality" if has_cause else ("fuzzy" if has_which else "single"),
            "intents": intents
        }

    def _extract_company(self, text: str) -> Optional[str]:
        """提取公司名"""
        if "华润三九" in text:
            return "华润三九"
        if "金花股份" in text:
            return "金花股份"
        if "两家公司" in text or "全部" in text:
            return "全部"
        return None


class CausalAnalysisLLM:
    """基于LLM的归因分析（支持CoT思维链）"""

    # CoT思维链版本
    SYSTEM_PROMPT_COT = """你是一个专业的金融分析师。分析财务数据变化时，请严格按照以下步骤进行推理：

## 分析步骤（思维链）

### 步骤1：数据观察
- 仔细查看所有数据点
- 识别数值的具体变化
- 计算变化的幅度和比例

### 步骤2：趋势判断
- 判断整体趋势：上升、下降还是稳定
- 评估变化的显著性
- 确定是否是实质性变化

### 步骤3：因素分析
列出可能的影响因素，从以下维度考虑：
- 市场因素：需求变化、竞争格局、价格变动
- 产品因素：新产品推出、产品结构优化
- 经营因素：成本控制、渠道拓展、品牌提升
- 政策因素：行业政策、监管变化
- 特殊事件：并购、资产处置、一次性收益

### 步骤4：证据验证
- 结合研报信息验证哪些因素成立
- 评估每个因素的重要性
- 给出置信度评估

### 步骤5：结论生成
- 综合分析给出归因结论
- 按重要性排序原因
- 提供简洁的总结

## 输出格式（JSON）
```json
{
    "reasoning": "步骤1-5的完整推理过程，展示你的分析思路",
    "trend": "上升/下降/稳定",
    "trend_analysis": "对趋势的详细分析",
    "causes": [
        {
            "title": "原因标题",
            "description": "原因描述",
            "evidence": "支持证据（来自研报）"
        }
    ],
    "confidence": 0.0-1.0,
    "summary": "总结性描述"
}
```

请严格按照思维链步骤进行分析，确保reasoning字段完整展示你的推理过程。"""

    # 简化版（用于快速响应）
    SYSTEM_PROMPT_SIMPLE = """你是一个专业的金融分析师，负责分析财务数据变化的原因。

## 你的任务
1. 分析数据的变化趋势
2. 结合研报信息生成原因分析
3. 输出结构化的归因结果

## 输出格式（JSON）
{
    "trend": "上升/下降/稳定",
    "causes": [
        {
            "title": "原因标题",
            "description": "原因描述"
        }
    ],
    "confidence": 0.0-1.0,
    "summary": "总结性描述"
}

只输出JSON，不要有其他内容。"""

    def __init__(self, llm: LLM):
        self.llm = llm

    def analyze(self, question: str, data: List[Dict],
               references: List[Dict] = None) -> Dict[str, Any]:
        """归因分析"""
        # 构建数据摘要
        data_summary = self._summarize_data(data)

        # 构建参考文献摘要
        ref_summary = ""
        if references:
            ref_summary = "\n\n## 参考文献信息:\n"
            for i, ref in enumerate(references[:3], 1):
                ref_summary += f"{i}. {ref.get('text', '')[:200]}...\n"

        prompt = f"""分析以下财务数据变化的原因：

## 问题
{question}

## 数据摘要
{data_summary}
{ref_summary}

请分析数据变化的原因，给出结构化的归因结果。"""

        # 选择提示词版本
        system_prompt = self.SYSTEM_PROMPT_COT if self.use_cot else self.SYSTEM_PROMPT_SIMPLE

        response = self.llm.generate(
            prompt=prompt,
            system=system_prompt,
            max_tokens=2048 if self.use_cot else 1024,
            temperature=0.3
        )

        if not response:
            return self._fallback_result(question, data)

        result = self.llm.extract_json(response)
        if not result:
            return self._fallback_result(question, data)

        return result

    def _summarize_data(self, data: List[Dict]) -> str:
        """生成数据摘要"""
        if not data:
            return "无数据"

        lines = ["## 财务数据:"]

        # 尝试识别数据中的关键指标
        numeric_fields = []
        for key in data[0].keys():
            if key not in ['id', 'serial_number', 'stock_code', 'stock_abbr',
                          'report_period', 'report_year', 'created_at', 'updated_at']:
                numeric_fields.append(key)

        if numeric_fields:
            lines.append(f"指标字段: {', '.join(numeric_fields)}")

        # 按年份排序
        sorted_data = sorted(data, key=lambda x: (x.get('report_year', 0), x.get('report_period', '')))

        # 按公司分组显示
        companies = set(row.get('stock_abbr', '') for row in sorted_data)
        for company in companies:
            lines.append(f"\n### {company}")
            company_data = [row for row in sorted_data if row.get('stock_abbr') == company]
            for row in company_data:
                parts = []
                for k, v in row.items():
                    if v is not None and k not in ['stock_code', 'id', 'serial_number', 'created_at', 'updated_at']:
                        if isinstance(v, (int, float)):
                            if isinstance(v, float):
                                parts.append(f"{k}={v:,.2f}")
                            else:
                                parts.append(f"{k}={v}")
                        else:
                            parts.append(f"{k}={v}")
                lines.append("  " + ", ".join(parts))

        if len(data) > 5:
            lines.append(f"\n  ... 共 {len(data)} 条记录")

        return "\n".join(lines)

    def _fallback_result(self, question: str, data: List[Dict]) -> Dict[str, Any]:
        """回退结果"""
        # 检测趋势
        trend = self._detect_trend_fallback(data)

        # 根据趋势生成原因
        if trend == "上升":
            causes = [
                {"title": "市场拓展", "description": "公司积极拓展市场渠道，产品销量增长"},
                {"title": "产品优化", "description": "产品结构优化，高毛利产品占比提升"},
                {"title": "品牌提升", "description": "品牌影响力增强，客户认可度提高"}
            ]
        elif trend == "下降":
            causes = [
                {"title": "市场因素", "description": "市场竞争加剧导致价格压力"},
                {"title": "成本因素", "description": "原材料成本上升压缩利润空间"},
                {"title": "需求波动", "description": "市场需求出现阶段性波动"}
            ]
        else:
            causes = [
                {"title": "整体稳定", "description": "公司经营整体保持稳定"}
            ]

        return {
            "trend": trend,
            "causes": causes,
            "confidence": 0.5,
            "summary": f"数据显示{trend}趋势。"
        }

    def _detect_trend_fallback(self, data: List[Dict]) -> str:
        """简单趋势检测"""
        if not data or len(data) < 2:
            return "稳定"

        # 获取数值字段
        numeric_fields = []
        for key in data[0].keys():
            if key not in ['stock_code', 'stock_abbr', 'report_period', 'report_year',
                          'id', 'serial_number', 'created_at', 'updated_at']:
                numeric_fields.append(key)

        if not numeric_fields:
            return "稳定"

        # 按年份和报告期排序
        sorted_data = sorted(data, key=lambda x: (x.get('report_year', 0), x.get('report_period', '')))

        # 只取FY(全年)数据比较
        fy_data = [row for row in sorted_data if row.get('report_period') == 'FY']
        if len(fy_data) >= 2:
            field = numeric_fields[0]
            values = [float(row.get(field, 0) or 0) for row in fy_data if row.get(field)]
            if values:
                first = values[0]
                last = values[-1]
                if last > first * 1.1:
                    return "上升"
                elif last < first * 0.9:
                    return "下降"
                return "稳定"

        # 取第一条和最后一条比较
        field = numeric_fields[0]
        try:
            values = [float(row.get(field, 0) or 0) for row in sorted_data if row.get(field)]
            if len(values) >= 2:
                first = values[0]
                last = values[-1]
                if last > first * 1.1:
                    return "上升"
                elif last < first * 0.9:
                    return "下降"
        except:
            pass

        return "稳定"


class AnswerSynthesizerLLM:
    """基于LLM的回答整合器"""

    SYSTEM_PROMPT = """你是一个专业的金融问答助手，负责整合数据和文字生成最终回答。

## 你的任务
1. 整合查询到的数据
2. 结合研报信息生成连贯的回答
3. 用自然流畅的语言输出"""

    def __init__(self, llm: LLM):
        self.llm = llm

    def synthesize(self, question: str, data: List[Dict],
                  causal_analysis: Dict = None,
                  references: List[Dict] = None) -> str:
        """整合生成最终回答"""
        # 构建数据描述
        data_desc = self._describe_data(data)

        # 构建归因描述
        causal_desc = ""
        if causal_analysis:
            causal_desc = f"\n\n## 归因分析:\n"
            causal_desc += causal_analysis.get("summary", "")
            if causal_analysis.get("causes"):
                causal_desc += "\n\n主要原因:\n"
                for cause in causal_analysis["causes"]:
                    causal_desc += f"- {cause.get('title')}: {cause.get('description')}\n"

        # 构建参考文献描述
        ref_desc = ""
        if references:
            ref_desc = "\n\n## 参考来源:\n"
            for i, ref in enumerate(references[:2], 1):
                source = ref.get("paper_path", "未知来源")
                text = ref.get("text", "")[:100]
                ref_desc += f"[{i}] {source}\n  摘要: {text}...\n"

        prompt = f"""请用自然流畅的语言回答用户的问题：

## 用户问题
{question}

## 查询到的数据
{data_desc}
{causal_desc}
{ref_desc}

请生成一个完整、连贯的回答。"""

        response = self.llm.generate(
            prompt=prompt,
            system=self.SYSTEM_PROMPT,
            max_tokens=1024,
            temperature=0.4
        )

        if not response:
            return self._fallback_synthesize(question, data, causal_analysis)

        return response

    def _describe_data(self, data: List[Dict]) -> str:
        """描述数据"""
        if not data:
            return "未查询到相关数据"

        lines = [f"共查询到 {len(data)} 条记录:"]

        # 按公司分组
        companies = set(row.get('stock_abbr', '') for row in data)
        for company in companies:
            company_data = [row for row in data if row.get('stock_abbr') == company]
            years = sorted(set(str(row.get("report_year", "")) for row in company_data))
            if years:
                lines.append(f"- {company}: {min(years)}-{max(years)}年")

        return "\n".join(lines)

    def _fallback_synthesize(self, question: str, data: List[Dict],
                            causal_analysis: Dict = None) -> str:
        """回退回答"""
        if not data:
            return f"抱歉，未查询到与'{question}'相关的数据。"

        parts = [f"根据查询，共找到 {len(data)} 条相关数据。"]

        if causal_analysis:
            parts.append(causal_analysis.get("summary", ""))

        return " ".join(parts)


# 测试
if __name__ == "__main__":
    print("=" * 60)
    print("LLM模块测试")
    print("=" * 60)

    llm = LLM()

    # 测试连接
    print("\n检查 llama-server 连接...")
    if llm.check_connection():
        print("llama-server 已连接!")
    else:
        print("llama-server 未运行，请先启动:")
        print("  cd C:\\Users\\34084\\Desktop\\teddy_cup\\models")
        print("  llama-server.exe --model Qwen3.5\\Qwen3.5-0.8B-Q4_K_M.gguf --host 127.0.0.1 --port 8080")

    # 测试生成（如果有连接）
    if llm.load():
        print("\n--- 意图规划测试 ---")
        planner = IntentPlannerLLM(llm)
        result = planner.parse("华润三九近三年的主营业务收入上升的原因是什么")
        print(f"结果: {result}")

        print("\n--- 归因分析测试 ---")
        analyzer = CausalAnalysisLLM(llm)
        test_data = [
            {"stock_abbr": "华润三九", "report_year": 2022, "report_period": "FY",
             "total_operating_revenue": 4194386685.74},
            {"stock_abbr": "华润三九", "report_year": 2023, "report_period": "FY",
             "total_operating_revenue": 24738963319.76},
            {"stock_abbr": "华润三九", "report_year": 2024, "report_period": "FY",
             "total_operating_revenue": 7294070557.82},
        ]
        result = analyzer.analyze("华润三九近三年营业收入变化原因", test_data)
        print(f"结果: {result}")

        print("\n--- 回答整合测试 ---")
        synthesizer = AnswerSynthesizerLLM(llm)
        answer = synthesizer.synthesize(
            "华润三九近三年营业收入如何",
            test_data,
            causal_analysis=result
        )
        print(f"回答:\n{answer}")
