"""
Text2SQL 模块 - 自然语言转SQL
支持多种实现方式：
1. 基于规则的备用方案
2. LLM生成（Qwen/DeepSeek等）
3. API调用方式
4. 多轮对话上下文支持
"""
import re
import json
import pymysql
import requests
from typing import Optional, List, Dict, Any
from config import DB_CONFIG, DATABASE_SCHEMA, MODEL_CONFIG
from context_handler import ContextHandler


class RuleBasedSQLGenerator:
    """基于规则的SQL生成器（备用方案）"""

    def __init__(self):
        """初始化规则生成器"""
        # 表名映射
        self.table_mapping = {
            "核心业绩": "core_performance_indicators_sheet",
            "业绩指标": "core_performance_indicators_sheet",
            "资产负债": "balance_sheet",
            "现金流量": "cash_flow_sheet",
            "利润": "income_sheet"
        }

        # 字段映射
        self.field_mapping = {
            "营业收入": "total_operating_revenue",
            "净利润": "net_profit_10k_yuan",
            "每股收益": "eps",
            "总资产": "asset_total_assets",
            "负债": "liability_total_liabilities",
            "权益": "equity_total_equity",
            "经营现金流": "operating_cf_net_amount",
            "投资现金流": "investing_cf_net_amount",
            "筹资现金流": "financing_cf_net_amount",
            "营业利润": "operating_profit"
        }

        # 表名和字段对应关系
        self.table_fields = {
            "core_performance_indicators_sheet": ["total_operating_revenue", "net_profit_10k_yuan", "eps"],
            "balance_sheet": ["asset_total_assets", "liability_total_liabilities", "equity_total_equity"],
            "cash_flow_sheet": ["operating_cf_net_amount", "investing_cf_net_amount", "financing_cf_net_amount"],
            "income_sheet": ["total_operating_revenue", "operating_profit", "net_profit"]
        }

        # 公司映射
        self.company_mapping = {
            "金花": "金花股份",
            "600080": "金花股份",
            "华润": "华润三九",
            "000999": "华润三九"
        }

    def generate(self, question: str) -> str:
        """
        根据问题生成SQL

        Args:
            question: 用户问题

        Returns:
            SQL语句
        """
        question = question.lower()

        # 解析问题
        companies = self._extract_companies(question)
        fields = self._extract_fields(question)
        periods = self._extract_periods(question)
        table = self._determine_table(question, fields)

        # 构建SQL
        sql = self._build_sql(companies, fields, periods, table)
        return sql

    def _extract_companies(self, question: str) -> List[str]:
        """提取公司名称"""
        companies = []
        for key, value in self.company_mapping.items():
            if key in question:
                companies.append(value)
        return companies if companies else ["金花股份", "华润三九"]

    def _extract_fields(self, question: str) -> List[str]:
        """提取字段"""
        fields = []
        for key, value in self.field_mapping.items():
            if key in question:
                fields.append(value)
        return fields if fields else ["*"]

    def _extract_periods(self, question: str) -> Dict[str, Any]:
        """提取报告期，返回year和period条件"""
        condition = {}

        # 提取年份
        year_match = re.search(r'20(22|23|24|25)', question)
        if year_match:
            year = int(year_match.group(0))
            condition["report_year"] = year

            # 判断报告类型
            if "年度" in question:
                condition["report_period"] = ["FY"]
            elif "半年度" in question or "中期" in question:
                condition["report_period"] = ["HY"]
            elif "一季度" in question or "q1" in question:
                condition["report_period"] = ["Q1"]
            elif "三季度" in question or "q3" in question:
                condition["report_period"] = ["Q3"]
            else:
                # 默认查询所有
                condition["report_period"] = ["FY", "HY", "Q1", "Q3"]

        return condition

    def _determine_table(self, question: str, fields: List[str]) -> str:
        """确定查询表"""
        # 根据字段确定表
        for field in fields:
            if field in ["operating_cf_net_amount", "investing_cf_net_amount", "financing_cf_net_amount"]:
                return "cash_flow_sheet"
            elif field in ["asset_total_assets", "liability_total_liabilities", "equity_total_equity"]:
                return "balance_sheet"
            elif field == "operating_profit":
                return "income_sheet"
            elif field in ["total_operating_revenue", "net_profit_10k_yuan", "eps"]:
                # 默认用核心业绩指标表
                return "core_performance_indicators_sheet"

        # 根据问题关键词确定表
        for key, table in self.table_mapping.items():
            if key in question:
                return table

        return "core_performance_indicators_sheet"  # 默认表

    def _build_sql(self, companies: List[str], fields: List[str],
                   periods: Dict[str, Any], table: str) -> str:
        """构建SQL语句"""
        # 选择字段
        if "*" in fields:
            select_fields = "stock_abbr, report_year, report_period, *"
        else:
            # 根据表确定实际字段名
            actual_fields = []
            for f in fields:
                # 从对应表中查找实际字段名
                for tab, flds in self.table_fields.items():
                    if f in flds and tab == table:
                        actual_fields.append(f)
                        break
            select_fields = ", ".join(["stock_abbr", "report_year", "report_period"] + list(set(actual_fields)))

        # 构建查询
        sql = f"SELECT {select_fields} FROM {table}"

        # WHERE条件
        conditions = []

        # 公司条件 - 使用stock_abbr
        if len(companies) == 1:
            conditions.append(f"stock_abbr = '{companies[0]}'")
        elif len(companies) > 1:
            company_list = ", ".join([f"'{c}'" for c in companies])
            conditions.append(f"stock_abbr IN ({company_list})")

        # 年份条件
        if "report_year" in periods:
            conditions.append(f"report_year = {periods['report_year']}")

        # 期间条件
        if "report_period" in periods:
            period_list = periods["report_period"]
            if len(period_list) == 1:
                conditions.append(f"report_period = '{period_list[0]}'")
            else:
                period_str = ", ".join([f"'{p}'" for p in period_list])
                conditions.append(f"report_period IN ({period_str})")

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        # 排序
        sql += " ORDER BY stock_abbr, report_year, report_period"

        return sql + ";"


class DatabaseQuerier:
    """数据库查询执行器"""

    def __init__(self, db_config: Optional[Dict] = None):
        """初始化查询器"""
        self.db_config = db_config or DB_CONFIG
        self.connection = None

    def connect(self) -> bool:
        """连接数据库"""
        try:
            self.connection = pymysql.connect(**self.db_config)
            print("[OK] 数据库连接成功")
            return True
        except Exception as e:
            print(f"[ERROR] 数据库连接失败: {e}")
            return False

    def execute_query(self, sql: str) -> Optional[List[Dict]]:
        """执行查询"""
        if self.connection is None:
            if not self.connect():
                return None

        try:
            with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(sql)
                result = cursor.fetchall()
                return result
        except Exception as e:
            print(f"[ERROR] 查询执行失败: {e}")
            print(f"SQL: {sql}")
            return None

    def execute_query_with_retry(self, sql: str, max_retries: int = 2) -> Dict[str, Any]:
        """
        执行SQL，失败时尝试修正 (RSL-SQL风格)

        Args:
            sql: SQL语句
            max_retries: 最大重试次数

        Returns:
            {"success": bool, "data": list, "sql": str, "error": str}
        """
        current_sql = sql
        last_error = None

        for attempt in range(max_retries + 1):
            try:
                if self.connection is None:
                    if not self.connect():
                        return {"success": False, "error": "数据库连接失败", "sql": current_sql}

                with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
                    cursor.execute(current_sql)
                    result = cursor.fetchall()

                    if attempt > 0:
                        print(f"[SQL修正] 第 {attempt} 次修正成功!")

                    return {
                        "success": True,
                        "data": result,
                        "sql": current_sql,
                        "original_sql": sql,
                        "attempts": attempt + 1
                    }

            except Exception as e:
                last_error = str(e)
                error_type = type(e).__name__

                if attempt < max_retries:
                    print(f"[SQL修正] 尝试 {attempt + 1}/{max_retries}: {error_type}: {last_error[:100]}")
                    # 尝试修正SQL
                    fixed_sql = self._fix_sql_with_rules(current_sql, last_error, error_type)
                    if fixed_sql and fixed_sql != current_sql:
                        current_sql = fixed_sql
                        print(f"[SQL修正] 修正后的SQL: {current_sql[:150]}...")
                        continue
                else:
                    print(f"[SQL修正] 达到最大重试次数，放弃修正")

        return {
            "success": False,
            "error": last_error,
            "sql": current_sql,
            "original_sql": sql
        }

    def _fix_sql_with_rules(self, broken_sql: str, error_msg: str, error_type: str) -> Optional[str]:
        """
        使用规则修正错误的SQL

        Args:
            broken_sql: 错误的SQL
            error_msg: 错误信息
            error_type: 错误类型

        Returns:
            修正后的SQL，或None
        """
        import re

        sql = broken_sql.strip()

        # 1. 修复列名不存在的问题
        if "Unknown column" in error_msg:
            # 提取不存在的列名
            match = re.search(r"Unknown column '([^']+)'", error_msg)
            if match:
                bad_column = match.group(1)
                print(f"[SQL修正] 检测到无效列名: {bad_column}")

                # 尝试映射到正确的列名
                column_mapping = {
                    "net_profit": "net_profit_10k_yuan",
                    "净利润": "net_profit_10k_yuan",
                    "营业收入": "total_operating_revenue",
                    "总资产": "asset_total_assets",
                }

                for wrong, correct in column_mapping.items():
                    if wrong in bad_column:
                        sql = sql.replace(bad_column, correct)
                        print(f"[SQL修正] 列名映射: {wrong} -> {correct}")
                        return sql

        # 2. 修复表名不存在的问题
        if "Table" in error_msg and "doesn't exist" in error_msg:
            match = re.search(r"Table '([^']+)'", error_msg)
            if match:
                bad_table = match.group(1).split(".")[-1] if "." in match.group(1) else match.group(1)
                print(f"[SQL修正] 检测到无效表名: {bad_table}")

                # 表名映射
                table_mapping = {
                    "core_performance": "core_performance_indicators_sheet",
                    "performance": "core_performance_indicators_sheet",
                    "balance": "balance_sheet",
                    "cash_flow": "cash_flow_sheet",
                    "income": "income_sheet",
                }

                if bad_table in table_mapping:
                    sql = sql.replace(bad_table, table_mapping[bad_table])
                    print(f"[SQL修正] 表名映射: {bad_table} -> {table_mapping[bad_table]}")
                    return sql

        # 3. 修复引号问题
        if error_type in ["SyntaxError", "ProgrammingError"] and "syntax" in error_msg.lower():
            # 检查是否有未闭合的引号
            if sql.count("'") % 2 != 0:
                print(f"[SQL修正] 检测到未闭合的引号")
                # 在末尾添加引号
                sql = sql + "'"
                return sql

        # 4. 修复report_period值的问题
        if "report_period" in error_msg.lower():
            # 将中文报告期转换为英文代码
            period_mapping = {
                "年度": "FY",
                "半年度": "HY",
                "一季度": "Q1",
                "三季报": "Q3",
                "三季度": "Q3",
            }

            for chinese, english in period_mapping.items():
                if f"'{chinese}" in sql or f'"{chinese}' in sql:
                    sql = sql.replace(f"'{chinese}'", f"'{english}'")
                    sql = sql.replace(f'"{chinese}"', f"'{english}'")
                    print(f"[SQL修正] 报告期映射: {chinese} -> {english}")
                    return sql

        # 5. 移除多余的分号
        if sql.count(";") > 1:
            sql = sql.rstrip(";") + ";"
            return sql

        # 6. 检查是否缺少基本结构
        if not sql.strip().upper().startswith("SELECT"):
            print(f"[SQL修正] SQL不是以SELECT开头，尝试修复")
            return None

        return None

    def execute_sql_with_result(self, sql: str) -> Dict[str, Any]:
        """执行SQL并返回结构化结果"""
        result = self.execute_query(sql)
        if result is None:
            return {"success": False, "error": "查询失败"}
        if not result:
            return {"success": True, "data": [], "message": "未查询到数据"}

        return {
            "success": True,
            "data": result,
            "count": len(result),
            "sql": sql
        }

    def get_table_schema(self, table_name: str) -> Optional[List[Dict]]:
        """获取表结构"""
        sql = f"DESCRIBE {table_name}"
        return self.execute_query(sql)

    def close(self):
        """关闭连接"""
        if self.connection:
            self.connection.close()
            print("[OK] 数据库连接已关闭")


class Text2SQL:
    """Text2SQL转换器"""

    def __init__(self, use_llm: bool = False, model_path: str = None):
        """
        初始化Text2SQL

        Args:
            use_llm: 是否使用LLM生成SQL
            model_path: LLM模型路径
        """
        self.use_llm = use_llm
        self.rule_generator = RuleBasedSQLGenerator()
        self.querier = DatabaseQuerier()
        self.context_handler = ContextHandler()
        self.last_context = None  # 保存上一次查询的上下文

        # LLM相关（后续实现）
        self.model = None
        self.tokenizer = None

        if use_llm and model_path:
            self._load_llm(model_path)

    def _load_llm(self, model_path: str):
        """验证llama-server连接"""
        try:
            # 检查服务器是否可用
            response = requests.get(
                MODEL_CONFIG["llama_server_url"].replace("/v1/chat/completions", "/v1/models"),
                timeout=5
            )
            if response.status_code == 200:
                print(f"[OK] llama-server连接成功")
                print(f"    模型: {MODEL_CONFIG['model_name']}")
                self.model = True  # 标记LLM可用
            else:
                print(f"[ERROR] llama-server响应异常: {response.status_code}")
                self.use_llm = False
        except Exception as e:
            print(f"[ERROR] 无法连接到llama-server: {e}")
            print("请确保 llama-server 已启动")
            print(f"启动命令: ./llama-server.exe --model Qwen3.5/{MODEL_CONFIG['model_name']} --host 127.0.0.1 --port 8080 -t 4 -c 2048")
            self.use_llm = False

    def generate_sql(self, question: str) -> str:
        """
        生成SQL语句

        Args:
            question: 用户问题

        Returns:
            SQL语句
        """
        if self.use_llm and self.model:
            return self._llm_generate(question)
        else:
            return self.rule_generator.generate(question)

    def _llm_generate(self, question: str) -> str:
        """使用LLM生成SQL（通过HTTP API）"""
        prompt = f"""根据以下数据库schema，将用户问题转换为MySQL查询语句。

{DATABASE_SCHEMA}

用户问题: {question}

要求：
1. 只输出SQL语句，不要有任何解释、说明、思考过程
2. 直接输出SQL，不要用反引号包裹
3. 用户问题【未指定具体报告期】（如：2024年营收）→ 不添加 report_period 条件，仅按年份查询
4. 用户问题【明确指定】（如：2024年度/一季报）→ 添加对应 report_period 条件

SQL:"""

        try:
            messages = [
                {"role": "system", "content": "你是一个专业的SQL生成助手。"},
                {"role": "user", "content": prompt}
            ]

            payload = {
                "model": MODEL_CONFIG["model_name"],
                "messages": messages,
                "max_tokens": 512,
                "temperature": 0.1,
                "stream": False
            }

            response = requests.post(
                MODEL_CONFIG["llama_server_url"],
                json=payload,
                timeout=60
            )

            print(f"[DEBUG] API响应状态: {response.status_code}")
            if response.status_code == 200:
                result = response.json()
                message = result["choices"][0]["message"]
                # Qwen3.5 thinking模式可能把内容放在reasoning_content
                content = message.get("content", "") or message.get("reasoning_content", "")
                print(f"[DEBUG] 原始响应: {content[:200]}...")
                # 清理thinking内容
                if "<think>" in content:
                    parts = content.split("</think>")
                    content = parts[-1].strip()
                    print(f"[DEBUG] 清理后内容: {content[:200]}...")
                sql = self._extract_sql(content)
                print(f"[DEBUG] 提取的SQL: {sql}")
                return sql
            else:
                print(f"API错误: {response.status_code}")
                return self.rule_generator.generate(question)

        except Exception as e:
            print(f"LLM生成失败: {e}，使用规则生成器")
            return self.rule_generator.generate(question)

    def _extract_sql(self, response: str) -> str:
        """从响应中提取SQL"""
        response = response.strip()

        # 找到SELECT关键字
        idx = response.lower().find("select")
        if idx == -1:
            return ""

        sql = response[idx:]

        # 提取SQL直到遇到：分号换行、中文、说明文字等
        # 找到SQL结束位置（分号后换行或明确的中文说明）
        lines = sql.split('\n')
        clean_lines = []
        for line in lines:
            # 如果行以中文开头（说明是解释而不是SQL），停止
            if line and '\u4e00' <= line[0] <= '\u9fff':
                break
            clean_lines.append(line)
            # 如果行以分号结尾，认为是SQL结束
            if line.strip().endswith(';'):
                break

        sql = '\n'.join(clean_lines).strip()

        # 移除反引号
        sql = sql.replace('`', '')

        # 确保以分号结尾
        if sql and not sql.strip().endswith(';'):
            sql = sql + ';'

        return sql

    def query(self, question: str, use_context: bool = True) -> Dict[str, Any]:
        """
        执行完整查询流程

        Args:
            question: 用户问题
            use_context: 是否使用上下文处理追问

        Returns:
            查询结果
        """
        actual_question = question

        # 处理上下文（追问）
        if use_context and self.last_context:
            actual_question = self.context_handler.resolve_reference(
                question, self.last_context
            )

        # 生成SQL
        sql = self.generate_sql(actual_question)

        # 执行查询
        result = self.querier.execute_sql_with_result(sql)

        # 添加SQL信息
        result["sql"] = sql
        result["question"] = question
        result["actual_question"] = actual_question  # 解析后的完整问题
        result["is_followup"] = (actual_question != question)

        # 保存上下文
        if result.get("success") and result.get("data"):
            self.last_context = self.context_handler.extract_context(
                actual_question, result
            )

        return result

    def reset_context(self):
        """重置上下文（清空历史）"""
        self.last_context = None

    def close(self):
        """关闭连接"""
        self.querier.close()


# 测试
if __name__ == "__main__":
    #t2s = Text2SQL(use_llm=False)
    t2s = Text2SQL(use_llm=True, model_path=MODEL_CONFIG["llama_server_url"])
    test_questions = [
        "金花股份2024年的营业收入是多少？",
        "华润三九2024年度的净利润是多少？",
        "两家公司2024年的总资产对比",
        "华润三近2024年一季度经营现金流"
    ]

    for q in test_questions:
        print(f"\n问题: {q}")
        print("-" * 50)
        result = t2s.query(q)
        print(f"SQL: {result['sql']}")
        if result["success"]:
            print(f"结果: {result['count']}条记录")
            for row in result["data"]:
                print(row)

    t2s.close()
