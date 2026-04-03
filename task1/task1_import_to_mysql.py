"""
泰迪杯竞赛 B 题 - 任务一：数据校验与MySQL导入脚本
"""

import pandas as pd
import pymysql
import json
from pathlib import Path
from typing import List, Dict, Any
import sys

sys.stdout.reconfigure(encoding='utf-8')


class DataValidator:
    """数据校验器"""

    @staticmethod
    def validate_financial_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        校验财务数据

        Args:
            df: 待校验的数据

        Returns:
            清洗后的数据
        """
        print("开始数据校验...")

        # 1. 检查必填字段
        required_fields = ['stock_code', 'report_period', 'report_year']
        missing_fields = [f for f in required_fields if f not in df.columns]
        if missing_fields:
            print(f"警告: 缺少必填字段: {missing_fields}")

        # 2. 删除完全空的行
        df_clean = df.dropna(how='all')

        # 3. 检查股票代码格式
        if 'stock_code' in df_clean.columns:
            df_clean = df_clean[df_clean['stock_code'].notna()]
            print(f"有效股票代码记录数: {len(df_clean)}")

        # 4. 检查报告期格式
        valid_periods = ['FY', 'Q1', 'HY', 'Q3']
        if 'report_period' in df_clean.columns:
            df_clean = df_clean[df_clean['report_period'].isin(valid_periods) | df_clean['report_period'].isna()]

        # 5. 检查年份合理性
        if 'report_year' in df_clean.columns:
            df_clean = df_clean[(df_clean['report_year'] >= 2020) & (df_clean['report_year'] <= 2026) | df_clean['report_year'].isna()]

        # 6. 数值字段校验：检查异常值
        numeric_columns = df_clean.select_dtypes(include=['number']).columns
        for col in numeric_columns:
            # 检查是否有异常大的值（可能是单位问题）
            if df_clean[col].max() > 1e15:
                print(f"警告: 字段 {col} 可能存在单位问题，最大值: {df_clean[col].max()}")
                # 尝试除以10000转换为万元
                if df_clean[col].max() > 1e12:
                    df_clean[col] = df_clean[col] / 10000

        # 7. 去重检查
        if 'stock_code' in df_clean.columns and 'report_period' in df_clean.columns and 'report_year' in df_clean.columns:
            duplicates = df_clean.duplicated(subset=['stock_code', 'report_period', 'report_year'], keep='first')
            if duplicates.sum() > 0:
                print(f"发现 {duplicates.sum()} 条重复记录，将保留第一条")
                df_clean = df_clean[~duplicates]

        print(f"校验完成: 原始 {len(df)} 条 -> 清洗后 {len(df_clean)} 条")
        return df_clean

    @staticmethod
    def cross_validate_tables(tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        跨表校验：确保不同表间的一致性

        Args:
            tables: 包含所有表的字典

        Returns:
            校验后的表字典
        """
        print("\n开始跨表校验...")

        # 1. 检查各表记录数是否一致
        counts = {name: len(df) for name, df in tables.items() if not df.empty}
        print(f"各表记录数: {counts}")

        # 2. 检查净利润在不同表的一致性
        if 'core_performance_indicators_sheet' in tables and 'income_sheet' in tables:
            core = tables['core_performance_indicators_sheet']
            income = tables['income_sheet']

            # 检查共同记录的净利润是否一致
            merged = pd.merge(
                core[['stock_code', 'report_year', 'report_period', 'net_profit_10k_yuan']],
                income[['stock_code', 'report_year', 'report_period', 'net_profit']],
                on=['stock_code', 'report_year', 'report_period'],
                how='inner',
                suffixes=('_core', '_income')
            )

            if not merged.empty and 'net_profit_10k_yuan' in merged.columns and 'net_profit' in merged.columns:
                # 计算差异
                merged['diff'] = merged['net_profit_10k_yuan'] - merged['net_profit']
                max_diff = merged['diff'].abs().max()

                if max_diff > 1000:  # 差异超过1000万元
                    print(f"警告: 利润表与核心指标表净利润存在差异，最大差异: {max_diff:.2f} 万元")
                else:
                    print("净利润跨表校验通过")

        return tables


class MySQLImporter:
    """MySQL数据导入器"""

    def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306):
        """
        初始化数据库连接

        Args:
            host: 数据库主机
            user: 用户名
            password: 密码
            database: 数据库名
            port: 端口号
        """
        self.connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def import_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        将DataFrame导入MySQL表

        Args:
            df: 要导入的数据
            table_name: 目标表名
        """
        if df.empty:
            print(f"表 {table_name} 数据为空，跳过导入")
            return

        cursor = self.connection.cursor()

        # 获取表的字段信息
        cursor.execute(f"DESCRIBE {table_name}")
        columns_info = cursor.fetchall()
        valid_columns = [col['Field'] for col in columns_info if col['Field'] not in ['id', 'created_at', 'updated_at']]

        # 只保留DataFrame中存在于表中的列
        df_to_import = df[[col for col in df.columns if col in valid_columns]].copy()

        # 替换NaN为NULL
        df_to_import = df_to_import.where(pd.notnull(df_to_import), None)

        # 批量插入
        insert_sql = f"""
            INSERT INTO {table_name} ({', '.join(df_to_import.columns)})
            VALUES ({', '.join(['%s'] * len(df_to_import.columns))})
            ON DUPLICATE KEY UPDATE
            {', '.join([f"{col} = VALUES({col})" for col in df_to_import.columns if col not in ['stock_code', 'report_period', 'report_year']])}
        """

        # 转换数据为列表格式
        data_to_insert = []
        for _, row in df_to_import.iterrows():
            data_to_insert.append(tuple(row))

        # 执行批量插入
        try:
            cursor.executemany(insert_sql, data_to_insert)
            self.connection.commit()
            print(f"✓ 成功导入 {len(data_to_insert)} 条记录到 {table_name}")
        except Exception as e:
            self.connection.rollback()
            print(f"✗ 导入到 {table_name} 失败: {e}")
        finally:
            cursor.close()

    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()


def main():
    """主函数"""
    print("="*60)
    print("泰迪杯竞赛 B 题 - 任务一：数据校验与MySQL导入")
    print("="*60)

    # 读取提取的数据
    base_path = Path(r"C:\Users\34084\Desktop\泰迪杯\示例数据")

    # 从JSON读取
    json_path = base_path / "extracted_data.json"
    if json_path.exists():
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        tables = {}
        for table_name, records in data.items():
            if records:
                tables[table_name] = pd.DataFrame(records)

    # 或从Excel读取
    else:
        excel_path = base_path / "extracted_data.xlsx"
        if excel_path.exists():
            tables = {}
            excel_file = pd.ExcelFile(excel_path)
            for sheet_name in excel_file.sheet_names:
                tables[sheet_name] = pd.read_excel(excel_file, sheet_name=sheet_name)

    # 数据校验
    validator = DataValidator()
    validated_tables = {}

    for table_name, df in tables.items():
        if not df.empty:
            print(f"\n校验表: {table_name}")
            validated_tables[table_name] = validator.validate_financial_data(df)

    # 跨表校验
    validated_tables = validator.cross_validate_tables(validated_tables)

    # 保存校验后的数据
    output_path = base_path / "validated_data.xlsx"
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for table_name, df in validated_tables.items():
            if not df.empty:
                df.to_excel(writer, sheet_name=table_name[:31], index=False)

    print(f"\n校验后的数据已保存到: {output_path}")

    # 询问是否导入MySQL
    print("\n" + "="*60)
    print("MySQL导入配置")
    print("="*60)

    use_mysql = input("\n是否将数据导入MySQL？(y/n): ").lower().strip()

    if use_mysql == 'y':
        host = input("MySQL主机地址 (默认: localhost): ") or 'localhost'
        port = int(input("MySQL端口 (默认: 3306): ") or '3306')
        user = input("MySQL用户名: ")
        password = input("MySQL密码: ")
        database = input("数据库名 (默认: teddy_cup_financial): ") or 'teddy_cup_financial'

        try:
            importer = MySQLImporter(host, user, password, database, port)

            # 导入各表数据
            table_mapping = {
                'core_performance_indicators_sheet': 'core_performance_indicators_sheet',
                'balance_sheet': 'balance_sheet',
                'cash_flow_sheet': 'cash_flow_sheet',
                'income_sheet': 'income_sheet'
            }

            for source_table, target_table in table_mapping.items():
                if source_table in validated_tables and not validated_tables[source_table].empty:
                    importer.import_dataframe(validated_tables[source_table], target_table)

            importer.close()
            print("\n✓ 数据导入完成！")

        except Exception as e:
            print(f"\n✗ 导入失败: {e}")
    else:
        print("\n跳过MySQL导入。数据已保存到Excel，可手动导入。")


if __name__ == "__main__":
    main()
