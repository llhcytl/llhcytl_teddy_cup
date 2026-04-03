"""
泰迪杯竞赛 B 题 - 任务一：数据导入脚本（带配置）
"""

import pandas as pd
import pymysql
import json
from pathlib import Path
import sys

sys.stdout.reconfigure(encoding='utf-8')


# MySQL配置 - 请修改为你的实际配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',          # 请修改为你的MySQL用户名
    'password': '340841',          # 请修改为你的MySQL密码
    'database': 'teddy_cup_financial',
    'charset': 'utf8mb4'
}


def validate_and_clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """数据清洗和校验"""
    print("  - 删除完全空的行...")
    df_clean = df.dropna(how='all')

    print("  - 去除重复记录（同一股票+报告期+年份）...")
    if 'stock_code' in df_clean.columns and 'report_period' in df_clean.columns and 'report_year' in df_clean.columns:
        before_count = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=['stock_code', 'report_period', 'report_year'], keep='first')
        after_count = len(df_clean)
        if before_count > after_count:
            print(f"    删除了 {before_count - after_count} 条重复记录")

    print("  - 验证报告期格式...")
    valid_periods = ['FY', 'Q1', 'HY', 'Q3']
    if 'report_period' in df_clean.columns:
        invalid = df_clean[~df_clean['report_period'].isin(valid_periods) & df_clean['report_period'].notna()]
        if len(invalid) > 0:
            print(f"    警告: 发现 {len(invalid)} 条无效报告期记录")
            df_clean = df_clean[df_clean['report_period'].isin(valid_periods) | df_clean['report_period'].isna()]

    return df_clean


def import_to_mysql(df: pd.DataFrame, table_name: str, config: dict):
    """导入数据到MySQL"""
    if df.empty:
        print(f"  ! 表 {table_name} 数据为空，跳过")
        return

    try:
        conn = pymysql.connect(**config)
        cursor = conn.cursor()

        # 获取表字段
        cursor.execute(f"DESCRIBE {table_name}")
        columns_info = cursor.fetchall()
        valid_columns = [col[0] for col in columns_info if col[0] not in ['id', 'created_at', 'updated_at']]

        # 只保留存在于表中的列
        df_to_import = df[[col for col in df.columns if col in valid_columns]].copy()

        # 替换NaN为None
        df_to_import = df_to_import.where(pd.notnull(df_to_import), None)

        # 构建插入SQL
        columns_str = ', '.join(df_to_import.columns)
        placeholders = ', '.join(['%s'] * len(df_to_import.columns))

        insert_sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE
        """

        # 添加更新子句
        update_clauses = [f"{col} = VALUES({col})" for col in df_to_import.columns if col not in ['stock_code', 'report_period', 'report_year']]
        if update_clauses:
            insert_sql += ', '.join(update_clauses)

        # 转换数据并批量插入
        data_to_insert = [tuple(row) for row in df_to_import.values]

        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()

        print(f"  ✓ 成功导入 {len(data_to_insert)} 条记录到 {table_name}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"  ✗ 导入失败: {e}")
        raise


def main():
    print("="*60)
    print("泰迪杯竞赛 B 题 - 任务一：数据导入")
    print("="*60)

    # 显示当前配置
    print("\nMySQL配置:")
    print(f"  主机: {MYSQL_CONFIG['host']}")
    print(f"  端口: {MYSQL_CONFIG['port']}")
    print(f"  用户: {MYSQL_CONFIG['user']}")
    print(f"  数据库: {MYSQL_CONFIG['database']}")

    # 如果密码为空，提示用户设置
    if not MYSQL_CONFIG['password']:
        print("\n[警告] 未设置MySQL密码！")
        print("请编辑本文件，在 MYSQL_CONFIG 中设置正确的密码")
        print("\n或者直接输入密码（不保存）:")
        password = input("MySQL密码: ").strip()
        MYSQL_CONFIG['password'] = password

    # 读取提取的数据
    base_path = Path(r"C:\Users\34084\Desktop\泰迪杯\示例数据")
    excel_path = base_path / "extracted_data.xlsx"

    if not excel_path.exists():
        print(f"\n错误: 找不到数据文件 {excel_path}")
        print("请先运行 task1_pdf_extractor.py 提取数据")
        return

    print(f"\n读取数据文件: {excel_path}")

    # 读取Excel数据
    excel_file = pd.ExcelFile(excel_path)

    # 表名映射
    table_mapping = {
        'core_performance_indicators_she': 'core_performance_indicators_sheet',
        'balance_sheet': 'balance_sheet',
        'cash_flow_sheet': 'cash_flow_sheet',
        'income_sheet': 'income_sheet'
    }

    print("\n开始处理数据...")
    print("-"*60)

    for sheet_name in excel_file.sheet_names:
        target_table = table_mapping.get(sheet_name, sheet_name)

        print(f"\n处理表: {sheet_name} -> {target_table}")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        print(f"  原始记录数: {len(df)}")

        # 数据清洗
        df_clean = validate_and_clean_data(df)
        print(f"  清洗后记录数: {len(df_clean)}")

        # 导入MySQL
        if MYSQL_CONFIG['password']:
            import_to_mysql(df_clean, target_table, MYSQL_CONFIG)

    print("\n" + "="*60)
    print("✓ 数据导入完成！")
    print("="*60)

    # 验证导入结果
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        print("\n验证导入结果:")
        for table in table_mapping.values():
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {table}: {count} 条记录")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"验证失败: {e}")


if __name__ == "__main__":
    main()
