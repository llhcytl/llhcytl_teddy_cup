"""
简单查询脚本 - 查看财报数据库
"""

import pymysql
import sys

sys.stdout.reconfigure(encoding='utf-8')

# 连接数据库
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='340841',
    database='teddy_cup_financial',
    charset='utf8mb4'
)

cursor = conn.cursor()

print("="*60)
print("财报数据库查询")
print("="*60)

# 菜单
while True:
    print("\n请选择查询：")
    print("1. 查看所有表")
    print("2. 查看利润表数据")
    print("3. 查看核心指标表数据")
    print("4. 查看资产负债表数据")
    print("5. 查看现金流量表数据")
    print("6. 自定义SQL查询")
    print("0. 退出")

    choice = input("\n请输入选项 (0-6): ").strip()

    if choice == '0':
        print("再见！")
        break

    elif choice == '1':
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print("\n数据库中的表：")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"  - {table[0]}: {count} 条记录")

    elif choice == '2':
        cursor.execute("""
            SELECT stock_code, stock_abbr, report_year, report_period,
                   total_operating_revenue, net_profit
            FROM income_sheet
            ORDER BY stock_code, report_year DESC, report_period
        """)
        rows = cursor.fetchall()
        print(f"\n{'股票代码':<10} {'简称':<10} {'年份':<6} {'报告期':<8} {'营业收入':<18} {'净利润':<18}")
        print("-"*70)
        for row in rows:
            print(f"{row[0]:<10} {row[1]:<10} {row[2]:<6} {row[3]:<8} {row[4] or 0:>18,.2f} {row[5] or 0:>18,.2f}")

    elif choice == '3':
        cursor.execute("""
            SELECT stock_code, stock_abbr, report_year, report_period,
                   eps, roe, total_operating_revenue
            FROM core_performance_indicators_sheet
            ORDER BY stock_code, report_year DESC, report_period
        """)
        rows = cursor.fetchall()
        print(f"\n{'股票代码':<10} {'简称':<10} {'年份':<6} {'报告期':<8} {'每股收益':<10} {'ROE(%)':<10} {'营业收入':<18}")
        print("-"*70)
        for row in rows:
            print(f"{row[0]:<10} {row[1]:<10} {row[2]:<6} {row[3]:<8} {row[4] or 'N/A':<10} {row[5] or 0:>10.2f} {row[6] or 0:>18,.2f}")

    elif choice == '4':
        cursor.execute("""
            SELECT stock_code, stock_abbr, report_year, report_period,
                   asset_total_assets, liability_total_liabilities, equity_total_equity
            FROM balance_sheet
            ORDER BY stock_code, report_year DESC, report_period
        """)
        rows = cursor.fetchall()
        print(f"\n{'股票代码':<10} {'简称':<10} {'年份':<6} {'报告期':<8} {'总资产':<18} {'总负债':<18} {'股东权益':<18}")
        print("-"*70)
        for row in rows:
            print(f"{row[0]:<10} {row[1]:<10} {row[2]:<6} {row[3]:<8} {row[4] or 0:>18,.2f} {row[5] or 0:>18,.2f} {row[6] or 0:>18,.2f}")

    elif choice == '5':
        cursor.execute("""
            SELECT stock_code, stock_abbr, report_year, report_period,
                   operating_cf_net_amount, investing_cf_net_amount, financing_cf_net_amount
            FROM cash_flow_sheet
            ORDER BY stock_code, report_year DESC, report_period
        """)
        rows = cursor.fetchall()
        print(f"\n{'股票代码':<10} {'简称':<10} {'年份':<6} {'报告期':<8} {'经营现金流':<18} {'投资现金流':<18} {'筹资现金流':<18}")
        print("-"*70)
        for row in rows:
            print(f"{row[0]:<10} {row[1]:<10} {row[2]:<6} {row[3]:<8} {row[4] or 0:>18,.2f} {row[5] or 0:>18,.2f} {row[6] or 0:>18,.2f}")

    elif choice == '6':
        sql = input("\n请输入SQL查询语句: ").strip()
        if sql.lower().startswith('select'):
            try:
                cursor.execute(sql)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                print("\n查询结果：")
                print(" | ".join(columns))
                print("-" * (len(" | ".join(columns))))
                for row in rows:
                    print(" | ".join(str(v) if v is not None else 'NULL' for v in row))
            except Exception as e:
                print(f"查询错误: {e}")
        else:
            print("只能执行SELECT查询！")

cursor.close()
conn.close()
