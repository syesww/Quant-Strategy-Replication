from datetime import datetime
import sqlite3
import pandas as pd

DATABASE = '/Users/syesw/Desktop/Quant DataBase/AShares.db'

def get_price(order_book_id, fields=None, start_date=None, end_date=None):
    # 连接到数据库
    conn = sqlite3.connect(DATABASE)

    # 如果 fields 为空列表或者未传入，则查询所有列
    if not fields:
        fields = ["open", "high", "low", "close", "prev_close", "limit_up", "limit_down", "volume", "total_turnover", "num_trades"]
    else:
        # 如果 fields 是字符串，则将其转换为包含单个元素的列表
        if isinstance(fields, str):
            fields = [fields]
    # 将 fields 列表转换为逗号分隔的字符串
    fields_str = ', '.join(fields)

    # 构造查询语句的基本部分
    base_query = f"SELECT order_book_id, date, {fields_str} FROM stocks_fundamentals WHERE order_book_id = '{order_book_id}'"

    if start_date:
        base_query += f" AND date >= '{start_date}'"

    # 如果传入了 end_date，则添加筛选条件
    if end_date:
        base_query += f" AND date <= '{end_date}'"
    
    query = base_query

    # 执行查询语句并将结果读取为 DataFrame
    df = pd.read_sql_query(query, conn)

    # 关闭连接
    conn.close()

    return df

def info(order_book_id, fields=None):
    conn = sqlite3.connect(DATABASE)  
    
    if fields is None:
        sql = f"SELECT * FROM stocks_info WHERE order_book_id = '{order_book_id}'"
    else:
        fields_str = ', '.join(fields)
        sql = f"SELECT order_book_id, {fields_str} FROM stocks_info WHERE order_book_id = '{order_book_id}'"

    result = pd.read_sql_query(sql, conn)

    conn.close()
    return result

def get_financials(order_book_id, fields=None, start_date=None, end_date=None):
    conn = sqlite3.connect(DATABASE)
    if not fields:
        fields = [
        "revenue",
        "operating_revenue",
        "profit_before_tax",
        "profit_from_operation",
        "net_profit",
        "gross_profit",
        "total_expense",
        "operating_expense",
        "r_n_d",
        "basic_earnings_per_share"
        ]
    else:
        # 如果 fields 是字符串，则将其转换为包含单个元素的列表
        if isinstance(fields, str):
            fields = [fields]
    # 将 fields 列表转换为逗号分隔的字符串
    fields_str = ', '.join(fields)

    base_query = f"SELECT order_book_id, date, {fields_str} FROM stocks_financials WHERE order_book_id = '{order_book_id}'"

    if start_date:
        base_query += f" AND date >= '{start_date}'"

    # 如果传入了 end_date，则添加筛选条件
    if end_date:
        base_query += f" AND date <= '{end_date}'"
    
    query = base_query
    df = pd.read_sql_query(query, conn)
    conn.close()

    return df

def get_factors(order_book_id, fields=None, start_date=None, end_date=None):
    conn = sqlite3.connect(DATABASE)
    if not fields:
        fields = [
        "market_cap",
        "market_cap_2",
        "pe_ratio_ttm"
        ]
    else:
        # 如果 fields 是字符串，则将其转换为包含单个元素的列表
        if isinstance(fields, str):
            fields = [fields]
    # 将 fields 列表转换为逗号分隔的字符串
    fields_str = ', '.join(fields)

    base_query = f"SELECT order_book_id, date, {fields_str} FROM stocks_factors WHERE order_book_id = '{order_book_id}'"

    if start_date:
        base_query += f" AND date >= '{start_date}'"

    # 如果传入了 end_date，则添加筛选条件
    if end_date:
        base_query += f" AND date <= '{end_date}'"
    
    query = base_query
    df = pd.read_sql_query(query, conn)
    conn.close()

    return df
